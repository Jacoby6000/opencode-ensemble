import { beforeEach, describe, expect, test } from "bun:test"
import { DEFAULT_CONFIG } from "../src/config"
import type { Database } from "../src/db"
import { DurableScheduler } from "../src/scheduler-runtime"
import { findRunBySession, markRunInjected, queueWake, tryAcquireRun } from "../src/scheduler"
import { insertMember, insertTeam, mockClient, setupDb } from "./helpers"

describe("durable scheduler runtime", () => {
  let db: Database

  beforeEach(() => {
    db = setupDb()
    insertTeam(db, "t1", "alpha", "lead-1")
    insertMember(db, "t1", "alice", "session-a")
  })

  test("dispatches a queued wake without awaiting prompt completion", async () => {
    const client = mockClient()
    client.session.promptAsync = async options => {
      client.calls.push({ method: "session.promptAsync", args: [options] })
      return new Promise(() => {})
    }
    queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "spawn", coalesceKey: "member", prompt: "initial context", now: 10,
    })
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler)

    scheduler.kick()
    await Bun.sleep(0)

    expect(client.calls.filter(call => call.method === "session.promptAsync")).toHaveLength(1)
    expect(findRunBySession(db, "session-a")?.state).toBe("active")
  })

  test("marks accepted delivery injected and finishes it on idle", async () => {
    const client = mockClient()
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "spawn", coalesceKey: "member", prompt: "initial context", now: Date.now(),
    })
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler)

    scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)
    expect(findRunBySession(db, "session-a")?.injectedAt).not.toBeNull()

    scheduler.onSessionStatus("session-a", "idle")
    expect(findRunBySession(db, "session-a")).toBeUndefined()
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "completed" })
  })

  test("ignores idle evidence that predates injection acknowledgement", async () => {
    const client = mockClient()
    client.session.promptAsync = () => new Promise(() => {})
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "spawn", coalesceKey: "member", prompt: "initial context", now: Date.now(),
    })
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler)

    scheduler.kick()
    await Bun.sleep(0)
    scheduler.onSessionStatus("session-a", "idle")

    expect(findRunBySession(db, "session-a")?.injectedAt).toBeNull()
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "leased" })
  })

  test("requeues a wake when prompt dispatch rejects", async () => {
    const client = mockClient()
    client.session.promptAsync = async () => { throw new Error("offline") }
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", prompt: "work", now: Date.now(),
    })
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler)

    scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)

    expect(findRunBySession(db, "session-a")).toBeUndefined()
    expect(db.query("SELECT state, last_error FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "queued", last_error: "offline" })
  })

  test("does not dispatch beyond configured run capacity", async () => {
    insertMember(db, "t1", "bob", "session-b")
    const client = mockClient()
    client.session.promptAsync = async options => {
      client.calls.push({ method: "session.promptAsync", args: [options] })
      return new Promise(() => {})
    }
    const now = Date.now()
    const first = queueWake(db, { teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build", reason: "message", coalesceKey: "alice", now })
    queueWake(db, { teamId: "t1", memberName: "bob", sessionId: "session-b", agent: "build", reason: "message", coalesceKey: "bob", now })
    expect(tryAcquireRun(db, first.wakeId, { global: 1, perAgent: {} }, 60_000, now).acquired).toBe(true)
    const scheduler = new DurableScheduler(db, client, { ...DEFAULT_CONFIG.scheduler, runLimits: { global: 1, perAgent: {} } })

    scheduler.kick()
    await Bun.sleep(0)
    expect(client.calls.filter(call => call.method === "session.promptAsync")).toHaveLength(0)
  })

  test("bounds startup reconciliation when session status never settles", async () => {
    const client = mockClient()
    client.session.status = () => new Promise(() => {})
    const now = Date.now()
    const wake = queueWake(db, { teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build", reason: "message", coalesceKey: "alice", now })
    const acquired = tryAcquireRun(db, wake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 1, now)
    if (!acquired.acquired) throw new Error("expected lease")
    const scheduler = new DurableScheduler(db, client, { ...DEFAULT_CONFIG.scheduler, leaseTtlMs: 10 })

    await scheduler.recover()

    expect(findRunBySession(db, "session-a")?.state).toBe("expired")
  })

  test("keeps a fenced run when session lookup times out", async () => {
    const client = mockClient()
    client.session.get = () => new Promise(() => {})
    const now = Date.now()
    const wake = queueWake(db, { teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build", reason: "message", coalesceKey: "alice", now })
    const acquired = tryAcquireRun(db, wake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 1, now)
    if (!acquired.acquired) throw new Error("expected lease")
    const scheduler = new DurableScheduler(db, client, { ...DEFAULT_CONFIG.scheduler, leaseTtlMs: 10 })

    await Bun.sleep(2)
    await scheduler.recover()

    expect(findRunBySession(db, "session-a")?.state).toBe("expired")
    expect(db.query("SELECT status FROM team_member WHERE name = 'alice'").get()).toEqual({ status: "ready" })
  })

  test("does not requeue an unexpired in-flight dispatch during maintenance", async () => {
    const client = mockClient()
    client.session.promptAsync = options => {
      client.calls.push({ method: "session.promptAsync", args: [options] })
      return new Promise(() => {})
    }
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "spawn", coalesceKey: "member", prompt: "initial context",
    })
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler)
    scheduler.kick()
    await Bun.sleep(0)

    await scheduler.recover()
    await Bun.sleep(0)

    expect(client.calls.filter(call => call.method === "session.promptAsync")).toHaveLength(1)
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "leased" })
  })

  test("does not overwrite lifecycle status when a scheduled run goes idle", async () => {
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member",
    })
    const acquired = tryAcquireRun(db, wake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 60_000)
    if (!acquired.acquired) throw new Error("expected lease")
    markRunInjected(db, acquired.leaseId)
    db.run("UPDATE team_member SET status = 'shutdown_requested' WHERE name = 'alice'")
    const scheduler = new DurableScheduler(db, mockClient(), DEFAULT_CONFIG.scheduler)

    scheduler.onSessionStatus("session-a", "idle")

    expect(db.query("SELECT status FROM team_member WHERE name = 'alice'").get()).toEqual({ status: "shutdown_requested" })
  })
})

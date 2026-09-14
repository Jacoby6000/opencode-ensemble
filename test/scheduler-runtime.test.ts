import { beforeEach, describe, expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { DEFAULT_CONFIG } from "../src/config"
import type { Database } from "../src/db"
import { DurableScheduler } from "../src/scheduler-runtime"
import { findRunBySession, markRunInjected, queueMessageWake, queueWake, tryAcquireRun } from "../src/scheduler"
import { insertMember, insertTeam, mockClient, setupDb } from "./helpers"

async function git(cwd: string, args: string[]): Promise<string> {
  const process = Bun.spawn(["git", ...args], { cwd, stdout: "pipe", stderr: "pipe" })
  const [stdout, stderr, exitCode] = await Promise.all([new Response(process.stdout).text(), new Response(process.stderr).text(), process.exited])
  if (exitCode !== 0) throw new Error(stderr.trim() || `git ${args.join(" ")} failed`)
  return stdout.trim()
}

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

  test("keeps an accepted delivery recoverable when idle arrives without start evidence", async () => {
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
    expect(db.query("SELECT state, last_error FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({
      state: "queued",
      last_error: "session became idle before execution started",
    })
  })

  test("merges an accepted-without-start wake into its queued coalescing successor", async () => {
    const first = queueMessageWake(db, {
      messageId: "message-old", fromName: "lead", toName: "alice", content: "old message",
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "messages", prompt: "old prompt", now: Date.now(),
    })
    const scheduler = new DurableScheduler(db, mockClient(), DEFAULT_CONFIG.scheduler)
    scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)
    expect(findRunBySession(db, "session-a")?.injectedAt).not.toBeNull()

    const successor = queueMessageWake(db, {
      messageId: "message-new", fromName: "lead", toName: "alice", content: "new message",
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "messages", prompt: "new prompt", now: Date.now(),
    })
    expect(successor.wakeId).not.toBe(first.wakeId)

    scheduler.onSessionStatus("session-a", "idle")

    expect(findRunBySession(db, "session-a")).toBeUndefined()
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(first.wakeId)).toEqual({ state: "cancelled" })
    expect(db.query("SELECT state, prompt FROM scheduler_wake WHERE id = ?").get(successor.wakeId)).toEqual({
      state: "queued",
      prompt: "old prompt\n\nnew prompt",
    })
    expect(db.query("SELECT message_id FROM scheduler_message_wake WHERE wake_id = ? ORDER BY message_id").all(successor.wakeId)).toEqual([
      { message_id: "message-new" },
      { message_id: "message-old" },
    ])
    expect(db.query("SELECT id, delivery_state FROM team_message ORDER BY id").all()).toEqual([
      { id: "message-new", delivery_state: "wake_queued" },
      { id: "message-old", delivery_state: "wake_queued" },
    ])
  })

  test("finishes a started delivery exactly once when busy is followed by idle", async () => {
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "spawn", coalesceKey: "member", prompt: "initial context", now: Date.now(),
    })
    const scheduler = new DurableScheduler(db, mockClient(), DEFAULT_CONFIG.scheduler)

    scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)
    scheduler.onSessionStatus("session-a", "busy")
    scheduler.onSessionStatus("session-a", "idle")
    scheduler.onSessionStatus("session-a", "idle")

    expect(findRunBySession(db, "session-a")).toBeUndefined()
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "completed" })
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_event WHERE wake_id = ? AND type = 'run_processed'").get(wake.wakeId)).toEqual({ count: 1 })
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

  test("restart 404 preserves a worker for recovery and releases its assigned task", async () => {
    const client = mockClient()
    const missing = new Error("session not found") as Error & { status: number }
    missing.status = 404
    client.session.status = async () => ({ data: {} })
    client.session.get = async () => { throw missing }
    db.run(
      "INSERT INTO team_task (id, team_id, content, status, priority, assignee, time_created, time_updated) VALUES ('task-a', 't1', 'work', 'in_progress', 'medium', 'alice', ?, ?)",
      [Date.now(), Date.now()],
    )
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "alice", now: Date.now() - 100,
    })
    const acquired = tryAcquireRun(db, wake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 1, Date.now() - 100)
    if (!acquired.acquired) throw new Error("expected lease")
    const scheduler = new DurableScheduler(db, client, { ...DEFAULT_CONFIG.scheduler, leaseTtlMs: 10 })

    await scheduler.recover()

    expect(db.query("SELECT status, execution_status FROM team_member WHERE name = 'alice'").get()).toEqual({ status: "error", execution_status: "failed" })
    expect(db.query("SELECT status, assignee FROM team_task WHERE id = 'task-a'").get()).toEqual({ status: "pending", assignee: null })
  })

  test("restart 404 snapshots dirty worktree progress before terminalizing the worker", async () => {
    const repository = await mkdtemp(path.join(tmpdir(), "ensemble-restart-404-"))
    const worktree = path.join(repository, "worker")
    try {
      await git(repository, ["init"])
      await git(repository, ["config", "user.name", "Test User"])
      await git(repository, ["config", "user.email", "test@example.com"])
      await Bun.write(path.join(repository, "base.txt"), "base\n")
      await git(repository, ["add", "base.txt"])
      await git(repository, ["commit", "-m", "base"])
      await git(repository, ["worktree", "add", "-b", "ensemble-worker", worktree])
      await Bun.write(path.join(worktree, "progress.txt"), "restart-safe\n")
      db.run("UPDATE project SET name = 'restart-project', path = ? WHERE id = (SELECT project_id FROM team WHERE id = 't1')", [repository])
      db.run("UPDATE team_member SET worktree_dir = ?, worktree_branch = 'ensemble-worker' WHERE name = 'alice'", [worktree])

      const client = mockClient()
      const missing = new Error("session not found") as Error & { status: number }
      missing.status = 404
      client.session.status = async () => ({ data: {} })
      client.session.get = async () => { throw missing }
      const wake = queueWake(db, { teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build", reason: "message", coalesceKey: "alice", now: Date.now() - 100 })
      const acquired = tryAcquireRun(db, wake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 1, Date.now() - 100)
      if (!acquired.acquired) throw new Error("expected lease")

      await new DurableScheduler(db, client, { ...DEFAULT_CONFIG.scheduler, leaseTtlMs: 10 }).recover()

      const branch = (db.query("SELECT worktree_branch FROM team_member WHERE name = 'alice'").get() as { worktree_branch: string }).worktree_branch
      expect(branch).toBe("ensemble/preserved/restart-project/alpha#t1/alice")
      expect(await git(repository, ["show", `${branch}:progress.txt`])).toBe("restart-safe")
    } finally {
      await rm(repository, { recursive: true, force: true })
    }
  })

  test("restart 404 fails closed when worktree progress cannot be preserved", async () => {
    db.run("UPDATE team_member SET worktree_dir = '/tmp/missing-ensemble-worktree', worktree_branch = 'ensemble/preserved/test-project/alpha#t1/alice' WHERE name = 'alice'")
    db.run(
      "INSERT INTO team_task (id, team_id, content, status, priority, assignee, time_created, time_updated) VALUES ('task-a', 't1', 'work', 'in_progress', 'medium', 'alice', ?, ?)",
      [Date.now(), Date.now()],
    )
    const client = mockClient()
    const missing = new Error("session not found") as Error & { status: number }
    missing.status = 404
    client.session.status = async () => ({ data: {} })
    client.session.get = async () => { throw missing }
    const wake = queueWake(db, { teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build", reason: "message", coalesceKey: "alice", now: Date.now() - 100 })
    const acquired = tryAcquireRun(db, wake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 1, Date.now() - 100)
    if (!acquired.acquired) throw new Error("expected lease")

    await new DurableScheduler(db, client, { ...DEFAULT_CONFIG.scheduler, leaseTtlMs: 10 }).recover()

    expect(db.query("SELECT status FROM team_member WHERE name = 'alice'").get()).toEqual({ status: "ready" })
    expect(db.query("SELECT status, assignee FROM team_task WHERE id = 'task-a'").get()).toEqual({ status: "in_progress", assignee: "alice" })
    expect(findRunBySession(db, "session-a")?.state).toBe("expired")
  })

  test("restart 404 accepts a verified preserved ref after its worktree is gone", async () => {
    const repository = await mkdtemp(path.join(tmpdir(), "ensemble-restart-preserved-"))
    try {
      await git(repository, ["init"])
      await git(repository, ["config", "user.name", "Test User"])
      await git(repository, ["config", "user.email", "test@example.com"])
      await Bun.write(path.join(repository, "progress.txt"), "already safe\n")
      await git(repository, ["add", "progress.txt"])
      await git(repository, ["commit", "-m", "preserved progress"])
      const preserved = "ensemble/preserved/restart-project/alpha#t1/alice"
      await git(repository, ["branch", preserved])
      db.run("UPDATE project SET name = 'restart-project', path = ? WHERE id = (SELECT project_id FROM team WHERE id = 't1')", [repository])
      db.run("UPDATE team_member SET worktree_dir = ?, worktree_branch = ? WHERE name = 'alice'", [path.join(repository, "removed-worker"), preserved])
      db.run(
        "INSERT INTO team_task (id, team_id, content, status, priority, assignee, time_created, time_updated) VALUES ('task-a', 't1', 'work', 'in_progress', 'medium', 'alice', ?, ?)",
        [Date.now(), Date.now()],
      )
      const client = mockClient()
      const missing = new Error("session not found") as Error & { status: number }
      missing.status = 404
      client.session.status = async () => ({ data: {} })
      client.session.get = async () => { throw missing }
      const wake = queueWake(db, { teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build", reason: "message", coalesceKey: "alice", now: Date.now() - 100 })
      const acquired = tryAcquireRun(db, wake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 1, Date.now() - 100)
      if (!acquired.acquired) throw new Error("expected lease")

      await new DurableScheduler(db, client, { ...DEFAULT_CONFIG.scheduler, leaseTtlMs: 10 }).recover()

      expect(db.query("SELECT status, execution_status FROM team_member WHERE name = 'alice'").get()).toEqual({ status: "error", execution_status: "failed" })
      expect(db.query("SELECT status, assignee FROM team_task WHERE id = 'task-a'").get()).toEqual({ status: "pending", assignee: null })
      expect(findRunBySession(db, "session-a")).toBeUndefined()
    } finally {
      await rm(repository, { recursive: true, force: true })
    }
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

  test("arms a missed idle epoch only during maintenance and preserves its timestamp", async () => {
    const scheduler = new DurableScheduler(db, mockClient(), DEFAULT_CONFIG.scheduler)

    scheduler.kick()
    await Bun.sleep(0)
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: null })

    const before = Date.now()
    await scheduler.recover()
    const after = Date.now()
    const state = db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get() as { quiet_since: number }
    expect(state.quiet_since).toBeGreaterThanOrEqual(before)
    expect(state.quiet_since).toBeLessThanOrEqual(after)

    await scheduler.recover()
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual(state)
  })

  test("does not arm zero-worker, busy-worker, or queued-worker teams during maintenance", async () => {
    db.run("UPDATE team_member SET member_kind = 'supervisor' WHERE team_id = 't1' AND name = 'alice'")
    insertTeam(db, "t2", "busy-team", "lead-2")
    insertMember(db, "t2", "bob", "session-b", "busy", "running")
    insertTeam(db, "t3", "queued-team", "lead-3")
    insertMember(db, "t3", "carol", "session-c")
    queueWake(db, {
      teamId: "t3", memberName: "carol", sessionId: "session-c", agent: "build",
      reason: "message", coalesceKey: "carol", now: Date.now(),
    })
    const scheduler = new DurableScheduler(db, mockClient(), DEFAULT_CONFIG.scheduler, false)

    await scheduler.recover()

    expect(db.query("SELECT team_id, quiet_since FROM team_supervision ORDER BY team_id").all()).toEqual([
      { team_id: "t1", quiet_since: null },
      { team_id: "t2", quiet_since: null },
      { team_id: "t3", quiet_since: null },
    ])
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

  test("only dispatches wakes owned by its project", async () => {
    db.run(
      "INSERT INTO project (id, name, path, status, time_created, time_updated) VALUES (?, ?, ?, 'active', ?, ?)",
      ["/tmp/other-project", "other-project", "/tmp/other-project", Date.now(), Date.now()],
    )
    db.run(
      "INSERT INTO team (id, name, project_id, lead_session_id, status, delegate, time_created, time_updated) VALUES (?, ?, ?, ?, 'active', 0, ?, ?)",
      ["t2", "beta", "/tmp/other-project", "lead-2", Date.now(), Date.now()],
    )
    insertMember(db, "t2", "bob", "session-b")
    queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "alice", prompt: "project A",
    })
    const otherWake = queueWake(db, {
      teamId: "t2", memberName: "bob", sessionId: "session-b", agent: "build",
      reason: "message", coalesceKey: "bob", prompt: "project B", now: Date.now() - 100,
    })
    const otherLease = tryAcquireRun(db, otherWake.wakeId, DEFAULT_CONFIG.scheduler.runLimits, 1, Date.now() - 100)
    if (!otherLease.acquired) throw new Error("expected other-project lease")
    const client = mockClient()
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler, true, "/tmp/test-project")

    scheduler.kick()
    await Bun.sleep(0)

    const prompts = client.calls
      .filter(call => call.method === "session.promptAsync")
      .map(call => (call.args[0] as { sessionID: string }).sessionID)
    expect(prompts).toEqual(["session-a"])
    expect(db.query("SELECT state FROM scheduler_wake WHERE team_id = 't2'").get()).toEqual({ state: "leased" })
    expect(db.query("SELECT state FROM scheduler_run_lease WHERE id = ?").get(otherLease.leaseId)).toEqual({ state: "active" })
  })
})

import { beforeEach, describe, expect, test } from "bun:test"
import type { Database } from "../src/db"
import { activateIdentity, activateIdentityAndQueue, expireStaleRuns, findRunBySession, finishRun, getWakePayload, listReadyWakes, markRunInjected, queueBroadcastWakes, queueMessageWake, queueWake, reconcileExpiredRun, releaseIdentity, renewRunLease, requeueRun, terminateMemberScheduling, tryAcquireRun, tryReserveIdentity } from "../src/scheduler"
import { insertMember, insertTeam, setupDb } from "./helpers"

describe("durable scheduler", () => {
  let db: Database

  beforeEach(() => {
    db = setupDb()
    insertTeam(db, "t1", "alpha", "lead-1")
    insertMember(db, "t1", "alice", "session-a")
    insertMember(db, "t1", "bob", "session-b")
  })

  test("coalesces duplicate queued wakes and links every message", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES (?, ?, ?, ?, ?, ?)", ["msg-1", "t1", "lead", "alice", "one", 1])
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES (?, ?, ?, ?, ?, ?)", ["msg-2", "t1", "lead", "alice", "two", 2])

    const first = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member:alice", messageId: "msg-1", now: 10,
    })
    const second = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member:alice", messageId: "msg-2", now: 11,
    })

    expect(second).toEqual({ wakeId: first.wakeId, coalesced: true })
    const wakes = db.query("SELECT id, state FROM scheduler_wake").all() as Array<{ id: string; state: string }>
    expect(wakes).toEqual([{ id: first.wakeId, state: "queued" }])
    const links = db.query("SELECT message_id, wake_id FROM scheduler_message_wake ORDER BY message_id").all()
    expect(links).toEqual([
      { message_id: "msg-1", wake_id: first.wakeId },
      { message_id: "msg-2", wake_id: first.wakeId },
    ])
    const states = db.query("SELECT id, delivery_state FROM team_message ORDER BY id").all()
    expect(states).toEqual([
      { id: "msg-1", delivery_state: "wake_queued" },
      { id: "msg-2", delivery_state: "wake_queued" },
    ])
  })

  test("atomically enforces global run capacity", () => {
    const alice = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member:alice", now: 10,
    })
    const bob = queueWake(db, {
      teamId: "t1", memberName: "bob", sessionId: "session-b", agent: "build",
      reason: "message", coalesceKey: "member:bob", now: 10,
    })

    expect(tryAcquireRun(db, alice.wakeId, { global: 1, perAgent: {} }, 1_000, 20).acquired).toBe(true)
    expect(tryAcquireRun(db, bob.wakeId, { global: 1, perAgent: {} }, 1_000, 20)).toEqual({ acquired: false, reason: "global_capacity" })
  })

  test("enforces per-agent capacity independently of the global limit", () => {
    const alice = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member:alice", now: 10,
    })
    const bob = queueWake(db, {
      teamId: "t1", memberName: "bob", sessionId: "session-b", agent: "build",
      reason: "message", coalesceKey: "member:bob", now: 10,
    })

    expect(tryAcquireRun(db, alice.wakeId, { global: 4, perAgent: { build: 1 } }, 1_000, 20).acquired).toBe(true)
    expect(tryAcquireRun(db, bob.wakeId, { global: 4, perAgent: { build: 1 } }, 1_000, 20)).toEqual({ acquired: false, reason: "agent_capacity" })
  })

  test("fences stale leases until external reconciliation", () => {
    const alice = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member:alice", now: 10,
    })
    const bob = queueWake(db, {
      teamId: "t1", memberName: "bob", sessionId: "session-b", agent: "build",
      reason: "message", coalesceKey: "member:bob", now: 10,
    })

    expect(tryAcquireRun(db, alice.wakeId, { global: 1, perAgent: {} }, 10, 20).acquired).toBe(true)
    expect(tryAcquireRun(db, bob.wakeId, { global: 1, perAgent: {} }, 10, 31)).toEqual({ acquired: false, reason: "global_capacity" })
    const expired = db.query("SELECT state FROM scheduler_run_lease WHERE member_name = 'alice'").get() as { state: string }
    expect(expired.state).toBe("expired")
  })

  test("reserves and releases identity capacity independently of run leases", () => {
    const first = tryReserveIdentity(db, {
      teamId: "t1", memberName: "charlie", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 1_000, now: 10,
    })
    expect(first.reserved).toBe(true)
    expect(tryReserveIdentity(db, {
      teamId: "t1", memberName: "dana", agent: "explore",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 1_000, now: 11,
    })).toEqual({ reserved: false, reason: "global_capacity" })

    if (!first.reserved) throw new Error("expected reservation")
    insertMember(db, "t1", "charlie", "session-c")
    expect(activateIdentity(db, first.reservationId, 12)).toBe(true)
    expect(releaseIdentity(db, first.reservationId, 13)).toBe(true)
    expect(tryReserveIdentity(db, {
      teamId: "t1", memberName: "dana", agent: "explore",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 1_000, now: 14,
    }).reserved).toBe(true)
  })

  test("expires abandoned reservations and enforces per-agent identity limits", () => {
    expect(tryReserveIdentity(db, {
      teamId: "t1", memberName: "charlie", agent: "build",
      limits: { global: 4, perAgent: { build: 1 } }, reservationTtlMs: 10, now: 10,
    }).reserved).toBe(true)
    expect(tryReserveIdentity(db, {
      teamId: "t1", memberName: "dana", agent: "build",
      limits: { global: 4, perAgent: { build: 1 } }, reservationTtlMs: 10, now: 15,
    })).toEqual({ reserved: false, reason: "agent_capacity" })
    expect(tryReserveIdentity(db, {
      teamId: "t1", memberName: "dana", agent: "build",
      limits: { global: 4, perAgent: { build: 1 } }, reservationTtlMs: 10, now: 21,
    }).reserved).toBe(true)
  })

  test("rejects identity reservations for archived teams", () => {
    db.run("UPDATE team SET status = 'archived' WHERE id = 't1'")
    expect(() => tryReserveIdentity(db, {
      teamId: "t1", memberName: "charlie", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 10, now: 10,
    })).toThrow(/active team/)
    expect(db.query("SELECT id FROM scheduler_identity").all()).toEqual([])
  })

  test("rejects identity reservations for existing member names", () => {
    expect(tryReserveIdentity(db, {
      teamId: "t1", memberName: "alice", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 10, now: 10,
    })).toEqual({ reserved: false, reason: "identity_exists" })
  })

  test("does not activate an identity reservation after its deadline", () => {
    const reservation = tryReserveIdentity(db, {
      teamId: "t1", memberName: "charlie", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 10, now: 10,
    })
    if (!reservation.reserved) throw new Error("expected reservation")

    expect(activateIdentity(db, reservation.reservationId, 21)).toBe(false)
    expect(db.query("SELECT state FROM scheduler_identity WHERE id = ?").get(reservation.reservationId)).toEqual({ state: "expired" })
  })

  test("does not activate an identity reservation without its persisted member", () => {
    const reservation = tryReserveIdentity(db, {
      teamId: "t1", memberName: "charlie", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 10, now: 10,
    })
    if (!reservation.reserved) throw new Error("expected reservation")

    expect(activateIdentity(db, reservation.reservationId, 11)).toBe(false)
    expect(db.query("SELECT state FROM scheduler_identity WHERE id = ?").get(reservation.reservationId)).toEqual({ state: "reserved" })
  })

  test("activates identity and queues its initial wake atomically", () => {
    const reservation = tryReserveIdentity(db, {
      teamId: "t1", memberName: "charlie", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 100, now: 10,
    })
    if (!reservation.reserved) throw new Error("expected reservation")
    insertMember(db, "t1", "charlie", "session-c")

    const wake = activateIdentityAndQueue(db, reservation.reservationId, {
      teamId: "t1", memberName: "charlie", sessionId: "session-c", agent: "build",
      reason: "spawn", coalesceKey: "member:charlie", prompt: "start", now: 11,
    })
    expect(wake?.coalesced).toBe(false)
    expect(db.query("SELECT state FROM scheduler_identity WHERE id = ?").get(reservation.reservationId)).toEqual({ state: "active" })
    expect(db.query("SELECT prompt FROM scheduler_wake WHERE id = ?").get(wake?.wakeId)).toEqual({ prompt: "start" })
  })

  test("transitions linked messages through injected and processed states", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES (?, ?, ?, ?, ?, ?)", ["msg-1", "t1", "lead", "alice", "work", 1])
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member:alice", messageId: "msg-1", now: 10,
    })
    const acquired = tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 1_000, 20)
    if (!acquired.acquired) throw new Error("expected lease")

    expect(markRunInjected(db, acquired.leaseId, 21)).toBe(true)
    expect(db.query("SELECT delivered, delivery_state FROM team_message WHERE id = 'msg-1'").get()).toEqual({ delivered: 1, delivery_state: "injected" })
    expect(finishRun(db, acquired.leaseId, "processed", undefined, 22)).toBe(true)
    expect(db.query("SELECT delivery_state FROM team_message WHERE id = 'msg-1'").get()).toEqual({ delivery_state: "processed" })
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "completed" })
    expect(db.query("SELECT state FROM scheduler_run_lease WHERE id = ?").get(acquired.leaseId)).toEqual({ state: "released" })
  })

  test("records failed runs without consuming capacity", () => {
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "recovery", coalesceKey: "member:alice", now: 10,
    })
    const acquired = tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 1_000, 20)
    if (!acquired.acquired) throw new Error("expected lease")

    expect(finishRun(db, acquired.leaseId, "failed", "session missing", 21)).toBe(true)
    expect(db.query("SELECT state, last_error FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "failed", last_error: "session missing" })
    expect(db.query("SELECT state FROM scheduler_run_lease WHERE id = ?").get(acquired.leaseId)).toEqual({ state: "failed" })
  })

  test("does not erase legacy delivered state when an injected run later fails", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-1', 't1', 'lead', 'alice', 'work', 1)")
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", messageId: "msg-1", now: 10,
    })
    const acquired = tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 100, 20)
    if (!acquired.acquired) throw new Error("expected lease")

    markRunInjected(db, acquired.leaseId, 21)
    finishRun(db, acquired.leaseId, "failed", "transport closed", 22)
    expect(db.query("SELECT delivered, delivery_state FROM team_message WHERE id = 'msg-1'").get()).toEqual({ delivered: 1, delivery_state: "failed" })
  })

  test("lists only ready queued wakes in deterministic order", () => {
    const delayed = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "delayed", notBefore: 30, now: 10,
    })
    const ready = queueWake(db, {
      teamId: "t1", memberName: "bob", sessionId: "session-b", agent: "build",
      reason: "message", coalesceKey: "ready", notBefore: 20, now: 11,
    })

    expect(listReadyWakes(db, 20).map(wake => wake.id)).toEqual([ready.wakeId])
    expect(listReadyWakes(db, 30).map(wake => wake.id)).toEqual([delayed.wakeId, ready.wakeId])
  })

  test("explicit stale-run reconciliation requeues leased wakes", () => {
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member:alice", now: 10,
    })
    expect(tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 10, 20).acquired).toBe(true)

    expect(expireStaleRuns(db, 31)).toBe(1)
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "leased" })
    const lease = db.query("SELECT id FROM scheduler_run_lease WHERE wake_id = ?").get(wake.wakeId) as { id: string }
    expect(reconcileExpiredRun(db, lease.id, "requeue", undefined, 32)).toBe(true)
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "queued" })
  })

  test("queues a successor when work arrives during a leased wake", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-1', 't1', 'lead', 'alice', 'one', 1)")
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-2', 't1', 'lead', 'alice', 'two', 2)")
    const first = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", messageId: "msg-1", now: 10,
    })
    const acquired = tryAcquireRun(db, first.wakeId, { global: 1, perAgent: {} }, 1_000, 20)
    if (!acquired.acquired) throw new Error("expected lease")
    markRunInjected(db, acquired.leaseId, 21)

    const second = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", messageId: "msg-2", now: 22,
    })
    expect(second.wakeId).not.toBe(first.wakeId)
    finishRun(db, acquired.leaseId, "processed", undefined, 23)
    expect(db.query("SELECT delivery_state FROM team_message WHERE id = 'msg-2'").get()).toEqual({ delivery_state: "wake_queued" })
  })

  test("scopes coalescing keys to the target member", () => {
    const alice = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "messages", now: 10,
    })
    const bob = queueWake(db, {
      teamId: "t1", memberName: "bob", sessionId: "session-b", agent: "build",
      reason: "message", coalesceKey: "messages", now: 11,
    })
    expect(bob.wakeId).not.toBe(alice.wakeId)
  })

  test("persists a message and wake atomically", () => {
    const result = queueMessageWake(db, {
      messageId: "msg-atomic", fromName: "lead", toName: "alice", content: "work",
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "messages", now: 10,
    })
    expect(db.query("SELECT delivery_state FROM team_message WHERE id = 'msg-atomic'").get()).toEqual({ delivery_state: "wake_queued" })
    expect(db.query("SELECT wake_id FROM scheduler_message_wake WHERE message_id = 'msg-atomic'").get()).toEqual({ wake_id: result.wakeId })

    expect(() => queueMessageWake(db, {
      messageId: "msg-rollback", fromName: "lead", toName: "missing", content: "work",
      teamId: "t1", memberName: "missing", sessionId: "missing", agent: "build",
      reason: "message", coalesceKey: "messages", now: 11,
    })).toThrow()
    expect(db.query("SELECT id FROM team_message WHERE id = 'msg-rollback'").get()).toBeNull()
  })

  test("renews only unexpired active leases", () => {
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", now: 10,
    })
    const acquired = tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 10, 20)
    if (!acquired.acquired) throw new Error("expected lease")
    expect(renewRunLease(db, acquired.leaseId, 10, 25)).toBe(true)
    expect(renewRunLease(db, acquired.leaseId, 10, 36)).toBe(false)
  })

  test("merges an expired wake into an already queued successor", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-old', 't1', 'lead', 'alice', 'old', 1)")
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-new', 't1', 'lead', 'alice', 'new', 2)")
    const oldWake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "spawn", coalesceKey: "member", messageId: "msg-old", prompt: "critical initial task", now: 10,
    })
    const acquired = tryAcquireRun(db, oldWake.wakeId, { global: 1, perAgent: {} }, 10, 20)
    if (!acquired.acquired) throw new Error("expected lease")
    const newWake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", messageId: "msg-new", now: 21,
    })
    expireStaleRuns(db, 31)

    expect(reconcileExpiredRun(db, acquired.leaseId, "requeue", undefined, 32)).toBe(true)
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(oldWake.wakeId)).toEqual({ state: "cancelled" })
    expect(db.query("SELECT message_id FROM scheduler_message_wake WHERE wake_id = ? ORDER BY message_id").all(newWake.wakeId)).toEqual([
      { message_id: "msg-new" },
      { message_id: "msg-old" },
    ])
    expect(getWakePayload(db, newWake.wakeId)?.prompt).toBe("critical initial task")
  })

  test("rejects linking a direct message to the wrong recipient", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-bob', 't1', 'lead', 'bob', 'work', 1)")
    expect(() => queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", messageId: "msg-bob", now: 10,
    })).toThrow(/does not target/)
    expect(db.query("SELECT id FROM scheduler_wake").all()).toEqual([])
  })

  test("rejects caller-supplied routing that differs from the member registry", () => {
    expect(() => queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-b", agent: "build",
      reason: "message", coalesceKey: "member", now: 10,
    })).toThrow(/routing does not match/)
    expect(() => queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "explore",
      reason: "message", coalesceKey: "member", now: 10,
    })).toThrow(/routing does not match/)
    db.run("UPDATE team_member SET status = 'shutdown' WHERE team_id = 't1' AND name = 'alice'")
    expect(() => queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", now: 10,
    })).toThrow(/not schedulable/)
  })

  test("cancels a queued wake if member routing changes before acquisition", () => {
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", now: 10,
    })
    db.run("UPDATE team_member SET session_id = 'session-new' WHERE team_id = 't1' AND name = 'alice'")

    expect(tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 10, 20)).toEqual({ acquired: false, reason: "wake_unavailable" })
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "cancelled" })
  })

  test("rejects relinking a terminal direct message", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, delivery_state, time_created) VALUES ('msg-done', 't1', 'lead', 'alice', 'done', 'processed', 1)")

    expect(() => queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "new-key", messageId: "msg-done", now: 10,
    })).toThrow(/terminal/)
    expect(db.query("SELECT id FROM scheduler_wake").all()).toEqual([])
  })

  test("tracks broadcast delivery independently for each recipient", () => {
    const [alice, bob] = queueBroadcastWakes(db, {
      messageId: "broadcast", teamId: "t1", fromName: "lead", content: "work", now: 10,
      recipients: [
        { memberName: "alice", sessionId: "session-a", agent: "build", reason: "broadcast", coalesceKey: "broadcast:alice" },
        { memberName: "bob", sessionId: "session-b", agent: "build", reason: "broadcast", coalesceKey: "broadcast:bob" },
      ],
    })
    if (!alice || !bob) throw new Error("expected broadcast wakes")
    const aliceLease = tryAcquireRun(db, alice.wakeId, { global: 2, perAgent: {} }, 100, 20)
    const bobLease = tryAcquireRun(db, bob.wakeId, { global: 2, perAgent: {} }, 100, 20)
    if (!aliceLease.acquired || !bobLease.acquired) throw new Error("expected leases")

    markRunInjected(db, aliceLease.leaseId, 21)
    finishRun(db, aliceLease.leaseId, "processed", undefined, 22)
    expect(db.query("SELECT delivery_state FROM team_message WHERE id = 'broadcast'").get()).toEqual({ delivery_state: "wake_queued" })
    expect(db.query("SELECT w.member_name, mw.delivery_state FROM scheduler_message_wake mw JOIN scheduler_wake w ON w.id = mw.wake_id WHERE mw.message_id = 'broadcast' ORDER BY w.member_name").all()).toEqual([
      { member_name: "alice", delivery_state: "processed" },
      { member_name: "bob", delivery_state: "wake_queued" },
    ])

    markRunInjected(db, bobLease.leaseId, 23)
    expect(db.query("SELECT delivery_state FROM team_message WHERE id = 'broadcast'").get()).toEqual({ delivery_state: "injected" })
    finishRun(db, bobLease.leaseId, "processed", undefined, 24)
    expect(db.query("SELECT delivery_state FROM team_message WHERE id = 'broadcast'").get()).toEqual({ delivery_state: "processed" })
  })

  test("requires broadcasts to register their complete recipient set atomically", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, content, time_created) VALUES ('broadcast', 't1', 'lead', 'work', 1)")
    expect(() => queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "broadcast", coalesceKey: "broadcast:alice", messageId: "broadcast", now: 10,
    })).toThrow(/queueBroadcastWakes/)
    expect(db.query("SELECT id FROM scheduler_wake").all()).toEqual([])
  })

  test("rejects oversized atomic messages without persisting a wake", () => {
    expect(() => queueMessageWake(db, {
      messageId: "msg-large", fromName: "lead", toName: "alice", content: "x".repeat(10 * 1024 + 1),
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "messages", now: 10,
    })).toThrow(/10KB/)
    expect(db.query("SELECT id FROM team_message WHERE id = 'msg-large'").get()).toBeNull()
    expect(db.query("SELECT id FROM scheduler_wake").all()).toEqual([])
  })

  test("persists restart-safe wake prompts and linked message payloads", () => {
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-1', 't1', 'lead', 'alice', 'follow-up', 1)")
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "spawn", coalesceKey: "member", messageId: "msg-1", prompt: "initial context", now: 10,
    })
    expect(getWakePayload(db, wake.wakeId)).toEqual({
      prompt: "initial context",
      messages: [{ id: "msg-1", fromName: "lead", content: "follow-up" }],
    })
  })

  test("records injection evidence and can requeue a rejected active run", () => {
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", now: 10,
    })
    const acquired = tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 100, 20)
    if (!acquired.acquired) throw new Error("expected lease")
    expect(findRunBySession(db, "session-a")).toMatchObject({ leaseId: acquired.leaseId, state: "active", injectedAt: null })
    expect(markRunInjected(db, acquired.leaseId, 21)).toBe(true)
    expect(findRunBySession(db, "session-a")?.injectedAt).toBe(21)
    expect(requeueRun(db, acquired.leaseId, "transport rejected", 30, 22)).toBe(true)
    expect(findRunBySession(db, "session-a")).toBeUndefined()
    expect(db.query("SELECT state, not_before FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "queued", not_before: 52 })
  })

  test("terminalizes member wakes, leases, messages, and identity together", () => {
    db.run("INSERT INTO scheduler_identity (id, team_id, member_name, agent, state, reserved_at, activated_at) VALUES ('identity-a', 't1', 'alice', 'build', 'active', 1, 1)")
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, time_created) VALUES ('msg-1', 't1', 'lead', 'alice', 'work', 1)")
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "session-a", agent: "build",
      reason: "message", coalesceKey: "member", messageId: "msg-1", now: 10,
    })
    expect(tryAcquireRun(db, wake.wakeId, { global: 1, perAgent: {} }, 100, 20).acquired).toBe(true)

    expect(terminateMemberScheduling(db, "t1", "alice", "shutdown", 21)).toBe(true)
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "failed" })
    expect(db.query("SELECT state FROM scheduler_run_lease WHERE wake_id = ?").get(wake.wakeId)).toEqual({ state: "failed" })
    expect(db.query("SELECT delivery_state FROM team_message WHERE id = 'msg-1'").get()).toEqual({ delivery_state: "failed" })
    expect(db.query("SELECT state FROM scheduler_identity WHERE id = 'identity-a'").get()).toEqual({ state: "released" })
  })
})

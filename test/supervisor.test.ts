import { beforeEach, describe, expect, test } from "bun:test"
import type { Database } from "../src/db"
import { activateIdentity, finishRun, markRunInjected, queueWake, releaseIdentity, tryAcquireRun, tryReserveIdentity } from "../src/scheduler"
import {
  buildSupervisorReviewPrompt,
  claimSupervisorBroadcast,
  provisionSupervisorForTeam,
  reconcileSupervisors,
  reconcileTeamSupervision,
  recordWorkerQuiescenceEvent,
  SUPERVISOR_AGENT,
  SUPERVISOR_MEMBER_NAME,
} from "../src/supervisor"
import { MemberRegistry } from "../src/state"
import { checkToolIsolation, handleSessionStatusEvent } from "../src/hooks"
import { DescendantTracker } from "../src/state"
import { buildLeadSystemPrompt } from "../src/system-prompt"
import { executeTeamStatus, lastCallTime, lastKnownState } from "../src/tools/team-status"
import { setupDeps } from "./helpers"
import { executeTeamBroadcast } from "../src/tools/team-broadcast"
import { executeTeamCleanup } from "../src/tools/team-cleanup"
import { executeTeamMessage } from "../src/tools/team-message"
import { DEFAULT_CONFIG } from "../src/config"
import { insertMember, insertTeam, mockClient, setupDb } from "./helpers"
import { DurableScheduler } from "../src/scheduler-runtime"
import { executeTeamCreate } from "../src/tools/team-create"
import { executeTeamTasksAdd } from "../src/tools/team-tasks-add"
import { executeTeamTasksComplete } from "../src/tools/team-tasks-complete"
import { executeTeamClaim } from "../src/tools/team-claim"

function insertSupervisor(db: Database): void {
  insertMember(db, "t1", SUPERVISOR_MEMBER_NAME, "supervisor-session")
  db.run(
    "UPDATE team_member SET member_kind = 'supervisor', agent = ? WHERE team_id = 't1' AND name = ?",
    [SUPERVISOR_AGENT, SUPERVISOR_MEMBER_NAME],
  )
}

describe("implicit Supervisor", () => {
  let db: Database

  beforeEach(() => {
    db = setupDb()
    insertTeam(db, "t1", "alpha", "lead-session")
  })

  test("provisions and recovers exactly one hidden read-only Supervisor", async () => {
    const client = mockClient()
    const registry = new MemberRegistry()

    expect(await reconcileSupervisors(db, client, registry, "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(1)
    expect(await reconcileSupervisors(db, client, registry, "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(0)

    expect(db.query("SELECT name, agent, member_kind, worktree_dir FROM team_member WHERE team_id = 't1'").all()).toEqual([
      { name: SUPERVISOR_MEMBER_NAME, agent: SUPERVISOR_AGENT, member_kind: "supervisor", worktree_dir: null },
    ])
    const creates = client.calls.filter(call => call.method === "session.create")
    expect(creates).toHaveLength(1)
    expect(SUPERVISOR_AGENT).toBe("opencode-ensemble-supervisor")
    expect(creates[0]?.args[0]).toMatchObject({
      parentID: "lead-session",
      agent: SUPERVISOR_AGENT,
      title: "Supervisor (alpha)",
      permission: [
        { permission: "*", pattern: "*", action: "deny" },
        { permission: "team_status", pattern: "*", action: "allow" },
        { permission: "team_tasks_list", pattern: "*", action: "allow" },
        { permission: "team_message", pattern: "*", action: "allow" },
        { permission: "team_broadcast", pattern: "*", action: "allow" },
      ],
    })
    expect(db.query("SELECT state, expires_at FROM scheduler_identity WHERE team_id = 't1' AND member_name = ?").get(SUPERVISOR_MEMBER_NAME)).toEqual({
      state: "active",
      expires_at: null,
    })
    expect(registry.getByName("t1", SUPERVISOR_MEMBER_NAME)?.sessionId).toBeTruthy()
  })

  test("does not replace or duplicate an in-flight Supervisor reservation", async () => {
    const client = mockClient()
    const registry = new MemberRegistry()
    client.session.create = async options => {
      client.calls.push({ method: "session.create", args: [options] })
      await Bun.sleep(0)
      return { data: { id: "concurrent-supervisor-session" } }
    }

    const results = await Promise.all([
      provisionSupervisorForTeam(db, client, registry, "t1", DEFAULT_CONFIG.scheduler),
      provisionSupervisorForTeam(db, client, registry, "t1", DEFAULT_CONFIG.scheduler),
    ])

    expect(results.map(result => result.status).sort()).toEqual(["capacity_denied", "provisioned"])
    expect(client.calls.filter(call => call.method === "session.create")).toHaveLength(1)
    expect(db.query("SELECT COUNT(*) AS count FROM team_member WHERE team_id = 't1' AND member_kind = 'supervisor'").get()).toEqual({ count: 1 })
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_identity WHERE team_id = 't1' AND member_name = ? AND state = 'active'").get(SUPERVISOR_MEMBER_NAME)).toEqual({ count: 1 })
  })

  test("replaces a persisted Supervisor that uses the legacy collision-prone agent identity", async () => {
    insertSupervisor(db)
    db.run("UPDATE team_member SET agent = 'Supervisor' WHERE team_id = 't1' AND name = ?", [SUPERVISOR_MEMBER_NAME])
    const client = mockClient()

    expect(await reconcileSupervisors(db, client, new MemberRegistry(), "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(1)
    expect(db.query("SELECT agent, status FROM team_member WHERE team_id = 't1' AND name = ?").get(SUPERVISOR_MEMBER_NAME)).toEqual({
      agent: "opencode-ensemble-supervisor",
      status: "ready",
    })
    expect(client.calls).toContainEqual({ method: "session.abort", args: [{ sessionID: "supervisor-session" }] })
  })

  test("respects configured global and per-agent identity capacity before session creation", async () => {
    const globalReservation = tryReserveIdentity(db, {
      teamId: "t1", memberName: "alice", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 60_000,
    })
    if (!globalReservation.reserved) throw new Error("expected worker reservation")
    insertMember(db, "t1", "alice", "worker-session")
    expect(activateIdentity(db, globalReservation.reservationId)).toBe(true)
    const client = mockClient()

    expect(await provisionSupervisorForTeam(db, client, new MemberRegistry(), "t1", {
      ...DEFAULT_CONFIG.scheduler,
      identityLimits: { global: 1, perAgent: {} },
    })).toEqual({ status: "capacity_denied", reason: "global_capacity" })
    expect(client.calls.filter(call => call.method === "session.create")).toHaveLength(0)

    insertTeam(db, "t2", "beta", "other-lead")
    const supervisorReservation = tryReserveIdentity(db, {
      teamId: "t2", memberName: "internal", agent: SUPERVISOR_AGENT,
      limits: { global: 10, perAgent: {} }, reservationTtlMs: 60_000,
    })
    if (!supervisorReservation.reserved) throw new Error("expected Supervisor reservation")
    insertMember(db, "t2", "internal", "other-supervisor-session")
    db.run("UPDATE team_member SET agent = ?, member_kind = 'supervisor' WHERE team_id = 't2' AND name = 'internal'", [SUPERVISOR_AGENT])
    expect(activateIdentity(db, supervisorReservation.reservationId)).toBe(true)

    expect(await provisionSupervisorForTeam(db, client, new MemberRegistry(), "t1", {
      ...DEFAULT_CONFIG.scheduler,
      identityLimits: { global: 10, perAgent: { [SUPERVISOR_AGENT]: 1 } },
    })).toEqual({ status: "capacity_denied", reason: "agent_capacity" })
  })

  test("compensates an expired reservation after session creation", async () => {
    const client = mockClient()
    client.session.create = async options => {
      client.calls.push({ method: "session.create", args: [options] })
      db.run("UPDATE scheduler_identity SET expires_at = 0 WHERE team_id = 't1' AND member_name = ? AND state = 'reserved'", [SUPERVISOR_MEMBER_NAME])
      return { data: { id: "expired-supervisor-session" } }
    }

    await expect(provisionSupervisorForTeam(db, client, new MemberRegistry(), "t1", DEFAULT_CONFIG.scheduler)).rejects.toThrow("expired")
    expect(db.query("SELECT state FROM scheduler_identity WHERE team_id = 't1' AND member_name = ?").get(SUPERVISOR_MEMBER_NAME)).toEqual({ state: "expired" })
    expect(db.query("SELECT COUNT(*) AS count FROM team_member WHERE team_id = 't1'").get()).toEqual({ count: 0 })
    expect(client.calls).toContainEqual({ method: "session.abort", args: [{ sessionID: "expired-supervisor-session" }] })
  })

  test("releases reserved capacity when Supervisor session creation rejects", async () => {
    const client = mockClient()
    client.session.create = async () => {
      throw new Error("transport unavailable")
    }

    await expect(provisionSupervisorForTeam(db, client, new MemberRegistry(), "t1", DEFAULT_CONFIG.scheduler)).rejects.toThrow("transport unavailable")
    expect(db.query("SELECT state FROM scheduler_identity WHERE team_id = 't1' AND member_name = ?").get(SUPERVISOR_MEMBER_NAME)).toEqual({ state: "released" })
    expect(db.query("SELECT COUNT(*) AS count FROM team_member WHERE team_id = 't1'").get()).toEqual({ count: 0 })
  })

  test("fails team creation closed when mandatory Supervisor session creation fails", async () => {
    const deps = setupDeps(db)
    deps.client.session.create = async options => {
      deps.client.calls.push({ method: "session.create", args: [options] })
      return { data: undefined }
    }

    await expect(executeTeamCreate(deps, { name: "mandatory" }, "new-lead", {}, async teamId => {
      const result = await provisionSupervisorForTeam(db, deps.client, deps.registry, teamId, DEFAULT_CONFIG.scheduler)
      if (result.status === "capacity_denied") throw new Error(result.reason)
    })).rejects.toThrow("mandatory Supervisor")
    expect(db.query("SELECT COUNT(*) AS count FROM team WHERE name = 'mandatory'").get()).toEqual({ count: 0 })
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_identity WHERE state IN ('reserved', 'active') AND member_name = ?").get(SUPERVISOR_MEMBER_NAME)).toEqual({ count: 0 })
  })

  test("fails team creation closed when mandatory Supervisor capacity is denied", async () => {
    const reservation = tryReserveIdentity(db, {
      teamId: "t1", memberName: "alice", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 60_000,
    })
    if (!reservation.reserved) throw new Error("expected worker reservation")
    insertMember(db, "t1", "alice", "worker-session")
    activateIdentity(db, reservation.reservationId)
    const deps = setupDeps(db)
    const config = { ...DEFAULT_CONFIG.scheduler, identityLimits: { global: 1, perAgent: {} } }

    await expect(executeTeamCreate(deps, { name: "capacity-denied" }, "new-lead", {}, async teamId => {
      const result = await provisionSupervisorForTeam(db, deps.client, deps.registry, teamId, config)
      if (result.status === "capacity_denied") throw new Error(result.reason)
    })).rejects.toThrow("mandatory Supervisor")
    expect(db.query("SELECT COUNT(*) AS count FROM team WHERE name = 'capacity-denied'").get()).toEqual({ count: 0 })
  })

  test("maintenance retries missing Supervisor provisioning after capacity is released", async () => {
    const reservation = tryReserveIdentity(db, {
      teamId: "t1", memberName: "alice", agent: "build",
      limits: { global: 1, perAgent: {} }, reservationTtlMs: 60_000,
    })
    if (!reservation.reserved) throw new Error("expected worker reservation")
    insertMember(db, "t1", "alice", "worker-session")
    activateIdentity(db, reservation.reservationId)
    const client = mockClient()
    const registry = new MemberRegistry()
    const schedulerConfig = { ...DEFAULT_CONFIG.scheduler, identityLimits: { global: 1, perAgent: {} } }
    const scheduler = new DurableScheduler(db, client, schedulerConfig, false, "/tmp/test-project", async () => {
      await reconcileSupervisors(db, client, registry, "/tmp/test-project", schedulerConfig)
    })

    await scheduler.recover()
    expect(db.query("SELECT COUNT(*) AS count FROM team_member WHERE member_kind = 'supervisor'").get()).toEqual({ count: 0 })
    releaseIdentity(db, reservation.reservationId)
    await scheduler.recover()
    expect(db.query("SELECT COUNT(*) AS count FROM team_member WHERE member_kind = 'supervisor'").get()).toEqual({ count: 1 })
  })

  test("replaces a terminal Supervisor without retaining stale registry authority", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)
    const originalReview = reconcileTeamSupervision(db, "t1", 61_000)
    if (!originalReview.wakeId) throw new Error("expected original review wake")
    db.run("UPDATE team_member SET status = 'error' WHERE team_id = 't1' AND name = ?", [SUPERVISOR_MEMBER_NAME])
    const client = mockClient()
    const registry = new MemberRegistry()
    registry.register("t1", SUPERVISOR_MEMBER_NAME, "supervisor-session")

    expect(await reconcileSupervisors(db, client, registry, "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(1)
    expect(db.query("SELECT COUNT(*) AS count FROM team_member WHERE team_id = 't1' AND member_kind = 'supervisor'").get()).toEqual({ count: 1 })
    expect(db.query("SELECT status FROM team_member WHERE team_id = 't1' AND member_kind = 'supervisor'").get()).toEqual({ status: "ready" })
    expect(registry.getBySession("supervisor-session")).toBeUndefined()
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(originalReview.wakeId)).toEqual({ state: "failed" })
    expect(db.query("SELECT last_reviewed FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ last_reviewed: -1 })
    const replacementReview = reconcileTeamSupervision(db, "t1", 61_001)
    expect(replacementReview.state).toBe("queued")
    expect(db.query("SELECT session_id FROM scheduler_wake WHERE id = ?").get(replacementReview.wakeId)).not.toEqual({ session_id: "supervisor-session" })
  })

  test("does not attach a newly created Supervisor after the team is archived", async () => {
    const client = mockClient()
    client.session.create = async options => {
      client.calls.push({ method: "session.create", args: [options] })
      db.run("UPDATE team SET status = 'archived' WHERE id = 't1'")
      return { data: { id: "orphan-supervisor-session" } }
    }

    expect(await reconcileSupervisors(db, client, new MemberRegistry(), "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(0)
    expect(db.query("SELECT COUNT(*) AS count FROM team_member WHERE team_id = 't1'").get()).toEqual({ count: 0 })
    expect(db.query("SELECT state FROM scheduler_identity WHERE team_id = 't1' AND member_name = ?").get(SUPERVISOR_MEMBER_NAME)).toEqual({ state: "released" })
    expect(client.calls).toContainEqual({ method: "session.abort", args: [{ sessionID: "orphan-supervisor-session" }] })
  })

  test("queues one review only after exactly 60 seconds of continuous quiescence", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)

    expect(reconcileTeamSupervision(db, "t1", 900)).toEqual({ state: "active", generation: 0 })
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: null })
    expect(recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)).toMatchObject({ state: "quiet", generation: 0 })
    expect(reconcileTeamSupervision(db, "t1", 60_999).state).toBe("quiet")
    const eligible = reconcileTeamSupervision(db, "t1", 61_000)
    expect(eligible.state).toBe("queued")
    expect(reconcileTeamSupervision(db, "t1", 61_000).state).toBe("reviewed")
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE reason = 'supervisor_review'").get()).toEqual({ count: 1 })
  })

  test("rearms at the task-add mutation time while all workers are idle", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    const deps = setupDeps(db)

    await executeTeamTasksAdd(deps, { tasks: [{ content: "new work", priority: "high" }] }, "lead-session")

    const task = db.query("SELECT time_updated FROM team_task WHERE team_id = 't1'").get() as { time_updated: number }
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: task.time_updated })
    expect(reconcileTeamSupervision(db, "t1", task.time_updated + 59_999).state).toBe("quiet")
    expect(reconcileTeamSupervision(db, "t1", task.time_updated + 60_000).state).toBe("queued")
  })

  test("rearms at the completion time when completing work unblocks an idle team task", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    const deps = setupDeps(db)
    const prerequisiteResult = await executeTeamTasksAdd(deps, {
      tasks: [{ content: "prerequisite", priority: "high" }],
    }, "lead-session")
    const prerequisiteId = prerequisiteResult.match(/task_\S+/)?.[0]
    if (!prerequisiteId) throw new Error("expected prerequisite task ID")
    await executeTeamTasksAdd(deps, {
      tasks: [{ content: "dependent", priority: "medium", depends_on: [prerequisiteId] }],
    }, "lead-session")

    await executeTeamTasksComplete(deps, { task_id: prerequisiteId }, "lead-session")

    const prerequisite = db.query("SELECT time_updated FROM team_task WHERE id = ?").get(prerequisiteId) as { time_updated: number }
    expect(db.query("SELECT status FROM team_task WHERE content = 'dependent'").get()).toEqual({ status: "pending" })
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: prerequisite.time_updated })
    expect(reconcileTeamSupervision(db, "t1", prerequisite.time_updated + 59_999).state).toBe("quiet")
    expect(reconcileTeamSupervision(db, "t1", prerequisite.time_updated + 60_000).state).toBe("queued")
  })

  test("rearms at the claim mutation time while all workers are idle", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    const deps = setupDeps(db)
    const addResult = await executeTeamTasksAdd(deps, {
      tasks: [{ content: "lead-owned work", priority: "medium" }],
    }, "lead-session")
    const taskId = addResult.match(/task_\S+/)?.[0]
    if (!taskId) throw new Error("expected task ID")

    await executeTeamClaim(deps, { task_id: taskId }, "lead-session")

    const task = db.query("SELECT status, assignee, time_updated FROM team_task WHERE id = ?").get(taskId) as {
      status: string
      assignee: string
      time_updated: number
    }
    expect(task).toMatchObject({ status: "in_progress", assignee: "lead" })
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: task.time_updated })
    expect(reconcileTeamSupervision(db, "t1", task.time_updated + 59_999).state).toBe("quiet")
    expect(reconcileTeamSupervision(db, "t1", task.time_updated + 60_000).state).toBe("queued")
  })

  test("does not arm or review a team that has no ordinary workers", () => {
    insertSupervisor(db)

    expect(reconcileTeamSupervision(db, "t1", 1_000)).toEqual({ state: "active", generation: 0 })
    expect(recordWorkerQuiescenceEvent(db, "t1", SUPERVISOR_MEMBER_NAME, 1_000)).toEqual({ state: "active", generation: 0 })
    expect(reconcileTeamSupervision(db, "t1", 61_000)).toEqual({ state: "active", generation: 0 })
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: null })
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE reason = 'supervisor_review'").get()).toEqual({ count: 0 })
  })

  test("anchors quiet_since to the final worker idle event after its scheduler lease completes", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    const registry = new MemberRegistry()
    registry.register("t1", "alice", "worker-session")
    const now = Date.now()
    const wake = queueWake(db, {
      teamId: "t1", memberName: "alice", sessionId: "worker-session", agent: "build",
      reason: "message", coalesceKey: "worker-event", now,
    })
    const lease = tryAcquireRun(db, wake.wakeId, { global: 10, perAgent: {} }, 60_000, now)
    if (!lease.acquired) throw new Error("expected worker lease")
    markRunInjected(db, lease.leaseId, now)
    handleSessionStatusEvent(db, registry, "worker-session", "busy")
    const scheduler = new DurableScheduler(db, mockClient(), DEFAULT_CONFIG.scheduler, false)

    const eventTime = now + 1
    const transition = handleSessionStatusEvent(db, registry, "worker-session", "idle")
    scheduler.onSessionStatus("worker-session", "idle")
    if (!transition) throw new Error("expected final worker idle transition")
    expect(recordWorkerQuiescenceEvent(db, transition.teamId, transition.memberName, eventTime)).toMatchObject({ state: "quiet" })
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: eventTime })
    expect(db.query("SELECT state FROM scheduler_run_lease WHERE id = ?").get(lease.leaseId)).toEqual({ state: "released" })
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(wake.wakeId)).toEqual({ state: "completed" })

    expect(handleSessionStatusEvent(db, registry, "worker-session", "idle")).toBeUndefined()
    expect(db.query("SELECT quiet_since FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ quiet_since: eventTime })
  })

  test("worker work cancels the quiet generation and stale review authority", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)
    const queued = reconcileTeamSupervision(db, "t1", 61_000)
    if (!queued.wakeId) throw new Error("expected review wake")
    const lease = tryAcquireRun(db, queued.wakeId, { global: 10, perAgent: {} }, 60_000, 61_000)
    if (!lease.acquired) throw new Error("expected review lease")

    db.run("UPDATE team_member SET status = 'busy', execution_status = 'running' WHERE team_id = 't1' AND name = 'alice'")
    expect(reconcileTeamSupervision(db, "t1", 61_001)).toMatchObject({ state: "active", generation: 1 })
    expect(() => claimSupervisorBroadcast(db, "supervisor-session")).toThrow("stale")
  })

  test("rearms the unchanged generation after a failed review but not after a processed silent review", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)
    const failedReview = reconcileTeamSupervision(db, "t1", 61_000)
    if (!failedReview.wakeId) throw new Error("expected failed review wake")
    const failedLease = tryAcquireRun(db, failedReview.wakeId, { global: 10, perAgent: {} }, 60_000, 61_000)
    if (!failedLease.acquired) throw new Error("expected failed review lease")

    expect(finishRun(db, failedLease.leaseId, "failed", "injected failure", 61_001)).toBe(true)
    expect(db.query("SELECT last_reviewed FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ last_reviewed: -1 })
    const retry = reconcileTeamSupervision(db, "t1", 61_001)
    expect(retry.state).toBe("queued")
    if (!retry.wakeId) throw new Error("expected replacement review wake")
    const retryLease = tryAcquireRun(db, retry.wakeId, { global: 10, perAgent: {} }, 60_000, 61_001)
    if (!retryLease.acquired) throw new Error("expected replacement review lease")
    markRunInjected(db, retryLease.leaseId, 61_001)
    expect(finishRun(db, retryLease.leaseId, "processed", undefined, 61_002)).toBe(true)

    expect(reconcileTeamSupervision(db, "t1", 121_002).state).toBe("reviewed")
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE reason = 'supervisor_review'").get()).toEqual({ count: 2 })
    db.run("UPDATE team_member SET status = 'error' WHERE team_id = 't1' AND name = ?", [SUPERVISOR_MEMBER_NAME])
    expect(await reconcileSupervisors(db, mockClient(), new MemberRegistry(), "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(1)
    expect(db.query("SELECT last_reviewed FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ last_reviewed: 0 })
    expect(reconcileTeamSupervision(db, "t1", 121_003).state).toBe("reviewed")
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE reason = 'supervisor_review'").get()).toEqual({ count: 2 })
  })

  test("rearms a cancelled review after Supervisor routing changes", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)
    const cancelledReview = reconcileTeamSupervision(db, "t1", 61_000)
    if (!cancelledReview.wakeId) throw new Error("expected cancelled review wake")
    db.run("UPDATE team_member SET session_id = 'replacement-supervisor-session' WHERE team_id = 't1' AND name = ?", [SUPERVISOR_MEMBER_NAME])

    expect(tryAcquireRun(db, cancelledReview.wakeId, { global: 10, perAgent: {} }, 60_000, 61_001)).toEqual({ acquired: false, reason: "wake_unavailable" })
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(cancelledReview.wakeId)).toEqual({ state: "cancelled" })
    expect(db.query("SELECT last_reviewed FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ last_reviewed: -1 })
    const replacementReview = reconcileTeamSupervision(db, "t1", 61_001)
    expect(replacementReview.state).toBe("queued")
    expect(db.query("SELECT session_id FROM scheduler_wake WHERE id = ?").get(replacementReview.wakeId)).toEqual({ session_id: "replacement-supervisor-session" })
  })

  test("worker busy events invalidate review authority synchronously across plugin instances", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    const registry = new MemberRegistry()
    registry.register("t1", "alice", "worker-session")
    const now = Date.now()
    recordWorkerQuiescenceEvent(db, "t1", "alice", now - 60_000)
    const review = reconcileTeamSupervision(db, "t1", now)
    if (!review.wakeId) throw new Error("expected review wake")
    const lease = tryAcquireRun(db, review.wakeId, { global: 10, perAgent: {} }, 60_000, now)
    if (!lease.acquired) throw new Error("expected review lease")

    expect(handleSessionStatusEvent(db, registry, "worker-session", "busy")?.to).toBe("busy")
    expect(() => claimSupervisorBroadcast(db, "supervisor-session")).toThrow("stale")
  })

  test("queued worker wakes and duplicate idle reconciliation reset without duplicate reviews", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)
    queueWake(db, {
      teamId: "t1",
      memberName: "alice",
      sessionId: "worker-session",
      agent: "build",
      reason: "message",
      coalesceKey: "alice",
      now: 2_000,
    })

    expect(reconcileTeamSupervision(db, "t1", 2_000)).toMatchObject({ state: "active", generation: 1 })
    expect(reconcileTeamSupervision(db, "t1", 2_000)).toMatchObject({ state: "active", generation: 1 })
  })

  test("review prompt contains coordination evidence and non-mutating instructions", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    db.run("UPDATE team_member SET reported_to_lead = 1 WHERE name = 'alice'")
    db.run("INSERT INTO team_task (id, team_id, content, status, priority, assignee, time_created, time_updated) VALUES ('pending', 't1', 'unowned work', 'pending', 'high', NULL, 1, 1)")
    db.run("INSERT INTO team_task (id, team_id, content, status, priority, assignee, time_created, time_updated) VALUES ('done', 't1', 'finished work', 'completed', 'medium', 'alice', 1, 2)")
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, delivered, read, delivery_state, time_created) VALUES ('report', 't1', 'alice', 'lead', 'report details', 1, 1, 'processed', 3)")

    const prompt = buildSupervisorReviewPrompt(db, "t1", 7)
    expect(prompt).toContain("unowned work")
    expect(prompt).toContain("finished work")
    expect(prompt).toContain("report details")
    expect(prompt).toContain("alice")
    expect(prompt).toContain("at most one")
    expect(prompt).toContain("specific agents")
    expect(prompt).toContain("send nothing")
    expect(prompt).toContain("lead-only blockers")
    expect(prompt).toContain("Do not mutate tasks")
  })

  test("dispatches Supervisor reviews with exactly the four approved tools", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)
    expect(reconcileTeamSupervision(db, "t1", 61_000).state).toBe("queued")
    const client = mockClient()
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler)

    scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)

    const prompt = client.calls.find(call => call.method === "session.promptAsync" && (call.args[0] as { sessionID: string }).sessionID === "supervisor-session")
    expect((prompt?.args[0] as { tools: Record<string, boolean> }).tools).toEqual({
      team_status: true,
      team_tasks_list: true,
      team_message: true,
      team_broadcast: true,
    })
  })

  test("terminalizes a missing Supervisor session and maintenance provisions a replacement review", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    const registry = new MemberRegistry()
    registry.register("t1", SUPERVISOR_MEMBER_NAME, "supervisor-session")
    const now = Date.now()
    recordWorkerQuiescenceEvent(db, "t1", "alice", now - 60_000)
    const originalReview = reconcileTeamSupervision(db, "t1", now)
    if (!originalReview.wakeId) throw new Error("expected original review wake")
    const client = mockClient()
    client.session.promptAsync = async options => {
      client.calls.push({ method: "session.promptAsync", args: [options] })
      if (options.sessionID === "supervisor-session") {
        const error = new Error("session not found") as Error & { status: number }
        error.status = 404
        throw error
      }
      return {}
    }
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler, true, "/tmp/test-project", async () => {
      await reconcileSupervisors(db, client, registry, "/tmp/test-project", DEFAULT_CONFIG.scheduler)
    })

    scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)
    expect(db.query("SELECT status, execution_status FROM team_member WHERE team_id = 't1' AND member_kind = 'supervisor'").get()).toEqual({ status: "error", execution_status: "failed" })
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(originalReview.wakeId)).toEqual({ state: "failed" })
    expect(db.query("SELECT last_reviewed FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ last_reviewed: -1 })

    await scheduler.recover()
    await Bun.sleep(0)
    await Bun.sleep(0)
    const replacement = db.query("SELECT session_id, status FROM team_member WHERE team_id = 't1' AND member_kind = 'supervisor'").get() as { session_id: string; status: string }
    expect(replacement.status).toBe("ready")
    expect(replacement.session_id).not.toBe("supervisor-session")
    expect(client.calls.filter(call => call.method === "session.promptAsync").map(call => (call.args[0] as { sessionID: string }).sessionID)).toContain(replacement.session_id)
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE reason = 'supervisor_review' AND state = 'queued'").get()).toEqual({ count: 0 })
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE reason = 'supervisor_review' AND state = 'leased'").get()).toEqual({ count: 1 })
  })

  test("is invisible to worker-facing status and prompts and cannot mutate tasks", async () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    const deps = setupDeps(db)
    deps.registry.register("t1", "alice", "worker-session")
    deps.registry.register("t1", SUPERVISOR_MEMBER_NAME, "supervisor-session")
    lastCallTime.clear()
    lastKnownState.clear()

    expect(await executeTeamStatus(deps, "lead-session")).not.toContain(SUPERVISOR_MEMBER_NAME)
    expect(buildLeadSystemPrompt(db, "t1")).not.toContain(SUPERVISOR_MEMBER_NAME)
    for (const tool of ["team_status", "team_tasks_list", "team_message", "team_broadcast"]) {
      expect(() => checkToolIsolation(deps.registry, new DescendantTracker(), tool, "supervisor-session", db)).not.toThrow()
    }
    expect(() => checkToolIsolation(deps.registry, new DescendantTracker(), "team_tasks_complete", "supervisor-session", db)).toThrow("Supervisor")
    lastCallTime.clear()
    lastKnownState.clear()
    expect(await executeTeamStatus(deps, "supervisor-session")).toContain("alice")
    const crossInstanceRegistry = new MemberRegistry()
    expect(() => checkToolIsolation(crossInstanceRegistry, new DescendantTracker(), "team_status", "supervisor-session", db)).not.toThrow()
    expect(() => checkToolIsolation(crossInstanceRegistry, new DescendantTracker(), "team_tasks_add", "supervisor-session", db)).toThrow("Supervisor")
    await expect(executeTeamMessage(deps, { to: SUPERVISOR_MEMBER_NAME, text: "wake up" }, "lead-session")).rejects.toThrow("not found")

    db.run("DELETE FROM team_member WHERE team_id = 't1' AND name = ?", [SUPERVISOR_MEMBER_NAME])
    expect(() => checkToolIsolation(deps.registry, new DescendantTracker(), "team_tasks_complete", "supervisor-session", db)).toThrow("Supervisor")
  })

  test("can message only the lead during a current review lease", async () => {
    insertMember(db, "t1", "alice", "alice-session")
    insertSupervisor(db)
    const now = Date.now()
    recordWorkerQuiescenceEvent(db, "t1", "alice", now - 60_000)
    const review = reconcileTeamSupervision(db, "t1", now)
    if (!review.wakeId) throw new Error("expected review wake")
    const lease = tryAcquireRun(db, review.wakeId, { global: 10, perAgent: {} }, 60_000, now)
    if (!lease.acquired) throw new Error("expected review lease")
    const deps = setupDeps(db)
    deps.registry.register("t1", SUPERVISOR_MEMBER_NAME, "supervisor-session")

    await expect(executeTeamMessage(deps, { to: "alice", text: "side channel" }, "supervisor-session")).rejects.toThrow("only message the lead")
    await expect(executeTeamMessage(deps, { to: "lead", text: "lead-only blocker" }, "supervisor-session")).resolves.toBe("Message sent to lead.")
  })

  test("rejects review authority after its lease expires", () => {
    insertMember(db, "t1", "alice", "worker-session")
    insertSupervisor(db)
    recordWorkerQuiescenceEvent(db, "t1", "alice", 1_000)
    const review = reconcileTeamSupervision(db, "t1", 61_000)
    if (!review.wakeId) throw new Error("expected review wake")
    const lease = tryAcquireRun(db, review.wakeId, { global: 10, perAgent: {} }, 60_000, 61_000)
    if (!lease.acquired) throw new Error("expected review lease")

    expect(() => claimSupervisorBroadcast(db, "supervisor-session", 121_001)).toThrow("stale")
  })

  test("broadcasts one current review assignment to every active worker including reported-complete workers", async () => {
    insertMember(db, "t1", "alice", "alice-session")
    insertMember(db, "t1", "bob", "bob-session")
    db.run("UPDATE team_member SET reported_to_lead = 1 WHERE name = 'bob'")
    insertSupervisor(db)
    const now = Date.now()
    recordWorkerQuiescenceEvent(db, "t1", "alice", now - 60_000)
    const review = reconcileTeamSupervision(db, "t1", now)
    if (!review.wakeId) throw new Error("expected review wake")
    const lease = tryAcquireRun(db, review.wakeId, { global: 10, perAgent: {} }, 60_000, now)
    if (!lease.acquired) throw new Error("expected review lease")
    const deps = setupDeps(db)
    deps.registry.register("t1", SUPERVISOR_MEMBER_NAME, "supervisor-session")

    await executeTeamBroadcast(deps, { text: "alice owns A; bob owns B" }, "supervisor-session")
    deps.scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)

    const recipients = deps.client.calls.filter(call => call.method === "session.promptAsync")
      .map(call => (call.args[0] as { sessionID: string }).sessionID).sort()
    expect(recipients).toEqual(["alice-session", "bob-session", "lead-session"])
    await expect(executeTeamBroadcast(deps, { text: "second" }, "supervisor-session")).rejects.toThrow(/already|stale/)
  })

  test("rolls back Supervisor broadcast authorization when durable routing fails", async () => {
    insertMember(db, "t1", "alice", "alice-session")
    insertSupervisor(db)
    const now = Date.now()
    recordWorkerQuiescenceEvent(db, "t1", "alice", now - 60_000)
    const review = reconcileTeamSupervision(db, "t1", now)
    if (!review.wakeId) throw new Error("expected review wake")
    const lease = tryAcquireRun(db, review.wakeId, { global: 10, perAgent: {} }, 60_000, now)
    if (!lease.acquired) throw new Error("expected review lease")
    const deps = setupDeps(db)
    deps.registry.register("t1", SUPERVISOR_MEMBER_NAME, "supervisor-session")
    db.exec(`CREATE TRIGGER inject_supervisor_broadcast_failure BEFORE INSERT ON scheduler_wake
      WHEN NEW.reason = 'broadcast' BEGIN SELECT RAISE(ABORT, 'injected broadcast failure'); END`)

    await expect(executeTeamBroadcast(deps, { text: "alice owns retry" }, "supervisor-session")).rejects.toThrow("injected broadcast failure")
    expect(db.query("SELECT generation, last_reviewed, broadcast_generation FROM team_supervision WHERE team_id = 't1'").get()).toEqual({
      generation: 0,
      last_reviewed: 0,
      broadcast_generation: null,
    })
    expect(db.query("SELECT COUNT(*) AS count FROM team_message WHERE from_name = ?").get(SUPERVISOR_MEMBER_NAME)).toEqual({ count: 0 })

    db.exec("DROP TRIGGER inject_supervisor_broadcast_failure")
    await expect(executeTeamBroadcast(deps, { text: "alice owns retry" }, "supervisor-session")).resolves.toContain("Broadcast sent")
    expect(db.query("SELECT broadcast_generation FROM team_supervision WHERE team_id = 't1'").get()).toEqual({ broadcast_generation: 0 })
  })

  test("cleanup ignores Supervisor in the worker quorum and stops it before archiving", async () => {
    insertSupervisor(db)
    const deps = setupDeps(db)

    const result = await executeTeamCleanup(deps, { force: false }, "lead-session")

    expect(result).toContain("cleaned up")
    expect(deps.client.calls.filter(call => call.method === "session.abort")).toEqual([
      { method: "session.abort", args: [{ sessionID: "supervisor-session" }] },
    ])
    expect(db.query("SELECT status FROM team WHERE id = 't1'").get()).toEqual({ status: "archived" })
  })
})

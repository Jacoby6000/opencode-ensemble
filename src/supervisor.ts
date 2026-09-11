import type { Database } from "./db"
import type { MemberRegistry } from "./state"
import type { PluginClient } from "./types"
import { generateId } from "./util"
import { activateIdentityWithPersistence, immediateTransaction, releaseIdentity, terminateMemberScheduling, tryReserveIdentity } from "./scheduler"
import type { SchedulerIdentityLimits } from "./scheduler"
import { invalidateTeamSupervision, rearmTerminalSupervisorReview } from "./supervision-state"
import { log } from "./log"

export const SUPERVISOR_MEMBER_NAME = "__ensemble-supervisor"
export const SUPERVISOR_AGENT = "opencode-ensemble-supervisor"
export const SUPERVISOR_QUIET_MS = 60_000

export interface SupervisionReconcileResult {
  state: "active" | "quiet" | "queued" | "reviewed"
  generation: number
  wakeId?: string
}

/** Capacity and reservation settings used for mandatory Supervisor provisioning. */
export interface SupervisorProvisioningConfig {
  identityLimits: SchedulerIdentityLimits
  reservationTtlMs: number
}

/** Result of provisioning one mandatory Supervisor identity. */
export type SupervisorProvisionResult =
  | { status: "provisioned"; sessionId: string }
  | { status: "already_provisioned"; sessionId: string }
  | { status: "capacity_denied"; reason: "global_capacity" | "agent_capacity" | "identity_exists" }

function reconcileTeamSupervisionState(
  _db: Database,
  _teamId: string,
  _now: number,
  armQuietPeriod: boolean,
): SupervisionReconcileResult {
  _db.exec("BEGIN IMMEDIATE")
  try {
    _db.run("INSERT OR IGNORE INTO team_supervision (team_id) VALUES (?)", [_teamId])
    const state = _db.query("SELECT generation, quiet_since, last_reviewed FROM team_supervision WHERE team_id = ?")
      .get(_teamId) as { generation: number; quiet_since: number | null; last_reviewed: number }
    const workerCount = (_db.query(
      "SELECT COUNT(*) AS count FROM team_member WHERE team_id = ? AND member_kind = 'worker'",
    ).get(_teamId) as { count: number }).count
    if (workerCount === 0) {
      if (state.quiet_since !== null) {
        _db.run("UPDATE team_supervision SET quiet_since = NULL WHERE team_id = ?", [_teamId])
      }
      _db.exec("COMMIT")
      return { state: "active", generation: state.generation }
    }
    const activeWorkers = (_db.query(
      `SELECT COUNT(*) AS count FROM team_member
       WHERE team_id = ? AND member_kind = 'worker'
         AND status NOT IN ('shutdown', 'error') AND (status <> 'ready' OR execution_status <> 'idle')`,
    ).get(_teamId) as { count: number }).count
    const workerWakes = (_db.query(
      `SELECT COUNT(*) AS count FROM scheduler_wake w
       JOIN team_member tm ON tm.team_id = w.team_id AND tm.name = w.member_name
       WHERE w.team_id = ? AND tm.member_kind = 'worker' AND w.state IN ('queued', 'leased')`,
    ).get(_teamId) as { count: number }).count
    const reservations = (_db.query(
      `SELECT COUNT(*) AS count FROM scheduler_identity i
       LEFT JOIN team_member tm ON tm.team_id = i.team_id AND tm.name = i.member_name
       WHERE i.team_id = ? AND i.member_name <> ? AND i.state = 'reserved'
         AND COALESCE(tm.member_kind, 'worker') = 'worker'`,
    ).get(_teamId, SUPERVISOR_MEMBER_NAME) as { count: number }).count

    if (activeWorkers + workerWakes + reservations > 0) {
      const generation = invalidateTeamSupervision(_db, _teamId, _now)
      _db.exec("COMMIT")
      return { state: "active", generation }
    }

    if (state.quiet_since === null) {
      if (!armQuietPeriod) {
        _db.exec("COMMIT")
        return { state: "active", generation: state.generation }
      }
      _db.run("UPDATE team_supervision SET quiet_since = ? WHERE team_id = ?", [_now, _teamId])
      _db.exec("COMMIT")
      return { state: "quiet", generation: state.generation }
    }
    if (_now - state.quiet_since < SUPERVISOR_QUIET_MS) {
      _db.exec("COMMIT")
      return { state: "quiet", generation: state.generation }
    }
    if (state.last_reviewed === state.generation) {
      _db.exec("COMMIT")
      return { state: "reviewed", generation: state.generation }
    }
    const supervisor = _db.query(
      `SELECT session_id, agent FROM team_member
       WHERE team_id = ? AND member_kind = 'supervisor' AND status IN ('ready', 'busy')`,
    ).get(_teamId) as { session_id: string; agent: string } | undefined
    if (!supervisor) {
      _db.exec("COMMIT")
      return { state: "quiet", generation: state.generation }
    }
    const wakeId = generateId("wake")
    _db.run(
      `INSERT INTO scheduler_wake
        (id, team_id, member_name, session_id, agent, reason, coalesce_key, prompt, state,
         not_before, time_created, time_updated, supervision_generation)
       VALUES (?, ?, ?, ?, ?, 'supervisor_review', ?, ?, 'queued', ?, ?, ?, ?)`,
      [wakeId, _teamId, SUPERVISOR_MEMBER_NAME, supervisor.session_id, supervisor.agent,
        `supervisor:${state.generation}`, buildSupervisorReviewPrompt(_db, _teamId, state.generation),
        _now, _now, _now, state.generation],
    )
    _db.run("UPDATE team_supervision SET last_reviewed = ? WHERE team_id = ?", [state.generation, _teamId])
    _db.exec("COMMIT")
    return { state: "queued", generation: state.generation, wakeId }
  } catch (error) {
    _db.exec("ROLLBACK")
    throw error
  }
}

/** Reconcile an already-armed durable worker-quiescence epoch. */
export function reconcileTeamSupervision(_db: Database, _teamId: string, _now = Date.now()): SupervisionReconcileResult {
  return reconcileTeamSupervisionState(_db, _teamId, _now, false)
}

/** Start a quiet epoch at the supplied time only when the team is currently truly quiescent. */
export function armTeamSupervisionIfQuiescent(db: Database, teamId: string, armTime = Date.now()): SupervisionReconcileResult {
  return reconcileTeamSupervisionState(db, teamId, armTime, true)
}

/** Arm a quiet epoch from the actual final ordinary-worker idle or done event. */
export function recordWorkerQuiescenceEvent(
  db: Database,
  teamId: string,
  memberName: string,
  eventTime = Date.now(),
): SupervisionReconcileResult {
  const worker = db.query(
    `SELECT 1 FROM team_member WHERE team_id = ? AND name = ? AND member_kind = 'worker'
     AND (status = 'ready' AND execution_status = 'idle' OR status IN ('shutdown', 'error'))`,
  ).get(teamId, memberName)
  if (!worker) return reconcileTeamSupervisionState(db, teamId, eventTime, false)
  return armTeamSupervisionIfQuiescent(db, teamId, eventTime)
}

/** Build the immutable snapshot supplied to one Supervisor review. */
export function buildSupervisorReviewPrompt(_db: Database, _teamId: string, _generation: number): string {
  const tasks = _db.query(
    `SELECT id, content, status, assignee FROM team_task WHERE team_id = ?
     AND status IN ('pending', 'in_progress', 'blocked') ORDER BY time_updated DESC LIMIT 20`,
  ).all(_teamId) as Array<{ id: string; content: string; status: string; assignee: string | null }>
  const completed = _db.query(
    "SELECT id, content, assignee FROM team_task WHERE team_id = ? AND status = 'completed' ORDER BY time_updated DESC LIMIT 10",
  ).all(_teamId) as Array<{ id: string; content: string; assignee: string | null }>
  const messages = _db.query(
    `SELECT from_name, to_name, content FROM team_message WHERE team_id = ?
     AND from_name <> ? ORDER BY time_created DESC LIMIT 10`,
  ).all(_teamId, SUPERVISOR_MEMBER_NAME) as Array<{ from_name: string; to_name: string | null; content: string }>
  const workers = _db.query(
    `SELECT name, agent, reported_to_lead FROM team_member WHERE team_id = ?
     AND member_kind = 'worker' AND status = 'ready' ORDER BY time_created`,
  ).all(_teamId) as Array<{ name: string; agent: string; reported_to_lead: number }>
  const lines = [
    `Supervisor review generation ${_generation}. Review this immutable coordination snapshot only.`,
    "Outstanding tasks:",
    ...tasks.map(task => `- ${task.id} [${task.status}] ${task.content}${task.assignee ? ` (owner: ${task.assignee})` : " (unowned)"}`),
    "Recently completed work:",
    ...completed.map(task => `- ${task.id} ${task.content}${task.assignee ? ` (by ${task.assignee})` : ""}`),
    "Recent team reports/messages:",
    ...messages.map(message => `- ${message.from_name} -> ${message.to_name ?? "team"}: ${message.content}`),
    "Eligible workers:",
    ...workers.map(worker => `- ${worker.name} (${worker.agent})${worker.reported_to_lead ? " [reported complete]" : ""}`),
    "Do not mutate tasks or call lifecycle tools. You are read-only.",
    "If action is needed, send at most one ownership-explicit team_broadcast that must name specific agents and assign concrete work.",
    "If no action is needed, send nothing.",
    "Escalate lead-only blockers to lead with team_message instead of attempting them.",
  ]
  return lines.join("\n")
}

/** Provision one active team's mandatory Supervisor through durable identity capacity. */
export async function provisionSupervisorForTeam(
  db: Database,
  client: PluginClient,
  registry: MemberRegistry,
  teamId: string,
  config: SupervisorProvisioningConfig,
): Promise<SupervisorProvisionResult> {
  const team = db.query(
    "SELECT id, name, lead_session_id FROM team WHERE id = ? AND status = 'active'",
  ).get(teamId) as { id: string; name: string; lead_session_id: string } | undefined
  if (!team) throw new Error(`Cannot provision Supervisor for inactive team ${teamId}`)

  const existing = db.query(
    "SELECT session_id, status, agent FROM team_member WHERE team_id = ? AND member_kind = 'supervisor'",
  ).get(team.id) as { session_id: string; status: string; agent: string } | undefined
  if (existing && !["shutdown", "error"].includes(existing.status) && existing.agent === SUPERVISOR_AGENT) {
    registry.register(team.id, SUPERVISOR_MEMBER_NAME, existing.session_id)
    return { status: "already_provisioned", sessionId: existing.session_id }
  }

  const stale = registry.getByName(team.id, SUPERVISOR_MEMBER_NAME)
  if (stale) registry.unregister(stale.sessionId)
  if (existing) {
    terminateMemberScheduling(db, team.id, SUPERVISOR_MEMBER_NAME, "terminal Supervisor replacement")
    rearmTerminalSupervisorReview(db, team.id)
    if (!["shutdown", "error"].includes(existing.status)) {
      db.run(
        "UPDATE team_member SET status = 'error', execution_status = 'failed', time_updated = ? WHERE team_id = ? AND name = ?",
        [Date.now(), team.id, SUPERVISOR_MEMBER_NAME],
      )
      // Internal Supervisors never create a worktree or branch, so there is no branch to preserve before abort.
      await client.session.abort({ sessionID: existing.session_id }).catch(() => {})
    }
  }

  const reservation = tryReserveIdentity(db, {
    teamId: team.id,
    memberName: SUPERVISOR_MEMBER_NAME,
    agent: SUPERVISOR_AGENT,
    limits: config.identityLimits,
    reservationTtlMs: config.reservationTtlMs,
    affectsSupervision: false,
    allowExistingTerminalMember: existing !== undefined,
  })
  if (!reservation.reserved) return { status: "capacity_denied", reason: reservation.reason }

  let sessionId: string | undefined
  try {
    const result = await client.session.create({
      parentID: team.lead_session_id,
      title: `Supervisor (${team.name})`,
      agent: SUPERVISOR_AGENT,
      permission: [
        { permission: "*", pattern: "*", action: "deny" },
        { permission: "team_status", pattern: "*", action: "allow" },
        { permission: "team_tasks_list", pattern: "*", action: "allow" },
        { permission: "team_message", pattern: "*", action: "allow" },
        { permission: "team_broadcast", pattern: "*", action: "allow" },
      ],
    })
    sessionId = result.data?.id
    if (!sessionId) throw new Error(`Failed to create Supervisor session for team ${team.name}`)
    const now = Date.now()
    const activated = activateIdentityWithPersistence(db, reservation.reservationId, () => {
      const activeTeam = db.query("SELECT 1 FROM team WHERE id = ? AND status = 'active'").get(team.id)
      if (!activeTeam) throw new Error("Supervisor team is no longer active")
      if (existing) {
        const updated = db.run(
          `UPDATE team_member SET session_id = ?, agent = ?, status = 'ready', execution_status = 'idle',
             reported_to_lead = 0, retry_until = NULL, retry_attempt = NULL, retry_provider = NULL,
             retry_message = NULL, time_updated = ?
           WHERE team_id = ? AND name = ? AND member_kind = 'supervisor' AND status IN ('shutdown', 'error')`,
          [sessionId, SUPERVISOR_AGENT, now, team.id, SUPERVISOR_MEMBER_NAME],
        ).changes
        if (updated !== 1) throw new Error("Terminal Supervisor changed during replacement")
      } else {
        db.run(
          `INSERT INTO team_member
            (team_id, name, session_id, agent, member_kind, status, execution_status, time_created, time_updated)
           VALUES (?, ?, ?, ?, 'supervisor', 'ready', 'idle', ?, ?)`,
          [team.id, SUPERVISOR_MEMBER_NAME, sessionId, SUPERVISOR_AGENT, now, now],
        )
      }
      db.run("INSERT OR IGNORE INTO team_supervision (team_id) VALUES (?)", [team.id])
    }, now)
    if (!activated) throw new Error(`Supervisor identity reservation expired for team ${team.name}`)
  } catch (error) {
    releaseIdentity(db, reservation.reservationId)
    // Internal Supervisors never create a worktree or branch, so there is no branch to preserve before abort.
    if (sessionId) await client.session.abort({ sessionID: sessionId }).catch(() => {})
    throw error
  }

  registry.register(team.id, SUPERVISOR_MEMBER_NAME, sessionId)
  return { status: "provisioned", sessionId }
}

/** Reconcile missing internal Supervisor sessions for active project teams. */
export async function reconcileSupervisors(
  db: Database,
  client: PluginClient,
  registry: MemberRegistry,
  projectId: string,
  config: SupervisorProvisioningConfig,
): Promise<number> {
  const teams = db.query(
    `SELECT t.id, t.name, t.lead_session_id FROM team t WHERE t.project_id = ? AND t.status = 'active'
     AND NOT EXISTS (SELECT 1 FROM team_member tm WHERE tm.team_id = t.id AND tm.member_kind = 'supervisor'
        AND tm.status NOT IN ('shutdown', 'error') AND tm.agent = ?)`,
  ).all(projectId, SUPERVISOR_AGENT) as Array<{ id: string; name: string; lead_session_id: string }>
  let created = 0
  for (const team of teams) {
    try {
      const result = await provisionSupervisorForTeam(db, client, registry, team.id, config)
      if (result.status === "provisioned") created++
    } catch (error) {
      log(`supervisor:provision:failed team=${team.id} err=${error instanceof Error ? error.message : String(error)}`)
    }
  }
  return created
}

/** Verify that a Supervisor session owns the current unexpired review lease. */
export function requireCurrentSupervisorReview(
  _db: Database,
  _sessionId: string,
  _now = Date.now(),
): { teamId: string; generation: number; broadcastGeneration: number | null } {
  const review = _db.query(
    `SELECT l.team_id, w.supervision_generation AS generation, ts.generation AS current_generation,
            ts.last_reviewed, ts.broadcast_generation
     FROM scheduler_run_lease l
     JOIN scheduler_wake w ON w.id = l.wake_id
     JOIN team_member tm ON tm.team_id = l.team_id AND tm.name = l.member_name
     JOIN team_supervision ts ON ts.team_id = l.team_id
     JOIN team t ON t.id = l.team_id
     WHERE l.session_id = ? AND l.state = 'active' AND l.expires_at > ? AND tm.member_kind = 'supervisor'
       AND w.reason = 'supervisor_review' AND t.status = 'active'
     ORDER BY l.acquired_at DESC LIMIT 1`,
  ).get(_sessionId, _now) as {
    team_id: string
    generation: number | null
    current_generation: number
    last_reviewed: number
    broadcast_generation: number | null
  } | undefined
  if (!review || review.generation === null || review.generation !== review.current_generation || review.last_reviewed !== review.generation) {
    throw new Error("Supervisor review is stale or inactive")
  }
  return {
    teamId: review.team_id,
    generation: review.generation,
    broadcastGeneration: review.broadcast_generation,
  }
}

/** Authorize at most one non-stale Supervisor broadcast inside the caller's transaction. */
export function claimSupervisorBroadcastInTransaction(_db: Database, _sessionId: string, _now = Date.now()): { teamId: string; generation: number } {
  const review = requireCurrentSupervisorReview(_db, _sessionId, _now)
  if (review.broadcastGeneration === review.generation) throw new Error("Supervisor already broadcast for this review")
  _db.run("UPDATE team_supervision SET broadcast_generation = ? WHERE team_id = ?", [review.generation, review.teamId])
  return { teamId: review.teamId, generation: review.generation }
}

/** Atomically authorize at most one non-stale Supervisor broadcast per review. */
export function claimSupervisorBroadcast(_db: Database, _sessionId: string, _now = Date.now()): { teamId: string; generation: number } {
  return immediateTransaction(_db, () => claimSupervisorBroadcastInTransaction(_db, _sessionId, _now))
}

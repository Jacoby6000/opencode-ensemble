import type { ResolvedEnsembleConfig } from "./config"
import type { Database } from "./db"
import { log } from "./log"
import { activateIdentityWithPersistence, queueWake, releaseIdentity, terminateMemberScheduling, tryReserveIdentity } from "./scheduler"
import type { MemberRegistry } from "./state"
import type { PluginClient } from "./types"

export const ANNALIST_MEMBER_NAME = "__ensemble-annalist"
export const ANNALIST_AGENT = "Annalist"

/** Result of provisioning one team's hidden Annalist identity. */
export type AnnalistProvisionResult =
  | { status: "provisioned"; sessionId: string }
  | { status: "already_provisioned"; sessionId: string }
  | { status: "capacity_denied"; reason: "global_capacity" | "agent_capacity" | "identity_exists" }

interface PendingTaskAnnal {
  task_id: string
  content: string
  assignee: string | null
  completed_by: string
  time_completed: number
}

/** Persist the one-per-task Annalist event inside the task-completion transaction. */
export function recordTaskAnnalInTransaction(
  db: Database,
  input: { taskId: string; teamId: string; completedBy: string; completedAt: number },
): void {
  db.run(
    "INSERT INTO team_task_annal (task_id, team_id, completed_by, time_completed) VALUES (?, ?, ?, ?)",
    [input.taskId, input.teamId, input.completedBy, input.completedAt],
  )
}

/** Build the task-specific instructions for one Annalist invocation. */
export function buildTaskAnnalPrompt(task: PendingTaskAnnal): string {
  return [
    `Task ${task.task_id} was completed at ${task.time_completed}.`,
    `Task: ${task.content}`,
    `Assignee: ${task.assignee ?? "unassigned"}`,
    `Completed by: ${task.completed_by}`,
    "Preserve any durable decisions, rationale, evidence, and outcomes from this task in the repository annals.",
    "Inspect relevant direct mailboxes and group inboxes with team_results before writing so cross-agent communication is represented.",
    "Use mailbox history non-destructively for the lead, assignee, and named collaborators; list groups and inspect relevant group histories.",
    "Do not mutate team state or send coordination messages. Complete the annal update in this invocation.",
  ].join("\n")
}

/** Queue every recorded task completion that does not have a live or completed Annalist wake. */
export function queuePendingTaskAnnals(db: Database, teamId: string): number {
  const annalist = db.query(
    "SELECT session_id, agent FROM team_member WHERE team_id = ? AND member_kind = 'annalist' AND status IN ('ready', 'busy')",
  ).get(teamId) as { session_id: string; agent: string } | undefined
  if (!annalist) return 0

  const tasks = db.query(
    `SELECT a.task_id, t.content, t.assignee, a.completed_by, a.time_completed
     FROM team_task_annal a
     JOIN team_task t ON t.id = a.task_id AND t.team_id = a.team_id
     LEFT JOIN scheduler_wake w ON w.id = a.wake_id
     WHERE a.team_id = ? AND (a.wake_id IS NULL OR w.state IN ('failed', 'cancelled'))
     ORDER BY a.time_completed, a.task_id`,
  ).all(teamId) as PendingTaskAnnal[]

  tasks.forEach(task => {
    const queued = queueWake(db, {
      teamId,
      memberName: ANNALIST_MEMBER_NAME,
      sessionId: annalist.session_id,
      agent: annalist.agent,
      reason: "task_annal",
      coalesceKey: `task-annal:${task.task_id}`,
      prompt: buildTaskAnnalPrompt(task),
      now: task.time_completed,
    })
    db.run("UPDATE team_task_annal SET wake_id = ? WHERE task_id = ?", [queued.wakeId, task.task_id])
  })
  return tasks.length
}

/** Provision one active team's hidden Annalist through durable identity capacity. */
export async function provisionAnnalistForTeam(
  db: Database,
  client: PluginClient,
  registry: MemberRegistry,
  teamId: string,
  config: ResolvedEnsembleConfig["scheduler"],
): Promise<AnnalistProvisionResult> {
  const team = db.query(
    "SELECT id, name, project_id, lead_session_id FROM team WHERE id = ? AND status = 'active'",
  ).get(teamId) as { id: string; name: string; project_id: string; lead_session_id: string } | undefined
  if (!team) throw new Error(`Cannot provision Annalist for inactive team ${teamId}`)

  const existing = db.query(
    "SELECT session_id, status, agent FROM team_member WHERE team_id = ? AND member_kind = 'annalist'",
  ).get(team.id) as { session_id: string; status: string; agent: string } | undefined
  if (existing && !["shutdown", "error"].includes(existing.status) && existing.agent === ANNALIST_AGENT) {
    registry.register(team.id, ANNALIST_MEMBER_NAME, existing.session_id)
    queuePendingTaskAnnals(db, team.id)
    return { status: "already_provisioned", sessionId: existing.session_id }
  }

  const stale = registry.getByName(team.id, ANNALIST_MEMBER_NAME)
  if (stale) registry.unregister(stale.sessionId)
  if (existing) {
    terminateMemberScheduling(db, team.id, ANNALIST_MEMBER_NAME, "terminal Annalist replacement")
    if (!["shutdown", "error"].includes(existing.status)) {
      db.run(
        "UPDATE team_member SET status = 'error', execution_status = 'failed', time_updated = ? WHERE team_id = ? AND name = ?",
        [Date.now(), team.id, ANNALIST_MEMBER_NAME],
      )
      // Internal Annalists run in the project directory and never own a worktree branch.
      await client.session.abort({ sessionID: existing.session_id }).catch(() => {})
    }
  }

  const reservation = tryReserveIdentity(db, {
    teamId: team.id,
    memberName: ANNALIST_MEMBER_NAME,
    agent: ANNALIST_AGENT,
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
      title: `Annalist (${team.name})`,
      agent: ANNALIST_AGENT,
      directory: team.project_id,
      permission: [{ permission: "team_results", pattern: "*", action: "allow" }],
    })
    sessionId = result.data?.id
    if (!sessionId) throw new Error(`Failed to create Annalist session for team ${team.name}`)
    const now = Date.now()
    const activated = activateIdentityWithPersistence(db, reservation.reservationId, () => {
      const activeTeam = db.query("SELECT 1 FROM team WHERE id = ? AND status = 'active'").get(team.id)
      if (!activeTeam) throw new Error("Annalist team is no longer active")
      if (existing) {
        const updated = db.run(
          `UPDATE team_member SET session_id = ?, agent = ?, status = 'ready', execution_status = 'idle',
             reported_to_lead = 0, retry_until = NULL, retry_attempt = NULL, retry_provider = NULL,
             retry_message = NULL, time_updated = ?
           WHERE team_id = ? AND name = ? AND member_kind = 'annalist' AND status IN ('shutdown', 'error')`,
          [sessionId, ANNALIST_AGENT, now, team.id, ANNALIST_MEMBER_NAME],
        ).changes
        if (updated !== 1) throw new Error("Terminal Annalist changed during replacement")
      } else {
        db.run(
          `INSERT INTO team_member
            (team_id, name, session_id, agent, member_kind, status, execution_status, time_created, time_updated)
           VALUES (?, ?, ?, ?, 'annalist', 'ready', 'idle', ?, ?)`,
          [team.id, ANNALIST_MEMBER_NAME, sessionId, ANNALIST_AGENT, now, now],
        )
      }
    }, now)
    if (!activated) throw new Error(`Annalist identity reservation expired for team ${team.name}`)
  } catch (error) {
    releaseIdentity(db, reservation.reservationId)
    if (sessionId) await client.session.abort({ sessionID: sessionId }).catch(() => {})
    throw error
  }

  registry.register(team.id, ANNALIST_MEMBER_NAME, sessionId)
  queuePendingTaskAnnals(db, team.id)
  return { status: "provisioned", sessionId }
}

/** Reconcile missing internal Annalist sessions for active project teams. */
export async function reconcileAnnalists(
  db: Database,
  client: PluginClient,
  registry: MemberRegistry,
  projectId: string,
  config: ResolvedEnsembleConfig["scheduler"],
): Promise<number> {
  const teams = db.query(
    `SELECT t.id FROM team t WHERE t.project_id = ? AND t.status = 'active'
     AND NOT EXISTS (SELECT 1 FROM team_member tm WHERE tm.team_id = t.id AND tm.member_kind = 'annalist'
        AND tm.status NOT IN ('shutdown', 'error') AND tm.agent = ?)`,
  ).all(projectId, ANNALIST_AGENT) as Array<{ id: string }>
  let created = 0
  for (const team of teams) {
    try {
      const result = await provisionAnnalistForTeam(db, client, registry, team.id, config)
      if (result.status === "provisioned") created++
    } catch (error) {
      log(`annalist:provision:failed team=${team.id} err=${error instanceof Error ? error.message : String(error)}`)
    }
  }
  const activeTeams = db.query(
    "SELECT id FROM team WHERE project_id = ? AND status = 'active'",
  ).all(projectId) as Array<{ id: string }>
  activeTeams.forEach(team => {
    queuePendingTaskAnnals(db, team.id)
  })
  return created
}

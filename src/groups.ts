import type { Database } from "./db"
import type { QueueBroadcastWakesInput } from "./scheduler"
import { immediateTransaction, persistBroadcastWakesInTransaction, refreshMessageDeliveryAggregate } from "./scheduler"
import { generateId, validateTeamName } from "./util"

const RESERVED_GROUP_NAMES = new Set(["lead", "broadcast", "all", "team"])
const MAX_MESSAGE_BYTES = 10 * 1024

/** A durable group as returned to coordination callers and the dashboard. */
export interface TeamGroup {
  id: string
  name: string
  creator: string
  participants: string[]
  createdAt: number
  canSend: boolean
}

/** One immutable group-history entry. */
export interface GroupMessage {
  id: string
  from: string
  content: string
  delivered: boolean
  createdAt: number
}

/** Result of atomically accepting one group message. */
export interface GroupSendResult {
  messageId: string
  timeCreated: number
  group: TeamGroup
  workerRecipientCount: number
  leadRecipient: boolean
  leadSessionId?: string
}

/** One fenced lead-recipient delivery claimed for non-blocking dispatch. */
export interface ClaimedGroupLeadDelivery {
  messageId: string
  teamId: string
  claimToken: string
  leadSessionId: string
  groupName: string
  fromName: string
  content: string
}

/** Validate a public group name using the repository's lowercase-hyphen convention. */
export function validateGroupName(name: string): string | undefined {
  const baseError = validateTeamName(name)
  if (baseError) return baseError.replace("Team name", "Group name")
  if (RESERVED_GROUP_NAMES.has(name)) return `Group name "${name}" is reserved`
  return undefined
}

function requireOrdinaryCaller(db: Database, teamId: string, caller: string): void {
  if (caller === "lead") return
  const worker = db.query(
    "SELECT 1 FROM team_member WHERE team_id = ? AND name = ? AND member_kind = 'worker' AND status IN ('ready', 'busy')",
  ).get(teamId, caller)
  if (!worker) throw new Error("Group inboxes are available only to the lead and active ordinary workers")
}

function requireHistoryObserver(db: Database, teamId: string, caller: string): void {
  if (caller === "lead") return
  const observer = db.query(
    "SELECT 1 FROM team_member WHERE team_id = ? AND name = ? AND member_kind IN ('worker', 'annalist') AND status IN ('ready', 'busy')",
  ).get(teamId, caller)
  if (!observer) throw new Error("Group inboxes are available only to the lead, active workers, and the Annalist")
}

function loadGroup(db: Database, teamId: string, name: string, caller: string): TeamGroup | undefined {
  const row = db.query(
    "SELECT id, name, created_by, time_created FROM team_group WHERE team_id = ? AND name = ? AND sealed = 1",
  ).get(teamId, name) as { id: string; name: string; created_by: string; time_created: number } | undefined
  if (!row) return undefined
  const participants = db.query(
    "SELECT participant_name FROM team_group_participant WHERE team_id = ? AND group_id = ? ORDER BY participant_name",
  ).all(teamId, row.id) as Array<{ participant_name: string }>
  const names = participants.map(participant => participant.participant_name)
  return { id: row.id, name: row.name, creator: row.created_by, participants: names, createdAt: row.time_created, canSend: names.includes(caller) }
}

/** List every group in a team without changing any mailbox or scheduler state. */
export function listTeamGroups(db: Database, teamId: string, caller: string): TeamGroup[] {
  requireHistoryObserver(db, teamId, caller)
  const names = db.query("SELECT name FROM team_group WHERE team_id = ? AND sealed = 1 ORDER BY time_created ASC, name ASC").all(teamId) as Array<{ name: string }>
  return names.flatMap(row => {
    const group = loadGroup(db, teamId, row.name, caller)
    return group ? [group] : []
  })
}

/** Read deterministic newest-first group history without changing delivery or read state. */
export function inspectTeamGroup(db: Database, teamId: string, caller: string, name: string, limit = 20): { group: TeamGroup; messages: GroupMessage[] } {
  requireHistoryObserver(db, teamId, caller)
  if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw new Error("Group history limit must be an integer from 1 to 50")
  const group = loadGroup(db, teamId, name, caller)
  if (!group) throw new Error(`Group "${name}" not found`)
  const rows = db.query(
    "SELECT id, from_name, content, delivered, time_created FROM team_message WHERE team_id = ? AND group_id = ? ORDER BY time_created DESC, id DESC LIMIT ?",
  ).all(teamId, group.id, limit) as Array<{ id: string; from_name: string; content: string; delivered: number; time_created: number }>
  return {
    group,
    messages: rows.map(row => ({ id: row.id, from: row.from_name, content: row.content, delivered: row.delivered === 1, createdAt: row.time_created })),
  }
}

/** Create a group with its first message, or send to an existing group, in one immediate transaction. */
export function sendGroupMessage(
  db: Database,
  input: { teamId: string; sender: string; group: string; members?: readonly string[]; content: string; now?: number },
): GroupSendResult {
  const nameError = validateGroupName(input.group)
  if (nameError) throw new Error(nameError)
  if (new TextEncoder().encode(input.content).length > MAX_MESSAGE_BYTES) throw new Error("Message content exceeds 10KB limit")
  const now = input.now ?? Date.now()
  return immediateTransaction(db, () => {
    const team = db.query("SELECT lead_session_id FROM team WHERE id = ? AND status = 'active'").get(input.teamId) as { lead_session_id: string } | undefined
    if (!team) throw new Error("Team is not active")
    requireOrdinaryCaller(db, input.teamId, input.sender)
    let group = loadGroup(db, input.teamId, input.group, input.sender)
    if (group && input.members) throw new Error(`Group "${input.group}" already exists; membership is immutable`)
    if (!group) {
      if (!input.members) throw new Error(`Group "${input.group}" not found`)
      const members = [...new Set(input.members)]
      if (members.length !== input.members.length) throw new Error("Group participants must be unique")
      if (members.length < 2) throw new Error("Group creation requires at least two unique participants")
      if (!members.includes(input.sender)) throw new Error("Group creator must be included in members")
      const collision = db.query("SELECT 1 FROM team_member WHERE team_id = ? AND name = ?").get(input.teamId, input.group)
      if (collision) throw new Error(`Group name "${input.group}" collides with a worker name`)
      const invalid = members.find(member => member !== "lead" && !db.query(
        "SELECT 1 FROM team_member WHERE team_id = ? AND name = ? AND member_kind = 'worker' AND status IN ('ready', 'busy')",
      ).get(input.teamId, member))
      if (invalid) throw new Error(`Group participant "${invalid}" is not an active worker in this team`)
      const groupId = generateId("group")
      db.run("INSERT INTO team_group (id, team_id, name, created_by, time_created) VALUES (?, ?, ?, ?, ?)", [groupId, input.teamId, input.group, input.sender, now])
      members.forEach(member => {
        db.run(
          "INSERT INTO team_group_participant (team_id, group_id, participant_name, time_created) VALUES (?, ?, ?, ?)",
          [input.teamId, groupId, member, now],
        )
      })
      db.run("UPDATE team_group SET sealed = 1 WHERE id = ?", [groupId])
      group = loadGroup(db, input.teamId, input.group, input.sender)
    }
    if (!group?.canSend) throw new Error(`Only participants may send to group "${input.group}"`)
    const workers = db.query(
      `SELECT m.name, m.session_id, m.agent FROM team_group_participant p
       JOIN team_member m ON m.team_id = p.team_id AND m.name = p.participant_name
       WHERE p.team_id = ? AND p.group_id = ? AND m.member_kind = 'worker'
         AND m.name <> ? AND m.status IN ('ready', 'busy') AND m.reported_to_lead = 0
       ORDER BY m.time_created ASC`,
    ).all(input.teamId, group.id, input.sender) as Array<{ name: string; session_id: string; agent: string }>
    const leadRecipient = input.sender !== "lead" && group.participants.includes("lead")
    if (workers.length === 0 && !leadRecipient) throw new Error("Group send requires at least one currently eligible recipient other than the sender")
    const messageId = generateId("msg")
    const wakeInput: QueueBroadcastWakesInput = {
      messageId,
      teamId: input.teamId,
      fromName: input.sender,
      groupId: group.id,
      content: input.content,
      now,
      groupRecipients: [
        ...workers.map(worker => ({ recipientName: worker.name, recipientKind: "worker" as const })),
        ...(leadRecipient ? [{ recipientName: "lead", recipientKind: "lead" as const }] : []),
      ],
      recipients: workers.map(worker => ({
        memberName: worker.name,
        sessionId: worker.session_id,
        agent: worker.agent,
        reason: "group",
        coalesceKey: `member:${worker.name}`,
      })),
    }
    persistBroadcastWakesInTransaction(db, wakeInput)
    return {
      messageId,
      timeCreated: now,
      group,
      workerRecipientCount: workers.length,
      leadRecipient,
      ...(leadRecipient ? { leadSessionId: team.lead_session_id } : {}),
    }
  })
}

/** Claim queued group deliveries to participating leads, fenced by a unique token. */
export function claimGroupLeadDeliveries(
  db: Database,
  projectId: string | undefined,
  claimTtlMs: number,
  limit = 50,
  now = Date.now(),
): ClaimedGroupLeadDelivery[] {
  if (!Number.isSafeInteger(claimTtlMs) || claimTtlMs <= 0) throw new Error("Lead delivery claim TTL must be positive")
  return immediateTransaction(db, () => {
    const stale = (projectId
      ? db.query(
        `SELECT r.message_id FROM team_group_message_recipient r
         JOIN team_message m ON m.id = r.message_id JOIN team t ON t.id = m.team_id
         WHERE r.recipient_kind = 'lead' AND r.delivery_state = 'claimed' AND r.claimed_at <= ? AND t.project_id = ?`,
      ).all(now - claimTtlMs, projectId)
      : db.query(
        "SELECT message_id FROM team_group_message_recipient WHERE recipient_kind = 'lead' AND delivery_state = 'claimed' AND claimed_at <= ?",
      ).all(now - claimTtlMs)) as Array<{ message_id: string }>
    stale.forEach(row => {
      db.run(
        "UPDATE team_group_message_recipient SET delivery_state = 'queued', claim_token = NULL, claimed_at = NULL, not_before = ?, last_error = 'delivery claim expired', time_updated = ? WHERE message_id = ? AND recipient_name = 'lead' AND delivery_state = 'claimed'",
        [now, now, row.message_id],
      )
      refreshMessageDeliveryAggregate(db, row.message_id)
    })
    const rows = (projectId
      ? db.query(
        `SELECT r.message_id, m.team_id, t.lead_session_id, g.name AS group_name, m.from_name, m.content
         FROM team_group_message_recipient r JOIN team_message m ON m.id = r.message_id
         JOIN team t ON t.id = m.team_id JOIN team_group g ON g.id = m.group_id
         WHERE r.recipient_kind = 'lead' AND r.delivery_state = 'queued' AND r.not_before <= ?
           AND t.status = 'active' AND t.project_id = ?
         ORDER BY r.time_created ASC, r.message_id ASC LIMIT ?`,
      ).all(now, projectId, limit)
      : db.query(
        `SELECT r.message_id, m.team_id, t.lead_session_id, g.name AS group_name, m.from_name, m.content
         FROM team_group_message_recipient r JOIN team_message m ON m.id = r.message_id
         JOIN team t ON t.id = m.team_id JOIN team_group g ON g.id = m.group_id
         WHERE r.recipient_kind = 'lead' AND r.delivery_state = 'queued' AND r.not_before <= ?
           AND t.status = 'active' ORDER BY r.time_created ASC, r.message_id ASC LIMIT ?`,
      ).all(now, limit)) as Array<{ message_id: string; team_id: string; lead_session_id: string; group_name: string; from_name: string; content: string }>
    return rows.flatMap(row => {
      const claimToken = generateId("group_lead_claim")
      const updated = db.run(
        `UPDATE team_group_message_recipient SET delivery_state = 'claimed', claim_token = ?, claimed_at = ?,
           attempt_count = attempt_count + 1, last_error = NULL, time_updated = ?
         WHERE message_id = ? AND recipient_name = 'lead' AND delivery_state = 'queued' AND not_before <= ?`,
        [claimToken, now, now, row.message_id, now],
      ).changes
      if (updated !== 1) return []
      refreshMessageDeliveryAggregate(db, row.message_id)
      return [{
        messageId: row.message_id,
        teamId: row.team_id,
        claimToken,
        leadSessionId: row.lead_session_id,
        groupName: row.group_name,
        fromName: row.from_name,
        content: row.content,
      }]
    })
  })
}

/** Mark one fenced lead delivery as accepted without touching other recipients directly. */
export function markGroupLeadDeliveryInjected(db: Database, messageId: string, claimToken: string, now = Date.now()): boolean {
  return immediateTransaction(db, () => {
    const updated = db.run(
      `UPDATE team_group_message_recipient SET delivery_state = 'injected', claim_token = NULL,
         claimed_at = NULL, last_error = NULL, time_updated = ?
       WHERE message_id = ? AND recipient_name = 'lead' AND delivery_state = 'claimed' AND claim_token = ?`,
      [now, messageId, claimToken],
    ).changes
    if (updated === 1) refreshMessageDeliveryAggregate(db, messageId)
    return updated === 1
  })
}

/** Requeue one rejected fenced lead delivery for maintenance retry. */
export function requeueGroupLeadDelivery(
  db: Database,
  messageId: string,
  claimToken: string,
  error: string,
  backoffMs: number,
  now = Date.now(),
): boolean {
  return immediateTransaction(db, () => {
    const updated = db.run(
      `UPDATE team_group_message_recipient SET delivery_state = 'queued', claim_token = NULL,
         claimed_at = NULL, not_before = ?, last_error = ?, time_updated = ?
       WHERE message_id = ? AND recipient_name = 'lead' AND delivery_state = 'claimed' AND claim_token = ?`,
      [now + Math.max(0, backoffMs), error, now, messageId, claimToken],
    ).changes
    if (updated === 1) refreshMessageDeliveryAggregate(db, messageId)
    return updated === 1
  })
}

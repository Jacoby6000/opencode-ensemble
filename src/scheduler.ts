import type { Database } from "./db"
import { generateId } from "./util"
import { invalidateTeamSupervision, rearmTerminalSupervisorReview } from "./supervision-state"

const MAX_MESSAGE_BYTES = 10 * 1024

/** Teammate run limits. Missing per-agent entries are unlimited. */
export interface SchedulerRunLimits {
  global: number
  perAgent: Readonly<Record<string, number>>
}

/** Teammate identity limits. Missing per-agent entries are unlimited. */
export interface SchedulerIdentityLimits {
  global: number
  perAgent: Readonly<Record<string, number>>
}

/** Identity reservation input used before external session provisioning. */
export interface ReserveIdentityInput {
  teamId: string
  memberName: string
  agent: string
  limits: SchedulerIdentityLimits
  reservationTtlMs: number
  /** Whether this ordinary-worker reservation invalidates current supervision. */
  affectsSupervision?: boolean
  /** Allow reservation while replacing an existing terminal member row in place. */
  allowExistingTerminalMember?: boolean
  now?: number
}

/** Result of attempting to reserve teammate identity capacity. */
export type ReserveIdentityResult =
  | { reserved: true; reservationId: string }
  | { reserved: false; reason: "global_capacity" | "agent_capacity" | "identity_exists" }

/** Durable wake request input. */
export interface QueueWakeInput {
  teamId: string
  memberName: string
  sessionId: string
  agent: string
  reason: string
  coalesceKey: string
  prompt?: string
  messageId?: string
  notBefore?: number
  now?: number
}

/** Message and wake input persisted together without a crash window. */
export interface QueueMessageWakeInput extends Omit<QueueWakeInput, "messageId"> {
  messageId: string
  fromName: string
  toName: string | null
  content: string
}

/** Broadcast input whose complete recipient set is registered atomically. */
export interface QueueBroadcastWakesInput {
  messageId: string
  teamId: string
  fromName: string
  content: string
  recipients: ReadonlyArray<Omit<QueueWakeInput, "teamId" | "messageId" | "now">>
  /** Group destination ID; omitted for a whole-team broadcast. */
  groupId?: string
  /** Complete durable recipient set for a group message. */
  groupRecipients?: ReadonlyArray<{ recipientName: string; recipientKind: "worker" | "lead" }>
  now?: number
}

/** Result of queueing or coalescing a wake request. */
export interface QueueWakeResult {
  wakeId: string
  coalesced: boolean
}

/** Result of attempting to acquire teammate run capacity. */
export type AcquireRunResult =
  | { acquired: true; leaseId: string }
  | { acquired: false; reason: "global_capacity" | "agent_capacity" | "member_active" | "wake_unavailable" }

/** Durable queued wake returned to the scheduler pump. */
export interface ReadyWake {
  id: string
  teamId: string
  memberName: string
  sessionId: string
  agent: string
  reason: string
  attemptCount: number
}

/** Prompt and linked mailbox content needed to dispatch one wake. */
export interface WakePayload {
  prompt: string | null
  messages: Array<{ id: string; fromName: string; content: string; groupName?: string }>
}

/** Active or fenced run correlated to an external teammate session. */
export interface SchedulerRun {
  leaseId: string
  wakeId: string
  teamId: string
  memberName: string
  sessionId: string
  state: "active" | "expired"
  injectedAt: number | null
  expiresAt: number
}

/** Load the durable payload for one wake. */
export function getWakePayload(_db: Database, _wakeId: string): WakePayload | undefined {
  const wake = _db.query("SELECT prompt FROM scheduler_wake WHERE id = ?").get(_wakeId) as { prompt: string | null } | undefined
  if (!wake) return undefined
  const messages = _db.query(
    "SELECT m.id, m.from_name, m.content, g.name AS group_name FROM team_message m JOIN scheduler_message_wake mw ON mw.message_id = m.id LEFT JOIN team_group g ON g.id = m.group_id WHERE mw.wake_id = ? ORDER BY m.time_created ASC, m.id ASC",
  ).all(_wakeId) as Array<{ id: string; from_name: string; content: string; group_name: string | null }>
  return {
    prompt: wake.prompt,
    messages: messages.map(message => ({
      id: message.id,
      fromName: message.from_name,
      content: message.content,
      ...(message.group_name ? { groupName: message.group_name } : {}),
    })),
  }
}

/** Find the current active or fenced run for an external session. */
export function findRunBySession(_db: Database, _sessionId: string, projectId?: string): SchedulerRun | undefined {
  const run = (projectId
    ? _db.query(
      "SELECT l.id, l.wake_id, l.team_id, l.member_name, l.session_id, l.state, l.injected_at, l.expires_at FROM scheduler_run_lease l JOIN team t ON t.id = l.team_id WHERE l.session_id = ? AND t.project_id = ? AND l.state IN ('active', 'expired') ORDER BY l.acquired_at DESC LIMIT 1",
    ).get(_sessionId, projectId)
    : _db.query(
      "SELECT id, wake_id, team_id, member_name, session_id, state, injected_at, expires_at FROM scheduler_run_lease WHERE session_id = ? AND state IN ('active', 'expired') ORDER BY acquired_at DESC LIMIT 1",
    ).get(_sessionId)) as { id: string; wake_id: string; team_id: string; member_name: string; session_id: string; state: "active" | "expired"; injected_at: number | null; expires_at: number } | undefined
  if (!run) return undefined
  return {
    leaseId: run.id,
    wakeId: run.wake_id,
    teamId: run.team_id,
    memberName: run.member_name,
    sessionId: run.session_id,
    state: run.state,
    injectedAt: run.injected_at,
    expiresAt: run.expires_at,
  }
}

/** Release an active lease and return its wake to the queue after a dispatch rejection. */
export function requeueRun(_db: Database, _leaseId: string, _error: string, _backoffMs: number, _now = Date.now()): boolean {
  if (!Number.isSafeInteger(_backoffMs) || _backoffMs < 0) throw new Error("Run backoff must be a non-negative safe integer")
  return immediateTransaction(_db, () => {
    const lease = _db.query(
      "SELECT wake_id, team_id, member_name FROM scheduler_run_lease WHERE id = ? AND state = 'active'",
    ).get(_leaseId) as { wake_id: string; team_id: string; member_name: string } | undefined
    if (!lease) return false
    _db.run("UPDATE scheduler_run_lease SET state = 'released', released_at = ? WHERE id = ? AND state = 'active'", [_now, _leaseId])
    _db.run("UPDATE scheduler_wake SET state = 'queued', not_before = ?, last_error = ?, time_updated = ? WHERE id = ? AND state = 'leased'", [_now + _backoffMs, _error, _now, lease.wake_id])
    setWakeMessageState(_db, lease.wake_id, "wake_queued")
    recordEvent(_db, { teamId: lease.team_id, memberName: lease.member_name, wakeId: lease.wake_id, leaseId: _leaseId, type: "run_requeued", detail: _error, now: _now })
    return true
  })
}

/** Terminalize all durable scheduling state owned by one teammate. */
export function terminateMemberScheduling(_db: Database, _teamId: string, _memberName: string, _reason: string, _now = Date.now()): boolean {
  return immediateTransaction(_db, () => {
    const wakes = _db.query(
      "SELECT id FROM scheduler_wake WHERE team_id = ? AND member_name = ? AND state IN ('queued', 'leased')",
    ).all(_teamId, _memberName) as Array<{ id: string }>
    wakes.forEach(wake => {
      setWakeMessageState(_db, wake.id, "failed")
    })
    const leases = _db.run(
      "UPDATE scheduler_run_lease SET state = 'failed', released_at = ? WHERE team_id = ? AND member_name = ? AND state IN ('active', 'expired')",
      [_now, _teamId, _memberName],
    ).changes
    const wakeChanges = _db.run(
      "UPDATE scheduler_wake SET state = 'failed', last_error = ?, time_updated = ? WHERE team_id = ? AND member_name = ? AND state IN ('queued', 'leased')",
      [_reason, _now, _teamId, _memberName],
    ).changes
    const identities = _db.run(
      "UPDATE scheduler_identity SET state = 'released', expires_at = NULL, released_at = ? WHERE team_id = ? AND member_name = ? AND state IN ('reserved', 'active')",
      [_now, _teamId, _memberName],
    ).changes
    rearmTerminalSupervisorReview(_db, _teamId)
    if (leases + wakeChanges + identities === 0) return false
    recordEvent(_db, { teamId: _teamId, memberName: _memberName, type: "member_scheduler_terminated", detail: _reason, now: _now })
    return true
  })
}

interface WakeRow {
  id: string
  team_id: string
  member_name: string
  session_id: string
  agent: string
  state: string
  not_before: number
}

interface ExpiredLeaseRow {
  id: string
  wake_id: string
  team_id: string
  member_name: string
}

/** List queued wakes whose delay has elapsed, oldest first. */
export function listReadyWakes(db: Database, now = Date.now(), limit = 50, projectId?: string): ReadyWake[] {
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new Error("Ready wake limit must be an integer from 1 to 100")
  const rows = (projectId
    ? db.query(
      "SELECT w.id, w.team_id, w.member_name, w.session_id, w.agent, w.reason, w.attempt_count FROM scheduler_wake w JOIN team t ON t.id = w.team_id WHERE w.state = 'queued' AND w.not_before <= ? AND t.project_id = ? ORDER BY w.time_created ASC, w.id ASC LIMIT ?",
    ).all(now, projectId, limit)
    : db.query(
      "SELECT id, team_id, member_name, session_id, agent, reason, attempt_count FROM scheduler_wake WHERE state = 'queued' AND not_before <= ? ORDER BY time_created ASC, id ASC LIMIT ?",
    ).all(now, limit)) as Array<{ id: string; team_id: string; member_name: string; session_id: string; agent: string; reason: string; attempt_count: number }>
  return rows.map(row => ({
    id: row.id,
    teamId: row.team_id,
    memberName: row.member_name,
    sessionId: row.session_id,
    agent: row.agent,
    reason: row.reason,
    attemptCount: row.attempt_count,
  }))
}

/** Mark all messages linked to a leased wake as injected. */
export function markRunInjected(db: Database, leaseId: string, now = Date.now()): boolean {
  return immediateTransaction(db, () => {
    const lease = db.query(
      "SELECT wake_id, team_id, member_name FROM scheduler_run_lease WHERE id = ? AND state = 'active' AND expires_at > ?",
    ).get(leaseId, now) as { wake_id: string; team_id: string; member_name: string } | undefined
    if (!lease) return false
    db.run("UPDATE scheduler_run_lease SET injected_at = COALESCE(injected_at, ?) WHERE id = ? AND state = 'active'", [now, leaseId])
    setWakeMessageState(db, lease.wake_id, "injected")
    recordEvent(db, { teamId: lease.team_id, memberName: lease.member_name, wakeId: lease.wake_id, leaseId, type: "run_injected", now })
    return true
  })
}

/** Release a run lease and complete or fail its wake and linked messages. */
export function finishRun(db: Database, leaseId: string, outcome: "processed" | "failed", error?: string, now = Date.now(), projectId?: string): boolean {
  return immediateTransaction(db, () => {
    expireStaleRunsInTransaction(db, now, projectId)
    const lease = db.query(
      "SELECT wake_id, team_id, member_name FROM scheduler_run_lease WHERE id = ? AND state = 'active' AND expires_at > ?",
    ).get(leaseId, now) as { wake_id: string; team_id: string; member_name: string } | undefined
    if (!lease) return false

    const leaseState = outcome === "processed" ? "released" : "failed"
    const wakeState = outcome === "processed" ? "completed" : "failed"
    db.run("UPDATE scheduler_run_lease SET state = ?, released_at = ? WHERE id = ? AND state = 'active'", [leaseState, now, leaseId])
    db.run("UPDATE scheduler_wake SET state = ?, last_error = ?, time_updated = ? WHERE id = ? AND state = 'leased'", [wakeState, error ?? null, now, lease.wake_id])
    setWakeMessageState(db, lease.wake_id, outcome)
    if (outcome === "failed") rearmTerminalSupervisorReview(db, lease.team_id)
    recordEvent(db, {
      teamId: lease.team_id,
      memberName: lease.member_name,
      wakeId: lease.wake_id,
      leaseId,
      type: outcome === "processed" ? "run_processed" : "run_failed",
      detail: error,
      now,
    })
    return true
  })
}

/** Fence stale run leases pending external-session reconciliation. */
export function expireStaleRuns(db: Database, now = Date.now(), projectId?: string): number {
  return immediateTransaction(db, () => expireStaleRunsInTransaction(db, now, projectId))
}

/** Persist a message and its wake request in one immediate transaction. */
export function queueMessageWake(db: Database, input: QueueMessageWakeInput): QueueWakeResult {
  if (new TextEncoder().encode(input.content).length > MAX_MESSAGE_BYTES) throw new Error("Message content exceeds 10KB limit")
  const now = input.now ?? Date.now()
  return immediateTransaction(db, () => {
    db.run(
      "INSERT INTO team_message (id, team_id, from_name, to_name, content, delivered, read, delivery_state, time_created) VALUES (?, ?, ?, ?, ?, 0, 0, 'queued', ?)",
      [input.messageId, input.teamId, input.fromName, input.toName, input.content, now],
    )
    return queueWakeInTransaction(db, { ...input, messageId: input.messageId, now })
  })
}

/** Persist a broadcast and all recipient wakes in one immediate transaction. */
export function queueBroadcastWakes(db: Database, input: QueueBroadcastWakesInput): QueueWakeResult[] {
  if (input.recipients.length === 0) throw new Error("Broadcasts require at least one recipient")
  return immediateTransaction(db, () => persistBroadcastWakesInTransaction(db, input))
}

/** Persist a broadcast and zero or more recipient wakes inside the caller's transaction. */
export function persistBroadcastWakesInTransaction(db: Database, input: QueueBroadcastWakesInput): QueueWakeResult[] {
  if (new TextEncoder().encode(input.content).length > MAX_MESSAGE_BYTES) throw new Error("Message content exceeds 10KB limit")
  if (new Set(input.recipients.map(recipient => recipient.memberName)).size !== input.recipients.length) {
    throw new Error("Broadcast recipients must be unique")
  }
  const now = input.now ?? Date.now()
  db.run(
    "INSERT INTO team_message (id, team_id, from_name, to_name, group_id, content, delivered, read, delivery_state, time_created) VALUES (?, ?, ?, NULL, ?, ?, 0, 0, 'queued', ?)",
    [input.messageId, input.teamId, input.fromName, input.groupId ?? null, input.content, now],
  )
  if (input.groupId) {
    if (!input.groupRecipients || input.groupRecipients.length === 0) throw new Error("Group messages require durable recipients")
    if (new Set(input.groupRecipients.map(recipient => recipient.recipientName)).size !== input.groupRecipients.length) {
      throw new Error("Group recipients must be unique")
    }
    input.groupRecipients.forEach(recipient => {
      db.run(
        `INSERT INTO team_group_message_recipient
          (message_id, team_id, recipient_name, recipient_kind, delivery_state, not_before, attempt_count, time_created, time_updated)
         VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?)`,
        [input.messageId, input.teamId, recipient.recipientName, recipient.recipientKind, now, now, now],
      )
    })
  } else if (input.groupRecipients) {
    throw new Error("Whole-team broadcasts cannot register group recipients")
  }
  const wakes = input.recipients.map(recipient => queueWakeInTransaction(db, {
    ...recipient,
    teamId: input.teamId,
    messageId: input.messageId,
    now,
  }, true))
  if (input.groupId) refreshMessageDeliveryAggregate(db, input.messageId)
  return wakes
}

/** Renew an owned active lease before its expiry. */
export function renewRunLease(db: Database, leaseId: string, leaseTtlMs: number, now = Date.now(), projectId?: string): boolean {
  if (!Number.isFinite(leaseTtlMs) || leaseTtlMs <= 0) throw new Error("Lease TTL must be positive")
  return immediateTransaction(db, () => {
    const lease = db.query(
      "SELECT wake_id, team_id, member_name FROM scheduler_run_lease WHERE id = ? AND state = 'active' AND expires_at > ?",
    ).get(leaseId, now) as { wake_id: string; team_id: string; member_name: string } | undefined
    if (!lease) {
      expireStaleRunsInTransaction(db, now, projectId)
      return false
    }
    db.run("UPDATE scheduler_run_lease SET expires_at = ? WHERE id = ? AND state = 'active'", [now + leaseTtlMs, leaseId])
    recordEvent(db, { teamId: lease.team_id, memberName: lease.member_name, wakeId: lease.wake_id, leaseId, type: "lease_renewed", now })
    return true
  })
}

/** Resolve a fenced expired lease after checking the external session. */
export function reconcileExpiredRun(
  db: Database,
  leaseId: string,
  outcome: "requeue" | "processed" | "failed",
  error?: string,
  now = Date.now(),
  projectId?: string,
): boolean {
  return immediateTransaction(db, () => {
    expireStaleRunsInTransaction(db, now, projectId)
    const lease = db.query(
      "SELECT l.wake_id, l.team_id, l.member_name, w.coalesce_key FROM scheduler_run_lease l JOIN scheduler_wake w ON w.id = l.wake_id WHERE l.id = ? AND l.state = 'expired'",
    ).get(leaseId) as { wake_id: string; team_id: string; member_name: string; coalesce_key: string } | undefined
    if (!lease) return false

    const leaseState = outcome === "failed" ? "failed" : "released"
    const messageState = outcome === "requeue" ? "wake_queued" : outcome
    db.run("UPDATE scheduler_run_lease SET state = ?, released_at = ? WHERE id = ? AND state = 'expired'", [leaseState, now, leaseId])
    setWakeMessageState(db, lease.wake_id, messageState)
    if (outcome === "requeue") {
      const successor = db.query(
        "SELECT id FROM scheduler_wake WHERE team_id = ? AND member_name = ? AND coalesce_key = ? AND state = 'queued' AND id <> ? LIMIT 1",
      ).get(lease.team_id, lease.member_name, lease.coalesce_key, lease.wake_id) as { id: string } | undefined
      if (successor) {
        db.run("INSERT OR IGNORE INTO scheduler_message_wake (message_id, wake_id, time_created) SELECT message_id, ?, ? FROM scheduler_message_wake WHERE wake_id = ?", [successor.id, now, lease.wake_id])
        db.run(
          `UPDATE scheduler_wake SET prompt = CASE
             WHEN (SELECT prompt FROM scheduler_wake WHERE id = ?) IS NULL THEN prompt
             WHEN prompt IS NULL THEN (SELECT prompt FROM scheduler_wake WHERE id = ?)
             ELSE (SELECT prompt FROM scheduler_wake WHERE id = ?) || '\n\n' || prompt
           END, time_updated = ? WHERE id = ?`,
          [lease.wake_id, lease.wake_id, lease.wake_id, now, successor.id],
        )
        db.run("DELETE FROM scheduler_message_wake WHERE wake_id = ?", [lease.wake_id])
        db.run("UPDATE scheduler_wake SET state = 'cancelled', time_updated = ? WHERE id = ? AND state = 'leased'", [now, lease.wake_id])
        rearmTerminalSupervisorReview(db, lease.team_id)
      } else {
        db.run("UPDATE scheduler_wake SET state = 'queued', time_updated = ? WHERE id = ? AND state = 'leased'", [now, lease.wake_id])
      }
    } else {
      const wakeState = outcome === "processed" ? "completed" : "failed"
      db.run("UPDATE scheduler_wake SET state = ?, last_error = ?, time_updated = ? WHERE id = ? AND state = 'leased'", [wakeState, error ?? null, now, lease.wake_id])
      if (outcome === "failed") rearmTerminalSupervisorReview(db, lease.team_id)
    }
    recordEvent(db, { teamId: lease.team_id, memberName: lease.member_name, wakeId: lease.wake_id, leaseId, type: `expired_run_${outcome}`, detail: error, now })
    return true
  })
}

function expireStaleRunsInTransaction(db: Database, now: number, projectId?: string): number {
  const expired = (projectId
    ? db.query(
      "SELECT l.id, l.wake_id, l.team_id, l.member_name FROM scheduler_run_lease l JOIN team t ON t.id = l.team_id WHERE l.state = 'active' AND l.expires_at <= ? AND t.project_id = ?",
    ).all(now, projectId)
    : db.query(
      "SELECT id, wake_id, team_id, member_name FROM scheduler_run_lease WHERE state = 'active' AND expires_at <= ?",
    ).all(now)) as ExpiredLeaseRow[]
  expired.forEach(lease => {
    db.run("UPDATE scheduler_run_lease SET state = 'expired' WHERE id = ? AND state = 'active'", [lease.id])
    recordEvent(db, { teamId: lease.team_id, memberName: lease.member_name, wakeId: lease.wake_id, leaseId: lease.id, type: "lease_expired", now })
  })
  return expired.length
}

/** Atomically reserve identity capacity before provisioning external resources. */
export function tryReserveIdentity(db: Database, input: ReserveIdentityInput): ReserveIdentityResult {
  if (!input.teamId || !input.memberName || !input.agent) throw new Error("Identity reservations require team, member, and agent")
  if (!Number.isFinite(input.reservationTtlMs) || input.reservationTtlMs <= 0) throw new Error("Identity reservation TTL must be positive")
  const now = input.now ?? Date.now()

  return immediateTransaction(db, () => {
    const team = db.query("SELECT id FROM team WHERE id = ? AND status = 'active'").get(input.teamId)
    if (!team) throw new Error(`Identity reservations require an active team: ${input.teamId}`)
    const expired = db.query(
      "SELECT id, team_id, member_name FROM scheduler_identity WHERE state = 'reserved' AND expires_at <= ?",
    ).all(now) as Array<{ id: string; team_id: string; member_name: string }>
    expired.forEach(identity => {
      db.run("UPDATE scheduler_identity SET state = 'expired', released_at = ? WHERE id = ? AND state = 'reserved'", [now, identity.id])
      recordEvent(db, { teamId: identity.team_id, memberName: identity.member_name, type: "identity_reservation_expired", detail: identity.id, now })
    })

    const member = db.query("SELECT status FROM team_member WHERE team_id = ? AND name = ? LIMIT 1")
      .get(input.teamId, input.memberName) as { status: string } | undefined
    if (member && !(input.allowExistingTerminalMember && ["shutdown", "error"].includes(member.status))) {
      return { reserved: false, reason: "identity_exists" }
    }
    const existing = db.query(
      "SELECT id FROM scheduler_identity WHERE team_id = ? AND member_name = ? AND state IN ('reserved', 'active') LIMIT 1",
    ).get(input.teamId, input.memberName)
    if (existing) return { reserved: false, reason: "identity_exists" }

    const globalCount = db.query("SELECT COUNT(*) AS count FROM scheduler_identity WHERE state IN ('reserved', 'active')").get() as { count: number }
    if (globalCount.count >= input.limits.global) return { reserved: false, reason: "global_capacity" }

    const agentLimit = input.limits.perAgent[input.agent]
    if (agentLimit !== undefined) {
      const agentCount = db.query("SELECT COUNT(*) AS count FROM scheduler_identity WHERE state IN ('reserved', 'active') AND agent = ?").get(input.agent) as { count: number }
      if (agentCount.count >= agentLimit) return { reserved: false, reason: "agent_capacity" }
    }

    const reservationId = generateId("identity")
    db.run(
      "INSERT INTO scheduler_identity (id, team_id, member_name, agent, state, reserved_at, expires_at) VALUES (?, ?, ?, ?, 'reserved', ?, ?)",
      [reservationId, input.teamId, input.memberName, input.agent, now, now + input.reservationTtlMs],
    )
    if (input.affectsSupervision !== false) invalidateTeamSupervision(db, input.teamId, now)
    recordEvent(db, { teamId: input.teamId, memberName: input.memberName, type: "identity_reserved", detail: reservationId, now })
    return { reserved: true, reservationId }
  })
}

/** Mark a reserved identity as active after the member is persisted. */
export function activateIdentity(db: Database, reservationId: string, now = Date.now()): boolean {
  return immediateTransaction(db, () => activateIdentityInTransaction(db, reservationId, now))
}

/** Atomically activate a persisted teammate identity and queue its initial wake. */
export function activateIdentityAndQueue(_db: Database, _reservationId: string, _wake: QueueWakeInput): QueueWakeResult | undefined {
  const now = _wake.now ?? Date.now()
  return immediateTransaction(_db, () => {
    if (!activateIdentityInTransaction(_db, _reservationId, now)) return undefined
    return queueWakeInTransaction(_db, { ..._wake, now })
  })
}

/** Atomically persist a member and activate its previously reserved identity. */
export function activateIdentityWithPersistence(
  db: Database,
  reservationId: string,
  persistMember: () => void,
  now = Date.now(),
): boolean {
  return immediateTransaction(db, () => {
    const identity = db.query(
      "SELECT team_id, member_name, expires_at FROM scheduler_identity WHERE id = ? AND state = 'reserved'",
    ).get(reservationId) as { team_id: string; member_name: string; expires_at: number } | undefined
    if (!identity) return false
    if (identity.expires_at <= now) {
      db.run("UPDATE scheduler_identity SET state = 'expired', released_at = ? WHERE id = ? AND state = 'reserved'", [now, reservationId])
      recordEvent(db, { teamId: identity.team_id, memberName: identity.member_name, type: "identity_reservation_expired", detail: reservationId, now })
      return false
    }
    persistMember()
    if (!activateIdentityInTransaction(db, reservationId, now)) {
      throw new Error(`Persisted member does not match identity reservation ${reservationId}`)
    }
    return true
  })
}

function activateIdentityInTransaction(db: Database, reservationId: string, now: number): boolean {
  const identity = db.query("SELECT team_id, member_name, agent, expires_at FROM scheduler_identity WHERE id = ? AND state = 'reserved'").get(reservationId) as { team_id: string; member_name: string; agent: string; expires_at: number } | undefined
  if (!identity) return false
  if (identity.expires_at <= now) {
    db.run("UPDATE scheduler_identity SET state = 'expired', released_at = ? WHERE id = ? AND state = 'reserved'", [now, reservationId])
    recordEvent(db, { teamId: identity.team_id, memberName: identity.member_name, type: "identity_reservation_expired", detail: reservationId, now })
    return false
  }
  const member = getSchedulableMember(db, identity.team_id, identity.member_name)
  if (!member || member.agent !== identity.agent) return false
  db.run("UPDATE scheduler_identity SET state = 'active', expires_at = NULL, activated_at = ? WHERE id = ? AND state = 'reserved'", [now, reservationId])
  recordEvent(db, { teamId: identity.team_id, memberName: identity.member_name, type: "identity_activated", detail: reservationId, now })
  return true
}

/** Release reserved or active identity capacity. */
export function releaseIdentity(db: Database, reservationId: string, now = Date.now()): boolean {
  return immediateTransaction(db, () => {
    const identity = db.query("SELECT team_id, member_name FROM scheduler_identity WHERE id = ? AND state IN ('reserved', 'active')").get(reservationId) as { team_id: string; member_name: string } | undefined
    if (!identity) return false
    db.run("UPDATE scheduler_identity SET state = 'released', expires_at = NULL, released_at = ? WHERE id = ? AND state IN ('reserved', 'active')", [now, reservationId])
    recordEvent(db, { teamId: identity.team_id, memberName: identity.member_name, type: "identity_released", detail: reservationId, now })
    return true
  })
}

/** Execute one synchronous operation inside a SQLite BEGIN IMMEDIATE transaction. */
export function immediateTransaction<T>(db: Database, operation: () => T): T {
  db.exec("BEGIN IMMEDIATE")
  try {
    const result = operation()
    db.exec("COMMIT")
    return result
  } catch (err) {
    db.exec("ROLLBACK")
    throw err
  }
}

function recordEvent(
  db: Database,
  input: { teamId: string; memberName?: string; wakeId?: string; leaseId?: string; type: string; detail?: string; now: number },
): void {
  db.run(
    "INSERT INTO scheduler_event (id, team_id, member_name, wake_id, lease_id, type, detail, time_created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    [generateId("event"), input.teamId, input.memberName ?? null, input.wakeId ?? null, input.leaseId ?? null, input.type, input.detail ?? null, input.now],
  )
}

function getSchedulableMember(db: Database, teamId: string, memberName: string): { session_id: string; agent: string; member_kind: string } | undefined {
  return db.query(
    "SELECT tm.session_id, tm.agent, tm.member_kind FROM team_member tm JOIN team t ON t.id = tm.team_id WHERE tm.team_id = ? AND tm.name = ? AND t.status = 'active' AND tm.status IN ('ready', 'busy')",
  ).get(teamId, memberName) as { session_id: string; agent: string; member_kind: string } | undefined
}

/**
 * Recompute the coarse team_message delivery fields from durable group-recipient state.
 * Pending work wins and reports `wake_queued`; otherwise any partial success reports
 * `injected`, all-recipient processed success reports `processed`, and no success reports
 * `failed`. `delivered` means at least one recipient was injected or processed.
 */
export function refreshMessageDeliveryAggregate(db: Database, messageId: string): void {
  const message = db.query("SELECT group_id FROM team_message WHERE id = ?").get(messageId) as { group_id: string | null } | undefined
  if (!message?.group_id) return
  const aggregate = db.query(
    `SELECT COUNT(*) AS total,
       SUM(CASE WHEN delivery_state IN ('queued', 'wake_queued', 'claimed') THEN 1 ELSE 0 END) AS pending,
       SUM(CASE WHEN delivery_state IN ('injected', 'processed') THEN 1 ELSE 0 END) AS successful,
       SUM(CASE WHEN delivery_state = 'processed' THEN 1 ELSE 0 END) AS processed
     FROM team_group_message_recipient WHERE message_id = ?`,
  ).get(messageId) as { total: number; pending: number; successful: number; processed: number }
  if (aggregate.total === 0) throw new Error(`Group message ${messageId} has no durable recipients`)
  const deliveryState = aggregate.pending > 0
    ? "wake_queued"
    : aggregate.processed === aggregate.total
      ? "processed"
      : aggregate.successful > 0
        ? "injected"
        : "failed"
  db.run("UPDATE team_message SET delivered = ?, delivery_state = ? WHERE id = ?", [aggregate.successful > 0 ? 1 : 0, deliveryState, messageId])
}

function setWakeMessageState(db: Database, wakeId: string, state: "wake_queued" | "injected" | "processed" | "failed"): void {
  const messageIds = db.query("SELECT message_id FROM scheduler_message_wake WHERE wake_id = ?").all(wakeId) as Array<{ message_id: string }>
  if (messageIds.length === 0) return
  db.run("UPDATE scheduler_message_wake SET delivery_state = ? WHERE wake_id = ?", [state, wakeId])
  messageIds.forEach(({ message_id: messageId }) => {
    const groupRecipient = db.run(
      `UPDATE team_group_message_recipient SET delivery_state = ?, claim_token = NULL, claimed_at = NULL,
         last_error = CASE WHEN ? = 'failed' THEN last_error ELSE NULL END, time_updated = ?
       WHERE message_id = ? AND recipient_name = (SELECT member_name FROM scheduler_wake WHERE id = ?)`,
      [state, state, Date.now(), messageId, wakeId],
    ).changes
    if (groupRecipient > 0) {
      refreshMessageDeliveryAggregate(db, messageId)
      return
    }
    const aggregate = db.query(
      `SELECT
         COUNT(*) AS total,
         SUM(CASE WHEN delivery_state = 'processed' THEN 1 ELSE 0 END) AS processed,
         SUM(CASE WHEN delivery_state = 'wake_queued' THEN 1 ELSE 0 END) AS queued,
         SUM(CASE WHEN delivery_state = 'injected' THEN 1 ELSE 0 END) AS injected
       FROM scheduler_message_wake WHERE message_id = ?`,
    ).get(messageId) as { total: number; processed: number; queued: number; injected: number }
    const deliveryState = aggregate.processed === aggregate.total
      ? "processed"
      : aggregate.queued > 0
        ? "wake_queued"
        : aggregate.injected > 0
          ? "injected"
          : "failed"
    const delivered = aggregate.processed > 0 || aggregate.injected > 0 ? 1 : 0
    db.run("UPDATE team_message SET delivered = CASE WHEN ? = 1 THEN 1 ELSE delivered END, delivery_state = ? WHERE id = ?", [delivered, deliveryState, messageId])
  })
}

/** Persist a wake request, coalescing duplicate queued work. */
export function queueWake(db: Database, input: QueueWakeInput): QueueWakeResult {
  const now = input.now ?? Date.now()
  return immediateTransaction(db, () => queueWakeInTransaction(db, { ...input, now }))
}

function queueWakeInTransaction(db: Database, input: QueueWakeInput & { now: number }, allowBroadcast = false): QueueWakeResult {
  if (!input.teamId || !input.memberName || !input.sessionId || !input.agent || !input.reason || !input.coalesceKey) {
    throw new Error("Wake requests require team, member, session, agent, reason, and coalesce key")
  }
  const now = input.now
  const member = getSchedulableMember(db, input.teamId, input.memberName)
  if (!member) throw new Error(`Member ${input.teamId}/${input.memberName} is not schedulable`)
  if (member.session_id !== input.sessionId || member.agent !== input.agent) {
    throw new Error(`Wake routing does not match member ${input.teamId}/${input.memberName}`)
  }
  if (member.member_kind === "worker") invalidateTeamSupervision(db, input.teamId, now)
  const existing = db.query(
    "SELECT id FROM scheduler_wake WHERE team_id = ? AND member_name = ? AND coalesce_key = ? AND state = 'queued' ORDER BY time_created ASC LIMIT 1",
  ).get(input.teamId, input.memberName, input.coalesceKey) as { id: string } | undefined

  if (existing) {
    db.run("UPDATE scheduler_wake SET session_id = ?, agent = ?, prompt = COALESCE(prompt, ?), time_updated = ?, not_before = MIN(not_before, ?) WHERE id = ?", [input.sessionId, input.agent, input.prompt ?? null, now, input.notBefore ?? now, existing.id])
    if (input.messageId) {
      linkMessageToWake(db, input, existing.id, now, allowBroadcast)
    }
    recordEvent(db, { teamId: input.teamId, memberName: input.memberName, wakeId: existing.id, type: "wake_coalesced", now })
    return { wakeId: existing.id, coalesced: true }
  }

  const wakeId = generateId("wake")
  db.run(
    "INSERT INTO scheduler_wake (id, team_id, member_name, session_id, agent, reason, coalesce_key, prompt, state, not_before, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)",
    [wakeId, input.teamId, input.memberName, input.sessionId, input.agent, input.reason, input.coalesceKey, input.prompt ?? null, input.notBefore ?? now, now, now],
  )
  if (input.messageId) linkMessageToWake(db, input, wakeId, now, allowBroadcast)
  recordEvent(db, { teamId: input.teamId, memberName: input.memberName, wakeId, type: "wake_queued", now })
  return { wakeId, coalesced: false }
}

function linkMessageToWake(db: Database, input: QueueWakeInput, wakeId: string, now: number, allowBroadcast: boolean): void {
  if (!input.messageId) return
  const message = db.query("SELECT team_id, to_name, group_id, delivery_state FROM team_message WHERE id = ?").get(input.messageId) as { team_id: string; to_name: string | null; group_id: string | null; delivery_state: string } | undefined
  if (!message) throw new Error(`Message not found: ${input.messageId}`)
  if (message.team_id !== input.teamId || (message.to_name !== null && message.to_name !== input.memberName)) {
    throw new Error(`Message ${input.messageId} does not target ${input.teamId}/${input.memberName}`)
  }
  if (message.to_name === null && !allowBroadcast) throw new Error("Multi-recipient messages must use queueBroadcastWakes")
  if (message.delivery_state === "processed" || message.delivery_state === "failed") {
    throw new Error(`Message ${input.messageId} is already terminal`)
  }
  const links = db.query(
    "SELECT mw.wake_id, w.member_name FROM scheduler_message_wake mw JOIN scheduler_wake w ON w.id = mw.wake_id WHERE mw.message_id = ?",
  ).all(input.messageId) as Array<{ wake_id: string; member_name: string }>
  if (links.some(link => link.wake_id !== wakeId && (message.to_name !== null || link.member_name === input.memberName))) {
    throw new Error(`Message ${input.messageId} is already linked for ${input.memberName}`)
  }
  db.run("INSERT OR IGNORE INTO scheduler_message_wake (message_id, wake_id, delivery_state, time_created) VALUES (?, ?, 'wake_queued', ?)", [input.messageId, wakeId, now])
  setWakeMessageState(db, wakeId, "wake_queued")
}

/** Atomically acquire capacity and lease a queued wake. */
export function tryAcquireRun(
  db: Database,
  wakeId: string,
  limits: SchedulerRunLimits,
  leaseTtlMs: number,
  now = Date.now(),
  projectId?: string,
): AcquireRunResult {
  if (!Number.isFinite(leaseTtlMs) || leaseTtlMs <= 0) throw new Error("Lease TTL must be positive")

  return immediateTransaction(db, () => {
    expireStaleRunsInTransaction(db, now, projectId)

    const wake = db.query(
      "SELECT id, team_id, member_name, session_id, agent, state, not_before FROM scheduler_wake WHERE id = ?",
    ).get(wakeId) as WakeRow | undefined
    if (!wake || wake.state !== "queued" || wake.not_before > now) return { acquired: false, reason: "wake_unavailable" }

    const member = getSchedulableMember(db, wake.team_id, wake.member_name)
    if (!member || member.session_id !== wake.session_id || member.agent !== wake.agent) {
      db.run("UPDATE scheduler_wake SET state = 'cancelled', last_error = 'member routing changed', time_updated = ? WHERE id = ? AND state = 'queued'", [now, wake.id])
      setWakeMessageState(db, wake.id, "failed")
      rearmTerminalSupervisorReview(db, wake.team_id)
      recordEvent(db, { teamId: wake.team_id, memberName: wake.member_name, wakeId: wake.id, type: "wake_cancelled", detail: "member routing changed", now })
      return { acquired: false, reason: "wake_unavailable" }
    }

    const memberLease = db.query(
      "SELECT id FROM scheduler_run_lease WHERE team_id = ? AND member_name = ? AND state IN ('active', 'expired') LIMIT 1",
    ).get(wake.team_id, wake.member_name)
    if (memberLease) return { acquired: false, reason: "member_active" }

    const globalCount = db.query("SELECT COUNT(*) AS count FROM scheduler_run_lease WHERE state IN ('active', 'expired')").get() as { count: number }
    if (globalCount.count >= limits.global) return { acquired: false, reason: "global_capacity" }

    const agentLimit = limits.perAgent[wake.agent]
    if (agentLimit !== undefined) {
      const agentCount = db.query("SELECT COUNT(*) AS count FROM scheduler_run_lease WHERE state IN ('active', 'expired') AND agent = ?").get(wake.agent) as { count: number }
      if (agentCount.count >= agentLimit) return { acquired: false, reason: "agent_capacity" }
    }

    const leaseId = generateId("lease")
    db.run(
      "INSERT INTO scheduler_run_lease (id, wake_id, team_id, member_name, session_id, agent, state, acquired_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)",
      [leaseId, wake.id, wake.team_id, wake.member_name, wake.session_id, wake.agent, now, now + leaseTtlMs],
    )
    db.run("UPDATE scheduler_wake SET state = 'leased', attempt_count = attempt_count + 1, time_updated = ? WHERE id = ?", [now, wake.id])
    recordEvent(db, { teamId: wake.team_id, memberName: wake.member_name, wakeId: wake.id, leaseId, type: "lease_acquired", now })
    return { acquired: true, leaseId }
  })
}

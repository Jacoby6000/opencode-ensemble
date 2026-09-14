import { createServer } from "node:http"
import type { IncomingMessage, Server, ServerResponse } from "node:http"
import type { Database } from "./db"
import { DASHBOARD_HEAD } from "./dashboard-html"
import { DASHBOARD_JS_CORE } from "./dashboard-js-core"
import { DASHBOARD_JS_EVENTS } from "./dashboard-js-events"
import { DASHBOARD_JS_RENDER } from "./dashboard-js-render"
import { log } from "./log"
import type { ActivityBuffer, ActivityEntry } from "./activity"
import type { PluginClient } from "./types"
import { isDashboardAuthorized } from "./dashboard-auth"
import { queueBroadcastWakes, queueMessageWake } from "./scheduler"
import type { SchedulerController } from "./scheduler-runtime"
import { SUPERVISOR_MEMBER_NAME } from "./supervisor"
import { generateId } from "./util"
import { listTeamGroups, sendGroupMessage } from "./groups"

/** Loopback address used by the dashboard listener and singleton probe. */
export const DASHBOARD_HOST = "127.0.0.1"

const SECURITY_HEADERS = {
  "Cache-Control": "no-store",
  "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
  "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
  "Referrer-Policy": "no-referrer",
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
} as const

/** Assemble the full dashboard HTML from parts. */
const DASHBOARD_HTML = `${DASHBOARD_HEAD}\n<script>${DASHBOARD_JS_CORE}${DASHBOARD_JS_RENDER}${DASHBOARD_JS_EVENTS}</script>\n</body></html>`

interface TeamRow {
  id: string
  name: string
  project_id: string
  lead_session_id: string
  status: string
  lead_agent: string | null
  time_created: number
  time_updated: number
}

interface ProjectRow {
  id: string
  name: string
  path: string
  status: string
  time_created: number
  time_updated: number
}

interface MemberRow {
  name: string
  agent: string
  status: string
  execution_status: string
  session_id: string
  worktree_branch: string | null
  has_prompt: number
  model: string | null
  plan_approval: string
  time_created: number
  time_updated: number
  last_nudged_at: number | null
  retry_until: number | null
  retry_attempt: number | null
  retry_provider: string | null
  retry_message: string | null
}

interface TaskRow {
  id: string
  content: string
  status: string
  priority: string
  assignee: string | null
  depends_on: string | null
  time_created: number
  time_updated: number
}

interface MessageRow {
  id: string
  from_name: string
  to_name: string | null
  group_id: string | null
  group_name: string | null
  content: string
  delivered: number
  read: number
  time_created: number
}

interface MessageSummaryRow {
  id: string
  from_name: string
  to_name: string | null
  group_id: string | null
  group_name: string | null
  content_preview: string
  content_length: number
  delivered: number
  read: number
  time_created: number
}

interface SchedulerCountsRow {
  active_identities: number
  reserved_identities: number
  queued_wakes: number
  leased_wakes: number
  active_runs: number
  expired_runs: number
}

interface SchedulerEventRow {
  member_name: string | null
  event_type: string
  time_created: number
}

function parseDependsOn(value: string | null): string[] {
  if (!value) return []

  try {
    const parsed: unknown = JSON.parse(value)

    if (Array.isArray(parsed)) return parsed.filter((item): item is string => typeof item === "string")
    if (typeof parsed === "string") return [parsed]
  } catch {
    return [value]
  }

  return []
}

/**
 * Bare-integer version of the `/api/state` response shape. Bump on any
 * breaking change to the payload (field removed/renamed/retyped, not
 * additive fields). Consumers outside this repo compare this field with
 * strict equality — no semver ranges. Currently consumed externally by the
 * OpenCode sidebar TUI plugin that renders Team status from this endpoint.
 */
export const ENSEMBLE_STATE_VERSION = 2

/**
 * `/api/state` response shape. Exported and versioned because at least one
 * consumer outside this repo (an OpenCode sidebar TUI plugin) polls this
 * endpoint and depends on its shape. Treat additive-only changes as safe;
 * bump {@link ENSEMBLE_STATE_VERSION} for anything else.
 */
export interface EnsembleDashboardState {
  version: number
  projects: unknown[]
  teams: unknown[]
}

function buildState(db: Database): EnsembleDashboardState {
  const projects = db.query("SELECT id, name, path, status, time_created, time_updated FROM project ORDER BY time_updated DESC").all() as ProjectRow[]
  const teams = db.query("SELECT id, name, project_id, lead_session_id, status, lead_agent, time_created, time_updated FROM team ORDER BY time_created DESC").all() as TeamRow[]
  const memberStmt = db.query("SELECT name, agent, status, execution_status, session_id, worktree_branch, CASE WHEN prompt IS NOT NULL AND prompt <> '' THEN 1 ELSE 0 END AS has_prompt, model, plan_approval, time_created, time_updated, last_nudged_at, retry_until, retry_attempt, retry_provider, retry_message FROM team_member WHERE team_id = ? AND member_kind = 'worker'")
  const taskStmt = db.query("SELECT id, content, status, priority, assignee, depends_on, time_created, time_updated FROM team_task WHERE team_id = ?")
  const msgStmt = db.query("SELECT m.id, m.from_name, m.to_name, m.group_id, g.name AS group_name, substr(m.content, 1, 160) AS content_preview, length(m.content) AS content_length, m.delivered, m.read, m.time_created FROM team_message m LEFT JOIN team_group g ON g.id = m.group_id WHERE m.team_id = ? ORDER BY m.time_created DESC, m.id DESC LIMIT 50")
  const schedulerCountsStmt = db.query(`SELECT
    (SELECT COUNT(*) FROM scheduler_identity WHERE team_id = ? AND member_name <> ? AND state = 'active') AS active_identities,
    (SELECT COUNT(*) FROM scheduler_identity WHERE team_id = ? AND member_name <> ? AND state = 'reserved') AS reserved_identities,
    (SELECT COUNT(*) FROM scheduler_wake WHERE team_id = ? AND member_name <> ? AND state = 'queued') AS queued_wakes,
    (SELECT COUNT(*) FROM scheduler_wake WHERE team_id = ? AND member_name <> ? AND state = 'leased') AS leased_wakes,
    (SELECT COUNT(*) FROM scheduler_run_lease WHERE team_id = ? AND member_name <> ? AND state = 'active') AS active_runs,
    (SELECT COUNT(*) FROM scheduler_run_lease WHERE team_id = ? AND member_name <> ? AND state = 'expired') AS expired_runs`)
  const schedulerEventsStmt = db.query(
    "SELECT member_name, type AS event_type, time_created FROM scheduler_event WHERE team_id = ? AND (member_name IS NULL OR member_name <> ?) ORDER BY time_created DESC, id DESC LIMIT 20",
  )

  const mappedTeams = teams.map((t) => {
    const scheduler = schedulerCountsStmt.get(
      t.id, SUPERVISOR_MEMBER_NAME,
      t.id, SUPERVISOR_MEMBER_NAME,
      t.id, SUPERVISOR_MEMBER_NAME,
      t.id, SUPERVISOR_MEMBER_NAME,
      t.id, SUPERVISOR_MEMBER_NAME,
      t.id, SUPERVISOR_MEMBER_NAME,
    ) as SchedulerCountsRow
    const members = (memberStmt.all(t.id) as MemberRow[]).map((m) => ({
      name: m.name,
      agent: m.agent,
      status: m.status,
      executionStatus: m.execution_status,
      sessionId: m.session_id,
      worktreeBranch: m.worktree_branch,
      hasPrompt: m.has_prompt === 1,
      model: m.model,
      planApproval: m.plan_approval,
      timeCreated: m.time_created,
      timeUpdated: m.time_updated,
      lastNudgedAt: m.last_nudged_at,
      // Fix 4: derived, read-time TTL boolean (Fix 3) — never a stored enum.
      // Additive fields; existing consumers that don't know about them simply
      // don't render them.
      isRetrying: m.retry_until !== null && m.retry_until > Date.now(),
      retryUntil: m.retry_until,
      retryAttempt: m.retry_attempt,
      retryProvider: m.retry_provider,
      retryMessage: m.retry_message,
    }))
    return {
      id: t.id,
      name: t.name,
      projectId: t.project_id,
      leadSessionId: t.lead_session_id,
      status: t.status,
      leadAgent: t.lead_agent,
      timeCreated: t.time_created,
      timeUpdated: t.time_updated,
      members,
      tasks: (taskStmt.all(t.id) as TaskRow[]).map((tk) => ({
        id: tk.id,
        content: tk.content,
        status: tk.status,
        priority: tk.priority,
        assignee: tk.assignee,
        dependsOn: parseDependsOn(tk.depends_on),
        timeCreated: tk.time_created,
        timeUpdated: tk.time_updated,
      })),
      groups: listTeamGroups(db, t.id, "lead"),
      messages: (msgStmt.all(t.id) as MessageSummaryRow[]).map((msg) => ({
        id: msg.id,
        fromName: msg.from_name,
        toName: msg.to_name,
        groupId: msg.group_id,
        groupName: msg.group_name,
        preview: msg.content_preview,
        contentLength: msg.content_length,
        delivered: msg.delivered === 1,
        read: msg.read === 1,
        timeCreated: msg.time_created,
      })),
      scheduler: {
        activeIdentities: scheduler.active_identities,
        reservedIdentities: scheduler.reserved_identities,
        queuedWakes: scheduler.queued_wakes,
        leasedWakes: scheduler.leased_wakes,
        activeRuns: scheduler.active_runs,
        expiredRuns: scheduler.expired_runs,
        recentEvents: (schedulerEventsStmt.all(t.id, SUPERVISOR_MEMBER_NAME) as SchedulerEventRow[]).map(event => ({
          memberName: event.member_name,
          type: event.event_type,
          timeCreated: event.time_created,
        })),
      },
    }
  })

  const teamsByProject = new Map<string, unknown[]>()
  mappedTeams.forEach(team => {
    const projectId = (team as { projectId: string }).projectId
    teamsByProject.set(projectId, [...(teamsByProject.get(projectId) ?? []), team])
  })

  return {
    version: ENSEMBLE_STATE_VERSION,
    projects: projects.flatMap(project => {
      const projectTeams = teamsByProject.get(project.id) ?? []
      if (projectTeams.length === 0) return []
      return {
        id: project.id,
        name: project.name,
        path: project.path,
        status: project.status,
        timeCreated: project.time_created,
        timeUpdated: project.time_updated,
        activeTeams: projectTeams.filter(team => (team as { status: string }).status === "active").length,
        workingAgents: projectTeams.reduce<number>((count, team) => {
          const members = (team as { members: Array<{ status: string }> }).members
          return count + members.filter(member => member.status === "busy").length
        }, 0),
        teams: projectTeams,
      }
    }),
    teams: mappedTeams,
  }
}

interface MessageCursor {
  timeCreated: number
  id: string
}

function encodeMessageCursor(cursor: MessageCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url")
}

function decodeMessageCursor(value: string): MessageCursor | null {
  try {
    const parsed: unknown = JSON.parse(Buffer.from(value, "base64url").toString("utf8"))
    if (typeof parsed !== "object" || parsed === null) return null
    const cursor = parsed as { timeCreated?: unknown; id?: unknown }
    if (typeof cursor.timeCreated !== "number" || !Number.isFinite(cursor.timeCreated) || typeof cursor.id !== "string" || !cursor.id) return null
    return { timeCreated: cursor.timeCreated, id: cursor.id }
  } catch {
    return null
  }
}

function mapMessage(row: MessageRow) {
  return {
    id: row.id,
    fromName: row.from_name,
    toName: row.to_name,
    groupId: row.group_id,
    groupName: row.group_name,
    content: row.content,
    delivered: row.delivered === 1,
    read: row.read === 1,
    timeCreated: row.time_created,
  }
}

function handleMessagesRoute(db: Database, teamId: string, url: URL, res: ServerResponse): void {
  const limitValue = url.searchParams.get("limit")
  const limit = limitValue === null ? 20 : Number(limitValue)
  if (!Number.isInteger(limit) || limit < 1 || limit > 50 || (limitValue !== null && !/^\d+$/.test(limitValue))) {
    sendJson(res, { error: "limit must be an integer from 1 to 50" }, 400)
    return
  }

  const team = db.query("SELECT id FROM team WHERE id = ?").get(teamId)
  if (!team) {
    sendJson(res, { error: "Team not found" }, 404)
    return
  }

  const cursorValue = url.searchParams.get("cursor")
  const cursor = cursorValue === null ? null : decodeMessageCursor(cursorValue)
  if (cursorValue !== null && !cursor) {
    sendJson(res, { error: "Invalid message cursor" }, 400)
    return
  }

  const channel = url.searchParams.get("channel")
  const channelType = url.searchParams.get("channelType")
  const broadcastChannel = channelType === "broadcast"
  const requestedGroup = url.searchParams.get("group")
  const groupChannel = channelType === "group" ? requestedGroup : null
  if ((channel && channelType) || (requestedGroup && channelType !== "group") || (channelType === "group" && !groupChannel) || (channelType && channelType !== "broadcast" && channelType !== "group")) {
    sendJson(res, { error: "Choose exactly one valid member, broadcast, or group channel" }, 400)
    return
  }
  if (channel) {
    const member = db.query("SELECT 1 FROM team_member WHERE team_id = ? AND name = ? AND member_kind = 'worker'").get(teamId, channel)
    if (!member) {
      sendJson(res, { error: "Team member not found" }, 404)
      return
    }
  }
  let groupId: string | null = null
  if (groupChannel) {
    const group = db.query("SELECT id FROM team_group WHERE team_id = ? AND name = ? AND sealed = 1").get(teamId, groupChannel) as { id: string } | undefined
    if (!group) {
      sendJson(res, { error: "Team group not found" }, 404)
      return
    }
    groupId = group.id
  }

  const select = "SELECT m.id, m.from_name, m.to_name, m.group_id, g.name AS group_name, m.content, m.delivered, m.read, m.time_created FROM team_message m LEFT JOIN team_group g ON g.id = m.group_id"
  const rows = (groupId
    ? cursor
      ? db.query(`${select} WHERE m.team_id = ? AND m.group_id = ? AND (m.time_created < ? OR (m.time_created = ? AND m.id < ?)) ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, groupId, cursor.timeCreated, cursor.timeCreated, cursor.id, limit + 1)
      : db.query(`${select} WHERE m.team_id = ? AND m.group_id = ? ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, groupId, limit + 1)
    : broadcastChannel
    ? cursor
      ? db.query(`${select} WHERE m.team_id = ? AND m.group_id IS NULL AND m.to_name IS NULL AND (m.time_created < ? OR (m.time_created = ? AND m.id < ?)) ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, cursor.timeCreated, cursor.timeCreated, cursor.id, limit + 1)
      : db.query(`${select} WHERE m.team_id = ? AND m.group_id IS NULL AND m.to_name IS NULL ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, limit + 1)
    : channel
      ? cursor
        ? db.query(`${select} WHERE m.team_id = ? AND m.group_id IS NULL AND m.to_name IS NOT NULL AND (m.from_name = ? OR m.to_name = ?) AND (m.time_created < ? OR (m.time_created = ? AND m.id < ?)) ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, channel, channel, cursor.timeCreated, cursor.timeCreated, cursor.id, limit + 1)
        : db.query(`${select} WHERE m.team_id = ? AND m.group_id IS NULL AND m.to_name IS NOT NULL AND (m.from_name = ? OR m.to_name = ?) ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, channel, channel, limit + 1)
      : cursor
        ? db.query(`${select} WHERE m.team_id = ? AND (m.time_created < ? OR (m.time_created = ? AND m.id < ?)) ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, cursor.timeCreated, cursor.timeCreated, cursor.id, limit + 1)
        : db.query(`${select} WHERE m.team_id = ? ORDER BY m.time_created DESC, m.id DESC LIMIT ?`).all(teamId, limit + 1)) as MessageRow[]

  const hasNextPage = rows.length > limit
  const page = hasNextPage ? rows.slice(0, limit) : rows
  const last = page.at(-1)
  sendJson(res, {
    messages: page.map(mapMessage),
    nextCursor: hasNextPage && last ? encodeMessageCursor({ timeCreated: last.time_created, id: last.id }) : null,
  })
}

const MAX_DASHBOARD_MESSAGE_BYTES = 10 * 1024
const MAX_DASHBOARD_REQUEST_BYTES = MAX_DASHBOARD_MESSAGE_BYTES + 1024

async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const contentType = req.headers["content-type"] ?? ""
  if (!contentType.toLowerCase().startsWith("application/json")) throw new Error("unsupported_media_type")
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    size += buffer.length
    if (size > MAX_DASHBOARD_REQUEST_BYTES) throw new Error("body_too_large")
    chunks.push(buffer)
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown
  } catch {
    throw new Error("malformed_json")
  }
}

async function handleSendMessageRoute(
  db: Database,
  teamId: string,
  req: IncomingMessage,
  res: ServerResponse,
  scheduler?: SchedulerController,
): Promise<void> {
  let body: unknown
  try {
    body = await readJsonBody(req)
  } catch (error) {
    const reason = error instanceof Error ? error.message : "malformed_json"
    if (reason === "unsupported_media_type") {
      sendJson(res, { error: "Content-Type must be application/json" }, 415)
      return
    }
    if (reason === "body_too_large") {
      sendJson(res, { error: "Message content exceeds 10KB limit" }, 413)
      return
    }
    sendJson(res, { error: "Malformed JSON body" }, 400)
    return
  }
  if (typeof body !== "object" || body === null) {
    sendJson(res, { error: "Request body must be an object" }, 400)
    return
  }
  const input = body as { to?: unknown; group?: unknown; content?: unknown }
  if (typeof input.content !== "string" || input.content.trim().length === 0) {
    sendJson(res, { error: "content must be a non-empty string" }, 400)
    return
  }
  const content = input.content.trim()
  if (new TextEncoder().encode(content).length > MAX_DASHBOARD_MESSAGE_BYTES) {
    sendJson(res, { error: "Message content exceeds 10KB limit" }, 413)
    return
  }
  if (input.group !== undefined && (typeof input.group !== "string" || input.group.length === 0)) {
    sendJson(res, { error: "group must be a non-empty group name" }, 400)
    return
  }
  if (input.group !== undefined && input.to !== undefined) {
    sendJson(res, { error: "Choose either a direct/broadcast destination or group" }, 400)
    return
  }
  if (input.group === undefined && input.to !== null && (typeof input.to !== "string" || input.to.length === 0)) {
    sendJson(res, { error: "to must be a team member name or null for broadcast" }, 400)
    return
  }
  const team = db.query("SELECT status FROM team WHERE id = ?").get(teamId) as { status: string } | undefined
  if (!team) {
    sendJson(res, { error: "Team not found" }, 404)
    return
  }
  if (team.status !== "active") {
    sendJson(res, { error: "Team is not active" }, 409)
    return
  }

  if (typeof input.group === "string") {
    try {
      const sent = sendGroupMessage(db, { teamId, sender: "lead", group: input.group, content })
      scheduler?.kick()
      sendJson(res, {
        message: { id: sent.messageId, fromName: "lead", toName: null, groupId: sent.group.id, groupName: sent.group.name, content, delivered: false, read: false, timeCreated: sent.timeCreated },
        recipientCount: sent.workerRecipientCount,
      }, 202)
    } catch (error) {
      const message = error instanceof Error ? error.message : "Group message rejected"
      const status = /Only participants/.test(message) ? 403 : /eligible recipient/.test(message) ? 409 : /not found/.test(message) ? 404 : 400
      sendJson(res, { error: message }, status)
    }
    return
  }

  const messageId = generateId("msg")
  const now = Date.now()
  if (typeof input.to === "string") {
    const member = db.query(
      "SELECT session_id, agent FROM team_member WHERE team_id = ? AND name = ? AND member_kind = 'worker' AND status IN ('ready', 'busy')",
    ).get(teamId, input.to) as { session_id: string; agent: string } | undefined
    if (!member) {
      sendJson(res, { error: "Schedulable team member not found" }, 404)
      return
    }
    queueMessageWake(db, {
      messageId,
      teamId,
      fromName: "lead",
      toName: input.to,
      content,
      memberName: input.to,
      sessionId: member.session_id,
      agent: member.agent,
      reason: "dashboard_message",
      coalesceKey: `member:${input.to}`,
      now,
    })
    sendJson(res, { message: { id: messageId, fromName: "lead", toName: input.to, content, delivered: false, read: false, timeCreated: now }, recipientCount: 1 }, 202)
    return
  }

  const members = db.query(
    "SELECT name, session_id, agent FROM team_member WHERE team_id = ? AND member_kind = 'worker' AND status IN ('ready', 'busy') ORDER BY time_created ASC",
  ).all(teamId) as Array<{ name: string; session_id: string; agent: string }>
  if (members.length === 0) {
    sendJson(res, { error: "Team has no schedulable recipients" }, 409)
    return
  }
  queueBroadcastWakes(db, {
    messageId,
    teamId,
    fromName: "lead",
    content,
    recipients: members.map(member => ({
      memberName: member.name,
      sessionId: member.session_id,
      agent: member.agent,
      reason: "dashboard_broadcast",
      coalesceKey: `member:${member.name}`,
    })),
    now,
  })
  sendJson(res, { message: { id: messageId, fromName: "lead", toName: null, content, delivered: false, read: false, timeCreated: now }, recipientCount: members.length }, 202)
}

/** Dashboard server handle returned by startDashboard. */
export interface DashboardServer {
  readonly host: string
  stop(force?: boolean): void
}

/** Optional dependencies for the dashboard server. */
export interface DashboardOptions {
  /** Bearer token required for dashboard API requests. */
  token: string
  /** In-memory activity buffer for real-time per-session events. */
  activityBuffer?: ActivityBuffer
  /** SDK client for on-demand session message retrieval. */
  client?: PluginClient
  /** Durable scheduler used to dispatch newly accepted group messages immediately. */
  scheduler?: SchedulerController
}

function sendJson(res: ServerResponse, data: unknown, status = 200, headers?: Record<string, string>): void {
  res.writeHead(status, { ...SECURITY_HEADERS, "Content-Type": "application/json; charset=utf-8", ...headers })
  res.end(JSON.stringify(data))
}

function sendText(res: ServerResponse, text: string, status = 200, contentType = "text/plain; charset=utf-8", headers?: Record<string, string>): void {
  res.writeHead(status, { ...SECURITY_HEADERS, "Content-Type": contentType, ...headers })
  res.end(text)
}

function sanitizeActivityEntry(entry: ActivityEntry): ActivityEntry | null {
  if (entry.type === "reasoning" || entry.type === "text") return null
  if (entry.type === "tool_call" || entry.type === "tool_result") {
    return { type: entry.type, tool: entry.tool, title: entry.title, timestamp: entry.timestamp }
  }
  if (entry.type === "shell_command") {
    return { type: entry.type, exitCode: entry.exitCode, timestamp: entry.timestamp }
  }
  if (entry.type === "file") {
    return { type: entry.type, filePath: entry.filePath, timestamp: entry.timestamp }
  }
  return {
    type: entry.type,
    title: entry.title,
    tokensIn: entry.tokensIn,
    tokensOut: entry.tokensOut,
    cost: entry.cost,
    timestamp: entry.timestamp,
  }
}

/**
 * Resolve a message's creation timestamp (Unix ms) from an SDK message's
 * `info.time`. The SDK shape is an object `{ created: number }`; older/other
 * shapes (a numeric epoch or an ISO string) are tolerated. Falls back to
 * `Date.now()` when the value is missing or unparseable — never returns NaN.
 */
export function parseMessageTime(time: unknown): number {
  if (typeof time === "object" && time !== null) {
    const created = (time as { created?: unknown }).created
    if (typeof created === "number" && Number.isFinite(created)) return created
  }
  if (typeof time === "number" && Number.isFinite(time)) return time
  if (typeof time === "string") {
    const ms = new Date(time).getTime()
    if (!Number.isNaN(ms)) return ms
  }
  return Date.now()
}

/** Parse SDK message parts into ActivityEntry format for the fallback path. */
export function parseMessageParts(parts: unknown[], msgInfo: unknown): ActivityEntry[] {
  const entries: ActivityEntry[] = []
  const info = (msgInfo ?? {}) as { time?: unknown; role?: string; tokens?: { input?: number; output?: number } }
  const timestamp = parseMessageTime(info.time)

  for (const part of parts) {
    if (typeof part !== "object" || part === null) continue
    const p = part as {
      type?: string
      tool?: string
      state?: { status?: string; input?: unknown; output?: unknown; error?: string; title?: string }
      text?: string
      path?: string
      content?: string
      diff?: string
      label?: string
      step?: string
    }

    if (p.type === "tool" && p.tool) {
      const state = p.state ?? {}
      const inputStr = typeof state.input === "string" ? state.input : state.input != null ? JSON.stringify(state.input, null, 2) : undefined
      const outputStr = typeof state.output === "string" ? state.output : state.output != null ? JSON.stringify(state.output, null, 2) : undefined
      entries.push({
        type: state.status === "completed" ? "tool_result" : "tool_call",
        tool: p.tool,
        title: state.title,
        input: inputStr,
        output: outputStr,
        error: state.error,
        timestamp,
      })
    } else if (p.type === "reasoning" && p.text) {
      entries.push({ type: "reasoning", reasoning: p.text, timestamp })
    } else if (p.type === "file" && (p.path || p.content || p.diff)) {
      entries.push({
        type: "file",
        filePath: p.path,
        fileContent: p.content,
        fileDiff: p.diff,
        timestamp,
      })
    } else if (p.type === "text" && p.text) {
      entries.push({ type: "text", text: p.text, role: info.role, timestamp })
    } else if (p.type === "step-start") {
      entries.push({ type: "step", title: p.label ?? p.step ?? "step", timestamp })
    } else if (p.type === "step-finish") {
      entries.push({ type: "step", title: p.label ?? p.step ?? "step complete", timestamp })
    }
  }
  return entries
}

/** Handle the /api/session/:sessionId/activity endpoint. */
async function handleActivityRoute(
  sessionId: string,
  options: DashboardOptions | undefined,
  res: ServerResponse,
): Promise<void> {
  const buffer = options?.activityBuffer
  const client = options?.client

  const buffered = buffer?.getActivity(sessionId) ?? []

  const fallbackActivity: ActivityEntry[] = []

  if (client) {
    try {
      const msgResult = await client.session.messages({ sessionID: sessionId, limit: 100 })
      const messages = msgResult.data ?? []
      for (const msg of messages) {
        const parts = msg.parts ?? []
        fallbackActivity.push(...parseMessageParts(parts, msg.info))
      }
    } catch { /* best effort — return what we have */ }
  }

  const combined = [...buffered, ...fallbackActivity]
    .sort((a, b) => a.timestamp - b.timestamp)
    .map(sanitizeActivityEntry)
    .filter((entry): entry is ActivityEntry => entry !== null)

  sendJson(res, { activity: combined })
}

async function handleDashboardRequest(
  db: Database,
  port: number,
  req: IncomingMessage,
  res: ServerResponse,
  options: DashboardOptions,
): Promise<void> {
  let url: URL
  try {
    url = new URL(req.url ?? "/", `http://${DASHBOARD_HOST}:${port}`)
  } catch {
    sendJson(res, { error: "Malformed request URL" }, 400)
    return
  }

  if ((url.pathname === "/api" || url.pathname.startsWith("/api/")) && !isDashboardAuthorized(req.headers.authorization, options.token)) {
    sendJson(res, { error: "Unauthorized" }, 401, { "WWW-Authenticate": "Bearer" })
    return
  }

  const messagesMatch = url.pathname.match(/^\/api\/teams\/([^/]+)\/messages$/)
  if (messagesMatch && req.method === "POST") {
    const encodedTeamId = messagesMatch[1]
    if (!encodedTeamId) {
      sendJson(res, { error: "Malformed team ID" }, 400)
      return
    }
    let teamId: string
    try {
      teamId = decodeURIComponent(encodedTeamId)
    } catch {
      sendJson(res, { error: "Malformed team ID" }, 400)
      return
    }
    try {
      await handleSendMessageRoute(db, teamId, req, res, options.scheduler)
    } catch {
      if (!res.headersSent) sendJson(res, { error: "Failed to queue message" }, 500)
    }
    return
  }

  if (req.method !== "GET") {
    sendText(res, "Method Not Allowed", 405, "text/plain; charset=utf-8", { Allow: "GET" })
    return
  }

  if (url.pathname === "/api/health") {
    sendJson(res, { ensemble: true, pid: process.pid })
    return
  }

  if (url.pathname === "/api/state") {
    sendJson(res, buildState(db))
    return
  }

  if (messagesMatch) {
    const encodedTeamId = messagesMatch[1]
    if (!encodedTeamId) {
      sendJson(res, { error: "Malformed team ID" }, 400)
      return
    }
    try {
      handleMessagesRoute(db, decodeURIComponent(encodedTeamId), url, res)
    } catch {
      sendJson(res, { error: "Malformed team ID" }, 400)
    }
    return
  }

  const memberMatch = url.pathname.match(/^\/api\/teams\/([^/]+)\/members\/([^/]+)$/)
  if (memberMatch) {
    const encodedTeamId = memberMatch[1]
    const encodedMemberName = memberMatch[2]
    if (!encodedTeamId || !encodedMemberName) {
      sendJson(res, { error: "Malformed team member path" }, 400)
      return
    }
    try {
      const teamId = decodeURIComponent(encodedTeamId)
      const memberName = decodeURIComponent(encodedMemberName)
      const member = db.query("SELECT prompt FROM team_member WHERE team_id = ? AND name = ? AND member_kind = 'worker'").get(teamId, memberName) as { prompt: string | null } | undefined
      if (!member) {
        sendJson(res, { error: "Team member not found" }, 404)
        return
      }
      sendJson(res, { prompt: member.prompt })
    } catch {
      sendJson(res, { error: "Malformed team member path" }, 400)
    }
    return
  }

  const activityMatch = url.pathname.match(/^\/api\/session\/([^/]+)\/activity$/)
  if (activityMatch) {
    const encodedSessionId = activityMatch[1]
    if (!encodedSessionId) {
      sendJson(res, { error: "Malformed session ID" }, 400)
      return
    }
    let sessionId: string
    try {
      sessionId = decodeURIComponent(encodedSessionId)
    } catch {
      sendJson(res, { error: "Malformed session ID" }, 400)
      return
    }
    await handleActivityRoute(sessionId, options, res)
    return
  }

  if (url.pathname === "/") {
    sendText(res, DASHBOARD_HTML, 200, "text/html; charset=utf-8")
    return
  }

  sendText(res, "Not Found", 404)
}

function toDashboardServer(server: Server): DashboardServer {
  return {
    host: DASHBOARD_HOST,
    stop(force?: boolean) {
      server.close()
      // server.close() only stops accepting new connections — under Node's
      // node:http, idle keep-alive sockets keep the listener busy until the
      // keep-alive timeout. closeAllConnections() (Node ≥ 18.2) terminates
      // them promptly, matching the behaviour Bun.serve().stop(true) had.
      if (force) {
        const closeAll = (server as unknown as { closeAllConnections?: () => void }).closeAllConnections
        if (typeof closeAll === "function") closeAll.call(server)
      }
    },
  }
}

/**
 * Start the dashboard HTTP server.
 * Serves a JSON API for team state, session activity, and the dashboard HTML.
 * Singleton: if the port is already in use by another ensemble instance, skips silently.
 * Returns the server instance, or null if skipped.
 */
export async function startDashboard(db: Database, port: number, options: DashboardOptions): Promise<DashboardServer | null> {
  return new Promise((resolve) => {
    const server = createServer((req, res) => {
      handleDashboardRequest(db, port, req, res, options).catch(() => {
        if (!res.headersSent) sendJson(res, { error: "Dashboard request failed" }, 500)
      })
    })

    server.once("error", async (err: NodeJS.ErrnoException) => {
      if (err.code === "EADDRINUSE") {
        try {
          const res = await fetch(`http://${DASHBOARD_HOST}:${port}/api/health`, {
            headers: { Authorization: `Bearer ${options.token}` },
            signal: AbortSignal.timeout(2000),
          })
          const data = await res.json() as { ensemble?: boolean; pid?: number }
          if (data.ensemble && data.pid) {
            // Check if the other process is still alive
            let alive = false
            try { process.kill(data.pid, 0); alive = true } catch { /* process is dead */ }
            if (alive && data.pid !== process.pid) {
              log(`dashboard:already-running port=${port} pid=${data.pid}`)
              resolve(null)
              return
            }
            // Stale server from a dead process — warn the user
            log(`dashboard:stale-server port=${port} stale-pid=${data.pid} — run: kill -9 ${data.pid} || lsof -ti:${port} | xargs kill -9`)
            resolve(null)
            return
          }
        } catch { /* health check failed — port held by something else */ }
        log(`dashboard:port-in-use port=${port} (not an ensemble instance)`)
        resolve(null)
        return
      }

      log(`dashboard:failed err=${err.message}`)
      resolve(null)
    })

    server.listen(port, DASHBOARD_HOST, () => {
      log(`dashboard:started port=${port} url=http://${DASHBOARD_HOST}:${port}`)
      resolve(toDashboardServer(server))
    })
  })
}

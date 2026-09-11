import type { ToolDeps } from "../types"
import { requireTeamMember } from "./shared"
import { parseTaskResult, formatTaskResult } from "../result-parser"
import { inspectTeamGroup, listTeamGroups } from "../groups"

/** Row shape for unread messages query. */
interface UnreadMessageRow {
  id: string
  from_name: string
  content: string
  time_created: number
}

/**
 * Retrieve unread ordinary messages, list groups, or inspect group history.
 */
export async function executeTeamResults(
  deps: ToolDeps,
  args: { from?: string; list_groups?: boolean; group?: string; limit?: number },
  sessionId: string,
): Promise<string> {
  const team = requireTeamMember(deps, sessionId)
  const caller = team.role === "lead" ? "lead" : (team.memberName ?? "unknown")
  if (args.list_groups && args.group) throw new Error("Cannot combine list_groups and group")
  if ((args.list_groups || args.group) && args.from) throw new Error("Cannot combine group inspection with from")
  if (args.limit !== undefined && !args.group) throw new Error("limit requires group")
  if (args.list_groups) {
    const groups = listTeamGroups(deps.db, team.teamId, caller)
    if (groups.length === 0) return "No group inboxes."
    return groups.map(group => [
      `Group ${group.name}`,
      `  creator: ${group.creator}`,
      `  participants: ${group.participants.join(", ")}`,
      `  created: ${group.createdAt}`,
      `  canSend: ${group.canSend}`,
    ].join("\n")).join("\n\n")
  }
  if (args.group) {
    const history = inspectTeamGroup(deps.db, team.teamId, caller, args.group, args.limit ?? 20)
    if (history.messages.length === 0) return `No messages in group "${args.group}".`
    return history.messages.map(message => `[Group ${history.group.name} from ${message.from} at ${message.createdAt}]:\n${message.content}`).join("\n\n")
  }

  const rows = args.from
    ? (deps.db
        .query(
          "SELECT id, from_name, content, time_created FROM team_message WHERE team_id = ? AND group_id IS NULL AND read = 0 AND from_name = ? ORDER BY time_created ASC",
        )
        .all(team.teamId, args.from) as UnreadMessageRow[])
    : (deps.db
        .query(
          "SELECT id, from_name, content, time_created FROM team_message WHERE team_id = ? AND group_id IS NULL AND read = 0 ORDER BY time_created ASC",
        )
        .all(team.teamId) as UnreadMessageRow[])

  if (rows.length === 0) return "No unread messages."

  // Mark all returned messages as read
  const ids = rows.map((r) => r.id)
  const placeholders = ids.map(() => "?").join(", ")
  deps.db.run(`UPDATE team_message SET read = 1 WHERE id IN (${placeholders})`, ids)

  // Format output — parse structured task results when present
  return rows.map((r) => {
    const parsed = parseTaskResult(r.content)
    if (parsed) return formatTaskResult(r.from_name, parsed)
    return `[Message from ${r.from_name}]:\n${r.content}`
  }).join("\n\n")
}

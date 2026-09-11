import type { ToolDeps } from "../types"
import { requireTeamMember } from "./shared"
import { broadcastMessage } from "../messaging"
import { log } from "../log"
import { generateId } from "../util"
import { queueBroadcastWakes } from "../scheduler"
import { getLeadPromptOptions } from "../member-model"

/**
 * Execute the team_broadcast tool. Sends a message to all team members + lead (excluding sender).
 */
export async function executeTeamBroadcast(
  deps: ToolDeps,
  args: { text: string },
  sessionId: string,
): Promise<string> {
  const teamInfo = requireTeamMember(deps, sessionId)

  const senderName = teamInfo.role === "lead" ? "lead" : (teamInfo.memberName ?? "unknown")

  let leadSessionId: string | undefined
  if (teamInfo.role !== "lead") {
    const leadSession = deps.db.query("SELECT lead_session_id FROM team WHERE id = ?")
      .get(teamInfo.teamId) as { lead_session_id: string } | null
    leadSessionId = leadSession?.lead_session_id
  }

  const members = deps.db.query(
    "SELECT name, session_id, agent FROM team_member WHERE team_id = ? AND session_id <> ? AND status IN ('ready', 'busy') AND reported_to_lead = 0 ORDER BY time_created ASC",
  ).all(teamInfo.teamId, sessionId) as Array<{ name: string; session_id: string; agent: string }>
  const messageId = members.length > 0
    ? generateId("msg")
    : broadcastMessage(deps.db, { teamId: teamInfo.teamId, from: senderName, content: args.text })
  if (members.length > 0) {
    queueBroadcastWakes(deps.db, {
      messageId,
      teamId: teamInfo.teamId,
      fromName: senderName,
      content: args.text,
      recipients: members.map(member => ({
        memberName: member.name,
        sessionId: member.session_id,
        agent: member.agent,
        reason: "broadcast",
        coalesceKey: `member:${member.name}`,
      })),
    })
    deps.scheduler.kick()
  }

  if (leadSessionId) {
    deps.client.session.promptAsync({
      sessionID: leadSessionId,
      parts: [{ type: "text", text: `[Team broadcast from ${senderName}]: ${args.text}` }],
      ...getLeadPromptOptions(deps.db, teamInfo.teamId),
    }).then(() => {
      deps.db.run("UPDATE team_message SET delivered = 1 WHERE id = ?", [messageId])
    }).catch((err) => {
      log(`team_broadcast:deliver:failed to=lead err=${err instanceof Error ? err.message : String(err)}`)
    })
  }

  const sent = members.length + (leadSessionId ? 1 : 0)
  return `Broadcast sent to ${sent} recipient${sent !== 1 ? "s" : ""}.`
}

import type { ToolDeps } from "../types"
import { requireTeamMember } from "./shared"
import { broadcastMessage } from "../messaging"
import { log } from "../log"
import { generateId } from "../util"
import { immediateTransaction, persistBroadcastWakesInTransaction, queueBroadcastWakes } from "../scheduler"
import { getLeadPromptOptions } from "../member-model"
import { claimSupervisorBroadcastInTransaction } from "../supervisor"
import { sendGroupMessage } from "../groups"

/**
 * Execute the team_broadcast tool for whole-team broadcasts or durable group inboxes.
 */
export async function executeTeamBroadcast(
  deps: ToolDeps,
  args: { text: string; group?: string; members?: string[] },
  sessionId: string,
): Promise<string> {
  const teamInfo = requireTeamMember(deps, sessionId)

  const senderName = teamInfo.role === "lead" ? "lead" : (teamInfo.memberName ?? "unknown")
  const sender = teamInfo.role === "member"
    ? deps.db.query("SELECT member_kind FROM team_member WHERE team_id = ? AND name = ?").get(teamInfo.teamId, senderName) as { member_kind: string } | null
    : null
  const supervisorBroadcast = sender?.member_kind === "supervisor"

  if (args.members && !args.group) throw new Error("members requires group")
  if (args.group) {
    if (supervisorBroadcast) throw new Error("Supervisor cannot create, join, or send to group inboxes")
    const sent = sendGroupMessage(deps.db, {
      teamId: teamInfo.teamId,
      sender: senderName,
      group: args.group,
      members: args.members,
      content: args.text,
    })
    deps.scheduler.kick()
    const recipientCount = sent.workerRecipientCount + (sent.leadRecipient ? 1 : 0)
    return `Group "${sent.group.name}" message sent to ${recipientCount} recipient${recipientCount === 1 ? "" : "s"}.`
  }

  let leadSessionId: string | undefined
  if (teamInfo.role !== "lead") {
    const leadSession = deps.db.query("SELECT lead_session_id FROM team WHERE id = ?")
      .get(teamInfo.teamId) as { lead_session_id: string } | null
    leadSessionId = leadSession?.lead_session_id
  }

  const members = deps.db.query(
    `SELECT name, session_id, agent FROM team_member
     WHERE team_id = ? AND member_kind = 'worker' AND session_id <> ? AND status IN ('ready', 'busy')
       AND (? = 1 OR reported_to_lead = 0) ORDER BY time_created ASC`,
  ).all(teamInfo.teamId, sessionId, supervisorBroadcast ? 1 : 0) as Array<{ name: string; session_id: string; agent: string }>
  const broadcastInput = {
    messageId: generateId("msg"),
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
  }
  let messageId: string
  if (supervisorBroadcast) {
    immediateTransaction(deps.db, () => {
      claimSupervisorBroadcastInTransaction(deps.db, sessionId)
      persistBroadcastWakesInTransaction(deps.db, broadcastInput)
    })
    messageId = broadcastInput.messageId
  } else if (members.length > 0) {
    queueBroadcastWakes(deps.db, broadcastInput)
    messageId = broadcastInput.messageId
  } else {
    messageId = broadcastMessage(deps.db, { teamId: teamInfo.teamId, from: senderName, content: args.text })
  }
  if (members.length > 0) {
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

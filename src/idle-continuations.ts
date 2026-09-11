import type { Database } from "./db"
import { log } from "./log"
import { getMemberPromptOptions } from "./member-model"
import type { PluginClient } from "./types"
import type { SchedulerController } from "./scheduler-runtime"
import { queueWake } from "./scheduler"

/** Fire an identity-preserving nudge when an idle teammate has not reported. */
export function sendIdleWithoutReportNudge(
  scheduler: SchedulerController,
  db: Database,
  teamId: string,
  memberName: string,
  sessionId: string,
): void {
  const member = db.query("SELECT agent FROM team_member WHERE team_id = ? AND name = ?").get(teamId, memberName) as { agent: string } | null
  if (!member) return
  queueWake(db, {
    teamId,
    memberName,
    sessionId,
    agent: member.agent,
    reason: "idle_without_report",
    coalesceKey: `member:${memberName}`,
    prompt: "[System]: You completed your work but did not report results. Send your findings to the lead via team_message now.",
  })
  scheduler.kick()
}

/** Fire an identity-preserving wake for an idle teammate's pending peer messages. */
export function sendPeerMessageFlush(
  client: PluginClient,
  db: Database,
  teamId: string,
  memberName: string,
  sessionId: string,
  messageCount: number,
): void {
  client.session.promptAsync({
    sessionID: sessionId,
    parts: [{ type: "text", text: `[System: ${messageCount} new message(s) from teammates]` }],
    ...getMemberPromptOptions(db, teamId, memberName),
  }).catch((err) => {
    log(`wake-peer:failed err=${err instanceof Error ? err.message : String(err)}`)
  })
}

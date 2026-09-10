import type { Database } from "./db"
import { log } from "./log"
import { getMemberPromptOptions } from "./member-model"
import type { PluginClient } from "./types"

/** Fire an identity-preserving nudge when an idle teammate has not reported. */
export function sendIdleWithoutReportNudge(
  client: PluginClient,
  db: Database,
  teamId: string,
  memberName: string,
  sessionId: string,
): void {
  client.session.promptAsync({
    sessionID: sessionId,
    parts: [{ type: "text", text: "[System]: You completed your work but did not report results. Send your findings to the lead via team_message now." }],
    ...getMemberPromptOptions(db, teamId, memberName),
  }).catch((err) => {
    log(`nudge:idle-without-report:failed name=${memberName} team=${teamId} err=${err instanceof Error ? err.message : String(err)}`)
  })
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

import type { Database } from "./db"

/** A parsed provider/model pair, as accepted by session.promptAsync. */
export interface ParsedModel {
  providerID: string
  modelID: string
}

/** Stored teammate identity fields accepted by session.promptAsync. */
export interface MemberPromptOptions {
  agent?: string
  model?: ParsedModel
}

/** Read the stored lead agent and model as session.promptAsync options. */
export function getLeadPromptOptions(db: Database, teamId: string): MemberPromptOptions {
  const row = db.query("SELECT lead_agent, lead_model FROM team WHERE id = ?")
    .get(teamId) as { lead_agent: string | null; lead_model: string | null } | null
  if (!row) return {}
  const model = row.lead_model ? parseModelId(row.lead_model) : undefined
  return {
    ...(row.lead_agent ? { agent: row.lead_agent } : {}),
    ...(model ? { model } : {}),
  }
}

/**
 * Parse a "provider/model" string into { providerID, modelID } for the SDK.
 * Returns undefined if the string is not in "provider/model" form (empty
 * provider or empty model). The model portion may itself contain slashes.
 */
export function parseModelId(model: string): ParsedModel | undefined {
  const slash = model.indexOf("/")
  if (slash <= 0 || slash === model.length - 1) return undefined
  return { providerID: model.slice(0, slash), modelID: model.slice(slash + 1) }
}

/**
 * Read a team member's stored model and parse it for session.promptAsync.
 * Returns undefined when the member is unknown, has no model set, or the
 * stored value is malformed. Never throws — safe to call from delivery paths.
 */
export function getMemberModel(db: Database, teamId: string, memberName: string): ParsedModel | undefined {
  return getMemberPromptOptions(db, teamId, memberName).model
}

/**
 * Read a teammate's stored agent and model as session.promptAsync options.
 * Unknown members return empty options. A missing or malformed model is
 * omitted without dropping the stored agent.
 */
export function getMemberPromptOptions(db: Database, teamId: string, memberName: string): MemberPromptOptions {
  const row = db.query("SELECT agent, model FROM team_member WHERE team_id = ? AND name = ?")
    .get(teamId, memberName) as { agent: string; model: string | null } | null
  if (!row) return {}
  const model = row.model ? parseModelId(row.model) : undefined
  return {
    agent: row.agent,
    ...(model ? { model } : {}),
  }
}

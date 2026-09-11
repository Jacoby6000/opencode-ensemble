import type { Database } from "./db"

/** Invalidate a quiet/reviewed supervision generation inside the caller's transaction. */
export function invalidateTeamSupervision(db: Database, teamId: string, now = Date.now()): number {
  db.run("INSERT OR IGNORE INTO team_supervision (team_id) VALUES (?)", [teamId])
  const state = db.query("SELECT generation, quiet_since, last_reviewed FROM team_supervision WHERE team_id = ?")
    .get(teamId) as { generation: number; quiet_since: number | null; last_reviewed: number }
  if (state.quiet_since === null && state.last_reviewed !== state.generation) return state.generation
  const generation = state.generation + 1
  db.run("UPDATE team_supervision SET generation = ?, quiet_since = NULL WHERE team_id = ?", [generation, teamId])
  db.run(
    `UPDATE scheduler_wake SET state = 'cancelled', last_error = 'supervision generation invalidated', time_updated = ?
     WHERE team_id = ? AND reason = 'supervisor_review' AND state = 'queued'`,
    [now, teamId],
  )
  return generation
}

/** Rearm a current review only after all of its wakes are terminal and none completed successfully. */
export function rearmTerminalSupervisorReview(db: Database, teamId: string): boolean {
  return db.run(
    `UPDATE team_supervision SET last_reviewed = -1
     WHERE team_id = ? AND last_reviewed = generation
       AND EXISTS (
         SELECT 1 FROM scheduler_wake
         WHERE team_id = ? AND reason = 'supervisor_review'
           AND supervision_generation = team_supervision.generation
           AND state IN ('failed', 'cancelled')
       )
       AND NOT EXISTS (
         SELECT 1 FROM scheduler_wake
         WHERE team_id = ? AND reason = 'supervisor_review'
           AND supervision_generation = team_supervision.generation
           AND state IN ('queued', 'leased', 'completed')
       )`,
    [teamId, teamId, teamId],
  ).changes > 0
}

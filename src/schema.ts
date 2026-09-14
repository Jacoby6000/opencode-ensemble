import type { Database } from "./db"

/** Migration SQL statements, applied in order. Version = index + 1. */
export const MIGRATIONS: string[] = [
  // Migration 1: Initial schema — 4 tables
  `
  CREATE TABLE IF NOT EXISTS team (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    lead_session_id TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'archived')),
    delegate        INTEGER NOT NULL DEFAULT 0,
    time_created    INTEGER NOT NULL,
    time_updated    INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS team_lead_idx ON team(lead_session_id);
  CREATE INDEX IF NOT EXISTS team_status_idx ON team(status);

  CREATE TABLE IF NOT EXISTS team_member (
    team_id          TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    session_id       TEXT NOT NULL,
    agent            TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'ready'
                       CHECK(status IN ('ready', 'busy', 'shutdown_requested', 'shutdown', 'error')),
    execution_status TEXT NOT NULL DEFAULT 'idle'
                       CHECK(execution_status IN ('idle', 'starting', 'running',
                         'cancel_requested', 'cancelling', 'cancelled',
                         'completing', 'completed', 'failed', 'timed_out')),
    model            TEXT,
    prompt           TEXT,
    time_created     INTEGER NOT NULL,
    time_updated     INTEGER NOT NULL,
    PRIMARY KEY (team_id, name)
  );
  CREATE INDEX IF NOT EXISTS team_member_session_idx ON team_member(session_id);
  CREATE INDEX IF NOT EXISTS team_member_status_idx ON team_member(team_id, status);

  CREATE TABLE IF NOT EXISTS team_task (
    id            TEXT PRIMARY KEY,
    team_id       TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
    content       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'in_progress', 'completed', 'cancelled', 'blocked')),
    priority      TEXT NOT NULL DEFAULT 'medium'
                    CHECK(priority IN ('high', 'medium', 'low')),
    assignee      TEXT,
    depends_on    TEXT,
    time_created  INTEGER NOT NULL,
    time_updated  INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS team_task_team_idx ON team_task(team_id);
  CREATE INDEX IF NOT EXISTS team_task_assignee_idx ON team_task(assignee);
  CREATE INDEX IF NOT EXISTS team_task_status_idx ON team_task(team_id, status);

  CREATE TABLE IF NOT EXISTS team_message (
    id            TEXT PRIMARY KEY,
    team_id       TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
    from_name     TEXT NOT NULL,
    to_name       TEXT,
    content       TEXT NOT NULL,
    delivered     INTEGER NOT NULL DEFAULT 0,
    time_created  INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS team_message_team_idx ON team_message(team_id);
  CREATE INDEX IF NOT EXISTS team_message_to_idx ON team_message(to_name);
  CREATE INDEX IF NOT EXISTS team_message_undelivered_idx ON team_message(team_id, delivered)
    WHERE delivered = 0;
  `,
  // Migration 2: Add read column to team_message for team_results tracking
  `ALTER TABLE team_message ADD COLUMN read INTEGER NOT NULL DEFAULT 0;
   CREATE INDEX IF NOT EXISTS team_message_unread_idx ON team_message(team_id, read) WHERE read = 0;`,
  // Migration 3: Add worktree columns to team_member for git worktree isolation
  `ALTER TABLE team_member ADD COLUMN worktree_dir TEXT;
   ALTER TABLE team_member ADD COLUMN worktree_branch TEXT;`,
  // Migration 4: Add plan_approval column to team_member for plan-before-build workflow
  `ALTER TABLE team_member ADD COLUMN plan_approval TEXT NOT NULL DEFAULT 'none'
     CHECK(plan_approval IN ('none', 'pending', 'approved', 'rejected'));`,
  // Migration 5: Track lead's agent mode so message delivery preserves it
  `ALTER TABLE team ADD COLUMN lead_agent TEXT;`,
  // Migration 6: Track workspace ID for worktree-session binding
  `ALTER TABLE team_member ADD COLUMN workspace_id TEXT;`,
  // Migration 7: Track whether teammate has reported to lead (completion loop prevention, issue #3)
  `ALTER TABLE team_member ADD COLUMN reported_to_lead INTEGER NOT NULL DEFAULT 0;`,
  // Migration 8: Add project as the dashboard grouping level. For this initial
  // project-first step, project_id is the team lead's working directory.
  `PRAGMA foreign_keys=OFF;
   ALTER TABLE team RENAME TO team_old_m8;
   CREATE TABLE IF NOT EXISTS project (
     id              TEXT PRIMARY KEY,
     name            TEXT NOT NULL,
     path            TEXT NOT NULL,
     status          TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'archived')),
     time_created    INTEGER NOT NULL,
     time_updated    INTEGER NOT NULL
   );
   INSERT OR IGNORE INTO project (id, name, path, status, time_created, time_updated)
     VALUES ('default', 'Default Project', '', 'active', strftime('%s','now') * 1000, strftime('%s','now') * 1000);
   CREATE TABLE team (
     id              TEXT PRIMARY KEY,
     name            TEXT NOT NULL,
     project_id      TEXT NOT NULL DEFAULT 'default' REFERENCES project(id) ON DELETE CASCADE,
     lead_session_id TEXT NOT NULL,
     status          TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'archived')),
     delegate        INTEGER NOT NULL DEFAULT 0,
     time_created    INTEGER NOT NULL,
     time_updated    INTEGER NOT NULL,
     lead_agent      TEXT
   );
   INSERT INTO team (id, name, project_id, lead_session_id, status, delegate, time_created, time_updated, lead_agent)
     SELECT id, name, 'default', lead_session_id, status, delegate, time_created, time_updated, lead_agent FROM team_old_m8;
   DROP TABLE team_old_m8;

   ALTER TABLE team_member RENAME TO team_member_old_m8;
   CREATE TABLE team_member (
     team_id          TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     name             TEXT NOT NULL,
     session_id       TEXT NOT NULL,
     agent            TEXT NOT NULL,
     status           TEXT NOT NULL DEFAULT 'ready'
                        CHECK(status IN ('ready', 'busy', 'shutdown_requested', 'shutdown', 'error')),
     execution_status TEXT NOT NULL DEFAULT 'idle'
                        CHECK(execution_status IN ('idle', 'starting', 'running',
                          'cancel_requested', 'cancelling', 'cancelled',
                          'completing', 'completed', 'failed', 'timed_out')),
     model            TEXT,
     prompt           TEXT,
     time_created     INTEGER NOT NULL,
     time_updated     INTEGER NOT NULL,
     worktree_dir     TEXT,
     worktree_branch  TEXT,
     plan_approval    TEXT NOT NULL DEFAULT 'none'
                        CHECK(plan_approval IN ('none', 'pending', 'approved', 'rejected')),
     workspace_id     TEXT,
     reported_to_lead INTEGER NOT NULL DEFAULT 0,
     PRIMARY KEY (team_id, name)
   );
   INSERT INTO team_member (team_id, name, session_id, agent, status, execution_status, model, prompt, time_created, time_updated, worktree_dir, worktree_branch, plan_approval, workspace_id, reported_to_lead)
     SELECT team_id, name, session_id, agent, status, execution_status, model, prompt, time_created, time_updated, worktree_dir, worktree_branch, plan_approval, workspace_id, reported_to_lead FROM team_member_old_m8;
   DROP TABLE team_member_old_m8;

   ALTER TABLE team_task RENAME TO team_task_old_m8;
   CREATE TABLE team_task (
     id            TEXT PRIMARY KEY,
     team_id       TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     content       TEXT NOT NULL,
     status        TEXT NOT NULL DEFAULT 'pending'
                     CHECK(status IN ('pending', 'in_progress', 'completed', 'cancelled', 'blocked')),
     priority      TEXT NOT NULL DEFAULT 'medium'
                     CHECK(priority IN ('high', 'medium', 'low')),
     assignee      TEXT,
     depends_on    TEXT,
     time_created  INTEGER NOT NULL,
     time_updated  INTEGER NOT NULL
   );
   INSERT INTO team_task (id, team_id, content, status, priority, assignee, depends_on, time_created, time_updated)
     SELECT id, team_id, content, status, priority, assignee, depends_on, time_created, time_updated FROM team_task_old_m8;
   DROP TABLE team_task_old_m8;

   ALTER TABLE team_message RENAME TO team_message_old_m8;
   CREATE TABLE team_message (
     id            TEXT PRIMARY KEY,
     team_id       TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     from_name     TEXT NOT NULL,
     to_name       TEXT,
     content       TEXT NOT NULL,
     delivered     INTEGER NOT NULL DEFAULT 0,
     time_created  INTEGER NOT NULL,
     read          INTEGER NOT NULL DEFAULT 0
   );
   INSERT INTO team_message (id, team_id, from_name, to_name, content, delivered, time_created, read)
     SELECT id, team_id, from_name, to_name, content, delivered, time_created, read FROM team_message_old_m8;
   DROP TABLE team_message_old_m8;

   CREATE INDEX IF NOT EXISTS team_lead_idx ON team(lead_session_id);
   CREATE INDEX IF NOT EXISTS team_status_idx ON team(status);
   CREATE INDEX IF NOT EXISTS team_project_idx ON team(project_id, status);
   CREATE UNIQUE INDEX IF NOT EXISTS team_active_project_name_idx ON team(project_id, name) WHERE status = 'active';
   CREATE INDEX IF NOT EXISTS team_member_session_idx ON team_member(session_id);
   CREATE INDEX IF NOT EXISTS team_member_status_idx ON team_member(team_id, status);
   CREATE INDEX IF NOT EXISTS team_task_team_idx ON team_task(team_id);
   CREATE INDEX IF NOT EXISTS team_task_assignee_idx ON team_task(assignee);
   CREATE INDEX IF NOT EXISTS team_task_status_idx ON team_task(team_id, status);
   CREATE INDEX IF NOT EXISTS team_message_team_idx ON team_message(team_id);
   CREATE INDEX IF NOT EXISTS team_message_to_idx ON team_message(to_name);
   CREATE INDEX IF NOT EXISTS team_message_undelivered_idx ON team_message(team_id, delivered) WHERE delivered = 0;
   CREATE INDEX IF NOT EXISTS team_message_unread_idx ON team_message(team_id, read) WHERE read = 0;
   PRAGMA foreign_keys=ON;`,
  // Migration 9: Add last_nudged_at to team_member — additive-only display-staleness
  // signal for the watchdog's soft stall-nudge path. Deliberately NOT a new status
  // enum value (see checkStalled() in watchdog.ts for the rationale): a 6th CHECK
  // constraint literal would require a table-rebuild migration and touch every
  // consumer that switches on the 5 known status strings. This column is additive —
  // existing consumers that don't know about it simply don't render it.
  `ALTER TABLE team_member ADD COLUMN last_nudged_at INTEGER;`,
  // Migration 10: Add retry_* columns to team_member — additive-only provider-retry
  // display signal (see hooks.ts's "retry" branch). Same non-negotiable as Migration
  // 9: no 6th status/execution_status literal, no table rebuild. retry_until is a
  // TTL, not a stored enum — "currently retrying" is derived at read time
  // (retry_until > Date.now()), so there is no explicit clear-write anywhere in this
  // fix; it simply becomes stale and gets superseded by real activity or by time
  // elapsing.
  `ALTER TABLE team_member ADD COLUMN retry_until INTEGER;
   ALTER TABLE team_member ADD COLUMN retry_attempt INTEGER;
   ALTER TABLE team_member ADD COLUMN retry_provider TEXT;
   ALTER TABLE team_member ADD COLUMN retry_message TEXT;`,
  // Migration 11: Persist mailbox lifecycle, queued wakes, active run leases,
  // message/wake links, and append-only scheduler lifecycle events.
  `ALTER TABLE team_message ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'queued'
     CHECK(delivery_state IN ('queued', 'wake_queued', 'injected', 'processed', 'failed'));
   UPDATE team_message SET delivery_state = CASE
     WHEN read = 1 THEN 'processed'
     WHEN delivered = 1 THEN 'injected'
     ELSE 'queued'
   END;
   UPDATE team_message SET delivered = 1 WHERE read = 1;

   CREATE TABLE scheduler_identity (
     id             TEXT PRIMARY KEY,
     team_id        TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     member_name    TEXT NOT NULL,
     agent          TEXT NOT NULL,
     state          TEXT NOT NULL DEFAULT 'reserved'
                      CHECK(state IN ('reserved', 'active', 'released', 'expired')),
     reserved_at    INTEGER NOT NULL,
     expires_at     INTEGER,
     activated_at   INTEGER,
     released_at    INTEGER
   );
   CREATE UNIQUE INDEX scheduler_identity_live_member_idx ON scheduler_identity(team_id, member_name) WHERE state IN ('reserved', 'active');
   CREATE INDEX scheduler_identity_capacity_idx ON scheduler_identity(state, agent, expires_at);
   INSERT INTO scheduler_identity (id, team_id, member_name, agent, state, reserved_at, activated_at)
     SELECT 'identity_m11_' || lower(hex(randomblob(16))), tm.team_id, tm.name, tm.agent, 'active', tm.time_created, tm.time_created
     FROM team_member tm
     JOIN team t ON t.id = tm.team_id
     WHERE t.status = 'active' AND tm.status IN ('ready', 'busy', 'shutdown_requested');

   CREATE TABLE scheduler_wake (
     id             TEXT PRIMARY KEY,
     team_id        TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     member_name    TEXT NOT NULL,
     session_id     TEXT NOT NULL,
     agent          TEXT NOT NULL,
     reason         TEXT NOT NULL,
     coalesce_key   TEXT NOT NULL,
     state          TEXT NOT NULL DEFAULT 'queued'
                      CHECK(state IN ('queued', 'leased', 'completed', 'failed', 'cancelled')),
     not_before     INTEGER NOT NULL,
     attempt_count  INTEGER NOT NULL DEFAULT 0,
     last_error     TEXT,
     time_created   INTEGER NOT NULL,
     time_updated   INTEGER NOT NULL,
     FOREIGN KEY (team_id, member_name) REFERENCES team_member(team_id, name) ON DELETE CASCADE
   );
   CREATE UNIQUE INDEX scheduler_wake_queued_key_idx ON scheduler_wake(team_id, member_name, coalesce_key) WHERE state = 'queued';
   CREATE INDEX scheduler_wake_ready_idx ON scheduler_wake(state, not_before, time_created);
   CREATE INDEX scheduler_wake_member_idx ON scheduler_wake(team_id, member_name, state);

   CREATE TABLE scheduler_run_lease (
     id             TEXT PRIMARY KEY,
     wake_id        TEXT NOT NULL REFERENCES scheduler_wake(id) ON DELETE CASCADE,
     team_id        TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     member_name    TEXT NOT NULL,
     session_id     TEXT NOT NULL,
     agent          TEXT NOT NULL,
     state          TEXT NOT NULL DEFAULT 'active'
                      CHECK(state IN ('active', 'released', 'expired', 'failed')),
     acquired_at    INTEGER NOT NULL,
     expires_at     INTEGER NOT NULL,
     released_at    INTEGER,
     FOREIGN KEY (team_id, member_name) REFERENCES team_member(team_id, name) ON DELETE CASCADE
   );
   CREATE UNIQUE INDEX scheduler_run_lease_active_member_idx ON scheduler_run_lease(team_id, member_name) WHERE state = 'active';
   CREATE UNIQUE INDEX scheduler_run_lease_active_wake_idx ON scheduler_run_lease(wake_id) WHERE state = 'active';
   CREATE INDEX scheduler_run_lease_capacity_idx ON scheduler_run_lease(state, agent, expires_at);

   CREATE TABLE scheduler_message_wake (
     message_id     TEXT NOT NULL REFERENCES team_message(id) ON DELETE CASCADE,
     wake_id        TEXT NOT NULL REFERENCES scheduler_wake(id) ON DELETE CASCADE,
     delivery_state TEXT NOT NULL DEFAULT 'wake_queued'
                      CHECK(delivery_state IN ('wake_queued', 'injected', 'processed', 'failed')),
     time_created   INTEGER NOT NULL,
     PRIMARY KEY (message_id, wake_id)
   );
   CREATE INDEX scheduler_message_wake_wake_idx ON scheduler_message_wake(wake_id);

   CREATE TABLE scheduler_event (
     id             TEXT PRIMARY KEY,
     team_id        TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     member_name    TEXT,
     wake_id        TEXT REFERENCES scheduler_wake(id) ON DELETE SET NULL,
     lease_id       TEXT REFERENCES scheduler_run_lease(id) ON DELETE SET NULL,
     type           TEXT NOT NULL,
     detail         TEXT,
     time_created   INTEGER NOT NULL
   );
   CREATE INDEX scheduler_event_team_idx ON scheduler_event(team_id, time_created);
   CREATE INDEX scheduler_event_wake_idx ON scheduler_event(wake_id, time_created);`,
  // Migration 12: Persist restart-safe wake prompts and injection evidence.
  `ALTER TABLE scheduler_wake ADD COLUMN prompt TEXT;
   ALTER TABLE scheduler_run_lease ADD COLUMN injected_at INTEGER;`,
  // Migration 13: Preserve the lead's selected model on asynchronous wake-ups.
  `ALTER TABLE team ADD COLUMN lead_model TEXT;`,
  // Migration 14: Persist internal member identity and restart-safe supervision epochs.
  `ALTER TABLE team_member ADD COLUMN member_kind TEXT NOT NULL DEFAULT 'worker'
     CHECK(member_kind IN ('worker', 'supervisor'));
   CREATE UNIQUE INDEX team_member_one_supervisor_idx ON team_member(team_id) WHERE member_kind = 'supervisor';
   ALTER TABLE scheduler_wake ADD COLUMN supervision_generation INTEGER;
   CREATE TABLE team_supervision (
     team_id              TEXT PRIMARY KEY REFERENCES team(id) ON DELETE CASCADE,
     generation           INTEGER NOT NULL DEFAULT 0,
     quiet_since          INTEGER,
     last_reviewed        INTEGER NOT NULL DEFAULT -1,
     broadcast_generation INTEGER
    );`,
  // Migration 15: Durable, immutable, team-scoped group inboxes.
  `CREATE TABLE team_group (
     id           TEXT PRIMARY KEY,
     team_id      TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     name         TEXT NOT NULL,
     created_by   TEXT NOT NULL,
     time_created INTEGER NOT NULL,
     sealed       INTEGER NOT NULL DEFAULT 0 CHECK(sealed IN (0, 1)),
     UNIQUE(team_id, name),
     UNIQUE(team_id, id)
   );
   CREATE INDEX team_group_team_idx ON team_group(team_id, time_created);

   CREATE TABLE team_group_participant (
     team_id          TEXT NOT NULL,
     group_id         TEXT NOT NULL,
     participant_name TEXT NOT NULL,
     time_created     INTEGER NOT NULL,
     PRIMARY KEY(team_id, group_id, participant_name),
     FOREIGN KEY(team_id, group_id) REFERENCES team_group(team_id, id) ON DELETE CASCADE
   );
   CREATE INDEX team_group_participant_member_idx ON team_group_participant(team_id, participant_name, group_id);

   ALTER TABLE team_message ADD COLUMN group_id TEXT REFERENCES team_group(id) ON DELETE CASCADE;
   CREATE INDEX team_message_group_idx ON team_message(team_id, group_id, time_created DESC, id DESC) WHERE group_id IS NOT NULL;

   CREATE TABLE team_group_message_recipient (
     message_id      TEXT NOT NULL REFERENCES team_message(id) ON DELETE CASCADE,
     team_id         TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     recipient_name  TEXT NOT NULL,
     recipient_kind  TEXT NOT NULL CHECK(recipient_kind IN ('worker', 'lead')),
     delivery_state  TEXT NOT NULL DEFAULT 'queued'
                       CHECK(delivery_state IN ('queued', 'wake_queued', 'claimed', 'injected', 'processed', 'failed')),
     claim_token     TEXT,
     claimed_at      INTEGER,
     not_before      INTEGER NOT NULL,
     attempt_count   INTEGER NOT NULL DEFAULT 0,
     last_error      TEXT,
     time_created    INTEGER NOT NULL,
     time_updated    INTEGER NOT NULL,
     PRIMARY KEY(message_id, recipient_name)
   );
   CREATE UNIQUE INDEX team_group_message_recipient_claim_idx
     ON team_group_message_recipient(claim_token) WHERE claim_token IS NOT NULL;
   CREATE INDEX team_group_message_recipient_ready_idx
     ON team_group_message_recipient(recipient_kind, delivery_state, not_before, time_created);
   CREATE INDEX team_group_message_recipient_team_idx
     ON team_group_message_recipient(team_id, delivery_state);

   CREATE TRIGGER team_group_insert_guard
   BEFORE INSERT ON team_group BEGIN
     SELECT CASE WHEN length(NEW.name) < 1 OR length(NEW.name) > 64
       OR NEW.name GLOB '*[^a-z0-9-]*' OR substr(NEW.name, 1, 1) = '-'
       OR substr(NEW.name, -1, 1) = '-'
       THEN RAISE(ABORT, 'invalid group name') END;
     SELECT CASE WHEN NEW.name IN ('lead', 'broadcast', 'all', 'team')
       THEN RAISE(ABORT, 'reserved group name') END;
     SELECT CASE WHEN EXISTS (
       SELECT 1 FROM team_member WHERE team_id = NEW.team_id AND name = NEW.name
     ) THEN RAISE(ABORT, 'group name collides with worker') END;
   END;
   CREATE TRIGGER team_group_immutable
   BEFORE UPDATE ON team_group
   WHEN NEW.id <> OLD.id OR NEW.team_id <> OLD.team_id OR NEW.name <> OLD.name
     OR NEW.created_by <> OLD.created_by OR NEW.time_created <> OLD.time_created
     OR NOT (OLD.sealed = 0 AND NEW.sealed = 1)
   BEGIN
     SELECT RAISE(ABORT, 'group names are immutable');
   END;
   CREATE TRIGGER team_group_seal_guard
   BEFORE UPDATE OF sealed ON team_group WHEN OLD.sealed = 0 AND NEW.sealed = 1 BEGIN
     SELECT CASE WHEN (SELECT COUNT(*) FROM team_group_participant p
       WHERE p.team_id = OLD.team_id AND p.group_id = OLD.id) < 2
       THEN RAISE(ABORT, 'group requires at least two participants') END;
     SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM team_group_participant p
       WHERE p.team_id = OLD.team_id AND p.group_id = OLD.id AND p.participant_name = OLD.created_by)
       THEN RAISE(ABORT, 'group creator must be a participant') END;
   END;
   CREATE TRIGGER team_group_delete_guard
   BEFORE DELETE ON team_group
   WHEN EXISTS (SELECT 1 FROM team WHERE id = OLD.team_id)
   BEGIN
     SELECT RAISE(ABORT, 'group deletion is not supported');
   END;
   CREATE TRIGGER team_group_participant_insert_guard
   BEFORE INSERT ON team_group_participant BEGIN
     SELECT CASE WHEN NOT EXISTS (
       SELECT 1 FROM team_group g WHERE g.team_id = NEW.team_id AND g.id = NEW.group_id AND g.sealed = 0
     ) THEN RAISE(ABORT, 'group membership is immutable or belongs to another team') END;
     SELECT CASE WHEN NEW.participant_name <> 'lead' AND NOT EXISTS (
       SELECT 1 FROM team_member m WHERE m.team_id = NEW.team_id AND m.name = NEW.participant_name
         AND m.member_kind = 'worker' AND m.status IN ('ready', 'busy')
     ) THEN RAISE(ABORT, 'group participant is not an active worker') END;
   END;
   CREATE TRIGGER team_group_participant_immutable_update
   BEFORE UPDATE ON team_group_participant BEGIN
     SELECT RAISE(ABORT, 'group membership is immutable');
   END;
   CREATE TRIGGER team_group_participant_immutable_delete
   BEFORE DELETE ON team_group_participant
   WHEN EXISTS (SELECT 1 FROM team_group WHERE id = OLD.group_id)
   BEGIN
     SELECT RAISE(ABORT, 'group membership is immutable');
   END;
   CREATE TRIGGER team_member_group_name_insert_guard
   BEFORE INSERT ON team_member WHEN NEW.member_kind = 'worker' BEGIN
     SELECT CASE WHEN EXISTS (
       SELECT 1 FROM team_group WHERE team_id = NEW.team_id AND name = NEW.name
     ) THEN RAISE(ABORT, 'worker name collides with group') END;
   END;
   CREATE TRIGGER team_member_group_name_update_guard
   BEFORE UPDATE OF name, member_kind ON team_member WHEN NEW.member_kind = 'worker' BEGIN
     SELECT CASE WHEN EXISTS (
       SELECT 1 FROM team_group WHERE team_id = NEW.team_id AND name = NEW.name
     ) THEN RAISE(ABORT, 'worker name collides with group') END;
   END;
   CREATE TRIGGER team_message_destination_insert_guard
   BEFORE INSERT ON team_message BEGIN
     SELECT CASE WHEN NEW.group_id IS NOT NULL AND NEW.to_name IS NOT NULL
       THEN RAISE(ABORT, 'message has multiple destinations') END;
     SELECT CASE WHEN NEW.group_id IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM team_group g WHERE g.id = NEW.group_id AND g.team_id = NEW.team_id AND g.sealed = 1
     ) THEN RAISE(ABORT, 'message group belongs to another team') END;
   END;
   CREATE TRIGGER team_message_destination_update_guard
   BEFORE UPDATE OF team_id, to_name, group_id ON team_message BEGIN
     SELECT CASE WHEN NEW.group_id IS NOT NULL AND NEW.to_name IS NOT NULL
       THEN RAISE(ABORT, 'message has multiple destinations') END;
     SELECT CASE WHEN NEW.group_id IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM team_group g WHERE g.id = NEW.group_id AND g.team_id = NEW.team_id AND g.sealed = 1
     ) THEN RAISE(ABORT, 'message group belongs to another team') END;
   END;
   CREATE TRIGGER team_group_message_recipient_insert_guard
   BEFORE INSERT ON team_group_message_recipient BEGIN
     SELECT CASE WHEN NOT EXISTS (
       SELECT 1 FROM team_message m
       JOIN team_group_participant p ON p.team_id = m.team_id AND p.group_id = m.group_id
       WHERE m.id = NEW.message_id AND m.team_id = NEW.team_id AND m.group_id IS NOT NULL
         AND p.participant_name = NEW.recipient_name
     ) THEN RAISE(ABORT, 'group recipient does not match message destination') END;
     SELECT CASE WHEN (NEW.recipient_kind = 'lead') <> (NEW.recipient_name = 'lead')
       THEN RAISE(ABORT, 'group recipient kind mismatch') END;
     SELECT CASE WHEN NEW.recipient_kind = 'worker' AND NOT EXISTS (
       SELECT 1 FROM team_member m WHERE m.team_id = NEW.team_id AND m.name = NEW.recipient_name
         AND m.member_kind = 'worker' AND m.status IN ('ready', 'busy') AND m.reported_to_lead = 0
     ) THEN RAISE(ABORT, 'group worker recipient is not eligible') END;
   END;`,
  // Migration 16: Add a hidden Annalist member kind and durable one-per-task annal events.
  `DROP TRIGGER team_group_insert_guard;
   DROP TRIGGER team_group_participant_insert_guard;
   DROP TRIGGER team_group_message_recipient_insert_guard;
   DROP TRIGGER team_member_group_name_insert_guard;
   DROP TRIGGER team_member_group_name_update_guard;

   CREATE TABLE team_member_m16 (
     team_id          TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     name             TEXT NOT NULL,
     session_id       TEXT NOT NULL,
     agent            TEXT NOT NULL,
     status           TEXT NOT NULL DEFAULT 'ready'
                        CHECK(status IN ('ready', 'busy', 'shutdown_requested', 'shutdown', 'error')),
     execution_status TEXT NOT NULL DEFAULT 'idle'
                        CHECK(execution_status IN ('idle', 'starting', 'running',
                          'cancel_requested', 'cancelling', 'cancelled',
                          'completing', 'completed', 'failed', 'timed_out')),
     model            TEXT,
     prompt           TEXT,
     time_created     INTEGER NOT NULL,
     time_updated     INTEGER NOT NULL,
     worktree_dir     TEXT,
     worktree_branch  TEXT,
     plan_approval    TEXT NOT NULL DEFAULT 'none'
                        CHECK(plan_approval IN ('none', 'pending', 'approved', 'rejected')),
     workspace_id     TEXT,
     reported_to_lead INTEGER NOT NULL DEFAULT 0,
     last_nudged_at   INTEGER,
     retry_until      INTEGER,
     retry_attempt    INTEGER,
     retry_provider   TEXT,
     retry_message    TEXT,
     member_kind      TEXT NOT NULL DEFAULT 'worker'
                        CHECK(member_kind IN ('worker', 'supervisor', 'annalist')),
     PRIMARY KEY (team_id, name)
   );
   INSERT INTO team_member_m16
     (team_id, name, session_id, agent, status, execution_status, model, prompt,
      time_created, time_updated, worktree_dir, worktree_branch, plan_approval,
      workspace_id, reported_to_lead, last_nudged_at, retry_until, retry_attempt,
      retry_provider, retry_message, member_kind)
     SELECT team_id, name, session_id, agent, status, execution_status, model, prompt,
      time_created, time_updated, worktree_dir, worktree_branch, plan_approval,
      workspace_id, reported_to_lead, last_nudged_at, retry_until, retry_attempt,
      retry_provider, retry_message, member_kind FROM team_member;
   DROP TABLE team_member;
   ALTER TABLE team_member_m16 RENAME TO team_member;

   CREATE INDEX team_member_session_idx ON team_member(session_id);
   CREATE INDEX team_member_status_idx ON team_member(team_id, status);
   CREATE UNIQUE INDEX team_member_one_supervisor_idx ON team_member(team_id) WHERE member_kind = 'supervisor';
   CREATE UNIQUE INDEX team_member_one_annalist_idx ON team_member(team_id) WHERE member_kind = 'annalist';

   CREATE TRIGGER team_member_group_name_insert_guard
   BEFORE INSERT ON team_member WHEN NEW.member_kind = 'worker' BEGIN
     SELECT CASE WHEN EXISTS (
       SELECT 1 FROM team_group WHERE team_id = NEW.team_id AND name = NEW.name
     ) THEN RAISE(ABORT, 'worker name collides with group') END;
   END;
   CREATE TRIGGER team_member_group_name_update_guard
   BEFORE UPDATE OF name, member_kind ON team_member WHEN NEW.member_kind = 'worker' BEGIN
     SELECT CASE WHEN EXISTS (
       SELECT 1 FROM team_group WHERE team_id = NEW.team_id AND name = NEW.name
     ) THEN RAISE(ABORT, 'worker name collides with group') END;
   END;

   CREATE TRIGGER team_group_insert_guard
   BEFORE INSERT ON team_group BEGIN
     SELECT CASE WHEN length(NEW.name) < 1 OR length(NEW.name) > 64
       OR NEW.name GLOB '*[^a-z0-9-]*' OR substr(NEW.name, 1, 1) = '-'
       OR substr(NEW.name, -1, 1) = '-'
       THEN RAISE(ABORT, 'invalid group name') END;
     SELECT CASE WHEN NEW.name IN ('lead', 'broadcast', 'all', 'team')
       THEN RAISE(ABORT, 'reserved group name') END;
     SELECT CASE WHEN EXISTS (
       SELECT 1 FROM team_member WHERE team_id = NEW.team_id AND name = NEW.name
     ) THEN RAISE(ABORT, 'group name collides with worker') END;
   END;
   CREATE TRIGGER team_group_participant_insert_guard
   BEFORE INSERT ON team_group_participant BEGIN
     SELECT CASE WHEN NOT EXISTS (
       SELECT 1 FROM team_group g WHERE g.team_id = NEW.team_id AND g.id = NEW.group_id AND g.sealed = 0
     ) THEN RAISE(ABORT, 'group membership is immutable or belongs to another team') END;
     SELECT CASE WHEN NEW.participant_name <> 'lead' AND NOT EXISTS (
       SELECT 1 FROM team_member m WHERE m.team_id = NEW.team_id AND m.name = NEW.participant_name
         AND m.member_kind = 'worker' AND m.status IN ('ready', 'busy')
     ) THEN RAISE(ABORT, 'group participant is not an active worker') END;
   END;
   CREATE TRIGGER team_group_message_recipient_insert_guard
   BEFORE INSERT ON team_group_message_recipient BEGIN
     SELECT CASE WHEN NOT EXISTS (
       SELECT 1 FROM team_message m
       JOIN team_group_participant p ON p.team_id = m.team_id AND p.group_id = m.group_id
       WHERE m.id = NEW.message_id AND m.team_id = NEW.team_id AND m.group_id IS NOT NULL
         AND p.participant_name = NEW.recipient_name
     ) THEN RAISE(ABORT, 'group recipient does not match message destination') END;
     SELECT CASE WHEN (NEW.recipient_kind = 'lead') <> (NEW.recipient_name = 'lead')
       THEN RAISE(ABORT, 'group recipient kind mismatch') END;
     SELECT CASE WHEN NEW.recipient_kind = 'worker' AND NOT EXISTS (
       SELECT 1 FROM team_member m WHERE m.team_id = NEW.team_id AND m.name = NEW.recipient_name
         AND m.member_kind = 'worker' AND m.status IN ('ready', 'busy') AND m.reported_to_lead = 0
     ) THEN RAISE(ABORT, 'group worker recipient is not eligible') END;
   END;

   CREATE TABLE team_task_annal (
     task_id        TEXT PRIMARY KEY REFERENCES team_task(id) ON DELETE CASCADE,
     team_id        TEXT NOT NULL REFERENCES team(id) ON DELETE CASCADE,
     completed_by   TEXT NOT NULL,
     time_completed INTEGER NOT NULL,
     wake_id        TEXT REFERENCES scheduler_wake(id) ON DELETE SET NULL
   );
   CREATE INDEX team_task_annal_pending_idx ON team_task_annal(team_id, wake_id, time_completed);`,
  // Migration 17: Distinguish accepted prompt injection from observed execution.
  `ALTER TABLE scheduler_run_lease ADD COLUMN started_at INTEGER;`,
]

/**
 * Apply pending migrations to the database.
 * Uses PRAGMA user_version to track which migrations have been applied.
 */
export function applyMigrations(db: Database): void {
  while (true) {
    const { user_version: observedVersion } = db.query("PRAGMA user_version").get() as { user_version: number }
    if (observedVersion > MIGRATIONS.length) {
      throw new Error(`Database schema version ${observedVersion} is newer than this plugin supports (${MIGRATIONS.length}). Upgrade opencode-ensemble before continuing.`)
    }
    if (observedVersion === MIGRATIONS.length) return
    const { foreign_keys: foreignKeys } = db.query("PRAGMA foreign_keys").get() as { foreign_keys: number }

    db.exec("PRAGMA foreign_keys = OFF")
    db.exec("BEGIN IMMEDIATE")
    try {
      const { user_version: lockedVersion } = db.query("PRAGMA user_version").get() as { user_version: number }
      if (lockedVersion > MIGRATIONS.length) {
        throw new Error(`Database schema version ${lockedVersion} is newer than this plugin supports (${MIGRATIONS.length}). Upgrade opencode-ensemble before continuing.`)
      }
      if (lockedVersion === MIGRATIONS.length) {
        db.exec("COMMIT")
        return
      }
      const migration = MIGRATIONS[lockedVersion]
      if (migration) db.exec(migration)
      db.exec(`PRAGMA user_version = ${lockedVersion + 1}`)
      db.exec("COMMIT")
    } catch (err) {
      db.exec("ROLLBACK")
      throw err
    } finally {
      db.exec(`PRAGMA foreign_keys = ${foreignKeys ? "ON" : "OFF"}`)
    }
  }
}

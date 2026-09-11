import { beforeEach, describe, expect, test } from "bun:test"
import { setupDeps, insertMember, insertTeam } from "./helpers"
import { executeTeamBroadcast } from "../src/tools/team-broadcast"
import { executeTeamResults } from "../src/tools/team-results"
import { shouldNudgeIdleMember } from "../src/hooks"
import { executeTeamSpawn } from "../src/tools/team-spawn"
import { buildSupervisorReviewPrompt } from "../src/supervisor"
import { sendGroupMessage } from "../src/groups"
import { tryAcquireRun } from "../src/scheduler"
import { Database } from "bun:sqlite"
import { applyMigrations, MIGRATIONS } from "../src/schema"
import { mkdtempSync, rmSync } from "node:fs"
import { join } from "node:path"
import { tmpdir } from "node:os"

describe("durable group inboxes", () => {
  let deps: ReturnType<typeof setupDeps>

  beforeEach(() => {
    deps = setupDeps()
    insertTeam(deps.db, "t1", "team-one", "lead-sess")
    insertMember(deps.db, "t1", "alice", "sess-alice")
    insertMember(deps.db, "t1", "bob", "sess-bob")
    insertMember(deps.db, "t1", "carol", "sess-carol")
    deps.registry.register("t1", "alice", "sess-alice")
    deps.registry.register("t1", "bob", "sess-bob")
    deps.registry.register("t1", "carol", "sess-carol")
  })

  test("migration 15 creates normalized group storage and a group message destination", () => {
    const tables = deps.db.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'team_group%' ORDER BY name").all()
    expect(tables).toEqual([
      { name: "team_group" },
      { name: "team_group_message_recipient" },
      { name: "team_group_participant" },
    ])
    const columns = deps.db.query("PRAGMA table_info(team_message)").all() as Array<{ name: string }>
    expect(columns.some(column => column.name === "group_id")).toBe(true)
  })

  test("migration 15 preserves legacy destinations and enforces team-scoped group routing", () => {
    const db = new Database(":memory:")
    for (let index = 0; index < 14; index++) {
      db.exec(MIGRATIONS[index]!)
      db.exec(`PRAGMA user_version = ${index + 1}`)
    }
    db.run("INSERT INTO team (id, name, project_id, lead_session_id, status, delegate, time_created, time_updated) VALUES ('legacy', 'legacy', 'default', 'lead-old', 'active', 0, 1, 1)")
    db.run("INSERT INTO team_message (id, team_id, from_name, to_name, content, delivered, read, delivery_state, time_created) VALUES ('old-direct', 'legacy', 'alice', 'lead', 'old', 1, 1, 'processed', 1)")
    applyMigrations(db)
    expect((db.query("PRAGMA user_version").get() as { user_version: number }).user_version).toBe(MIGRATIONS.length)
    expect(db.query("SELECT to_name, group_id FROM team_message WHERE id = 'old-direct'").get()).toEqual({ to_name: "lead", group_id: null })
    db.run("INSERT INTO team (id, name, project_id, lead_session_id, status, delegate, time_created, time_updated) VALUES ('other', 'other', 'default', 'lead-other', 'active', 0, 1, 1)")
    db.run("INSERT INTO team_member (team_id, name, session_id, agent, time_created, time_updated) VALUES ('legacy', 'alice', 'legacy-alice', 'build', 1, 1)")
    db.run("INSERT INTO team_group (id, team_id, name, created_by, time_created) VALUES ('g1', 'legacy', 'architects', 'lead', 1)")
    db.run("INSERT INTO team_group_participant (team_id, group_id, participant_name, time_created) VALUES ('legacy', 'g1', 'lead', 1), ('legacy', 'g1', 'alice', 1)")
    db.run("UPDATE team_group SET sealed = 1 WHERE id = 'g1'")
    expect(() => db.run("INSERT INTO team_message (id, team_id, from_name, to_name, group_id, content, time_created) VALUES ('bad-team', 'other', 'lead', NULL, 'g1', 'bad', 1)")).toThrow(/another team/)
    expect(() => db.run("INSERT INTO team_message (id, team_id, from_name, to_name, group_id, content, time_created) VALUES ('bad-class', 'legacy', 'lead', 'alice', 'g1', 'bad', 1)")).toThrow(/multiple destinations/)
    db.close()
  })

  test("creates a group and first message atomically, delivering only to active participants", async () => {
    const result = await executeTeamBroadcast(deps, { text: "design", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    expect(result).toContain("architects")
    expect(deps.db.query("SELECT name, created_by FROM team_group WHERE team_id = 't1'").get()).toEqual({ name: "architects", created_by: "alice" })
    expect(deps.db.query("SELECT participant_name FROM team_group_participant WHERE team_id = 't1' ORDER BY participant_name").all()).toEqual([
      { participant_name: "alice" },
      { participant_name: "bob" },
    ])
    const message = deps.db.query("SELECT to_name, group_id FROM team_message WHERE team_id = 't1'").get() as { to_name: string | null; group_id: string | null }
    expect(message.to_name).toBeNull()
    expect(message.group_id).not.toBeNull()
    const wakes = deps.db.query("SELECT member_name FROM scheduler_wake WHERE team_id = 't1'").all()
    expect(wakes).toEqual([{ member_name: "bob" }])
    await Bun.sleep(0)
    const prompt = deps.client.calls.find(call => call.method === "session.promptAsync")?.args[0] as { parts: Array<{ text: string }> }
    expect(prompt.parts[0]!.text).toBe("[Group architects from alice]: design")
  })

  test("validates creation, immutable membership, and member-only sending", async () => {
    await expect(executeTeamBroadcast(deps, { text: "bad", group: "architects", members: ["bob", "carol"] }, "sess-alice"))
      .rejects.toThrow(/creator|included/)
    await expect(executeTeamBroadcast(deps, { text: "bad", group: "architects", members: ["alice"] }, "sess-alice"))
      .rejects.toThrow(/two/)
    await executeTeamBroadcast(deps, { text: "first", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    const groupId = (deps.db.query("SELECT id FROM team_group WHERE team_id = 't1' AND name = 'architects'").get() as { id: string }).id
    expect(() => deps.db.run("INSERT INTO team_group_participant (team_id, group_id, participant_name, time_created) VALUES ('t1', ?, 'carol', 2)", [groupId])).toThrow(/immutable/)
    await expect(executeTeamBroadcast(deps, { text: "change", group: "architects", members: ["alice", "carol"] }, "sess-alice"))
      .rejects.toThrow(/membership|already exists/)
    await expect(executeTeamBroadcast(deps, { text: "intrude", group: "architects" }, "sess-carol"))
      .rejects.toThrow(/participant|member/)
  })

  test("reserves names, rejects worker collisions, and rejects members without group", async () => {
    for (const group of ["lead", "broadcast", "all", "team", "alice"]) {
      await expect(executeTeamBroadcast(deps, { text: "bad", group, members: ["alice", "bob"] }, "sess-alice")).rejects.toThrow()
    }
    await expect(executeTeamBroadcast(deps, { text: "bad", members: ["alice", "bob"] }, "sess-alice")).rejects.toThrow(/group/)
  })

  test("requires an eligible recipient and does not reawaken reported workers", async () => {
    deps.db.run("UPDATE team_member SET reported_to_lead = 1 WHERE team_id = 't1' AND name = 'bob'")
    await expect(executeTeamBroadcast(deps, { text: "first", group: "architects", members: ["alice", "bob"] }, "sess-alice"))
      .rejects.toThrow(/eligible recipient/)
    expect(deps.db.query("SELECT COUNT(*) AS count FROM team_group").get()).toEqual({ count: 0 })
  })

  test("lead receives group messages only as a participant and has no override", async () => {
    await executeTeamBroadcast(deps, { text: "first", group: "leaders", members: ["alice", "lead"] }, "sess-alice")
    expect(deps.client.calls.filter(call => call.method === "session.promptAsync")).toHaveLength(1)
    await expect(executeTeamBroadcast(deps, { text: "override", group: "leaders" }, "lead-sess")).resolves.toContain("leaders")
    await executeTeamBroadcast(deps, { text: "worker-only", group: "workers", members: ["alice", "bob"] }, "sess-alice")
    await expect(executeTeamBroadcast(deps, { text: "override", group: "workers" }, "lead-sess")).rejects.toThrow(/participant|member/)
  })

  test("list and history are visible to observers without mutating message state", async () => {
    await executeTeamBroadcast(deps, { text: "first", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    await Bun.sleep(0)
    const before = deps.db.query("SELECT delivered, read, delivery_state FROM team_message").get()
    const groups = await executeTeamResults(deps, { list_groups: true }, "sess-carol")
    expect(groups).toContain("architects")
    expect(groups).toContain("canSend: false")
    const history = await executeTeamResults(deps, { group: "architects", limit: 20 }, "sess-carol")
    expect(history).toContain("first")
    expect(deps.db.query("SELECT delivered, read, delivery_state FROM team_message").get()).toEqual(before)
  })

  test("rejects ambiguous results arguments and ordinary results exclude group messages", async () => {
    await executeTeamBroadcast(deps, { text: "group-only", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    await expect(executeTeamResults(deps, { list_groups: true, group: "architects" }, "sess-alice")).rejects.toThrow(/combine|ambiguous/)
    await expect(executeTeamResults(deps, { from: "alice", group: "architects" }, "sess-alice")).rejects.toThrow(/combine|ambiguous/)
    await expect(executeTeamResults(deps, { group: "architects", limit: 51 }, "sess-alice")).rejects.toThrow(/1.*50/)
    expect(await executeTeamResults(deps, {}, "sess-alice")).toBe("No unread messages.")
  })

  test("group sends do not satisfy the completion-report loop", async () => {
    await executeTeamBroadcast(deps, { text: "peer update", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    expect(shouldNudgeIdleMember(deps.db, "t1", "alice")).toBe(true)
  })

  test("enforces 10KB group messages", async () => {
    await expect(executeTeamBroadcast(deps, { text: "x".repeat(10 * 1024 + 1), group: "architects", members: ["alice", "bob"] }, "sess-alice"))
      .rejects.toThrow("10KB")
  })

  test("team deletion cascades groups, participants, messages, and wakes", async () => {
    await executeTeamBroadcast(deps, { text: "first", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    deps.db.run("DELETE FROM team WHERE id = 't1'")
    for (const table of ["team_group", "team_group_participant", "team_group_message_recipient", "team_message", "scheduler_message_wake"]) {
      expect(deps.db.query(`SELECT COUNT(*) AS count FROM ${table}`).get()).toEqual({ count: 0 })
    }
  })

  test("serializes concurrent duplicate creation", async () => {
    const settled = await Promise.allSettled([
      executeTeamBroadcast(deps, { text: "one", group: "architects", members: ["alice", "bob"] }, "sess-alice"),
      executeTeamBroadcast(deps, { text: "two", group: "architects", members: ["alice", "bob"] }, "sess-alice"),
    ])
    expect(settled.filter(result => result.status === "fulfilled")).toHaveLength(1)
    expect(settled.filter(result => result.status === "rejected")).toHaveLength(1)
    expect(deps.db.query("SELECT COUNT(*) AS count FROM team_group WHERE team_id = 't1'").get()).toEqual({ count: 1 })
    expect(deps.db.query("SELECT COUNT(*) AS count FROM team_message WHERE team_id = 't1'").get()).toEqual({ count: 1 })
  })

  test("registers independent recipient links and creates a successor behind an in-flight wake", () => {
    const first = sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "architects", members: ["alice", "bob", "carol"], content: "first", now: 10 })
    expect(deps.db.query("SELECT COUNT(*) AS count FROM scheduler_message_wake WHERE message_id = ?").get(first.messageId)).toEqual({ count: 2 })
    const bobWake = deps.db.query("SELECT id FROM scheduler_wake WHERE team_id = 't1' AND member_name = 'bob'").get() as { id: string }
    expect(tryAcquireRun(deps.db, bobWake.id, { global: 10, perAgent: {} }, 1_000, 11)).toMatchObject({ acquired: true })
    const second = sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "architects", content: "second", now: 12 })
    expect(deps.db.query("SELECT COUNT(*) AS count FROM scheduler_message_wake WHERE message_id = ?").get(second.messageId)).toEqual({ count: 2 })
    expect(deps.db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE team_id = 't1' AND member_name = 'bob'").get()).toEqual({ count: 2 })
  })

  test("shares atomic groups safely across database instances", () => {
    const directory = mkdtempSync(join(tmpdir(), "ensemble-groups-"))
    const path = join(directory, "ensemble.db")
    const first = new Database(path)
    const second = new Database(path)
    try {
      first.exec("PRAGMA foreign_keys=ON")
      second.exec("PRAGMA foreign_keys=ON")
      applyMigrations(first)
      insertTeam(first, "shared", "shared", "lead-shared")
      insertMember(first, "shared", "alice", "shared-alice")
      insertMember(first, "shared", "bob", "shared-bob")
      sendGroupMessage(first, { teamId: "shared", sender: "alice", group: "architects", members: ["alice", "bob"], content: "first" })
      sendGroupMessage(second, { teamId: "shared", sender: "bob", group: "architects", content: "second" })
      expect(first.query("SELECT content FROM team_message WHERE team_id = 'shared' ORDER BY time_created, id").all()).toHaveLength(2)
      expect(second.query("SELECT COUNT(*) AS count FROM team_group WHERE team_id = 'shared'").get()).toEqual({ count: 1 })
    } finally {
      first.close()
      second.close()
      rmSync(directory, { recursive: true, force: true })
    }
  })

  test("rejects spawning a worker whose name collides with a group", async () => {
    await executeTeamBroadcast(deps, { text: "first", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    await expect(executeTeamSpawn(deps, { name: "architects", prompt: "work", worktree: false }, "lead-sess"))
      .rejects.toThrow(/collides with group/)
    expect(deps.client.calls.some(call => call.method === "session.create")).toBe(false)
  })

  test("Supervisor cannot use groups but snapshots label group history", async () => {
    await executeTeamBroadcast(deps, { text: "first", group: "architects", members: ["alice", "bob"] }, "sess-alice")
    deps.db.run(
      "INSERT INTO team_member (team_id, name, session_id, agent, member_kind, status, execution_status, time_created, time_updated) VALUES ('t1', 'opencode-ensemble-supervisor', 'supervisor-session', 'opencode-ensemble-supervisor', 'supervisor', 'ready', 'idle', 1, 1)",
    )
    deps.registry.register("t1", "opencode-ensemble-supervisor", "supervisor-session")
    await expect(executeTeamBroadcast(deps, { text: "intrude", group: "architects" }, "supervisor-session")).rejects.toThrow(/Supervisor/)
    await expect(executeTeamResults(deps, { list_groups: true }, "supervisor-session")).rejects.toThrow(/ordinary workers/)
    expect(buildSupervisorReviewPrompt(deps.db, "t1", 1)).toContain("alice -> group:architects: first")
  })
})

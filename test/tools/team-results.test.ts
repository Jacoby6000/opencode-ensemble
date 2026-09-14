import { describe, test, expect, beforeEach } from "bun:test"
import { setupDeps, insertTeam, insertMember } from "../helpers"
import { executeTeamResults } from "../../src/tools/team-results"
import { sendMessage } from "../../src/messaging"
import { ANNALIST_AGENT, ANNALIST_MEMBER_NAME } from "../../src/annalist"
import { sendGroupMessage } from "../../src/groups"

describe("team_results", () => {
  let deps: ReturnType<typeof setupDeps>

  beforeEach(() => {
    deps = setupDeps()
    insertTeam(deps.db, "t1", "my-team", "lead-sess")
    insertMember(deps.db, "t1", "alice", "sess-alice")
    insertMember(deps.db, "t1", "bob", "sess-bob")
    deps.registry.register("t1", "alice", "sess-alice")
    deps.registry.register("t1", "bob", "sess-bob")
  })

  test("returns unread messages and marks them as read", async () => {
    // Insert messages via sendMessage, then mark delivered
    const msgId1 = sendMessage(deps.db, { teamId: "t1", from: "alice", to: "lead", content: "task done" })
    const msgId2 = sendMessage(deps.db, { teamId: "t1", from: "bob", to: "lead", content: "need help" })
    deps.db.run("UPDATE team_message SET delivered = 1 WHERE id IN (?, ?)", [msgId1, msgId2])

    const result = await executeTeamResults(deps, {}, "lead-sess")

    expect(result).toContain("alice")
    expect(result).toContain("task done")
    expect(result).toContain("bob")
    expect(result).toContain("need help")

    // Verify messages are now marked as read
    const unread = deps.db.query("SELECT COUNT(*) as cnt FROM team_message WHERE team_id = ? AND read = 0").get("t1") as { cnt: number }
    expect(unread.cnt).toBe(0)
  })

  test("returns 'No unread messages.' when none exist", async () => {
    const result = await executeTeamResults(deps, {}, "lead-sess")
    expect(result).toBe("No unread messages.")
  })

  test("filters by from_name when args.from is provided", async () => {
    const msgId1 = sendMessage(deps.db, { teamId: "t1", from: "alice", to: "lead", content: "from alice" })
    const msgId2 = sendMessage(deps.db, { teamId: "t1", from: "bob", to: "lead", content: "from bob" })
    deps.db.run("UPDATE team_message SET delivered = 1 WHERE id IN (?, ?)", [msgId1, msgId2])

    const result = await executeTeamResults(deps, { from: "alice" }, "lead-sess")

    expect(result).toContain("alice")
    expect(result).toContain("from alice")
    expect(result).not.toContain("from bob")

    // Only alice's message should be marked read
    const aliceRead = deps.db.query("SELECT read FROM team_message WHERE id = ?").get(msgId1) as { read: number }
    expect(aliceRead.read).toBe(1)
    const bobRead = deps.db.query("SELECT read FROM team_message WHERE id = ?").get(msgId2) as { read: number }
    expect(bobRead.read).toBe(0)
  })

  test("does not return already-read messages", async () => {
    const msgId = sendMessage(deps.db, { teamId: "t1", from: "alice", to: "lead", content: "old message" })
    deps.db.run("UPDATE team_message SET delivered = 1, read = 1 WHERE id = ?", [msgId])

    const result = await executeTeamResults(deps, {}, "lead-sess")
    expect(result).toBe("No unread messages.")
  })

  test("returns messages ordered by time_created ASC", async () => {
    // Insert with explicit time ordering
    const now = Date.now()
    deps.db.run(
      "INSERT INTO team_message (id, team_id, from_name, to_name, content, delivered, read, time_created) VALUES (?, ?, ?, ?, ?, 1, 0, ?)",
      ["msg-late", "t1", "bob", "lead", "second message", now + 100],
    )
    deps.db.run(
      "INSERT INTO team_message (id, team_id, from_name, to_name, content, delivered, read, time_created) VALUES (?, ?, ?, ?, ?, 1, 0, ?)",
      ["msg-early", "t1", "alice", "lead", "first message", now],
    )

    const result = await executeTeamResults(deps, {}, "lead-sess")

    const aliceIdx = result.indexOf("first message")
    const bobIdx = result.indexOf("second message")
    expect(aliceIdx).toBeLessThan(bobIdx)
  })

  test("rejects if session is not in a team", async () => {
    await expect(executeTeamResults(deps, {}, "random-sess"))
      .rejects.toThrow("not in a team")
  })

  test("lets the Annalist inspect any direct or broadcast mailbox without consuming read state", async () => {
    deps.db.run(
      "INSERT INTO team_member (team_id, name, session_id, agent, member_kind, status, execution_status, time_created, time_updated) VALUES ('t1', ?, 'annalist-session', ?, 'annalist', 'ready', 'idle', 1, 1)",
      [ANNALIST_MEMBER_NAME, ANNALIST_AGENT],
    )
    deps.registry.register("t1", ANNALIST_MEMBER_NAME, "annalist-session")
    const direct = sendMessage(deps.db, { teamId: "t1", from: "alice", to: "bob", content: "cross-agent context" })
    deps.db.run(
      "INSERT INTO team_message (id, team_id, from_name, to_name, content, delivered, read, delivery_state, time_created) VALUES ('broadcast-1', 't1', 'lead', NULL, 'whole-team context', 1, 0, 'injected', 2)",
    )
    sendGroupMessage(deps.db, {
      teamId: "t1",
      sender: "alice",
      group: "builders",
      members: ["alice", "bob"],
      content: "group-only context",
      now: 3,
    })

    const mailbox = await executeTeamResults(deps, { mailbox: "bob", limit: 20 }, "annalist-session")
    const broadcast = await executeTeamResults(deps, { mailbox: "broadcast", limit: 20 }, "annalist-session")
    const groups = await executeTeamResults(deps, { list_groups: true }, "annalist-session")
    const group = await executeTeamResults(deps, { group: "builders", limit: 20 }, "annalist-session")

    expect(mailbox).toContain("alice -> bob")
    expect(mailbox).toContain("cross-agent context")
    expect(broadcast).toContain("whole-team context")
    expect(groups).toContain("builders")
    expect(groups).toContain("canSend: false")
    expect(group).toContain("group-only context")
    expect(deps.db.query("SELECT read FROM team_message WHERE id = ?").get(direct)).toEqual({ read: 0 })
    expect(deps.db.query("SELECT read FROM team_message WHERE id = 'broadcast-1'").get()).toEqual({ read: 0 })
  })

  test("rejects arbitrary mailbox inspection by ordinary teammates", async () => {
    await expect(executeTeamResults(deps, { mailbox: "bob" }, "sess-alice"))
      .rejects.toThrow("Annalist")
  })
})

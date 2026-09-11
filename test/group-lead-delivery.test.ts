import { beforeEach, describe, expect, test } from "bun:test"
import { DEFAULT_CONFIG } from "../src/config"
import { claimGroupLeadDeliveries, markGroupLeadDeliveryInjected, sendGroupMessage } from "../src/groups"
import { getLeadPromptOptions } from "../src/member-model"
import { DurableScheduler } from "../src/scheduler-runtime"
import { refreshMessageDeliveryAggregate, terminateMemberScheduling } from "../src/scheduler"
import { executeTeamBroadcast } from "../src/tools/team-broadcast"
import { insertMember, insertTeam, mockClient, setupDeps } from "./helpers"

const flush = async () => {
  await Bun.sleep(0)
  await Bun.sleep(0)
}

describe("durable group lead delivery", () => {
  let deps: ReturnType<typeof setupDeps>

  beforeEach(() => {
    deps = setupDeps()
    insertTeam(deps.db, "t1", "team-one", "lead-sess")
    insertMember(deps.db, "t1", "alice", "sess-alice")
    insertMember(deps.db, "t1", "bob", "sess-bob")
    deps.registry.register("t1", "alice", "sess-alice")
    deps.registry.register("t1", "bob", "sess-bob")
  })

  test("atomically registers and accepts a lead-only eligible delivery", async () => {
    deps.db.run("UPDATE team SET lead_agent = 'solutions-architect', lead_model = 'openrouter/openai/gpt-5' WHERE id = 't1'")
    sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "leaders", members: ["alice", "lead"], content: "lead only" })
    const recipient = deps.db.query(
      "SELECT recipient_name, recipient_kind, delivery_state, attempt_count FROM team_group_message_recipient",
    ).get()
    expect(recipient).toEqual({ recipient_name: "lead", recipient_kind: "lead", delivery_state: "queued", attempt_count: 0 })

    deps.scheduler.kick()
    await flush()

    expect(deps.client.calls.filter(call => call.method === "session.promptAsync")).toHaveLength(1)
    const options = deps.client.calls.find(call => call.method === "session.promptAsync")!.args[0] as { agent?: string; model?: unknown; parts: Array<{ text: string }> }
    expect(options).toMatchObject({
      agent: "solutions-architect",
      model: { providerID: "openrouter", modelID: "openai/gpt-5" },
    })
    expect(options.parts[0]!.text).toBe("[Group leaders from alice]: lead only")
    expect(deps.db.query("SELECT delivery_state, claim_token FROM team_group_message_recipient").get()).toEqual({ delivery_state: "injected", claim_token: null })
    expect(deps.db.query("SELECT delivered, delivery_state FROM team_message").get()).toEqual({ delivered: 1, delivery_state: "injected" })
  })

  test("requeues a rejected lead prompt and retries it during maintenance", async () => {
    let attempts = 0
    deps.client.session.promptAsync = async options => {
      deps.client.calls.push({ method: "session.promptAsync", args: [options] })
      attempts++
      if (attempts === 1) throw new Error("lead offline")
      return {}
    }
    deps.scheduler = new DurableScheduler(deps.db, deps.client, { ...DEFAULT_CONFIG.scheduler, pumpIntervalMs: 1 }, true, "/tmp/test-project")

    await executeTeamBroadcast(deps, { text: "retry me", group: "leaders", members: ["alice", "lead"] }, "sess-alice")
    await flush()
    expect(deps.db.query("SELECT delivery_state, last_error FROM team_group_message_recipient").get()).toEqual({ delivery_state: "queued", last_error: "lead offline" })

    await Bun.sleep(2)
    await deps.scheduler.recover()
    await flush()

    expect(attempts).toBe(2)
    expect(deps.db.query("SELECT delivery_state, attempt_count FROM team_group_message_recipient").get()).toEqual({ delivery_state: "injected", attempt_count: 2 })
  })

  test("restart recovery dispatches a previously queued lead recipient", async () => {
    sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "leaders", members: ["alice", "lead"], content: "after restart" })
    const client = mockClient()
    const scheduler = new DurableScheduler(deps.db, client, DEFAULT_CONFIG.scheduler, true, "/tmp/test-project")

    await scheduler.recover()
    await flush()

    expect(client.calls.filter(call => call.method === "session.promptAsync")).toHaveLength(1)
    expect(deps.db.query("SELECT delivery_state FROM team_group_message_recipient").get()).toEqual({ delivery_state: "injected" })
  })

  test("concurrent maintenance claims one lead delivery only once", async () => {
    sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "leaders", members: ["alice", "lead"], content: "once" })
    const client = mockClient()
    client.session.promptAsync = options => {
      client.calls.push({ method: "session.promptAsync", args: [options] })
      return new Promise(() => {})
    }
    const first = new DurableScheduler(deps.db, client, DEFAULT_CONFIG.scheduler, true, "/tmp/test-project")
    const second = new DurableScheduler(deps.db, client, DEFAULT_CONFIG.scheduler, true, "/tmp/test-project")

    await Promise.all([first.recover(), second.recover()])
    await flush()

    expect(client.calls.filter(call => call.method === "session.promptAsync")).toHaveLength(1)
    expect(deps.db.query("SELECT delivery_state, attempt_count FROM team_group_message_recipient").get()).toEqual({ delivery_state: "claimed", attempt_count: 1 })
  })

  test("expires stale claims and fences late completion from the prior owner", () => {
    const sent = sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "leaders", members: ["alice", "lead"], content: "fenced", now: 1 })
    const first = claimGroupLeadDeliveries(deps.db, "/tmp/test-project", 10, 1, 1)[0]!

    expect(claimGroupLeadDeliveries(deps.db, "/tmp/test-project", 10, 1, 10)).toEqual([])
    const second = claimGroupLeadDeliveries(deps.db, "/tmp/test-project", 10, 1, 11)[0]!

    expect(second.claimToken).not.toBe(first.claimToken)
    expect(markGroupLeadDeliveryInjected(deps.db, sent.messageId, first.claimToken, 12)).toBe(false)
    expect(markGroupLeadDeliveryInjected(deps.db, sent.messageId, second.claimToken, 12)).toBe(true)
    expect(deps.db.query("SELECT delivery_state, attempt_count FROM team_group_message_recipient").get()).toEqual({ delivery_state: "injected", attempt_count: 2 })
  })

  test("worker success and lead failure remain independently represented", async () => {
    deps.client.session.promptAsync = async options => {
      deps.client.calls.push({ method: "session.promptAsync", args: [options] })
      if (options.sessionID === "lead-sess") throw new Error("lead offline")
      return {}
    }
    deps.scheduler = new DurableScheduler(deps.db, deps.client, { ...DEFAULT_CONFIG.scheduler, pumpIntervalMs: 1 }, true, "/tmp/test-project")

    await executeTeamBroadcast(deps, { text: "mixed", group: "mixed", members: ["alice", "bob", "lead"] }, "sess-alice")
    await flush()
    deps.scheduler.onSessionStatus("sess-bob", "idle")

    expect(deps.db.query("SELECT recipient_name, delivery_state FROM team_group_message_recipient ORDER BY recipient_name").all()).toEqual([
      { recipient_name: "bob", delivery_state: "processed" },
      { recipient_name: "lead", delivery_state: "queued" },
    ])
    expect(deps.db.query("SELECT delivered, delivery_state FROM team_message").get()).toEqual({ delivered: 1, delivery_state: "wake_queued" })
  })

  test("lead success and worker failure remain independently represented", async () => {
    await executeTeamBroadcast(deps, { text: "mixed", group: "mixed", members: ["alice", "bob", "lead"] }, "sess-alice")
    await flush()
    terminateMemberScheduling(deps.db, "t1", "bob", "worker failed")

    expect(deps.db.query("SELECT recipient_name, delivery_state FROM team_group_message_recipient ORDER BY recipient_name").all()).toEqual([
      { recipient_name: "bob", delivery_state: "failed" },
      { recipient_name: "lead", delivery_state: "injected" },
    ])
    expect(deps.db.query("SELECT delivered, delivery_state FROM team_message").get()).toEqual({ delivered: 1, delivery_state: "injected" })
  })

  test("derives mixed queued, successful, processed, and failed aggregate states deterministically", () => {
    const sent = sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "mixed", members: ["alice", "bob", "lead"], content: "aggregate" })
    expect(deps.db.query("SELECT delivered, delivery_state FROM team_message WHERE id = ?").get(sent.messageId)).toEqual({ delivered: 0, delivery_state: "wake_queued" })

    deps.db.run("UPDATE team_group_message_recipient SET delivery_state = 'injected' WHERE message_id = ? AND recipient_name = 'bob'", [sent.messageId])
    refreshMessageDeliveryAggregate(deps.db, sent.messageId)
    expect(deps.db.query("SELECT delivered, delivery_state FROM team_message WHERE id = ?").get(sent.messageId)).toEqual({ delivered: 1, delivery_state: "wake_queued" })

    deps.db.run("UPDATE team_group_message_recipient SET delivery_state = 'processed' WHERE message_id = ? AND recipient_name = 'bob'", [sent.messageId])
    deps.db.run("UPDATE team_group_message_recipient SET delivery_state = 'failed' WHERE message_id = ? AND recipient_name = 'lead'", [sent.messageId])
    refreshMessageDeliveryAggregate(deps.db, sent.messageId)
    expect(deps.db.query("SELECT delivered, delivery_state FROM team_message WHERE id = ?").get(sent.messageId)).toEqual({ delivered: 1, delivery_state: "injected" })

    deps.db.run("UPDATE team_group_message_recipient SET delivery_state = 'failed' WHERE message_id = ?", [sent.messageId])
    refreshMessageDeliveryAggregate(deps.db, sent.messageId)
    expect(deps.db.query("SELECT delivered, delivery_state FROM team_message WHERE id = ?").get(sent.messageId)).toEqual({ delivered: 0, delivery_state: "failed" })
  })

  test("project-scoped maintenance never dispatches another project's lead recipients", async () => {
    deps.db.run("INSERT INTO project (id, name, path, status, time_created, time_updated) VALUES ('/other', 'other', '/other', 'active', 1, 1)")
    deps.db.run("INSERT INTO team (id, name, project_id, lead_session_id, status, delegate, time_created, time_updated) VALUES ('t2', 'team-two', '/other', 'lead-other', 'active', 0, 1, 1)")
    insertMember(deps.db, "t2", "carol", "sess-carol")
    sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "leaders-one", members: ["alice", "lead"], content: "one" })
    sendGroupMessage(deps.db, { teamId: "t2", sender: "carol", group: "leaders-two", members: ["carol", "lead"], content: "two" })
    const client = mockClient()
    const scheduler = new DurableScheduler(deps.db, client, DEFAULT_CONFIG.scheduler, true, "/tmp/test-project")

    await scheduler.recover()
    await flush()

    const calls = client.calls.filter(call => call.method === "session.promptAsync").map(call => (call.args[0] as { sessionID: string }).sessionID)
    expect(calls).toEqual(["lead-sess"])
    expect(deps.db.query("SELECT r.delivery_state FROM team_group_message_recipient r JOIN team_message m ON m.id = r.message_id WHERE m.team_id = 't2'").get()).toEqual({ delivery_state: "queued" })
  })

  test("recipient rows cascade with group and team purge", () => {
    const sent = sendGroupMessage(deps.db, { teamId: "t1", sender: "alice", group: "mixed", members: ["alice", "bob", "lead"], content: "purge" })
    expect(deps.db.query("SELECT COUNT(*) AS count FROM team_group_message_recipient WHERE message_id = ?").get(sent.messageId)).toEqual({ count: 2 })
    deps.db.run("DELETE FROM team WHERE id = 't1'")
    expect(deps.db.query("SELECT COUNT(*) AS count FROM team_group_message_recipient").get()).toEqual({ count: 0 })
  })

  test("lead prompt identity helper remains the durable source", () => {
    deps.db.run("UPDATE team SET lead_agent = 'architect', lead_model = 'provider/model' WHERE id = 't1'")
    expect(getLeadPromptOptions(deps.db, "t1")).toEqual({ agent: "architect", model: { providerID: "provider", modelID: "model" } })
  })
})

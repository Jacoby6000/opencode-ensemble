import { describe, expect, test, beforeEach } from "bun:test"
import { sendIdleWithoutReportNudge, sendPeerMessageFlush } from "../src/idle-continuations"
import { insertMember, insertTeam, setupDeps } from "./helpers"

describe("idle teammate continuations", () => {
  let deps: ReturnType<typeof setupDeps>

  beforeEach(() => {
    deps = setupDeps()
    insertTeam(deps.db, "t1", "my-team", "lead-sess")
    insertMember(deps.db, "t1", "alice", "sess-alice")
    deps.db.run(
      "UPDATE team_member SET agent = ?, model = ? WHERE team_id = ? AND name = ?",
      ["idle-specialist", "openrouter/anthropic/claude-sonnet", "t1", "alice"],
    )
  })

  test("idle-without-report nudge preserves the teammate's custom agent and model", async () => {
    sendIdleWithoutReportNudge(deps.scheduler, deps.db, "t1", "alice", "sess-alice")
    await Bun.sleep(0)

    const options = deps.client.calls.find(c => c.method === "session.promptAsync")!.args[0] as {
      agent?: string
      model?: { providerID: string; modelID: string }
    }
    expect(options.agent).toBe("idle-specialist")
    expect(options.model).toEqual({ providerID: "openrouter", modelID: "anthropic/claude-sonnet" })
  })

  test("peer flush preserves the teammate's custom agent and model", () => {
    sendPeerMessageFlush(deps.client, deps.db, "t1", "alice", "sess-alice", 2)

    const options = deps.client.calls.find(c => c.method === "session.promptAsync")!.args[0] as {
      agent?: string
      model?: { providerID: string; modelID: string }
    }
    expect(options.agent).toBe("idle-specialist")
    expect(options.model).toEqual({ providerID: "openrouter", modelID: "anthropic/claude-sonnet" })
  })

  test("both idle continuations remain fire-and-forget", () => {
    deps.client.session.promptAsync = () => new Promise(() => { /* never settles */ })

    expect(() => sendIdleWithoutReportNudge(deps.scheduler, deps.db, "t1", "alice", "sess-alice")).not.toThrow()
    expect(() => sendPeerMessageFlush(deps.client, deps.db, "t1", "alice", "sess-alice", 2)).not.toThrow()
  })
})

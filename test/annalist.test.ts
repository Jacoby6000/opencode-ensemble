import { beforeEach, describe, expect, test } from "bun:test"
import type { Database } from "../src/db"
import {
  ANNALIST_AGENT,
  ANNALIST_MEMBER_NAME,
  provisionAnnalistForTeam,
  queuePendingTaskAnnals,
  reconcileAnnalists,
} from "../src/annalist"
import { DEFAULT_CONFIG } from "../src/config"
import { DurableScheduler } from "../src/scheduler-runtime"
import { MemberRegistry } from "../src/state"
import { checkToolIsolation } from "../src/hooks"
import { DescendantTracker } from "../src/state"
import { executeTeamTasksAdd } from "../src/tools/team-tasks-add"
import { executeTeamTasksComplete } from "../src/tools/team-tasks-complete"
import { executeTeamClaim } from "../src/tools/team-claim"
import { insertTeam, mockClient, setupDeps, setupDb } from "./helpers"

function insertAnnalist(db: Database): void {
  db.run(
    "INSERT INTO team_member (team_id, name, session_id, agent, member_kind, status, execution_status, time_created, time_updated) VALUES ('t1', ?, 'annalist-session', ?, 'annalist', 'ready', 'idle', 1, 1)",
    [ANNALIST_MEMBER_NAME, ANNALIST_AGENT],
  )
}

describe("automatic Annalist", () => {
  let db: Database

  beforeEach(() => {
    db = setupDb()
    insertTeam(db, "t1", "alpha", "lead-session")
  })

  test("provisions and recovers exactly one hidden repository Annalist", async () => {
    const client = mockClient()
    const registry = new MemberRegistry()

    expect(await reconcileAnnalists(db, client, registry, "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(1)
    expect(await reconcileAnnalists(db, client, registry, "/tmp/test-project", DEFAULT_CONFIG.scheduler)).toBe(0)

    expect(db.query("SELECT name, agent, member_kind, worktree_dir FROM team_member WHERE team_id = 't1'").all()).toEqual([
      { name: ANNALIST_MEMBER_NAME, agent: ANNALIST_AGENT, member_kind: "annalist", worktree_dir: null },
    ])
    expect(client.calls.filter(call => call.method === "session.create")).toEqual([
      {
        method: "session.create",
        args: [{
          parentID: "lead-session",
          agent: ANNALIST_AGENT,
          title: "Annalist (alpha)",
          directory: "/tmp/test-project",
          permission: [{ permission: "team_results", pattern: "*", action: "allow" }],
        }],
      },
    ])
    expect(registry.getByName("t1", ANNALIST_MEMBER_NAME)?.sessionId).toBeTruthy()
  })

  test("records and dispatches one durable Annalist invocation for every completed task", async () => {
    insertAnnalist(db)
    const deps = setupDeps(db)
    deps.registry.register("t1", ANNALIST_MEMBER_NAME, "annalist-session")
    const first = await executeTeamTasksAdd(deps, { tasks: [{ content: "Decide storage", priority: "high" }] }, "lead-session")
    const second = await executeTeamTasksAdd(deps, { tasks: [{ content: "Implement storage", priority: "high" }] }, "lead-session")
    const firstId = first.match(/task_\S+/)?.[0]
    const secondId = second.match(/task_\S+/)?.[0]
    if (!firstId || !secondId) throw new Error("expected task IDs")

    await executeTeamClaim(deps, { task_id: firstId }, "lead-session")
    await executeTeamClaim(deps, { task_id: secondId }, "lead-session")
    await executeTeamTasksComplete(deps, { task_id: firstId }, "lead-session")
    await executeTeamTasksComplete(deps, { task_id: secondId }, "lead-session")

    expect(db.query("SELECT task_id FROM team_task_annal ORDER BY time_completed, task_id").all()).toHaveLength(2)
    const wakes = db.query("SELECT reason, coalesce_key, prompt FROM scheduler_wake WHERE member_name = ? ORDER BY time_created, id")
      .all(ANNALIST_MEMBER_NAME) as Array<{ reason: string; coalesce_key: string; prompt: string }>
    expect(wakes).toHaveLength(2)
    expect(wakes.map(wake => wake.reason)).toEqual(["task_annal", "task_annal"])
    expect(new Set(wakes.map(wake => wake.coalesce_key)).size).toBe(2)
    expect(wakes.map(wake => wake.prompt).join("\n")).toContain("Decide storage")
    expect(wakes.map(wake => wake.prompt).join("\n")).toContain("Inspect relevant direct mailboxes and group inboxes")

    await Bun.sleep(0)
    await Bun.sleep(0)
    const prompt = deps.client.calls.find(call => call.method === "session.promptAsync")
    expect((prompt?.args[0] as { sessionID: string; tools: Record<string, boolean> }).sessionID).toBe("annalist-session")
    expect((prompt?.args[0] as { tools: Record<string, boolean> }).tools).toEqual({ team_results: true })
  })

  test("queues a recorded completion after a missing Annalist is reprovisioned", async () => {
    const deps = setupDeps(db)
    const added = await executeTeamTasksAdd(deps, { tasks: [{ content: "Recovered work", priority: "medium" }] }, "lead-session")
    const taskId = added.match(/task_\S+/)?.[0]
    if (!taskId) throw new Error("expected task ID")
    await executeTeamTasksComplete(deps, { task_id: taskId }, "lead-session")
    expect(db.query("SELECT wake_id FROM team_task_annal WHERE task_id = ?").get(taskId)).toEqual({ wake_id: null })
    const client = mockClient()

    expect(await provisionAnnalistForTeam(db, client, new MemberRegistry(), "t1", DEFAULT_CONFIG.scheduler)).toMatchObject({ status: "provisioned" })
    expect(db.query("SELECT wake_id FROM team_task_annal WHERE task_id = ?").get(taskId)).not.toEqual({ wake_id: null })
    expect(db.query("SELECT reason FROM scheduler_wake WHERE member_name = ?").get(ANNALIST_MEMBER_NAME)).toEqual({ reason: "task_annal" })
  })

  test("is hidden from worker coordination and can call only team_results", () => {
    insertAnnalist(db)
    const registry = new MemberRegistry()
    registry.register("t1", ANNALIST_MEMBER_NAME, "annalist-session")

    expect(() => checkToolIsolation(registry, new DescendantTracker(), "team_results", "annalist-session", db)).not.toThrow()
    expect(() => checkToolIsolation(registry, new DescendantTracker(), "team_message", "annalist-session", db)).toThrow("Annalist")
    expect(() => checkToolIsolation(registry, new DescendantTracker(), "team_tasks_complete", "annalist-session", db)).toThrow("Annalist")
  })

  test("dispatch remains non-blocking when the Annalist transport does not settle", async () => {
    insertAnnalist(db)
    const client = mockClient()
    client.session.promptAsync = options => {
      client.calls.push({ method: "session.promptAsync", args: [options] })
      return new Promise(() => {})
    }
    const deps = setupDeps(db)
    deps.client = client
    deps.scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler)
    deps.registry.register("t1", ANNALIST_MEMBER_NAME, "annalist-session")
    const added = await executeTeamTasksAdd(deps, { tasks: [{ content: "Non-blocking", priority: "high" }] }, "lead-session")
    const taskId = added.match(/task_\S+/)?.[0]
    if (!taskId) throw new Error("expected task ID")

    await expect(executeTeamTasksComplete(deps, { task_id: taskId }, "lead-session")).resolves.toContain("Completed")
    await Bun.sleep(0)
    expect(client.calls.some(call => call.method === "session.promptAsync")).toBe(true)
  })

  test("reprovisions a missing Annalist session and retries its task invocation", async () => {
    insertAnnalist(db)
    db.run("INSERT INTO team_task (id, team_id, content, status, priority, assignee, time_created, time_updated) VALUES ('task-1', 't1', 'Recover annal', 'completed', 'medium', 'alice', 1, 2)")
    db.run("INSERT INTO team_task_annal (task_id, team_id, completed_by, time_completed) VALUES ('task-1', 't1', 'alice', 2)")
    expect(queuePendingTaskAnnals(db, "t1")).toBe(1)
    const originalWake = db.query("SELECT id FROM scheduler_wake WHERE member_name = ?").get(ANNALIST_MEMBER_NAME) as { id: string }
    const client = mockClient()
    client.session.promptAsync = async options => {
      client.calls.push({ method: "session.promptAsync", args: [options] })
      if (options.sessionID === "annalist-session") {
        const error = new Error("session not found") as Error & { status: number }
        error.status = 404
        throw error
      }
      return {}
    }
    const registry = new MemberRegistry()
    registry.register("t1", ANNALIST_MEMBER_NAME, "annalist-session")
    const scheduler = new DurableScheduler(db, client, DEFAULT_CONFIG.scheduler, true, "/tmp/test-project", async () => {
      await reconcileAnnalists(db, client, registry, "/tmp/test-project", DEFAULT_CONFIG.scheduler)
    })

    scheduler.kick()
    await Bun.sleep(0)
    await Bun.sleep(0)
    expect(db.query("SELECT state FROM scheduler_wake WHERE id = ?").get(originalWake.id)).toEqual({ state: "failed" })
    expect(db.query("SELECT status FROM team_member WHERE team_id = 't1' AND member_kind = 'annalist'").get()).toEqual({ status: "error" })

    await scheduler.recover()
    await Bun.sleep(0)
    await Bun.sleep(0)
    const replacement = db.query("SELECT session_id FROM team_member WHERE team_id = 't1' AND member_kind = 'annalist'").get() as { session_id: string }
    expect(replacement.session_id).not.toBe("annalist-session")
    expect(client.calls.filter(call => call.method === "session.promptAsync").map(call => (call.args[0] as { sessionID: string }).sessionID)).toContain(replacement.session_id)
    expect(db.query("SELECT COUNT(*) AS count FROM scheduler_wake WHERE member_name = ?").get(ANNALIST_MEMBER_NAME)).toEqual({ count: 2 })
  })
})

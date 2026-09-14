import type { ResolvedEnsembleConfig } from "./config"
import type { Database } from "./db"
import { claimGroupLeadDeliveries, markGroupLeadDeliveryInjected, requeueGroupLeadDelivery } from "./groups"
import { log } from "./log"
import { getLeadPromptOptions, getMemberPromptOptions } from "./member-model"
import { expireStaleRuns, findRunBySession, finishRun, getWakePayload, listReadyWakes, markRunInjected, markRunStarted, reconcileExpiredRun, renewRunLease, requeueRun, terminateMemberScheduling, tryAcquireRun } from "./scheduler"
import { releaseMemberTasks } from "./tasks"
import { preserveBranch, preservedBranchName, verifyPreservedBranch } from "./tools/merge-helper"
import type { PluginClient } from "./types"
import { armTeamSupervisionIfQuiescent, reconcileTeamSupervision, SUPERVISOR_MEMBER_NAME } from "./supervisor"
import { ANNALIST_MEMBER_NAME } from "./annalist"
import { stat } from "node:fs/promises"

type BoundedResult<T> = { state: "fulfilled"; value: T } | { state: "rejected"; error: unknown } | { state: "timed_out" }

function settleWithin<T>(promise: Promise<T>, timeoutMs: number): Promise<BoundedResult<T>> {
  return new Promise(resolve => {
    const timer = setTimeout(() => resolve({ state: "timed_out" }), timeoutMs)
    promise.then(
      value => {
        clearTimeout(timer)
        resolve({ state: "fulfilled", value })
      },
      error => {
        clearTimeout(timer)
        resolve({ state: "rejected", error })
      },
    )
  })
}

function isNotFound(error: unknown): boolean {
  if (!error || typeof error !== "object") return false
  const candidate = error as { status?: unknown; response?: { status?: unknown } }
  if (candidate.status === 404 || candidate.response?.status === 404) return true
  return error instanceof Error && /(?:\b404\b|not[ -]?found)/i.test(error.message)
}

/** Runtime scheduler operations used by tools and session event hooks. */
export interface SchedulerController {
  kick(): void
  onSessionStatus(sessionId: string, status: "idle" | "busy" | "retry", retryAt?: number): void
  recover(): Promise<void>
  terminateMember(teamId: string, memberName: string, reason: string): void
  start(): void
  stop(): void
}

/** Owns durable wake dispatch while SQLite remains the scheduling authority. */
export class DurableScheduler implements SchedulerController {
  private drainScheduled = false
  private maintenanceRunning = false
  private timer: ReturnType<typeof setInterval> | undefined

  constructor(
    private readonly db: Database,
    private readonly client: PluginClient,
    private readonly config: ResolvedEnsembleConfig["scheduler"],
    private readonly dispatchEnabled = true,
    private readonly projectId?: string,
    private readonly recoverInternalMembers?: () => Promise<void>,
  ) {}

  kick(): void {
    if (!this.dispatchEnabled) return
    if (this.drainScheduled) return
    this.drainScheduled = true
    queueMicrotask(() => {
      this.drainScheduled = false
      this.drain()
    })
  }

  onSessionStatus(sessionId: string, status: "idle" | "busy" | "retry", retryAt?: number): void {
    const run = findRunBySession(this.db, sessionId, this.projectId)
    if (!run) return
    const now = Date.now()
    if (status === "busy" || status === "retry") {
      if (run.state === "active") {
        markRunStarted(this.db, run.leaseId, now)
        const retryExtension = retryAt && retryAt > now ? retryAt - now + this.config.leaseTtlMs : this.config.leaseTtlMs
        renewRunLease(this.db, run.leaseId, retryExtension, now, this.projectId)
      }
      return
    }
    if (run.state === "expired") {
      if (run.startedAt === null) {
        reconcileExpiredRun(this.db, run.leaseId, "requeue", "session became idle before execution started", now, this.projectId)
      } else {
        reconcileExpiredRun(this.db, run.leaseId, "processed", undefined, now, this.projectId)
      }
      this.kick()
      return
    }
    if (run.startedAt !== null) {
      finishRun(this.db, run.leaseId, "processed", undefined, now, this.projectId)
      this.kick()
      return
    }
    if (run.injectedAt !== null) {
      requeueRun(this.db, run.leaseId, "session became idle before execution started", this.config.pumpIntervalMs, now)
      this.kick()
    }
  }

  async recover(): Promise<void> {
    if (this.maintenanceRunning) return
    this.maintenanceRunning = true
    try {
      await this.recoverInternalMembers?.()
      expireStaleRuns(this.db, Date.now(), this.projectId)
      const sessions = (this.projectId
        ? this.db.query(
          "SELECT DISTINCT l.session_id FROM scheduler_run_lease l JOIN team t ON t.id = l.team_id WHERE l.state IN ('active', 'expired') AND t.project_id = ?",
        ).all(this.projectId)
        : this.db.query(
          "SELECT DISTINCT session_id FROM scheduler_run_lease WHERE state IN ('active', 'expired')",
        ).all()) as Array<{ session_id: string }>
      if (sessions.length === 0) return
      const timeoutMs = Math.min(5_000, Math.max(1, this.config.leaseTtlMs))
      const statusResult = await settleWithin(this.client.session.status(), timeoutMs)
      if (statusResult.state !== "fulfilled") {
        const detail = statusResult.state === "timed_out" ? "timed out" : statusResult.error instanceof Error ? statusResult.error.message : String(statusResult.error)
        log(`scheduler:reconcile:status-failed err=${detail}`)
        expireStaleRuns(this.db, Date.now(), this.projectId)
        return
      }
      const statuses = statusResult.value.data ?? {}
      await Promise.all(sessions.map(async ({ session_id: sessionId }) => {
        const status = statuses[sessionId]?.type
        if (status === "busy" || status === "retry" || status === "idle") {
          this.onSessionStatus(sessionId, status)
          return
        }
        const run = findRunBySession(this.db, sessionId, this.projectId)
        if (!run) return
        if (run.state === "active") return
        const getResult = await settleWithin(this.client.session.get({ sessionID: sessionId }), timeoutMs)
        if (getResult.state === "fulfilled") {
          if (run.state === "expired") {
            if (run.startedAt === null) reconcileExpiredRun(this.db, run.leaseId, "requeue", "recovered before execution start", Date.now(), this.projectId)
          } else if (run.startedAt === null) {
            requeueRun(this.db, run.leaseId, "recovered before execution start", 0)
          }
          return
        }
        if (getResult.state === "timed_out" || !isNotFound(getResult.error)) {
          const detail = getResult.state === "timed_out" ? "timed out" : getResult.error instanceof Error ? getResult.error.message : String(getResult.error)
          log(`scheduler:reconcile:session-lookup-failed session=${sessionId} err=${detail}`)
          return
        }
        const message = getResult.error instanceof Error ? getResult.error.message : "session not found"
        if (!await this.preserveMissingSessionProgress(run.teamId, run.memberName)) {
          log(`scheduler:reconcile:session-missing-preserve-failed session=${sessionId}`)
          return
        }
        this.db.run("UPDATE team_member SET status = 'error', execution_status = 'failed', time_updated = ? WHERE team_id = ? AND name = ?", [Date.now(), run.teamId, run.memberName])
        terminateMemberScheduling(this.db, run.teamId, run.memberName, message)
        releaseMemberTasks(this.db, run.teamId, run.memberName)
      }))
    } finally {
      this.reconcileSupervision(Date.now(), true)
      this.maintenanceRunning = false
      this.kick()
    }
  }

  terminateMember(teamId: string, memberName: string, reason: string): void {
    terminateMemberScheduling(this.db, teamId, memberName, reason)
  }

  start(): void {
    if (!this.dispatchEnabled) return
    if (this.timer) return
    this.kick()
    this.timer = setInterval(() => {
      this.recover().catch(err => log(`scheduler:maintenance:failed err=${err instanceof Error ? err.message : String(err)}`))
    }, this.config.pumpIntervalMs)
  }

  stop(): void {
    if (!this.timer) return
    clearInterval(this.timer)
    this.timer = undefined
  }

  private drain(): void {
    const now = Date.now()
    this.reconcileSupervision(now)
    for (const delivery of claimGroupLeadDeliveries(this.db, this.projectId, this.config.leaseTtlMs, 50, now)) {
      this.client.session.promptAsync({
        sessionID: delivery.leadSessionId,
        parts: [{ type: "text", text: `[Group ${delivery.groupName} from ${delivery.fromName}]: ${delivery.content}` }],
        ...getLeadPromptOptions(this.db, delivery.teamId),
      }).then(() => {
        markGroupLeadDeliveryInjected(this.db, delivery.messageId, delivery.claimToken)
      }).catch(err => {
        const message = err instanceof Error ? err.message : String(err)
        log(`scheduler:group-lead-dispatch:failed message=${delivery.messageId} err=${message}`)
        requeueGroupLeadDelivery(this.db, delivery.messageId, delivery.claimToken, message, this.config.pumpIntervalMs)
      })
    }
    for (const wake of listReadyWakes(this.db, now, 50, this.projectId)) {
      const acquired = tryAcquireRun(this.db, wake.id, this.config.runLimits, this.config.leaseTtlMs, now, this.projectId)
      if (!acquired.acquired) continue
      const payload = getWakePayload(this.db, wake.id)
      if (!payload) {
        finishRun(this.db, acquired.leaseId, "failed", "wake payload missing", now, this.projectId)
        continue
      }
      const parts = [payload.prompt, ...payload.messages.map(message => (message.groupName
        ? `[Group ${message.groupName} from ${message.fromName}]: ${message.content}`
        : `[Team message from ${message.fromName}]: ${message.content}`))]
        .filter((part): part is string => Boolean(part))
      const text = parts.length > 0 ? parts.join("\n\n") : `[System: Resume queued ${wake.reason} work]`
      this.client.session.promptAsync({
        sessionID: wake.sessionId,
        parts: [{ type: "text", text }],
        ...getMemberPromptOptions(this.db, wake.teamId, wake.memberName),
        ...(wake.memberName === SUPERVISOR_MEMBER_NAME ? {
          tools: {
            team_status: true,
            team_message: true,
            team_broadcast: true,
            team_tasks_list: true,
          },
        } : wake.memberName === ANNALIST_MEMBER_NAME ? {
          tools: {
            team_results: true,
          },
        } : {}),
      }).then(() => {
        markRunInjected(this.db, acquired.leaseId)
      }).catch(err => {
        const message = err instanceof Error ? err.message : String(err)
        log(`scheduler:dispatch:failed wake=${wake.id} member=${wake.memberName} err=${message}`)
        if ((wake.memberName === SUPERVISOR_MEMBER_NAME || wake.memberName === ANNALIST_MEMBER_NAME) && isNotFound(err)) {
          const memberKind = wake.memberName === SUPERVISOR_MEMBER_NAME ? "supervisor" : "annalist"
          const current = this.db.query(
            "SELECT session_id FROM team_member WHERE team_id = ? AND name = ? AND member_kind = ?",
          ).get(wake.teamId, wake.memberName, memberKind) as { session_id: string } | undefined
          if (current?.session_id === wake.sessionId) {
            this.db.run(
              "UPDATE team_member SET status = 'error', execution_status = 'failed', time_updated = ? WHERE team_id = ? AND name = ? AND session_id = ?",
              [Date.now(), wake.teamId, wake.memberName, wake.sessionId],
            )
            terminateMemberScheduling(this.db, wake.teamId, wake.memberName, message)
          } else {
            finishRun(this.db, acquired.leaseId, "failed", message)
          }
          this.kick()
          return
        }
        requeueRun(this.db, acquired.leaseId, message, this.config.pumpIntervalMs)
      })
    }
  }

  private reconcileSupervision(now = Date.now(), armQuietPeriod = false): void {
    const teams = (this.projectId
      ? this.db.query("SELECT id FROM team WHERE status = 'active' AND project_id = ?").all(this.projectId)
      : this.db.query("SELECT id FROM team WHERE status = 'active'").all()) as Array<{ id: string }>
    teams.forEach(team => {
      if (armQuietPeriod) {
        armTeamSupervisionIfQuiescent(this.db, team.id, now)
      } else {
        reconcileTeamSupervision(this.db, team.id, now)
      }
    })
  }

  private async preserveMissingSessionProgress(teamId: string, memberName: string): Promise<boolean> {
    const member = this.db.query(
      `SELECT tm.worktree_dir, tm.worktree_branch, t.name AS team_name, p.name AS project_name, p.path AS project_path
       FROM team_member tm JOIN team t ON t.id = tm.team_id JOIN project p ON p.id = t.project_id
       WHERE tm.team_id = ? AND tm.name = ?`,
    ).get(teamId, memberName) as { worktree_dir: string | null; worktree_branch: string | null; team_name: string; project_name: string; project_path: string } | undefined
    if (!member || (!member.worktree_dir && !member.worktree_branch)) return true
    const target = member.worktree_branch?.startsWith("ensemble/preserved/")
      ? member.worktree_branch
      : preservedBranchName(member.project_name, member.team_name, teamId, memberName)
    if (member.worktree_branch === target && (!member.worktree_dir || await isMissing(member.worktree_dir))) {
      return verifyPreservedBranch(target, member.project_path)
    }
    const preserved = await preserveBranch(member.worktree_branch ?? "HEAD", target, member.project_path, member.worktree_dir)
    if (!preserved) return false
    this.db.run("UPDATE team_member SET worktree_branch = ? WHERE team_id = ? AND name = ?", [target, teamId, memberName])
    return true
  }
}

async function isMissing(filePath: string): Promise<boolean> {
  try {
    await stat(filePath)
    return false
  } catch (error) {
    return error instanceof Error && "code" in error && (error.code === "ENOENT" || error.code === "ENOTDIR")
  }
}

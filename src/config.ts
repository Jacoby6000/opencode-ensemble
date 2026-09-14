import { readFileSync } from "node:fs"
import path from "node:path"
import type { SchedulerIdentityLimits, SchedulerRunLimits } from "./scheduler"

/** Dashboard listener configuration. */
export interface DashboardConfig {
  port?: number
}

/** Partially specified scheduler capacity limits. */
export interface SchedulerLimitsConfig {
  global?: number
  perAgent?: Record<string, number>
}

/** Durable scheduler configuration. */
export interface SchedulerConfig {
  identityLimits?: SchedulerLimitsConfig
  runLimits?: SchedulerLimitsConfig
  reservationTtlMs?: number
  leaseTtlMs?: number
  pumpIntervalMs?: number
}

/** Plugin configuration shape. All fields optional — defaults applied. */
export interface EnsembleConfig {
  /** Auto-merge worktree branches on cleanup (default: false) */
  mergeOnCleanup?: boolean
  /** Custom agent names that must run without write or shell permissions. */
  readOnlyAgents?: string[]
  /** Stall detection threshold in ms (default: 180000 = 3 min, 0 to disable) */
  stallThresholdMs?: number
  /** Min steps before token-based stall check (default: 3) */
  stallMinSteps?: number
  /** Output token threshold for stall detection (default: 500) */
  stallTokenThreshold?: number
  /** Hard timeout for busy members in ms (default: 1800000 = 30 min, 0 to disable) */
  timeoutMs?: number
  /** Rate limit capacity (default: 10, 0 to disable) */
  rateLimitCapacity?: number
  /** Dashboard listener. */
  dashboard?: DashboardConfig
  /** Deprecated dashboard port alias retained for existing config files. */
  dashboardPort?: number
  /** Durable scheduler capacity and timing controls. */
  scheduler?: SchedulerConfig
  /** Max peer messages per agent per window before nudge (default: 5, 0 to disable) */
  peerMessageLimit?: number
  /** Time window for peer message rate limiting in ms (default: 300000 = 5 min) */
  peerMessageWindowMs?: number
  /** Default model for all agents (e.g. "opencode/zen-sonnet-4-6") */
  defaultModel?: string
  /** Pool of models for rotation/random assignment */
  modelPool?: string[]
  /** Map agent type to specific model e.g. {"build": "anthropic/claude-opus-4-6"} */
  modelsByAgent?: Record<string, string>
  /** How to assign models: "default" | "rotate" | "random" (default: "default") */
  modelAssignment?: "default" | "rotate" | "random"
  /** Lead asks user about model preferences before spawning (default: false) */
  promptForModels?: boolean
}

/** Fully resolved plugin configuration used by production code. */
export interface ResolvedEnsembleConfig extends Required<Omit<EnsembleConfig, "dashboard" | "dashboardPort" | "scheduler">> {
  dashboard: { port: number }
  scheduler: {
    identityLimits: SchedulerIdentityLimits
    runLimits: SchedulerRunLimits
    reservationTtlMs: number
    leaseTtlMs: number
    pumpIntervalMs: number
  }
}

/** Default configuration values. */
export const DEFAULT_CONFIG: ResolvedEnsembleConfig = {
  mergeOnCleanup: false,
  readOnlyAgents: [],
  stallThresholdMs: 300_000,
  stallMinSteps: 5,
  stallTokenThreshold: 200,
  timeoutMs: 30 * 60 * 1000,
  rateLimitCapacity: 10,
  dashboard: { port: 4747 },
  scheduler: {
    identityLimits: { global: 32, perAgent: {} },
    runLimits: { global: 4, perAgent: {} },
    reservationTtlMs: 10 * 60 * 1000,
    leaseTtlMs: 60 * 1000,
    pumpIntervalMs: 1000,
  },
  peerMessageLimit: 5,
  peerMessageWindowMs: 300_000,
  defaultModel: "",
  modelPool: [],
  modelsByAgent: {},
  modelAssignment: "default",
  promptForModels: false,
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0
}

function readLimits(value: unknown): SchedulerLimitsConfig | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined
  const raw = value as Record<string, unknown>
  const result: SchedulerLimitsConfig = {}
  if (isPositiveSafeInteger(raw.global)) result.global = raw.global
  if (typeof raw.perAgent === "object" && raw.perAgent !== null && !Array.isArray(raw.perAgent)) {
    const entries = Object.entries(raw.perAgent as Record<string, unknown>)
    if (entries.every(([agent, limit]) => agent.trim().length > 0 && isPositiveSafeInteger(limit))) {
      result.perAgent = Object.fromEntries(entries) as Record<string, number>
    }
  }
  return result
}

/** Read a JSON config file, returning an empty object on missing/invalid. */
function readConfigFile(filePath: string): Partial<EnsembleConfig> {
  try {
    const text = readFileSync(filePath, "utf-8")
    const raw = JSON.parse(text) as Record<string, unknown>
    // Validate types — only accept numbers for numeric fields, booleans for boolean fields
    const result: Partial<EnsembleConfig> = {}
    if (typeof raw.mergeOnCleanup === "boolean") result.mergeOnCleanup = raw.mergeOnCleanup
    if (Array.isArray(raw.readOnlyAgents) && raw.readOnlyAgents.every((agent: unknown) => typeof agent === "string")) {
      result.readOnlyAgents = raw.readOnlyAgents as string[]
    }
    if (typeof raw.stallThresholdMs === "number") result.stallThresholdMs = raw.stallThresholdMs
    if (typeof raw.stallMinSteps === "number") result.stallMinSteps = raw.stallMinSteps
    if (typeof raw.stallTokenThreshold === "number") result.stallTokenThreshold = raw.stallTokenThreshold
    if (typeof raw.timeoutMs === "number") result.timeoutMs = raw.timeoutMs
    if (typeof raw.rateLimitCapacity === "number") result.rateLimitCapacity = raw.rateLimitCapacity
    if (typeof raw.dashboardPort === "number" && Number.isInteger(raw.dashboardPort) && raw.dashboardPort >= 0 && raw.dashboardPort <= 65_535) {
      result.dashboard = { port: raw.dashboardPort }
    }
    if (typeof raw.dashboard === "object" && raw.dashboard !== null && !Array.isArray(raw.dashboard)) {
      const port = (raw.dashboard as Record<string, unknown>).port
      if (typeof port === "number" && Number.isInteger(port) && port >= 0 && port <= 65_535) result.dashboard = { port }
    }
    if (typeof raw.scheduler === "object" && raw.scheduler !== null && !Array.isArray(raw.scheduler)) {
      const scheduler = raw.scheduler as Record<string, unknown>
      const parsed: SchedulerConfig = {}
      const identityLimits = readLimits(scheduler.identityLimits)
      const runLimits = readLimits(scheduler.runLimits)
      if (identityLimits) parsed.identityLimits = identityLimits
      if (runLimits) parsed.runLimits = runLimits
      if (isPositiveSafeInteger(scheduler.reservationTtlMs)) parsed.reservationTtlMs = scheduler.reservationTtlMs
      if (isPositiveSafeInteger(scheduler.leaseTtlMs)) parsed.leaseTtlMs = scheduler.leaseTtlMs
      if (isPositiveSafeInteger(scheduler.pumpIntervalMs)) parsed.pumpIntervalMs = scheduler.pumpIntervalMs
      result.scheduler = parsed
    }
    if (typeof raw.peerMessageLimit === "number") result.peerMessageLimit = raw.peerMessageLimit
    if (typeof raw.peerMessageWindowMs === "number") result.peerMessageWindowMs = raw.peerMessageWindowMs
    if (typeof raw.defaultModel === "string") result.defaultModel = raw.defaultModel
    if (Array.isArray(raw.modelPool) && raw.modelPool.every((m: unknown) => typeof m === "string")) result.modelPool = raw.modelPool as string[]
    if (typeof raw.modelsByAgent === "object" && raw.modelsByAgent !== null && !Array.isArray(raw.modelsByAgent)) {
      const valid = Object.entries(raw.modelsByAgent as Record<string, unknown>).every(([, v]) => typeof v === "string")
      if (valid) result.modelsByAgent = raw.modelsByAgent as Record<string, string>
    }
    if (typeof raw.modelAssignment === "string" && ["default", "rotate", "random"].includes(raw.modelAssignment)) result.modelAssignment = raw.modelAssignment as "default" | "rotate" | "random"
    if (typeof raw.promptForModels === "boolean") result.promptForModels = raw.promptForModels
    return result
  } catch (err) {
    if (err && typeof err === "object" && "code" in err && err.code === "ENOENT") return {}
    console.warn(`[ensemble] Invalid config at ${filePath}, using defaults`)
    return {}
  }
}

function mergeConfig(base: ResolvedEnsembleConfig, override: Partial<EnsembleConfig>): ResolvedEnsembleConfig {
  const { dashboard, scheduler, dashboardPort: _dashboardPort, ...flat } = override
  return {
    ...base,
    ...flat,
    dashboard: { port: dashboard?.port ?? base.dashboard.port },
    scheduler: {
      identityLimits: {
        global: scheduler?.identityLimits?.global ?? base.scheduler.identityLimits.global,
        perAgent: scheduler?.identityLimits?.perAgent ?? base.scheduler.identityLimits.perAgent,
      },
      runLimits: {
        global: scheduler?.runLimits?.global ?? base.scheduler.runLimits.global,
        perAgent: scheduler?.runLimits?.perAgent ?? base.scheduler.runLimits.perAgent,
      },
      reservationTtlMs: scheduler?.reservationTtlMs ?? base.scheduler.reservationTtlMs,
      leaseTtlMs: scheduler?.leaseTtlMs ?? base.scheduler.leaseTtlMs,
      pumpIntervalMs: scheduler?.pumpIntervalMs ?? base.scheduler.pumpIntervalMs,
    },
  }
}

/**
 * Load plugin configuration. Merges global → project → env vars.
 * Missing files are silently skipped. Invalid JSON logs a warning.
 */
export function loadConfig(projectDir: string): ResolvedEnsembleConfig {
  const homeDir = process.env.HOME ?? process.env.USERPROFILE ?? ""
  const globalPath = path.join(homeDir, ".config", "opencode", "ensemble.json")
  const projectPath = path.join(projectDir, ".opencode", "ensemble.json")

  const global = readConfigFile(globalPath)
  const project = readConfigFile(projectPath)
  const merged = mergeConfig(mergeConfig(DEFAULT_CONFIG, global), project)

  // Env vars override everything
  const timeout = process.env.OPENCODE_ENSEMBLE_TIMEOUT
  if (timeout !== undefined) merged.timeoutMs = timeout === "0" ? 0 : (parseInt(timeout, 10) || merged.timeoutMs)

  const rateLimit = process.env.OPENCODE_ENSEMBLE_RATE_LIMIT
  if (rateLimit !== undefined) merged.rateLimitCapacity = rateLimit === "0" ? 0 : (parseInt(rateLimit, 10) || merged.rateLimitCapacity)

  const stall = process.env.STALL_THRESHOLD_MS
  if (stall !== undefined) merged.stallThresholdMs = stall === "0" ? 0 : (parseInt(stall, 10) || merged.stallThresholdMs)

  return merged
}

import { describe, test, expect, beforeEach, afterEach } from "bun:test"
import { mkdtempSync, writeFileSync, rmSync, mkdirSync } from "node:fs"
import path from "node:path"
import os from "node:os"
import { loadConfig, DEFAULT_CONFIG } from "../src/config"

describe("config", () => {
  let tmpDir: string
  let originalHome: string | undefined
  let originalUserProfile: string | undefined

  beforeEach(() => {
    originalHome = process.env.HOME
    originalUserProfile = process.env.USERPROFILE
    tmpDir = mkdtempSync(path.join(os.tmpdir(), "ensemble-config-"))
    process.env.HOME = path.join(tmpDir, "home")
    delete process.env.USERPROFILE
  })

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true })
    if (originalHome === undefined) delete process.env.HOME
    else process.env.HOME = originalHome
    if (originalUserProfile === undefined) delete process.env.USERPROFILE
    else process.env.USERPROFILE = originalUserProfile
    delete process.env.OPENCODE_ENSEMBLE_TIMEOUT
    delete process.env.OPENCODE_ENSEMBLE_RATE_LIMIT
    delete process.env.STALL_THRESHOLD_MS
  })

  test("DEFAULT_CONFIG has correct values", () => {
    expect(DEFAULT_CONFIG.mergeOnCleanup).toBe(false)
    expect(DEFAULT_CONFIG.readOnlyAgents).toEqual([])
    expect(DEFAULT_CONFIG.stallThresholdMs).toBe(300_000)
    expect(DEFAULT_CONFIG.stallMinSteps).toBe(5)
    expect(DEFAULT_CONFIG.stallTokenThreshold).toBe(200)
    expect(DEFAULT_CONFIG.timeoutMs).toBe(1_800_000)
    expect(DEFAULT_CONFIG.rateLimitCapacity).toBe(10)
    expect(DEFAULT_CONFIG.dashboard).toEqual({ port: 4747 })
    expect(DEFAULT_CONFIG.scheduler).toEqual({
      identityLimits: { global: 32, perAgent: {} },
      runLimits: { global: 4, perAgent: {} },
      reservationTtlMs: 600_000,
      leaseTtlMs: 60_000,
      pumpIntervalMs: 1_000,
    })
  })

  test("returns defaults when no config files exist", () => {
    const config = loadConfig(tmpDir)
    expect(config).toEqual(DEFAULT_CONFIG)
  })

  test("project config overrides defaults", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), JSON.stringify({ stallThresholdMs: 60_000 }))

    const config = loadConfig(tmpDir)
    expect(config.stallThresholdMs).toBe(60_000)
    expect(config.mergeOnCleanup).toBe(false) // other defaults preserved
  })

  test("partial config merges correctly", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), JSON.stringify({ mergeOnCleanup: false, rateLimitCapacity: 5 }))

    const config = loadConfig(tmpDir)
    expect(config.mergeOnCleanup).toBe(false)
    expect(config.rateLimitCapacity).toBe(5)
    expect(config.stallThresholdMs).toBe(300_000) // default preserved
  })

  test("deep-merges nested scheduler configuration by leaf", () => {
    const globalDir = path.join(process.env.HOME!, ".config", "opencode")
    mkdirSync(globalDir, { recursive: true })
    writeFileSync(path.join(globalDir, "ensemble.json"), JSON.stringify({
      scheduler: { identityLimits: { global: 12, perAgent: { build: 3 } }, leaseTtlMs: 90_000 },
    }))
    const projectDir = path.join(tmpDir, ".opencode")
    mkdirSync(projectDir, { recursive: true })
    writeFileSync(path.join(projectDir, "ensemble.json"), JSON.stringify({
      scheduler: { runLimits: { global: 2 }, pumpIntervalMs: 2_000 },
    }))

    const config = loadConfig(tmpDir)
    expect(config.scheduler).toEqual({
      identityLimits: { global: 12, perAgent: { build: 3 } },
      runLimits: { global: 2, perAgent: {} },
      reservationTtlMs: 600_000,
      leaseTtlMs: 90_000,
      pumpIntervalMs: 2_000,
    })
  })

  test("normalizes legacy dashboardPort with new spelling taking precedence", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), JSON.stringify({ dashboardPort: 4000, dashboard: { port: 5000 } }))
    expect(loadConfig(tmpDir).dashboard.port).toBe(5000)
  })

  test("project legacy dashboardPort overrides global nested port", () => {
    const globalDir = path.join(process.env.HOME!, ".config", "opencode")
    mkdirSync(globalDir, { recursive: true })
    writeFileSync(path.join(globalDir, "ensemble.json"), JSON.stringify({ dashboard: { port: 5000 } }))
    const projectDir = path.join(tmpDir, ".opencode")
    mkdirSync(projectDir, { recursive: true })
    writeFileSync(path.join(projectDir, "ensemble.json"), JSON.stringify({ dashboardPort: 0 }))
    expect(loadConfig(tmpDir).dashboard.port).toBe(0)
  })

  test("rejects unsafe nested scheduler and dashboard values", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), JSON.stringify({
      dashboard: { port: 70_000 },
      scheduler: {
        identityLimits: { global: 0, perAgent: { build: -1 } },
        runLimits: { global: 1.5 },
        reservationTtlMs: -1,
        leaseTtlMs: 0,
        pumpIntervalMs: null,
      },
    }))
    expect(loadConfig(tmpDir).dashboard).toEqual(DEFAULT_CONFIG.dashboard)
    expect(loadConfig(tmpDir).scheduler).toEqual(DEFAULT_CONFIG.scheduler)
  })

  test("loads configured custom read-only agents", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), JSON.stringify({ readOnlyAgents: ["Shit Tester", "Security Reviewer"] }))

    const config = loadConfig(tmpDir)
    expect(config.readOnlyAgents).toEqual(["Shit Tester", "Security Reviewer"])
  })

  test("ignores readOnlyAgents when any entry is not a string", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), JSON.stringify({ readOnlyAgents: ["Shit Tester", 42] }))

    const config = loadConfig(tmpDir)
    expect(config.readOnlyAgents).toEqual([])
  })

  test("invalid JSON logs warning and returns defaults", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), "not json{{{")

    const config = loadConfig(tmpDir)
    expect(config).toEqual(DEFAULT_CONFIG)
  })

  test("env var OPENCODE_ENSEMBLE_TIMEOUT overrides config", () => {
    process.env.OPENCODE_ENSEMBLE_TIMEOUT = "60000"
    const config = loadConfig(tmpDir)
    expect(config.timeoutMs).toBe(60_000)
  })

  test("env var OPENCODE_ENSEMBLE_RATE_LIMIT overrides config", () => {
    process.env.OPENCODE_ENSEMBLE_RATE_LIMIT = "0"
    const config = loadConfig(tmpDir)
    expect(config.rateLimitCapacity).toBe(0)
  })

  test("env var STALL_THRESHOLD_MS overrides config", () => {
    process.env.STALL_THRESHOLD_MS = "0"
    const config = loadConfig(tmpDir)
    expect(config.stallThresholdMs).toBe(0)
  })

  test("env vars override file values", () => {
    const configDir = path.join(tmpDir, ".opencode")
    mkdirSync(configDir, { recursive: true })
    writeFileSync(path.join(configDir, "ensemble.json"), JSON.stringify({ timeoutMs: 999 }))
    process.env.OPENCODE_ENSEMBLE_TIMEOUT = "123"

    const config = loadConfig(tmpDir)
    expect(config.timeoutMs).toBe(123)
  })
})

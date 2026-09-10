import { afterEach, describe, expect, test } from "bun:test"
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs"
import os from "node:os"
import path from "node:path"
import { loadOrCreateDashboardToken } from "../src/dashboard-auth"

const tempDirs: string[] = []

afterEach(() => {
  for (const directory of tempDirs.splice(0)) rmSync(directory, { recursive: true, force: true })
})

describe("dashboard authentication", () => {
  test("creates and persists an owner-only random token", () => {
    const directory = mkdtempSync(path.join(os.tmpdir(), "ensemble-dashboard-auth-"))
    tempDirs.push(directory)
    const tokenPath = path.join(directory, "dashboard-token")

    const token = loadOrCreateDashboardToken(tokenPath)

    expect(token).toHaveLength(64)
    expect(token).toMatch(/^[0-9a-f]+$/)
    expect(readFileSync(tokenPath, "utf8").trim()).toBe(token)
    expect(statSync(tokenPath).mode & 0o777).toBe(0o600)
    expect(loadOrCreateDashboardToken(tokenPath)).toBe(token)
  })

  test("rejects an empty persisted token", () => {
    const directory = mkdtempSync(path.join(os.tmpdir(), "ensemble-dashboard-auth-"))
    tempDirs.push(directory)
    const tokenPath = path.join(directory, "dashboard-token")
    writeFileSync(tokenPath, "")

    expect(() => loadOrCreateDashboardToken(tokenPath)).toThrow(/empty/i)
  })

  test("rejects a weak persisted token", () => {
    const directory = mkdtempSync(path.join(os.tmpdir(), "ensemble-dashboard-auth-"))
    tempDirs.push(directory)
    const tokenPath = path.join(directory, "dashboard-token")
    writeFileSync(tokenPath, "short-token\n")

    expect(() => loadOrCreateDashboardToken(tokenPath)).toThrow(/64-character hexadecimal/i)
  })
})

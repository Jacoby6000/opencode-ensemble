import { randomBytes, timingSafeEqual } from "node:crypto"
import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs"
import os from "node:os"
import path from "node:path"

/** Resolve the owner-only dashboard token file path. */
export function getDashboardTokenPath(env: Record<string, string | undefined> = process.env): string {
  const home = env.HOME ?? env.USERPROFILE ?? os.homedir()
  return path.join(home, ".config", "opencode", "ensemble-dashboard.token")
}

/** Load or create the dashboard bearer token at the requested path. */
export function loadOrCreateDashboardToken(tokenPath = getDashboardTokenPath()): string {
  try {
    const token = readFileSync(tokenPath, "utf8").trim()
    if (!token) throw new Error(`Dashboard token file is empty: ${tokenPath}`)
    if (!/^[a-f0-9]{64}$/.test(token)) {
      throw new Error(`Dashboard token file must contain one 64-character hexadecimal token: ${tokenPath}`)
    }
    chmodSync(tokenPath, 0o600)
    return token
  } catch (err) {
    if (!(err && typeof err === "object" && "code" in err && err.code === "ENOENT")) throw err
  }

  mkdirSync(path.dirname(tokenPath), { recursive: true })
  const token = randomBytes(32).toString("hex")
  try {
    writeFileSync(tokenPath, `${token}\n`, { encoding: "utf8", flag: "wx", mode: 0o600 })
    return token
  } catch (err) {
    if (err && typeof err === "object" && "code" in err && err.code === "EEXIST") {
      return loadOrCreateDashboardToken(tokenPath)
    }
    throw err
  }
}

/** Compare an Authorization header with the dashboard token in constant time. */
export function isDashboardAuthorized(authorization: string | undefined, token: string): boolean {
  if (!token || !authorization?.startsWith("Bearer ")) return false
  const supplied = Buffer.from(authorization.slice("Bearer ".length))
  const expected = Buffer.from(token)
  return supplied.length === expected.length && timingSafeEqual(supplied, expected)
}

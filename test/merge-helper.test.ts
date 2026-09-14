import { afterEach, describe, expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { preserveBranch } from "../src/tools/merge-helper"

const repositories: string[] = []

async function git(cwd: string, args: string[]): Promise<string> {
  const process = Bun.spawn(["git", ...args], { cwd, stdout: "pipe", stderr: "pipe" })
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ])
  if (exitCode !== 0) throw new Error(stderr.trim() || `git ${args.join(" ")} failed`)
  return stdout.trim()
}

async function repository(): Promise<string> {
  const cwd = await mkdtemp(path.join(tmpdir(), "ensemble-preserve-test-"))
  repositories.push(cwd)
  await git(cwd, ["init"])
  await git(cwd, ["config", "user.name", "Test User"])
  await git(cwd, ["config", "user.email", "test@example.com"])
  await Bun.write(path.join(cwd, "tracked.txt"), "base\n")
  await git(cwd, ["add", "tracked.txt"])
  await git(cwd, ["commit", "-m", "base"])
  await git(cwd, ["switch", "-c", "ensemble-worker"])
  return cwd
}

afterEach(async () => {
  await Promise.all(repositories.splice(0).map(cwd => rm(cwd, { recursive: true, force: true })))
})

describe("preserveBranch", () => {
  test("durably snapshots committed, staged, unstaged, and untracked work and refreshes a stale ref", async () => {
    const cwd = await repository()
    await Bun.write(path.join(cwd, "committed.txt"), "committed\n")
    await git(cwd, ["add", "committed.txt"])
    await git(cwd, ["commit", "-m", "worker commit"])
    await Bun.write(path.join(cwd, "tracked.txt"), "staged\n")
    await git(cwd, ["add", "tracked.txt"])
    await Bun.write(path.join(cwd, "tracked.txt"), "unstaged after staged\n")
    await Bun.write(path.join(cwd, "untracked.txt"), "untracked\n")

    const target = "ensemble/preserved/project/team#abc/alice"
    expect(await preserveBranch("ensemble-worker", target, cwd, cwd)).toBe(true)
    const first = await git(cwd, ["rev-parse", target])
    expect(await git(cwd, ["show", `${target}:committed.txt`])).toBe("committed")
    expect(await git(cwd, ["show", `${target}:tracked.txt`])).toBe("unstaged after staged")
    expect(await git(cwd, ["show", `${target}:untracked.txt`])).toBe("untracked")
    expect(await git(cwd, ["show", `${target}^:tracked.txt`])).toBe("staged")

    await Bun.write(path.join(cwd, "untracked.txt"), "newer progress\n")
    expect(await preserveBranch(target, target, cwd, cwd)).toBe(true)
    expect(await git(cwd, ["rev-parse", target])).not.toBe(first)
    expect(await git(cwd, ["show", `${target}:untracked.txt`])).toBe("newer progress")
  })

  test("fails closed when the worktree cannot be inspected", async () => {
    const cwd = await repository()
    expect(await preserveBranch("ensemble-worker", "ensemble/preserved/test", cwd, path.join(cwd, "missing"))).toBe(false)
    expect(await git(cwd, ["branch", "--list", "ensemble/preserved/test"])).toBe("")
  })
})

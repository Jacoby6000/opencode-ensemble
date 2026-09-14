import type { Database } from "../db"
import { log } from "../log"
import { runCommand } from "../process"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"

/** Result of merging a single branch. */
export interface MergeResult {
  ok: boolean
  error?: string
}

/** Injectable function for testing. */
export type MergeBranchFn = (branch: string, cwd: string) => Promise<MergeResult>

/** Injectable function for overlap detection before merge. */
export type OverlapCheckFn = (branch: string, cwd: string) => Promise<string[]>

/** Injectable function for preserving a branch before worktree deletion. */
export type PreserveBranchFn = (sourceBranch: string, targetBranch: string, cwd: string, worktreeDir?: string | null) => Promise<boolean>

/** Injectable function for deleting a branch. */
export type DeleteBranchFn = (branch: string, cwd: string) => Promise<boolean>

/** Human-readable resource identity for a team. */
export interface TeamResourceParts {
  projectName: string
  teamName: string
  teamId: string
}

function shortTeamId(teamId: string): string {
  return (teamId.split("_").at(-1) || teamId).slice(0, 6)
}

function resourcePart(value: string): string {
  const part = value.toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "")
  return part || "unnamed"
}

/** Load the human-readable pieces used to build team resource names. */
export function getTeamResourceParts(db: Database, teamId: string): TeamResourceParts {
  const row = db.query(
    `SELECT t.id as team_id, t.name as team_name, p.name as project_name
     FROM team t
     JOIN project p ON t.project_id = p.id
     WHERE t.id = ?`
  ).get(teamId) as { team_id: string; team_name: string; project_name: string } | null
  if (!row) throw new Error(`Team not found: ${teamId}`)
  return { projectName: row.project_name, teamName: row.team_name, teamId: row.team_id }
}

/** Build the readable namespace used for team-owned resources. */
export function teamResourceSlug(projectName: string, teamName: string, teamId: string): string {
  return `${resourcePart(projectName)}-${resourcePart(teamName)}#${shortTeamId(teamId)}`
}

/** Build the team-only resource segment used under project-scoped namespaces. */
export function teamResourceSegment(teamName: string, teamId: string): string {
  return `${resourcePart(teamName)}#${shortTeamId(teamId)}`
}

/** Build an OpenCode worktree name for a team member. */
export function teamWorktreeName(projectName: string, teamName: string, teamId: string, memberName: string): string {
  return `ensemble-${teamResourceSlug(projectName, teamName, teamId)}-${memberName}`
}

/** Serialize snapshots targeting the same repository ref in invocation order. */
export function serializeBranchPreserver(snapshot: PreserveBranchFn): PreserveBranchFn {
  const locks = new Map<string, Promise<void>>()
  return async (sourceBranch, targetBranch, cwd, worktreeDir) => {
    const key = `${path.resolve(cwd)}\0${targetBranch}`
    const predecessor = locks.get(key) ?? Promise.resolve()
    let release = () => {}
    const current = new Promise<void>(resolve => { release = resolve })
    locks.set(key, current)
    await predecessor
    try {
      return await snapshot(sourceBranch, targetBranch, cwd, worktreeDir)
    } finally {
      release()
      if (locks.get(key) === current) locks.delete(key)
    }
  }
}

/** Build and safely publish a verified snapshot of a worktree. */
async function preserveBranchLocked(sourceBranch: string, targetBranch: string, cwd: string, worktreeDir?: string | null): Promise<boolean> {
  let temporaryDirectory: string | undefined
  try {
    const targetRef = `refs/heads/${targetBranch}`
    const existing = await readBranchRef(targetRef, cwd)
    const source = worktreeDir
      ? await runCommand(["git", "-C", worktreeDir, "rev-parse", "HEAD^{commit}"], { cwd })
      : await runCommand(["git", "rev-parse", `${sourceBranch}^{commit}`], { cwd })
    if (source.exitCode !== 0) throw new Error(source.stderr.trim() || "source commit is unavailable")
    const sourceCommit = source.stdout.trim()
    let preservedCommit = sourceCommit

    if (worktreeDir) {
      const status = await runCommand(["git", "-C", worktreeDir, "status", "--porcelain=v1", "--untracked-files=all"], { cwd })
      if (status.exitCode !== 0) throw new Error(status.stderr.trim() || "worktree status failed")
      if (status.stdout.trim()) {
        const sourceTree = await runCommand(["git", "rev-parse", `${sourceCommit}^{tree}`], { cwd })
        if (sourceTree.exitCode !== 0) throw new Error(sourceTree.stderr.trim() || "source tree lookup failed")
        const stagedTree = await runCommand(["git", "-C", worktreeDir, "write-tree"], { cwd })
        if (stagedTree.exitCode !== 0) throw new Error(stagedTree.stderr.trim() || "staged tree snapshot failed")
        if (stagedTree.stdout.trim() !== sourceTree.stdout.trim()) {
          const stagedCommit = await createSnapshotCommit(worktreeDir, stagedTree.stdout.trim(), sourceCommit, `${targetBranch} staged state`, cwd)
          preservedCommit = stagedCommit
        }

        temporaryDirectory = await mkdtemp(path.join(tmpdir(), "ensemble-preserve-"))
        const env = { ...process.env, GIT_INDEX_FILE: path.join(temporaryDirectory, "index") }
        const readTree = await runCommand(["git", "-C", worktreeDir, "read-tree", preservedCommit], { cwd, env })
        if (readTree.exitCode !== 0) throw new Error(readTree.stderr.trim() || "temporary index initialization failed")
        const add = await runCommand(["git", "-C", worktreeDir, "add", "-A", "--", "."], { cwd, env })
        if (add.exitCode !== 0) throw new Error(add.stderr.trim() || "worktree snapshot staging failed")
        const tree = await runCommand(["git", "-C", worktreeDir, "write-tree"], { cwd, env })
        if (tree.exitCode !== 0) throw new Error(tree.stderr.trim() || "worktree snapshot tree failed")
        if (tree.stdout.trim() !== stagedTree.stdout.trim()) {
          preservedCommit = await createSnapshotCommit(worktreeDir, tree.stdout.trim(), preservedCommit, `${targetBranch} worktree state`, cwd, env)
        }
      }
    }

    const expected = existing ?? "0".repeat(sourceCommit.length)
    const update = await runCommand(["git", "update-ref", targetRef, preservedCommit, expected], { cwd })
    if (update.exitCode !== 0) throw new Error(update.stderr.trim() || "preserved ref update failed")
    const verify = await runCommand(["git", "rev-parse", `${targetBranch}^{commit}`], { cwd })
    if (verify.exitCode !== 0 || verify.stdout.trim() !== preservedCommit) throw new Error("preserved ref verification failed")
    return true
  } catch (error) {
    log(`merge-helper:preserve:failed src=${sourceBranch} target=${targetBranch} err=${error instanceof Error ? error.message : String(error)}`)
    return false
  } finally {
    if (temporaryDirectory) await rm(temporaryDirectory, { recursive: true, force: true })
  }
}

async function readBranchRef(targetRef: string, cwd: string): Promise<string | null> {
  const result = await runCommand(["git", "rev-parse", "--verify", "--quiet", `${targetRef}^{commit}`], { cwd })
  if (result.exitCode === 1) return null
  if (result.exitCode !== 0 || !result.stdout.trim()) throw new Error(result.stderr.trim() || "preserved ref inspection failed")
  return result.stdout.trim()
}

/** Snapshot committed and dirty worktree state to a refreshable standalone ref. */
export const preserveBranch: PreserveBranchFn = serializeBranchPreserver(preserveBranchLocked)

/** Verify that an exact local branch ref resolves to a commit. */
export async function verifyPreservedBranch(branch: string, cwd: string): Promise<boolean> {
  const targetRef = `refs/heads/${branch}`
  try {
    const commit = await readBranchRef(targetRef, cwd)
    if (!commit) return false
    const verify = await runCommand(["git", "cat-file", "-e", `${commit}^{commit}`], { cwd })
    return verify.exitCode === 0
  } catch {
    return false
  }
}

async function createSnapshotCommit(
  worktreeDir: string,
  tree: string,
  parent: string,
  description: string,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  const commit = await runCommand(
    ["git", "-C", worktreeDir, "commit-tree", tree, "-p", parent, "-m", `Preserve ${description}`],
    {
      cwd,
      env: {
        ...env,
        GIT_AUTHOR_NAME: "OpenCode Ensemble",
        GIT_AUTHOR_EMAIL: "ensemble@localhost",
        GIT_COMMITTER_NAME: "OpenCode Ensemble",
        GIT_COMMITTER_EMAIL: "ensemble@localhost",
      },
    },
  )
  if (commit.exitCode !== 0) throw new Error(commit.stderr.trim() || "worktree snapshot commit failed")
  return commit.stdout.trim()
}

/**
 * Delete a git branch. Returns true if successful.
 */
export async function deleteBranch(branch: string, cwd: string): Promise<boolean> {
  const result = await runCommand(["git", "branch", "-D", branch], { cwd })
  if (result.exitCode !== 0) {
    log(`merge-helper:delete:failed branch=${branch}`)
    return false
  }
  return true
}

/**
 * Raw squash merge of a single branch. No stash/pop — caller handles that.
 * Used by mergeBranch (single) and mergeMultipleBranches (batch).
 */
export async function mergeBranchRaw(branch: string, cwd: string): Promise<MergeResult> {
  const result = await runCommand(["git", "merge", "--squash", branch], { cwd })

  if (result.exitCode !== 0) {
    const stderr = result.stderr.trim()
    log(`merge-helper:merge:conflict branch=${branch} err=${stderr}`)
    await runCommand(["git", "merge", "--abort"], { cwd })
    return { ok: false, error: stderr || `merge exited with code ${result.exitCode}` }
  }

  return { ok: true }
}

/** Unstage all changes so merge results appear as unstaged. */
export async function gitReset(cwd: string): Promise<void> {
  await runCommand(["git", "reset", "HEAD"], { cwd })
}

/**
 * Squash merge a branch into the working directory as unstaged changes.
 * No stashing — existing unstaged changes from previous merges are preserved.
 * If the merge conflicts, the lead resolves it with git.
 */
export async function mergeBranch(branch: string, cwd: string): Promise<MergeResult> {
  const result = await mergeBranchRaw(branch, cwd)
  if (!result.ok) return result
  await gitReset(cwd)
  return { ok: true }
}

/**
 * Detect files that both the lead (local changes) and the agent (branch) modified.
 * Returns the list of overlapping file paths, or empty if safe to merge.
 */
export async function getOverlappingFiles(branch: string, cwd: string): Promise<string[]> {
  const run = async (args: string[]) => {
    const result = await runCommand(["git", ...args], { cwd })
    if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed with exit code ${result.exitCode}`)
    return result.stdout.split("\n").filter(Boolean)
  }
  const agentFiles = new Set(await run(["diff", "--name-only", "HEAD", branch]))
  const localChanged = await run(["diff", "--name-only", "HEAD"])
  const localUntracked = await run(["ls-files", "--others", "--exclude-standard"])
  const localFiles = [...new Set([...localChanged, ...localUntracked])]
  return localFiles.filter(f => agentFiles.has(f))
}

/**
 * Build the preserved branch name for a team member.
 */
export function preservedBranchName(projectName: string, teamName: string, teamId: string, memberName: string): string {
  return `ensemble/preserved/${resourcePart(projectName)}/${teamResourceSegment(teamName, teamId)}/${resourcePart(memberName)}`
}

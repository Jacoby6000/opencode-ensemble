import { describe, expect, test } from "bun:test"
import { DASHBOARD_COLORS, DASHBOARD_HEAD } from "../src/dashboard-html"
import { DASHBOARD_JS_CORE } from "../src/dashboard-js-core"
import { DASHBOARD_JS_EVENTS } from "../src/dashboard-js-events"
import { DASHBOARD_JS_RENDER } from "../src/dashboard-js-render"

function colorToken(group: string, key: string): string {
  const groupColors = DASHBOARD_COLORS[group as keyof typeof DASHBOARD_COLORS] as Record<string, string> | undefined
  const color = groupColors?.[key]
  if (!color) throw new Error(`Missing color token ${group}.${key}`)
  return color
}

function contrastRatio(foreground: string, background: string): number {
  const channel = (hex: string, index: number) => Number.parseInt(hex.slice(index, index + 2), 16) / 255
  const linear = (value: number) => value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4
  const luminance = (hex: string) => (0.2126 * linear(channel(hex, 0))) + (0.7152 * linear(channel(hex, 2))) + (0.0722 * linear(channel(hex, 4)))
  const lighter = Math.max(luminance(foreground), luminance(background))
  const darker = Math.min(luminance(foreground), luminance(background))
  return (lighter + 0.05) / (darker + 0.05)
}

describe("dashboard UI contract", () => {
  test("HTML shell exposes triage cockpit regions", () => {
    expect(DASHBOARD_HEAD).toContain('id="attention"')
    expect(DASHBOARD_HEAD).toContain('aria-label="Team attention"')
    expect(DASHBOARD_HEAD).toContain('aria-label="Project navigation"')
    expect(DASHBOARD_HEAD).toContain('aria-label="Agent roster"')
    expect(DASHBOARD_HEAD).toContain('aria-label="Task board"')
    expect(DASHBOARD_HEAD).toContain('aria-label="Activity feed"')
    expect(DASHBOARD_HEAD).toContain('aria-label="Event timeline"')
    expect(DASHBOARD_HEAD).toContain('id="drawer-title"')
    expect(DASHBOARD_HEAD).toContain('id="drawer" class="scroll p-4" tabindex="-1" inert')
  })

  test("dashboard shell has no third-party runtime resources", () => {
    expect(DASHBOARD_HEAD).not.toContain('src="https://')
    expect(DASHBOARD_HEAD).not.toContain('href="https://')
    expect(DASHBOARD_HEAD).not.toContain("cdn.tailwindcss.com")
    expect(DASHBOARD_HEAD).not.toContain("fonts.googleapis.com")
  })

  test("fixed dashboard chrome is constrained on narrow viewports", () => {
    expect(DASHBOARD_HEAD).toContain("px-3 sm:px-4")
    expect(DASHBOARD_HEAD).toContain("gap-2 sm:gap-3 min-w-0 flex-1")
    expect(DASHBOARD_HEAD).toContain("overflow-x-auto scroll whitespace-nowrap")
  })

  test("project navigation uses docs-style outline semantics", () => {
    expect(DASHBOARD_HEAD).toContain('id="projects"')
    expect(DASHBOARD_JS_RENDER).toContain('<nav class="text-[12px]"')
    expect(DASHBOARD_JS_RENDER).toContain('class="project-link')
    expect(DASHBOARD_JS_RENDER).toContain('class="team-link')
    expect(DASHBOARD_JS_RENDER).toContain('border-l-2')
    expect(DASHBOARD_JS_RENDER).toContain("function renderProjectNavHeader")
    expect(DASHBOARD_JS_RENDER).toContain("function renderProjectButton")
    expect(DASHBOARD_JS_RENDER).toContain("function renderTeamLink")
    expect(DASHBOARD_JS_RENDER).toContain("[...teams].sort")
    expect(DASHBOARD_JS_RENDER).toContain("statusTitleProject")
    expect(DASHBOARD_JS_RENDER).toContain("statusTitleTeam")
    expect(DASHBOARD_JS_CORE).toContain("function projectLabel")
    expect(DASHBOARD_JS_EVENTS).toContain("function selectProject")
    expect(DASHBOARD_JS_EVENTS).toContain("function selectTeam")
  })

  test("header exposes a project-grouped team switcher", () => {
    expect(DASHBOARD_HEAD).toContain('id="team-switcher"')
    expect(DASHBOARD_HEAD).toContain('aria-label="Switch team"')
    expect(DASHBOARD_HEAD).toContain('onchange="selectTeam(this.value)"')
    expect(DASHBOARD_JS_RENDER).toContain("function rTeamSwitcher")
    expect(DASHBOARD_JS_RENDER).toContain("<optgroup")
    expect(DASHBOARD_JS_EVENTS).toContain("rTeamSwitcher(t)")
  })

  test("project navigation can collapse", () => {
    expect(DASHBOARD_HEAD).not.toContain('<button id="nav-toggle"')
    expect(DASHBOARD_HEAD).toContain('id="project-rail"')
    expect(DASHBOARD_HEAD).toContain('id="nav-expand"')
    expect(DASHBOARD_JS_RENDER).toContain('id="nav-toggle"')
    expect(DASHBOARD_JS_RENDER).toContain('aria-label="Hide project navigation"')
    expect(DASHBOARD_HEAD).toContain("#content.nav-collapsed")
    expect(DASHBOARD_HEAD).toContain("#projects[hidden]")
    expect(DASHBOARD_HEAD).toContain("#project-rail[hidden]")
    expect(DASHBOARD_JS_EVENTS).toContain("function applyNavCollapse")
    expect(DASHBOARD_JS_EVENTS).toContain("id==='nav-toggle'")
    expect(DASHBOARD_JS_EVENTS).toContain("aria-expanded")
    expect(DASHBOARD_JS_EVENTS).toContain("projects.hidden=navCollapsed")
    expect(DASHBOARD_JS_EVENTS).toContain("rail.hidden=!navCollapsed")
    expect(DASHBOARD_JS_EVENTS).toContain("expand.focus()")
    expect(DASHBOARD_JS_EVENTS).toContain("toggle.focus()")
    expect(DASHBOARD_JS_EVENTS).toContain("aria-hidden")
  })

  test("dashboard polls state relative to the served page", () => {
    expect(DASHBOARD_JS_EVENTS).toContain("apiFetch('api/state')")
    expect(DASHBOARD_JS_EVENTS).not.toContain("fetch('/api/state')")
  })

  test("dashboard API requests use a fragment-supplied bearer token", () => {
    expect(DASHBOARD_JS_CORE).toContain("location.hash")
    expect(DASHBOARD_JS_CORE).toContain("sessionStorage")
    expect(DASHBOARD_JS_CORE).toContain("history.replaceState")
    expect(DASHBOARD_JS_CORE).toContain("Authorization")
    expect(DASHBOARD_JS_CORE).toContain("Bearer ")
  })

  test("group channels are collision-safe and nonmember lead composition is read-only", () => {
    expect(DASHBOARD_JS_CORE).toContain("function channelParts")
    expect(DASHBOARD_JS_RENDER).toContain("'member:'+m.name")
    expect(DASHBOARD_JS_RENDER).toContain("'group:'+g.name")
    expect(DASHBOARD_JS_RENDER).toContain("Groups")
    expect(DASHBOARD_JS_RENDER).toContain("Read-only: lead is not a group participant.")
    expect(DASHBOARD_JS_RENDER).toContain("text.disabled=unavailable")
    expect(DASHBOARD_JS_EVENTS).toContain("{group:cp.name,content:content}")
  })

  test("clears a fragment token even when session storage is unavailable", () => {
    const replacements: string[] = []
    const evaluate = new Function(
      "location",
      "sessionStorage",
      "history",
      "localStorage",
      "Headers",
      "fetch",
      `${DASHBOARD_JS_CORE};return dashboardToken`,
    )
    const token = evaluate(
      { hash: "#token=fragment-token", pathname: "/", search: "?view=team" },
      { setItem() { throw new Error("storage disabled") }, getItem() { return null } },
      { replaceState(_state: unknown, _title: string, url: string) { replacements.push(url) } },
      { getItem() { return null } },
      Headers,
      () => Promise.reject(new Error("unexpected fetch")),
    )

    expect(token).toBe("fragment-token")
    expect(replacements).toEqual(["/?view=team"])
  })

  test("full prompts and message bodies are fetched only for expanded details", () => {
    expect(DASHBOARD_JS_CORE).toContain("function ensureTeamMessages")
    expect(DASHBOARD_JS_CORE).toContain("function ensureMemberPrompt")
    expect(DASHBOARD_JS_CORE).toContain("api/teams/")
    expect(DASHBOARD_JS_RENDER).toContain("messageContent(")
    expect(DASHBOARD_JS_RENDER).toContain("memberPrompt(")
    expect(DASHBOARD_JS_EVENTS).toContain("ensureTeamMessages(t.id)")
  })

  test("agent prioritization helpers are defined", () => {
    expect(DASHBOARD_JS_CORE).toContain("function rankAgent")
    expect(DASHBOARD_JS_CORE).toContain("function deriveAttention")
  })

  test("attention renderer exposes urgent triage copy", () => {
    expect(DASHBOARD_JS_RENDER).toContain("function rAttention")
    expect(DASHBOARD_JS_RENDER).toContain("Needs attention")
  })

  test("attention renderer exposes durable scheduler pressure", () => {
    expect(DASHBOARD_JS_RENDER).toContain("queuedWakes")
    expect(DASHBOARD_JS_RENDER).toContain("activeRuns")
    expect(DASHBOARD_JS_RENDER).toContain("expiredRuns")
    expect(DASHBOARD_JS_RENDER).toContain("scheduler queued")
  })

  test("keyboard and accessibility hooks are present", () => {
    expect(DASHBOARD_JS_RENDER).toContain("onkeydown")
    expect(DASHBOARD_JS_RENDER).toContain("aria-expanded")
    expect(DASHBOARD_JS_EVENTS).toContain("e.key==='Enter'")
    expect(DASHBOARD_JS_EVENTS).toContain("e.key==='Escape'")
  })

  test("shortcut overlay exposes dialog semantics", () => {
    expect(DASHBOARD_HEAD).toContain('id="sco" role="dialog"')
    expect(DASHBOARD_HEAD).toContain('aria-modal="true"')
    expect(DASHBOARD_HEAD).toContain('aria-hidden="true"')
    expect(DASHBOARD_HEAD).toContain('aria-labelledby="shortcuts-title"')
    expect(DASHBOARD_HEAD).toContain('tabindex="-1"')
    expect(DASHBOARD_HEAD).toContain('id="shortcuts-title"')
  })

  test("shortcut overlay manages modal focus", () => {
    expect(DASHBOARD_JS_EVENTS).toContain("function openShortcuts")
    expect(DASHBOARD_JS_EVENTS).toContain("function closeShortcuts")
    expect(DASHBOARD_JS_EVENTS).toContain("function setBackgroundInert")
    expect(DASHBOARD_JS_EVENTS).toContain("function modalOpen")
    expect(DASHBOARD_JS_EVENTS).toContain("function trapFocus")
    expect(DASHBOARD_JS_EVENTS).toContain("el.inert=locked")
    expect(DASHBOARD_JS_EVENTS).toContain("e.key==='Tab'")
    expect(DASHBOARD_JS_EVENTS).toContain("document.getElementById('sco').focus()")
    expect(DASHBOARD_JS_EVENTS).toContain("aria-hidden")
    expect(DASHBOARD_JS_EVENTS).toContain("if(!modalOpen())setBackgroundInert(false)")
  })

  test("agent drawer exposes named close control and modal focus handling", () => {
    expect(DASHBOARD_JS_RENDER).toContain('id="drawer-close"')
    expect(DASHBOARD_JS_RENDER).toContain('aria-label="Close agent detail"')
    expect(DASHBOARD_JS_RENDER).toContain("setBackgroundInert(true)")
    expect(DASHBOARD_JS_RENDER).toContain("drawer.inert=false")
    expect(DASHBOARD_JS_RENDER).toContain("drawer.inert=true")
    expect(DASHBOARD_JS_RENDER).toContain("drawer.focus()")
    expect(DASHBOARD_JS_RENDER).toContain("if(!modalOpen())setBackgroundInert(false)")
    expect(DASHBOARD_JS_EVENTS).toContain("trapFocus(document.getElementById('drawer'),e)")
    expect(DASHBOARD_JS_EVENTS).toContain("drawerOpen&&e.key==='?'")
  })

  test("small dashboard text tokens stay readable on dark surfaces", () => {
    const darkSurfaces = [colorToken("base", "950"), colorToken("base", "900")]
    const smallText = [colorToken("txt", "400"), colorToken("txt", "500")]

    for (const text of smallText) {
      for (const surface of darkSurfaces) {
        expect(contrastRatio(text, surface)).toBeGreaterThanOrEqual(4.5)
      }
    }
  })

  test("agent cards do not dim operational text with whole-card opacity", () => {
    expect(DASHBOARD_JS_RENDER).not.toContain("opacity-50")
  })

  test("drawer includes activity timeline section", () => {
    expect(DASHBOARD_JS_RENDER).toContain("drawer-activity-list")
    expect(DASHBOARD_JS_RENDER).toContain("rDrawerActivityUpdate")
    expect(DASHBOARD_JS_RENDER).toContain("fetchActivity")
  })

  test("drawer has verbose toggle control", () => {
    expect(DASHBOARD_JS_EVENTS).toContain("function toggleVerbose")
    expect(DASHBOARD_JS_RENDER).toContain("toggleVerbose()")
    expect(DASHBOARD_JS_RENDER).toContain("id=\"verbose-toggle\"")
    expect(DASHBOARD_JS_RENDER).toContain("aria-pressed")
    expect(DASHBOARD_JS_RENDER).toContain("verbose:")
  })

  test("activity fetch uses relative path", () => {
    expect(DASHBOARD_JS_EVENTS).toContain("apiFetch('api/session/'")
    expect(DASHBOARD_JS_EVENTS).not.toContain("fetch('/api/session/'")
  })

  test("verbose preference persists to localStorage", () => {
    expect(DASHBOARD_JS_CORE).toContain("localStorage.getItem('ensemble-verbose')")
    expect(DASHBOARD_JS_EVENTS).toContain("localStorage.setItem('ensemble-verbose'")
  })

  test("v keyboard shortcut toggles verbose", () => {
    expect(DASHBOARD_JS_EVENTS).toContain("e.key==='v'")
    expect(DASHBOARD_JS_EVENTS).toContain("toggleVerbose()")
  })

  test("v shortcut appears in shortcuts overlay", () => {
    expect(DASHBOARD_HEAD).toContain(">v</kbd>")
    expect(DASHBOARD_HEAD).toContain("Toggle verbose")
  })

  test("activity timeline renders reasoning blocks", () => {
    expect(DASHBOARD_JS_RENDER).toContain("reasoning")
    expect(DASHBOARD_JS_RENDER).toContain("Reasoning")
  })

  test("activity timeline renders file parts", () => {
    expect(DASHBOARD_JS_RENDER).toContain("file")
    expect(DASHBOARD_JS_RENDER).toContain("filePath")
  })

  test("activity timeline renders text prompts and responses", () => {
    expect(DASHBOARD_JS_RENDER).toContain("text")
    expect(DASHBOARD_JS_RENDER).toContain("prompt")
    expect(DASHBOARD_JS_RENDER).toContain("response")
  })

  test("exposes overview and comprehensive conversation views", () => {
    expect(DASHBOARD_HEAD).toContain('data-view="overview"')
    expect(DASHBOARD_HEAD).toContain('data-view="conversations"')
    expect(DASHBOARD_HEAD).toContain('id="conversation-view"')
    expect(DASHBOARD_HEAD).toContain('id="conversation-channels"')
    expect(DASHBOARD_HEAD).toContain('id="conversation-history"')
    expect(DASHBOARD_HEAD).toContain('id="conversation-compose"')
    expect(DASHBOARD_JS_RENDER).toContain("function rConversations")
    expect(DASHBOARD_JS_EVENTS).toContain("function selectView")
  })

  test("supports direct and broadcast channels with authenticated composition", () => {
    expect(DASHBOARD_JS_RENDER).toContain("Broadcast mailbox")
    expect(DASHBOARD_JS_RENDER).toContain("Send broadcast")
    expect(DASHBOARD_JS_RENDER).toContain("Send message")
    expect(DASHBOARD_JS_EVENTS).toContain("function sendConversationMessage")
    expect(DASHBOARD_JS_EVENTS).toContain("method:'POST'")
    expect(DASHBOARD_JS_EVENTS).toContain("apiFetch('api/teams/'")
  })

  test("persists deep-linkable team, member, and view state", () => {
    expect(DASHBOARD_JS_CORE).toContain("URLSearchParams(location.search)")
    expect(DASHBOARD_JS_EVENTS).toContain("function syncLocation")
    expect(DASHBOARD_JS_EVENTS).toContain("popstate")
    expect(DASHBOARD_JS_EVENTS).toContain("view")
    expect(DASHBOARD_JS_EVENTS).toContain("team")
    expect(DASHBOARD_JS_EVENTS).toContain("member")
  })
})

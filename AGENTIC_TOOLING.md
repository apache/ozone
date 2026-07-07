# Agentic Tooling in Apache Ozone

This repository is set up to work with AI coding agents (Claude Code, OpenAI Codex,
Gemini CLI, Cursor, and other tools that read `AGENTS.md`). This page explains how that
setup is organized, why, and the rules to keep it healthy.

## Goal

- **One source of truth.** Project rules live once, in `AGENTS.md` — the cross-tool
  standard ([agents.md](https://agents.md)) that most agents read directly.
- **Small always-on context.** `AGENTS.md` stays a slim behavioral core; bulky reference
  material (commands, module map) lives in separate files that agents open only when needed.
- **Same knowledge for everyone; extra help where a tool supports it.** Every tool consumes
  the same rules and reference docs. Claude Code additionally gets skills and a subagent;
  those are conveniences layered on top, never a separate copy of the knowledge.

## File layout

```
AGENTS.md                          # source of truth: slim behavioral rules (all tools)
CLAUDE.md                          # Claude entry point  -> @AGENTS.md (+ Claude-only notes)
GEMINI.md                          # Gemini entry point  -> @./AGENTS.md

dev-support/agent/
├── commands.md                    # build/test/lint commands + tiered CI-check catalog
└── project-map.md                 # module map, key paths, service boundaries, test types

.claude/                           # Claude Code only
├── skills/ozone-map/SKILL.md      #   module/path lookup (auto-surfaces on demand)
└── agents/ozone-builder.md        #   builds/tests/checks in an isolated context

.agents/skills/                    # OpenAI Codex only (its skill discovery path)
├── ozone-map/SKILL.md             #   copy of the map skill
└── ozone-build/SKILL.md           #   build/test/lint commands (Codex has no subagent)
```

## What each piece does

| Path | Role |
| :--- | :--- |
| `AGENTS.md` | The rules and restrictions every agent loads each session. Keep it short. |
| `CLAUDE.md` | Claude Code reads this; it imports `AGENTS.md` and adds a few Claude-only lines. |
| `GEMINI.md` | Gemini CLI reads this; it imports `AGENTS.md`. |
| `dev-support/agent/commands.md` | Build, test, run, and the full tiered check catalog. Read on demand. |
| `dev-support/agent/project-map.md` | Where things live, service boundaries, test taxonomy. Read on demand. |
| `.claude/skills/ozone-map/` | Claude skill that surfaces the module/path lookup when relevant. |
| `.claude/agents/ozone-builder.md` | Claude subagent that runs builds/tests/checks in its own context and returns a concise pass/fail, keeping large logs out of the main conversation. |
| `.agents/skills/ozone-map/` | Byte-identical copy of the map skill for Codex. |
| `.agents/skills/ozone-build/` | Codex build/test/lint skill (Codex has no subagent equivalent). |

Build/test/lint by tool: **Claude** -> `ozone-builder` subagent; **Codex** -> `ozone-build`
skill; **Cursor/Gemini/others** -> read `dev-support/agent/commands.md` via the `AGENTS.md`
pointer.

## Caveats and maintenance rules

- **Edit knowledge once.** Behavioral rules go in `AGENTS.md`; reference material goes in
  `dev-support/agent/*.md`. The wrappers and skills only *point* — never copy content into
  them.
- **Reference docs are cited by plain path, never with `@`.** An `@path` in `AGENTS.md` or
  `CLAUDE.md` is eagerly pulled into context at launch, which defeats the point of keeping
  reference material lazy. `@` is used *only* in `CLAUDE.md`/`GEMINI.md` to import
  `AGENTS.md`.
- **`CLAUDE.md`/`GEMINI.md` are small import files, not symlinks.** A committed symlink is
  checked out as a plain text file on default Windows Git setups and breaks.
- **`ozone-map` exists as two identical copies** (Claude and Codex discover skills from
  different directories). Keep them in sync when you change one.
- **These agent-config files carry no Apache license header** — they are listed in
  `dev-support/rat/rat-exclusions.txt`.
- **Per-module `AGENTS.md` files are intentionally avoided** for now. If a module ever needs
  its own rules, add them sparingly; nested files load only when that module is touched.

## AI-generated contributions

When a contribution is authored in whole or in part with AI tooling, disclose it per the
[ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html): add
a `Generated-by: TOOL (MODEL)` line to the pull request description (and, where known, the
commit message). See `AGENTS.md` for the full commit/PR conventions.

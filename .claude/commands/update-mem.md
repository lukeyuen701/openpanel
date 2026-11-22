---
description: Review and update the OpenPanel Claude memory layers (AGENTS.md / CLAUDE.md, CLAUDE.local.md, .claude skills and commands, auto memory)
argument-hint: "[agents-md|local|skills|auto-memory] [instruction]"
---

# Update memory

Review and update what Claude Code reads about this repo: the committed
instruction files, the private local file, the project's skills and commands,
and the machine-local auto memory.

> Current shape: root `CLAUDE.md` is one line, `@AGENTS.md`, so `AGENTS.md` IS
> the root instructions (Codex reads it directly). Nested packages follow the
> same pair (`packages/core/CLAUDE.md` → `@AGENTS.md`); Claude Code loads a
> nested `AGENTS.md` only through such a `CLAUDE.md`. `CLAUDE.local.md` is
> private (in `.git/info/exclude`). There is no `.claude/rules` directory. Take counts
> from the filesystem, never from a number written here. The auto memory
> directory is keyed by the checkout's absolute path, so a worktree or a moved
> checkout starts with an empty one — check what is there. When a target points
> at a layer that does not exist yet, **create** it (a minimal, accurate file)
> rather than reporting "not found".

---

**Usage**: `/update-mem [target] [instruction]`

```bash
/update-mem                                     # Full audit of every layer
/update-mem agents-md                           # Root and nested AGENTS.md / CLAUDE.md
/update-mem local "Kiri is now 4 GiB"           # CLAUDE.local.md
/update-mem skills                              # .claude/skills/*/SKILL.md + .claude/commands/*.md
/update-mem auto-memory "remove outdated entries"
```

---

## Targets

| Target        | Scope                    | Files |
| ------------- | ------------------------ | ----- |
| `agents-md`   | Committed instructions   | `CLAUDE.md`, `AGENTS.md`, every nested `*/AGENTS.md` and `*/CLAUDE.md` (`git ls-files \| grep -E '(AGENTS\|CLAUDE)\.md$'`) |
| `local`       | Private, this checkout   | `CLAUDE.local.md` |
| `skills`      | Procedures               | `.claude/skills/*/SKILL.md`, `.claude/commands/*.md`, `.claude/scripts/*` |
| `auto-memory` | Machine-local memory     | `~/.claude/projects/<checkout abs path, every non-alphanumeric → ->/memory/` |
| _(empty)_     | Full audit               | All of the above |

---

## Where a fact belongs

This checkout is a fork; the committed files are shared with upstream.

| Fact | Layer |
| ---- | ----- |
| A convention every contributor needs (layout, verification, commit rules, ClickHouse and migration rules) | `AGENTS.md` (the nested one when it is about one package) |
| A rule for this fork or this machine (remotes, push permission, Docker context, local versions) | `CLAUDE.local.md` |
| How to do a recurring task end to end | a skill (`.claude/skills/<name>/SKILL.md`) or a command |
| Why something is the way it is, environment facts, user preferences, ongoing work, external pointers | auto memory |
| Anything the code, `git log` or a doc in the repo already says | nowhere — point at it |

An edit to a committed `AGENTS.md` is an upstream-visible change and a merge
conflict waiting for the next upstream sync: make it only when the fact is
true for every contributor, and say so in the report. Fork-only facts never go
there. Private details (emails, hosts, credentials, the production topology)
never go in a committed file.

---

## Process

### 1. Identify the target

Parse `$ARGUMENTS`: the first word is the target if it matches one above, the
rest is the instruction. No target → full audit.

### 2. Read the current state

- **agents-md**: every file in the target row, whole. Note which nested
  `AGENTS.md` has a `CLAUDE.md` importing it — one without is never loaded by
  Claude Code.
- **local**: `CLAUDE.local.md`, and `git check-ignore -v CLAUDE.local.md` (it
  must stay untracked).
- **skills**: each `SKILL.md` with its frontmatter (`name`, `description` — the
  description is the trigger, so it is part of what you review), each command
  with its `description` / `argument-hint`.
- **auto-memory**: `MEMORY.md` and every topic file.

### 3. Analyze and report

For each layer, check:

- **Accuracy**: do cited paths, scripts (`package.json` scripts by workspace),
  env vars, routes and module names exist? Do described behaviours match the
  code? The checker below catches dead paths; scripts and behaviours you check
  by reading.
- **Freshness**: removed features, renamed files, finished work still described
  as ongoing, relative dates ("yesterday", "next week") instead of absolute.
- **Completeness**: conventions the user corrected you on or confirmed, that a
  future session would otherwise repeat wrongly.
- **Routing**: nested `AGENTS.md` without a `CLAUDE.md` importing it; a skill
  whose `description` would not trigger on the words people actually use (keep
  the Chinese trigger phrases where a skill has them); a command and a skill
  with the same name.
- **Duplication**: the same fact in two layers. Keep it in the layer the table
  above names; the other becomes a pointer or is removed. A memory that
  restates `AGENTS.md` is removed.
- **Size**: `AGENTS.md` and `MEMORY.md` load every session. Report line
  counts; for `MEMORY.md` the unit is bytes and line length — an index line is
  a hook, not the memory's body.
- **Conflicts**: instructions that contradict each other (`CLAUDE.local.md`
  overrides `AGENTS.md`'s Git section on purpose — that one is not a conflict).
- **Privacy**: anything in a committed file that is private (see above).

### 4. Apply updates

Following the analysis and the user's instruction:

- **Remove** outdated entries — after verifying against the codebase.
- **Update** stale paths, scripts, behaviours.
- **Add** what the user asked for, in the layer the table names.
- **Create** a missing layer when the target calls for it.
- **Deduplicate** where one fact lives in two places that disagree.

When you shorten an index line, move its prose into the topic file FIRST —
nothing may be lost — then cut the line back to its hook. Relink with
`[[name]]` where memories relate.

### 5. Verify

```bash
# Dead paths cited by instruction files, skills, commands and CLAUDE.local.md;
# nested AGENTS.md that Claude Code never loads; MEMORY.md byte / line budget,
# topic frontmatter, every topic reachable from the index, no dangling links.
bun .claude/scripts/check-memory.mjs

# Root CLAUDE.md still only imports AGENTS.md.
cat CLAUDE.md

# CLAUDE.local.md is still untracked.
git check-ignore -v CLAUDE.local.md

# Formatting for a changed script (Markdown is not checked by ultracite).
npx ultracite fix <changed .claude/scripts files>
```

`CLAUDE_MEMORY_DIR=<dir>` points the checker at another memory directory
(a worktree's, or a scratch copy for testing). A failure you deliberately leave
(for example a stale path inside an upstream file you chose not to edit) is
listed under Recommendations, not silenced.

---

## Auto memory guidelines

- `MEMORY.md` is the index and the only memory file loaded per session: one
  line per memory, `- [Title](file.md) — hook`. **The hook is the recall
  surface**: a topic file is never read unless its line makes you open it, so a
  memory that exists to stop a specific accident names that accident in its
  hook, in the words it happens in.
- Topic files carry frontmatter `name`, `description`, `metadata.type`
  (`user` / `feedback` / `project` / `reference`). `feedback` and `project`
  bodies end with **Why:** and **How to apply:** lines.
- Every topic file is reachable from the index; an unlinked file is dead.
- Do NOT store: code patterns derivable from source, git history, fix recipes,
  anything `AGENTS.md` or a repo doc already records.
- DO store: environment facts (Docker on Kiri, local versions, production CPU
  limits), user preferences and corrections, standing constraints, ongoing work
  with absolute dates, external references.

---

## What NOT to change

- Never rename code identifiers, tables, routes or env vars because a memory
  says so.
- Never delete a memory without first verifying it is outdated.
- Never put secrets, credentials, `.env` values or the user's personal details
  in any memory layer.
- Never commit `CLAUDE.local.md` or anything under `.agents/`.

---

## Output

Report at the end, every cell from the commands you ran:

```
## Memory Update Report

### Changes Made
- <layer>: <action> — <what and why>

### Current Stats
| Layer          | Files | Size            | Checks                         |
|----------------|-------|-----------------|--------------------------------|
| AGENTS/CLAUDE  | <n>   | <lines each>    | paths: <ok/n>, routing: <ok/n> |
| CLAUDE.local   | <0/1> | <lines>         | untracked: <ok/no>             |
| Skills/commands| <n>   | —               | paths: <ok/n>                  |
| Auto memory    | <n>   | index <bytes> B | index + reachability: <ok/n>   |

### Recommendations
- <follow-ups, including checker failures left on purpose>
```

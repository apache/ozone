# AGENTS instructions

Apache Ozone is a multi-module Maven project (Java 8 bytecode, JDK 21 runtime). The root
version lives in `pom.xml`. Two aggregators: `hadoop-hdds/` (storage layer) and
`hadoop-ozone/` (services). This file is the source of truth read by all coding agents; see
`AGENTIC_TOOLING.md` for how the agent setup is organized.

## Working Style

- Prefer the smallest correct change. Do not add features, abstractions, refactors, or
  cleanup that were not asked for.
- Keep diffs surgical. Every changed line should trace to the task. Do not reformat, rewrap,
  or rename adjacent code "while you are here".
- Match the surrounding module before introducing a new pattern. Reuse existing Ozone and
  Ratis helpers, test scaffolding, and service abstractions; extend an existing helper rather
  than duplicating logic or adding a one-off abstraction.
- If there are multiple reasonable interpretations, state the tradeoff and ask instead of
  guessing.
- Do not wrap lines early. The project limit is 120 characters; use the space.
- Use established Ozone vocabulary (SCM, OM, datanode, container, pipeline, volume, bucket,
  key, snapshot, Recon, FSO, OBS, S3 Gateway). Avoid inventing new architecture terms.

## Change Boundaries

- Keep service responsibilities separate. Do not move OM logic into SCM paths, bypass existing
  request/response layers, or add cross-service shortcuts for convenience.
- When changing a wire type, update the Protobuf definition, translators, server-side logic,
  and relevant compatibility/integration tests.
- Prefer existing bucket-layout, snapshot, and upgrade abstractions over one-off conditionals.
- Do not hand-edit generated sources or generated web artifacts when a source file or
  generation step exists.
- For integration coverage, extend an existing suite, base class, or cluster provider before
  creating a new `MiniOzoneCluster` lifecycle.

## Coding Standards

- 2-space indentation; stay within 120 characters.
- Add the Apache license header to new files unless the area is RAT-exempt.
- Do not add `@author` tags.
- Keep comments concrete and local. Avoid vague architecture prose or invented terminology.
- Prefer existing helpers over new abstractions for single-call-site use.

## Testing

- New behavior and bug fixes come with tests.
- Start with the narrowest useful test: unit tests for local logic; integration tests when
  behavior depends on service boundaries, cluster lifecycle, storage, RPC, or upgrade flows.
- Prefer merging integration coverage into an existing suite over a new cluster class.
- Before wrapping up, run the quick checks (`checkstyle`, `rat`, `pmd`, `author`, `bats` as
  applicable); full tiered check list in `dev-support/agent/commands.md`.

## Commits & PRs

- Every change maps to an Apache Jira in the HDDS project.
- Branch, commit, and PR titles start with the Jira ID: `HDDS-1234. Short summary`.
- Use incremental commits for review; the committer squashes on merge — do not self-squash or
  rewrite history.
- Bring a branch up to date with `master` by merge, not rebase:
  `git merge --no-edit origin/master`. Avoid force-push unless a maintainer asks.
- PR description = Jira link, problem statement, chosen approach, how it was tested.
- Disclose AI tooling in the PR description: `Generated-by: TOOL (MODEL)` (ASF policy).

## Ask First

- Large features or design changes that may need an Ozone Enhancement Proposal (OEP design
  docs are Markdown-only).
- Large cross-module refactors not required for the task.
- New third-party dependencies.
- Protobuf or RPC changes with compatibility impact.
- RocksDB layout, metadata schema, or upgrade/finalization changes.
- Broad terminology or naming cleanups across many files.

## Never

- Commit secrets, credentials, or tokens.
- Use destructive git commands unless explicitly requested.
- Hand-edit generated files when the source or generation workflow exists.
- Add unrelated cleanup, formatting churn, or speculative abstractions to the same change.

## Build & reference (read on demand — do not @-import)

- Default local build flags: `-DskipShade -DskipRecon -DskipDocs` (full command reference and
  tiered CI checks: `dev-support/agent/commands.md`).
- Module map, key paths, service boundaries: `dev-support/agent/project-map.md`.
- Contribution process (Jira, PR workflow), OEP: `CONTRIBUTING.md` and
  `.github/pull_request_template.md`.

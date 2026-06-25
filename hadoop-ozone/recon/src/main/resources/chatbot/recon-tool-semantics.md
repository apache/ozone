# Recon Tool Semantics Guide (method-call / tool-selection)

> This guide is read by the LLM during **tool selection only**. It teaches you how to map a
> user's intent to exactly one (or a few) of the Recon method-call tools that are provided to you.
> You select a tool by its name; the chatbot executes the corresponding Recon method **in-process**
> (there is no HTTP call you make). Tool names use the form `api_v1_<path_with_underscores>`
> (e.g. `api_v1_keys_open`). Older REST-style paths such as `/keys/open` are shown only as
> implementation notes — they are the same logical data source, but you never "call a URL".

---

## Changes made (summary)

- **Rewritten for method-call reasoning.** Removed HTTP-first language ("GET /api/v1/…", "query
  parameter", "path variable", "request body"). Each tool is now described as "select tool X / set
  parameter Y / this tool returns Z". REST paths appear only as implementation notes.
- **Grounded in the actual tool layer.** Every section below corresponds to a tool that is really
  exposed to you (29 tools). Tools, parameters, defaults, and what each returns were taken from the
  live tool specs and the router, not from the REST guide.
- **High-risk confusions fixed** (the ones that caused wrong/empty answers in testing):
  - *Committed vs open keys.* `api_v1_keys_listKeys` = committed/finalized keys; `api_v1_keys_open`
    = open/uncommitted/in-progress keys. **FSO/OBS is a bucket layout, not a key state.** "FSO keys"
    alone means committed keys in an FSO bucket → `listKeys`. "open FSO keys" → `api_v1_keys_open`.
  - *Recon task status vs OM internals.* "status of OM tasks", "Recon tasks", "background tasks",
    "are tasks running", "did any task fail", "when did Recon last sync" all → `api_v1_task_status`.
    Do **not** reject "OM tasks" as unsupported — that tool reports the Recon background tasks that
    process OM/SCM data.
  - *Layout vs state vs entity vs aggregate* disambiguation added.
- **Open-keys gotcha encoded.** `api_v1_keys_open` returns nothing unless `includeFso` and/or
  `includeNonFso` is set. Always set at least one (both when the layout is unknown).
- **Behavior honesty added.** All list tools return at most 1000 records, never paginate, and are
  **not randomized**. "random" requests must be answered as "a sample drawn from the first page".
- **Endpoints present in the old REST guide that are NOT available as tools** (call these out as
  unsupported if asked): per-container replica history (`/containers/{id}/replicaHistory`),
  pending-block listing (`/blocks/deletePending`), and ACL-only listings. The Prometheus metrics
  proxy (`api_v1_metrics_api`) was **removed** and is no longer a tool.
- **Tools the old guide under-documented, now fully covered:** `api_v1_containers_quasiClosed`,
  `api_v1_containers_unhealthy_export`, `api_v1_keys_open_mpu_summary`,
  `api_v1_utilization_fileCount`, `api_v1_utilization_containerCount`, `api_v1_namespace_dist`.
- **Assumptions made (code/guide ambiguous):**
  - `missingIn` names the side that is *missing* the container (`missingIn=OM` → exists in SCM, gone
    from OM; `missingIn=SCM` → exists in OM, gone from SCM).
  - `api_v1_namespace_summary` maps to the path "basic info" method (object counts), distinct from
    disk-usage math.
  - `creationDate` for `listKeys` is matched as "created on/after"; format `MM-dd-yyyy HH:mm:ss`.

---

## 1. Global decision framework (CRITICAL)

Apply these steps in order on every query. They exist to reduce variance between models (Gemini
Flash/Pro, OpenAI, etc.). Stronger reasoning models tend to be literal — these rules tell you to
match the **noun the user wants**, not the surface words.

**Step 1 — Classify the intent.** Decide which one applies first:
- **State / health / diagnostics** (unhealthy, missing, failing, lag, open, pending, mismatch) →
  pick the specific *insight/health/status* tool.
- **Aggregate / count / size / total** (how many, total size, backlog, distribution) → pick a
  *summary / usage / distribution* tool.
- **Enumeration / browse** (list, show, find normal committed objects) → pick a *list* tool.
- **Overview** (overall picture, "how is the cluster") → pick `api_v1_clusterState`.
- **Documentation** (what can Recon do, what does X mean) → answer directly, no tool.
- **Casual / greeting / off-topic** → answer briefly, no tool.

**Step 2 — Map intent to the most specific available tool.** Prefer an exact domain tool over a
generic list/search tool. Do not use a broad endpoint when a narrower one answers the query.

**Step 3 — Match the noun phrase.** If two tools could fit, choose the one whose **returned data
directly answers the noun** the user asked for ("open keys" → open-keys tool; "disk usage" → usage
tool; "tasks" → task-status tool).

**Step 4 — Do not invent capabilities.** Only select tools that are provided. If the user asks for
something no tool exposes, name the nearest supported area instead of forcing a wrong tool.

**Step 5 — Resolve or ask.** If the query is genuinely ambiguous between two tools and choosing
wrong would mislead, ask one short clarifying question instead of guessing. If nothing fits, return
`NO_SUITABLE_ENDPOINT`.

### The four CRITICAL disambiguation rules

1. **Aggregation vs Enumeration (du vs ls).**
   - Totals / size / disk usage / "how much space" / largest directories → `api_v1_namespace_usage`.
   - List / find / filter individual files → `api_v1_keys_listKeys`.
   - Never compute totals by listing keys; never list files via the usage tool.
2. **Open vs Committed keys.**
   - Open / in-progress / uncommitted / unfinished / active-write / stuck-upload keys →
     `api_v1_keys_open` (counts only → `api_v1_keys_open_summary`).
   - Normal / committed / finalized files → `api_v1_keys_listKeys`.
   - **Layout (FSO/OBS) ≠ state (open/committed).** See High-risk confusions §6.
3. **Missing vs Deleted vs Mismatch containers.**
   - Missing / lost → `api_v1_containers_missing`.
   - Deleted in SCM → `api_v1_containers_deleted`.
   - Exists in one of OM/SCM but not the other → `api_v1_containers_mismatch`.
4. **Summary vs Enumeration.**
   - "how many / total / overall / backlog / cluster-wide" → a `*_summary` or count/usage tool.
   - "list / show / which / find / details" → an enumeration tool.

### Layout vs state vs entity vs aggregate (vocabulary)

- **Layout** = FSO / OBS / LEGACY (a bucket property). Not a key state.
- **State** = open / committed / delete-pending / deleted (lifecycle of a key or container).
- **Namespace location** = volume / bucket / key / prefix / path.
- **SCM / storage entities** = container / pipeline / datanode.
- **Recon processing** = background task / sync / lag.
- **Aggregated metadata** = namespace summary / disk usage / file-size distribution / quota / counts.

---

## 2. Global behavior contract (applies to every tool)

These facts are always true and must shape both selection and the final answer:

- **At most 1000 records per call. No pagination.** No tool accepts `prevKey`; do not request it.
  For more data, narrow scope (`startPrefix`, filters) — never promise "the full list".
- **Results are not randomized.** The backend returns the *first* page in its natural order. If the
  user asks for a "random" sample, you may still select the list tool, but the answer must say the
  result is **a sample from the first records returned, not a true random draw**.
- **`limit` only shrinks the page** (≤ 1000). Setting `limit` higher than 1000 has no effect.
- **Truncation is possible whenever a list fills the page.** Say "sample / first page / truncated"
  when the result count is at the cap.
- **Empty result ≠ unsupported.** "No matching records" (the tool ran, found nothing) is different
  from "no tool can answer this".

---

## 3. Tool index (one line each)

**Cluster / nodes / pipelines**
- `api_v1_clusterState` — overall cluster snapshot (capacity, counts, health).
- `api_v1_datanodes` — datanode inventory and health.
- `api_v1_pipelines` — pipeline inventory, leaders, members, state.

**Containers**
- `api_v1_containers` — general container inventory.
- `api_v1_containers_missing` — missing/lost containers.
- `api_v1_containers_unhealthy` — all unhealthy states combined (+ aggregate counts).
- `api_v1_containers_unhealthy_state` — unhealthy containers filtered to one state.
- `api_v1_containers_deleted` — containers deleted in SCM.
- `api_v1_containers_mismatch` — OM/SCM existence mismatches.
- `api_v1_containers_mismatch_deleted` — deleted in SCM but still in OM.
- `api_v1_containers_quasiClosed` — quasi-closed containers.
- `api_v1_containers_unhealthy_export` — export jobs for unhealthy-container data.

**Keys (files)**
- `api_v1_keys_open` — open/uncommitted keys (detailed).
- `api_v1_keys_open_summary` — open-key totals.
- `api_v1_keys_open_mpu_summary` — open multipart-upload totals.
- `api_v1_keys_deletePending` — keys pending deletion (detailed).
- `api_v1_keys_deletePending_summary` — pending-delete key totals.
- `api_v1_keys_deletePending_dirs` — directories pending deletion (detailed).
- `api_v1_keys_deletePending_dirs_summary` — pending-delete directory totals.
- `api_v1_keys_listKeys` — committed keys/files listing & filtering.

**Volumes / buckets**
- `api_v1_volumes` — volume inventory.
- `api_v1_buckets` — bucket inventory (optionally by volume).

**Tasks**
- `api_v1_task_status` — Recon background task status (success/failure, running, lag, last sync).

**Utilization / namespace**
- `api_v1_utilization_fileCount` — file-count distribution by size tier.
- `api_v1_utilization_containerCount` — container-count distribution by size tier.
- `api_v1_namespace_summary` — object counts under a path.
- `api_v1_namespace_usage` — disk usage (du-style totals) for a path.
- `api_v1_namespace_quota` — quota limit vs usage for a path.
- `api_v1_namespace_dist` — file-size distribution under a path.

---

## 4. Global parameter extraction rules

Apply consistently across tools.

- **"from volume X and bucket Y" / "under volume X bucket Y" / "in bucket Y of volume X"** → combine
  into a path/prefix `"/X/Y"` (leading slash included).
- **"path /a/b/c"** → use exactly `"/a/b/c"` (keep the leading slash, preserve case and separators).
- **"prefix a/b/c" / "starting with a/b/c"** → treat as the path portion; for `startPrefix`, ensure
  it begins with `/` and is at least `/<volume>/<bucket>`.
- **Volume given, bucket missing** → for `listKeys` you cannot proceed safely (needs `/vol/bucket`);
  ask for the bucket or pick a tool that accepts volume-only (`api_v1_buckets`, `api_v1_volumes`,
  `api_v1_namespace_usage` with `/vol`). For `api_v1_keys_open`, a volume-only `startPrefix` is
  allowed but returns volume-wide open keys.
- **"random sample" / "give me some"** → select the normal list tool; remember the result is a
  first-page sample, not random (say so).
- **"first N" / "top N" / "show N"** → set `limit=N` (≤ 1000).
- **"only unhealthy" / "bad" / "replication problems"** → unhealthy container tools.
- **"failed tasks" / "running tasks" / "last sync"** → `api_v1_task_status` (filter/describe in the
  answer; the tool returns all tasks with their state).
- **"replication factor/type" (RATIS/EC)** → `replicationType` filter on `listKeys`.
- **"keys in container N" / "containers for key K"** → block/container↔key mapping at this fidelity
  is **not** exposed as a tool; see §10 (unsupported) and offer the nearest tool.
- **"files under directory /a/b/c"** → `listKeys` with `startPrefix=/a/b/c` (committed) or
  `api_v1_keys_open` if they said open.
- **"large files" / "files over N bytes"** → `listKeys` with `keySize=N` (minimum size).
- **"small vs large files" (distribution)** → `api_v1_utilization_fileCount` or
  `api_v1_namespace_dist`, not a listing.
- **"namespace usage" / "disk usage"** → `api_v1_namespace_usage` with `path`.
- **"quota"** → `api_v1_namespace_quota` with `path`.
- **"missing / under-replicated / over-replicated / mis-replicated"** → unhealthy-state tool with
  the matching `state`.
- **"pipelines for datanode" / "datanodes in pipeline"** → use `api_v1_pipelines` (members + leader
  are in each pipeline record) and/or `api_v1_datanodes` (each node lists its pipelines); correlate
  in the answer.
- **"deleted / retired / stale / decommissioned" nodes** → `api_v1_datanodes` (state field); there
  is no separate removed-nodes tool.

**Slash & casing rules:** keep the leading `/` on paths and `startPrefix`. Never strip or rewrite
user-typed volume/bucket/key names — preserve exact case and separators (these are real identifiers).
Combine volume + bucket as `/<volume>/<bucket>` with a single slash between segments.

---

## 5. High-risk endpoint confusions

### 5.1 Committed keys vs open keys (and the FSO trap)

- `api_v1_keys_listKeys` → **committed/finalized** keys (the normal object listing).
- `api_v1_keys_open` → **open / uncommitted / in-progress / unfinished / active-write / not-yet-
  finalized** keys.
- **FSO and OBS are bucket layouts, not key states.** "FSO keys" describes *where* (an FSO bucket),
  not *whether the key is open*.
- Therefore:
  - "random list of FSO keys from volume preprd bucket mygov" (no open/uncommitted word) →
    **`api_v1_keys_listKeys`**, `startPrefix=/preprd/mygov`.
  - "open FSO keys", "uncommitted FSO keys", "in-progress keys in an FSO bucket", "active writes
    under preprd/mygov" → **`api_v1_keys_open`**, `startPrefix=/preprd/mygov`, `includeFso=true`.
  - "random open keys from preprd/mygov", "sample of open keys", "show open keys from volume X
    bucket Y" → **`api_v1_keys_open`** (set `includeFso=true` and `includeNonFso=true` if the layout
    is unknown).
- This chatbot does **not** treat "random FSO keys" as open keys. Only the words open / uncommitted
  / in-progress / unfinished / active-write switch to the open-keys tool.

### 5.2 Recon background task status vs OM internals

- `api_v1_task_status` returns **Recon's background tasks** and their status: last-run
  success/failure, currently-running flag, last-run timestamps, and sync/lag freshness.
- Select it for: "status of OM tasks", "Recon tasks", "background tasks", "are tasks running",
  "did any task fail", "task health", "OM sync task status", "last task run", "is Recon caught up",
  "when did Recon last sync with OM".
- **Do not** reject "OM tasks" as unsupported just because it sounds like internal Ozone Manager
  threads — the Recon task-status tool reports the tasks that process OM/SCM data, which is what the
  user means. Only if the user explicitly asks for *internal OM server threads / JVM internals* that
  Recon does not expose should you explain the limitation (§10).

### 5.3 Layout vs state vs entity type

Keep these axes separate when reading a query:
- **FSO / OBS / LEGACY** = bucket layout.
- **open / committed / delete-pending / deleted** = key (or container) state.
- **volume / bucket / key / prefix** = namespace location.
- **container / pipeline / datanode** = SCM / storage entity.
- **task / sync / lag** = Recon background processing.
- **namespace summary / file size / count / usage / quota** = aggregated metadata.

A query usually fixes one value on several axes (e.g. "open" = state, "FSO" = layout,
"preprd/mygov" = location) — combine them; do not let one axis hide another.

---

## 6. Tools — endpoint by endpoint

> Each section: Purpose · What it returns · Use when · Do not use when · Parameters · Inference ·
> Disambiguation · Example queries (select) · Example queries (do not select) · Answering guidance.

### `api_v1_clusterState`
**Purpose.** One-shot overall picture of the cluster.
**What it returns.** Aggregate snapshot: storage capacity/used/remaining (incl. non-Ozone used),
counts of datanodes (total/healthy), pipelines, containers (incl. open/missing/deleted),
volumes/buckets/keys, and keys-pending-deletion. Exact, not paginated.
**Use when the user asks about.** "cluster overview", "how healthy is the cluster", "how much
storage is used", "total volumes/buckets/keys", "give me a summary".
**Do not use when.** They want per-node detail (`api_v1_datanodes`), per-pipeline detail
(`api_v1_pipelines`), or to list individual keys/containers.
**Parameters.** None.
**Disambiguation.** Prefer this over firing many sub-tools when the user wants the big picture; add
sub-tools only when they ask for specifics.
**Select examples.** "cluster status"; "overall health"; "how full is the cluster"; "summary of
Ozone"; "total keys and capacity".
**Do-not-select examples.** "list dead datanodes" → `api_v1_datanodes`; "list missing containers" →
`api_v1_containers_missing`; "disk usage of /v1/b1" → `api_v1_namespace_usage`.
**Answering guidance.** Lead with health/capacity headline numbers; surface anomalies
(missing containers > 0, unhealthy datanodes) first.

### `api_v1_datanodes`
**Purpose.** Inventory and health of datanodes.
**What it returns.** Per-node: hostname, uuid, state (HEALTHY/STALE/DEAD/…), operational state,
storage report (capacity/used/remaining), last heartbeat, pipelines the node is in, container count,
leader count. List (≤ 1000).
**Use when the user asks about.** "datanodes", "nodes", "hosts", "storage nodes", "dead/stale node",
"node health", "how many datanodes are healthy", "decommissioned/retired/stale nodes".
**Do not use when.** They want a cluster-wide capacity headline (`api_v1_clusterState`), container
replica placement history (not available — §10), or pure pipeline topology (`api_v1_pipelines`).
**Parameters.** None.
**Inference.** "which node leads the most pipelines" → read `leaderCount`; "stale/dead nodes" →
filter by `state` in the answer.
**Select examples.** "list datanodes and their health"; "any dead nodes?"; "how many healthy
datanodes"; "storage used on each node"; "which nodes are stale".
**Do-not-select examples.** "cluster capacity total" → `api_v1_clusterState`; "pipeline leaders" →
`api_v1_pipelines`.
**Answering guidance.** Highlight unhealthy/stale/dead nodes first; note if the list hit the cap.

### `api_v1_pipelines`
**Purpose.** SCM pipeline inventory and topology.
**What it returns.** Per-pipeline: pipeline id, replication type/factor, state, leader node, member
datanodes. List.
**Use when the user asks about.** "pipelines", "replication pipelines", "pipeline leaders", "how
many pipelines", "datanodes in a pipeline", "pipeline state".
**Do not use when.** They want node health (`api_v1_datanodes`) or container health.
**Parameters.** None.
**Inference.** "datanodes in pipeline P" → read that pipeline's members; "pipelines for node N" →
prefer `api_v1_datanodes` (each node lists its pipelines) or correlate.
**Select examples.** "show pipelines"; "how many pipelines"; "who leads each pipeline"; "pipeline
replication factors"; "members of each pipeline".
**Do-not-select examples.** "node heartbeats" → `api_v1_datanodes`.
**Answering guidance.** Group by state; call out non-OPEN pipelines.

### `api_v1_containers`
**Purpose.** General container inventory.
**What it returns.** Container records: ContainerID, NumberOfKeys, pipeline association. List
(≤ 1000), `limit` supported.
**Use when the user asks about.** "list containers", "how many containers", "keys per container".
**Do not use when.** They ask about a health state (missing/unhealthy/deleted/mismatch/quasi-closed)
— use the specialized tool.
**Parameters.** `limit` (optional, ≤ 1000).
**Select examples.** "list all containers"; "how many containers exist"; "keys per container";
"container inventory"; "first 100 containers".
**Do-not-select examples.** "missing containers" → `api_v1_containers_missing`; "unhealthy
containers" → `api_v1_containers_unhealthy`.
**Answering guidance.** If count = 1000, say it is the first page.

### `api_v1_containers_missing`
**Purpose.** Containers SCM cannot locate (lost/unreachable).
**What it returns.** Missing containers with `missingSince`, affected `keys`, originating pipeline,
last-known replica history. List.
**Use when the user asks about.** "missing", "lost", "containers not found", "containers that
disappeared", "how many containers went missing".
**Do not use when.** They say "deleted" (`api_v1_containers_deleted`) or want all unhealthy types
combined (`api_v1_containers_unhealthy`).
**Parameters.** `limit` (optional).
**Disambiguation.** Prefer this over `api_v1_containers_unhealthy_state(state=MISSING)` when the user
explicitly says "missing containers"; use the state tool only if they're already talking about
unhealthy-state filtering.
**Select examples.** "which containers are missing"; "lost containers"; "missing container count";
"containers not found by SCM"; "how long has container 12 been missing".
**Do-not-select examples.** "deleted containers" → `api_v1_containers_deleted`; "under-replicated" →
`api_v1_containers_unhealthy_state`.
**Answering guidance.** Lead with count and impact (affected keys); note `missingSince` if asked.

### `api_v1_containers_unhealthy`
**Purpose.** All unhealthy containers across every state, with aggregate counts.
**What it returns.** `missingCount`, `underReplicatedCount`, `overReplicatedCount`,
`misReplicatedCount`, plus per-container details (state, unhealthySince, expected/actual replicas,
delta, affected keys). List.
**Use when the user asks about.** "unhealthy containers", "bad containers", "replication problems",
"replica imbalance" — without naming a single state.
**Do not use when.** They name one state (use `api_v1_containers_unhealthy_state`) or say "missing"
specifically (`api_v1_containers_missing`).
**Parameters.** `limit`, `maxContainerId`, `minContainerId` (all optional).
**Select examples.** "list unhealthy containers"; "any replication problems"; "how many unhealthy
containers"; "containers with replica issues"; "show all bad containers".
**Do-not-select examples.** "under-replicated containers" → `api_v1_containers_unhealthy_state`.
**Answering guidance.** Lead with the per-state counts, then details; if all counts are zero say the
containers are healthy.

### `api_v1_containers_unhealthy_state`
**Purpose.** Unhealthy containers filtered to exactly one state.
**What it returns.** Same shape as `api_v1_containers_unhealthy`, restricted to the chosen state.
**Use when the user asks about.** A specific state: "missing", "under-replicated",
"over-replicated", "mis-replicated" containers.
**Do not use when.** They want all unhealthy types together (`api_v1_containers_unhealthy`).
**Parameters.** `state` (**required**: one of `MISSING`, `UNDER_REPLICATED`, `OVER_REPLICATED`,
`MIS_REPLICATED`), plus optional `limit`, `maxContainerId`, `minContainerId`.
**Inference.** Map words → state: "under replicated"→`UNDER_REPLICATED`, "over replicated"→
`OVER_REPLICATED`, "mis-replicated"/"wrong placement"→`MIS_REPLICATED`, "missing"→`MISSING`.
**Select examples.** "show under-replicated containers"; "over-replicated containers";
"mis-replicated containers"; "list MISSING-state containers"; "how many under-replicated".
**Do-not-select examples.** "all unhealthy containers" → `api_v1_containers_unhealthy`.
**Answering guidance.** State the filter you applied; report count + sample.

### `api_v1_containers_deleted`
**Purpose.** Containers deleted in SCM.
**What it returns.** Deleted containers with state, state-enter time, last-used, replication config.
List.
**Use when the user asks about.** "deleted containers", "removed containers", "recently deleted
containers".
**Do not use when.** They say "missing"/"lost" (`api_v1_containers_missing`) or "deleted in SCM but
still in OM" (`api_v1_containers_mismatch_deleted`).
**Parameters.** `limit` (optional).
**Select examples.** "show deleted containers"; "which containers were deleted"; "removed containers
list"; "deleted container count"; "replication type of deleted containers".
**Do-not-select examples.** "missing containers" → `api_v1_containers_missing`.
**Answering guidance.** Distinguish clearly from missing (deleted = intentional removal).

### `api_v1_containers_mismatch`
**Purpose.** Containers whose existence disagrees between OM and SCM.
**What it returns.** Discrepancy records: containerId, numberOfKeys, pipelines, and `existsAt`
(OM or SCM). List.
**Use when the user asks about.** "mismatch", "inconsistent containers", "exists in OM not SCM",
"exists in SCM not OM", "OM/SCM reconciliation".
**Do not use when.** They mean physical health (`api_v1_containers_unhealthy*`/`_missing`).
**Parameters.** `missingIn` (optional: `OM` or `SCM`), `limit`.
**Inference.** `missingIn` names the side that **lacks** the container: `missingIn=OM` → exists in
SCM, missing from OM; `missingIn=SCM` → exists in OM, missing from SCM.
**Select examples.** "mismatched containers between OM and SCM"; "containers missing in OM";
"containers missing in SCM"; "inconsistent container metadata"; "reconcile OM and SCM containers".
**Do-not-select examples.** "missing containers" (physical) → `api_v1_containers_missing`.
**Answering guidance.** State which side each container is missing from.

### `api_v1_containers_mismatch_deleted`
**Purpose.** Containers deleted in SCM but still recorded in OM (stale OM remnants).
**What it returns.** Discrepancy records (containerId, numberOfKeys, pipelines). List.
**Use when the user asks about.** "deleted in SCM but still in OM", "orphaned container entries",
"stale container metadata", "cleanup reconciliation".
**Do not use when.** They want plain deleted containers (`api_v1_containers_deleted`) or general
mismatch (`api_v1_containers_mismatch`).
**Parameters.** `limit` (optional).
**Select examples.** "containers deleted in SCM but visible in OM"; "orphaned OM container records";
"stale deleted containers"; "OM cleanup backlog for containers"; "residual deleted containers".
**Do-not-select examples.** "all OM/SCM mismatches" → `api_v1_containers_mismatch`.
**Answering guidance.** Frame as a cleanup/reconciliation backlog.

### `api_v1_containers_quasiClosed`
**Purpose.** Containers stuck in the QUASI_CLOSED transitional state.
**What it returns.** Quasi-closed container records. List.
**Use when the user asks about.** "quasi-closed containers", "containers not fully closed",
"closing issues".
**Do not use when.** They ask about unhealthy/missing/deleted states.
**Parameters.** `limit`, `minContainerId` (optional).
**Select examples.** "quasi-closed containers"; "containers stuck closing"; "QUASI_CLOSED list";
"how many quasi-closed containers"; "containers not fully closed".
**Do-not-select examples.** "unhealthy containers" → `api_v1_containers_unhealthy`.
**Answering guidance.** Only select when the user explicitly says quasi-closed/closing.

### `api_v1_containers_unhealthy_export`
**Purpose.** Export jobs for unhealthy-container datasets (not the unhealthy list itself).
**What it returns.** Export/download job records for unhealthy-container data.
**Use when the user asks about.** "export unhealthy containers", "download the unhealthy report",
"unhealthy container export job status".
**Do not use when.** They simply want to *see* unhealthy containers (`api_v1_containers_unhealthy`).
**Parameters.** None.
**Select examples.** "export unhealthy containers"; "is my unhealthy-container export done"; "list
export jobs"; "download unhealthy container CSV"; "bulk unhealthy export status".
**Do-not-select examples.** "show unhealthy containers" → `api_v1_containers_unhealthy`.
**Answering guidance.** Describe job status; don't fabricate file contents.

### `api_v1_keys_open`
**Purpose.** Detailed listing of open (uncommitted/in-progress) keys.
**What it returns.** Open keys split into FSO and non-FSO arrays, each with path, size,
replicated size, replication info, time-in-open-state; plus batch replicated/unreplicated totals.
List (≤ 1000).
**Use when the user asks about.** "open keys", "uncommitted/in-progress keys", "unfinished uploads",
"active writes", "stuck open files", "open keys under volume/bucket", "random/sample of open keys".
**Do not use when.** They want committed files (`api_v1_keys_listKeys`), open-key *counts only*
(`api_v1_keys_open_summary`), or multipart-upload totals (`api_v1_keys_open_mpu_summary`).
**Parameters.** `limit` (optional), `startPrefix` (optional, scopes to a path),
`includeFso` (boolean), `includeNonFso` (boolean).
**Inference / important.** The tool returns nothing unless at least one of `includeFso` /
`includeNonFso` is true. Rules: FSO-only request → `includeFso=true`; OBS/legacy → `includeNonFso=
true`; **layout unknown / "open keys" generic → set both true.** Scope with `startPrefix`
(`/volume`, `/volume/bucket`, or deeper) when the user names a location.
**Disambiguation.** "FSO keys" alone is *committed* keys in an FSO bucket → `listKeys`. Only the
words open/uncommitted/in-progress/unfinished/active-write select this tool (§5.1).
**Select examples.** "show open keys in /preprd/mygov"; "uncommitted files cluster-wide"; "random
sample of open keys from preprd/mygov"; "in-progress writes under volume v1"; "open FSO keys in
bucket b1".
**Do-not-select examples.** "random list of FSO keys in preprd/mygov" → `api_v1_keys_listKeys`;
"how many open keys" → `api_v1_keys_open_summary`; "pending multipart uploads" →
`api_v1_keys_open_mpu_summary`.
**Answering guidance.** Say the result is the first page (≤ 1000) and, for "random" requests, a
sample from the first records (not truly random). If both arrays are empty, say no open keys matched
the scope/layout.

### `api_v1_keys_open_summary`
**Purpose.** Aggregate open-key statistics (no per-key listing).
**What it returns.** `totalOpenKeys`, total replicated and unreplicated data size.
**Use when the user asks about.** "how many open keys", "total size of open keys", "open-key
backlog", "open vs total keys".
**Do not use when.** They want the actual list (`api_v1_keys_open`).
**Parameters.** None.
**Select examples.** "how many open keys are there"; "total open-key size"; "open key count";
"space used by open keys"; "open keys vs total keys" (pair with `api_v1_clusterState`).
**Do-not-select examples.** "list open keys" → `api_v1_keys_open`.
**Answering guidance.** Report counts/sizes directly.

### `api_v1_keys_open_mpu_summary`
**Purpose.** Aggregate stats for open multipart-upload (MPU) keys.
**What it returns.** Totals for open MPU keys (counts/sizes).
**Use when the user asks about.** "multipart uploads", "MPU", "incomplete multipart writes",
"pending MPUs".
**Do not use when.** They mean general open keys (`api_v1_keys_open[_summary]`).
**Parameters.** None.
**Select examples.** "pending multipart uploads"; "MPU backlog"; "incomplete multipart writes"; "how
many open MPUs"; "multipart upload space".
**Do-not-select examples.** "open keys count" → `api_v1_keys_open_summary`.
**Answering guidance.** Clarify this is multipart-specific.

### `api_v1_keys_deletePending`
**Purpose.** Keys marked for deletion but not yet purged (detailed).
**What it returns.** Pending-delete key groups with OM key info and sizes; batch replicated/
unreplicated totals. List (≤ 1000).
**Use when the user asks about.** "keys pending deletion", "tombstoned keys", "deletion backlog
items", "files waiting to be deleted".
**Do not use when.** They want counts only (`api_v1_keys_deletePending_summary`) or directories
(`api_v1_keys_deletePending_dirs`).
**Parameters.** `limit`, `startPrefix` (optional).
**Select examples.** "which keys are pending deletion"; "deletion backlog under /v1/b1"; "largest
delete-pending keys"; "tombstoned files"; "files awaiting cleanup".
**Do-not-select examples.** "how many keys pending deletion" → `api_v1_keys_deletePending_summary`;
"pending-delete directories" → `api_v1_keys_deletePending_dirs`.
**Answering guidance.** Note truncation; mention sizes if asked.

### `api_v1_keys_deletePending_summary`
**Purpose.** Aggregate pending-delete key statistics.
**What it returns.** `totalDeletedKeys`, total replicated/unreplicated size.
**Use when the user asks about.** "how many keys pending deletion", "total delete backlog size",
"reclaimable space".
**Do not use when.** They want the list (`api_v1_keys_deletePending`).
**Parameters.** None.
**Select examples.** "delete-pending key count"; "total size pending deletion"; "deletion backlog
size"; "how much space will cleanup reclaim"; "pending-delete totals".
**Do-not-select examples.** "list pending-delete keys" → `api_v1_keys_deletePending`.
**Answering guidance.** Report totals.

### `api_v1_keys_deletePending_dirs`
**Purpose.** Directories pending deletion (FSO cleanup).
**What it returns.** Pending-delete directory records (path, size, time-in-state). List.
**Use when the user asks about.** "directories pending deletion", "folder cleanup backlog", "FSO
dir delete backlog".
**Do not use when.** They mean files (`api_v1_keys_deletePending`) or counts only
(`api_v1_keys_deletePending_dirs_summary`).
**Parameters.** `limit` (optional).
**Select examples.** "directories pending deletion"; "folder cleanup backlog"; "which dirs await
deletion"; "FSO directory delete queue"; "pending-delete folders".
**Do-not-select examples.** "files pending deletion" → `api_v1_keys_deletePending`.
**Answering guidance.** Clarify these are directories, not files.

### `api_v1_keys_deletePending_dirs_summary`
**Purpose.** Aggregate stats for directories pending deletion.
**What it returns.** Totals for pending-delete directories.
**Use when the user asks about.** "how many directories pending deletion", "dir deletion backlog
size".
**Do not use when.** They want the list (`api_v1_keys_deletePending_dirs`).
**Parameters.** None.
**Select examples.** "count of directories pending deletion"; "dir delete backlog size"; "pending
directory totals"; "how many folders await cleanup"; "directory deletion summary".
**Do-not-select examples.** "list those directories" → `api_v1_keys_deletePending_dirs`.
**Answering guidance.** Report totals.

### `api_v1_keys_listKeys`
**Purpose.** List and filter **committed** keys/files under a bucket-scoped path.
**What it returns.** Key metadata: volume, bucket, key, complete path, data size, versions, block
locations, creation/modification time. List (≤ 1000).
**Use when the user asks about.** "list files/keys", "find files", "files under a path/bucket",
"large keys", "EC keys", "RATIS keys", "keys created after a date", "random list of (FSO/OBS) keys".
**Do not use when.** They want disk-usage totals (`api_v1_namespace_usage`), open/uncommitted keys
(`api_v1_keys_open`), object counts without listing (`api_v1_namespace_summary`), or a size
histogram (`api_v1_namespace_dist` / `api_v1_utilization_fileCount`).
**Parameters.** `startPrefix` (**required**, must be at least `/<volume>/<bucket>`), `limit`,
`replicationType` (RATIS|EC), `creationDate` (`MM-dd-yyyy HH:mm:ss`, matched created-on/after),
`keySize` (minimum bytes).
**Safe-scope rule (enforced).** `startPrefix` must start with `/`, contain no `..`, and have at
least two path segments (`/volume/bucket`). `"/"` alone or volume-only is rejected by the chatbot.
If the user gives only a volume, ask for the bucket (or use a volume-capable tool).
**Inference.** "volume preprd bucket mygov" → `startPrefix=/preprd/mygov`; "under /preprd/mygov/a/b"
→ that exact prefix; "larger than 1 MB" → `keySize=1048576`; "EC keys" → `replicationType=EC`.
**Disambiguation.** "FSO keys"/"OBS keys" = layout, still committed → this tool. Add "open" to
switch to `api_v1_keys_open`.
**Select examples.** "list keys under volume preprd bucket mygov"; "random list of FSO keys from
preprd/mygov"; "EC keys in /v1/b1"; "files over 100MB in /v1/b1"; "keys created after 05-01-2025
00:00:00 in /v1/b1".
**Do-not-select examples.** "total size of /v1/b1" → `api_v1_namespace_usage`; "open keys in
/v1/b1" → `api_v1_keys_open`; "how many keys under /v1/b1" → `api_v1_namespace_summary`.
**Answering guidance.** Always state scope; if count = `limit`/1000 say it is the first page and more
may exist. For "random", say it is a sample from the first records, not a true random draw. Preserve
exact key paths/case.

### `api_v1_volumes`
**Purpose.** List Ozone volumes.
**What it returns.** Volume records: name, owner, creation time, quota, used bytes, bucket count.
List (≤ 1000).
**Use when the user asks about.** "list volumes", "how many volumes", "volume owners/quota".
**Do not use when.** They want buckets in a volume (`api_v1_buckets`) or per-path usage
(`api_v1_namespace_usage`).
**Parameters.** `limit` (optional).
**Select examples.** "list all volumes"; "how many volumes exist"; "volumes and their quota";
"who owns each volume"; "volume inventory".
**Do-not-select examples.** "buckets in volume v1" → `api_v1_buckets`.
**Answering guidance.** Report count; note cap if hit.

### `api_v1_buckets`
**Purpose.** List buckets, optionally within one volume.
**What it returns.** Bucket records: name, volume, owner, layout (FSO/OBS/LEGACY), quotas, used
bytes/namespace, versioning, encryption, storage type, ACLs. List (≤ 1000).
**Use when the user asks about.** "buckets", "buckets in volume X", "bucket owners", "FSO/OBS
buckets", "versioned buckets", "bucket layout/quota".
**Do not use when.** They want disk usage of a bucket (`api_v1_namespace_usage`) or to list keys
inside it (`api_v1_keys_listKeys`).
**Parameters.** `volume` (optional — set when a volume is named), `limit`.
**Inference.** "buckets under volume preprd" → `volume=preprd`; "FSO buckets" → list then filter by
layout in the answer.
**Select examples.** "list buckets in volume preprd"; "all buckets"; "which buckets use FSO"; "bucket
owners in v1"; "versioned buckets".
**Do-not-select examples.** "size of bucket b1" → `api_v1_namespace_usage`; "keys in b1" →
`api_v1_keys_listKeys`.
**Answering guidance.** Report count; surface layout/quota when relevant.

### `api_v1_task_status`
**Purpose.** Status of Recon's background tasks (the jobs that process OM/SCM data).
**What it returns.** Per-task: name, last-run status (success/failure), currently-running flag,
last-run timestamps; reflects sync/lag freshness.
**Use when the user asks about.** "status of OM tasks", "Recon tasks", "background tasks", "are
tasks running", "did any task fail", "task health", "OM sync task status", "last task run", "is
Recon caught up", "when did Recon last sync with OM".
**Do not use when.** They ask for internal OM server threads / JVM internals not exposed by Recon
(explain the limitation, §10).
**Parameters.** None.
**Disambiguation.** Do not treat "OM tasks" as unsupported — this tool answers it (§5.2).
**Select examples.** "show the status of OM tasks"; "are any Recon tasks failing"; "when did Recon
last sync"; "which background tasks are running"; "did the namespace summary task succeed".
**Do-not-select examples.** "internal OM RPC handler threads" → unsupported (§10).
**Answering guidance.** Lead with failures or not-yet-run tasks; then list tasks with their last
status and timestamps.

### `api_v1_utilization_fileCount`
**Purpose.** File-count distribution across size tiers (histogram), optionally scoped.
**What it returns.** Counts of files per size bucket; can be scoped to a volume/bucket.
**Use when the user asks about.** "how many small vs large files", "file size distribution by
count", "object-count analytics".
**Do not use when.** They want to *list* files (`api_v1_keys_listKeys`) or total disk usage
(`api_v1_namespace_usage`).
**Parameters.** `volume`, `bucket`, `fileSize` (all optional; set volume/bucket to scope).
**Select examples.** "file size distribution"; "how many files are under 1MB"; "small vs large file
counts in bucket b1"; "histogram of file sizes"; "object count by size tier".
**Do-not-select examples.** "list large files" → `api_v1_keys_listKeys` with `keySize`.
**Answering guidance.** Present as a distribution; don't imply it lists files.

### `api_v1_utilization_containerCount`
**Purpose.** Container-count distribution across size tiers.
**What it returns.** Counts of containers per size bucket (cluster level).
**Use when the user asks about.** "container size distribution", "container density by size", "how
many containers in each size band".
**Do not use when.** They want a container list (`api_v1_containers`) or health states.
**Parameters.** `containerSize` (optional).
**Select examples.** "container size distribution"; "how many large containers"; "container density
analysis"; "containers by size tier"; "container allocation histogram".
**Do-not-select examples.** "list containers" → `api_v1_containers`.
**Answering guidance.** Present as a distribution.

### `api_v1_namespace_summary`
**Purpose.** Object counts under a namespace path.
**What it returns.** For the path: type (VOLUME/BUCKET/DIRECTORY/KEY) and counts of volumes,
buckets, directories, keys.
**Use when the user asks about.** "what is under this path", "how many keys/dirs/buckets under X"
(as a count, not a listing).
**Do not use when.** They want disk usage (`api_v1_namespace_usage`), a file listing
(`api_v1_keys_listKeys`), or quota (`api_v1_namespace_quota`).
**Parameters.** `path` (e.g. `/v1`, `/v1/b1`, `/v1/b1/dir`).
**Select examples.** "how many keys under /v1/b1"; "what's in /v1/b1"; "number of directories in
this bucket"; "object summary for /v1"; "counts under this path".
**Do-not-select examples.** "size of /v1/b1" → `api_v1_namespace_usage`; "list keys under /v1/b1" →
`api_v1_keys_listKeys`.
**Answering guidance.** Report the counts; note `numVolume = -1` means the query was below volume
level.

### `api_v1_namespace_usage`
**Purpose.** Disk usage (du-style) for a path, with optional sub-path breakdown.
**What it returns.** Logical `size` and replicated `sizeWithReplica` for the path, child
breakdown (`subPaths[]`), direct-key size, sub-path count.
**Use when the user asks about.** "disk usage", "total size", "how much space", "largest
directories", "storage consumed by X".
**Do not use when.** They want to list files (`api_v1_keys_listKeys`), object counts
(`api_v1_namespace_summary`), or quota (`api_v1_namespace_quota`).
**Parameters.** `path` (required for a scoped answer), `files` (boolean — include keys in
breakdown), `replica` (boolean — include replicated sizes), `sortSubPaths` (boolean — sort by size).
**Inference.** "biggest subdirectories of /v1/b1" → `path=/v1/b1`, `sortSubPaths=true`; "with
replication" → `replica=true`.
**Select examples.** "disk usage of /v1/b1"; "how much space does volume v1 use"; "largest
directories under /v1/b1"; "replicated size of this bucket"; "total bytes under this path".
**Do-not-select examples.** "list files in /v1/b1" → `api_v1_keys_listKeys`; "how many keys" →
`api_v1_namespace_summary`.
**Answering guidance.** Distinguish logical vs replicated size; for "largest", lead with the top
sub-paths.

### `api_v1_namespace_quota`
**Purpose.** Quota limit vs usage for a namespace path.
**What it returns.** `allowed` (quota) and `used`.
**Use when the user asks about.** "quota", "quota usage", "near quota limit", "remaining quota".
**Do not use when.** They want general disk usage (`api_v1_namespace_usage`).
**Parameters.** `path` (volume or bucket).
**Select examples.** "quota for bucket b1"; "is volume v1 near its quota"; "remaining quota on
/v1/b1"; "quota utilization"; "how much quota is used".
**Do-not-select examples.** "disk usage of /v1/b1" → `api_v1_namespace_usage`.
**Answering guidance.** Compare used vs allowed; flag if used ≥ allowed.

### `api_v1_namespace_dist`
**Purpose.** File-size distribution under a namespace path.
**What it returns.** A distribution array over size buckets for the path.
**Use when the user asks about.** "file size distribution for this path", "size histogram under
/v1/b1".
**Do not use when.** They want to list files (`api_v1_keys_listKeys`) or cluster-wide count
histograms (`api_v1_utilization_fileCount`).
**Parameters.** `path`.
**Select examples.** "file size distribution under /v1/b1"; "size histogram for this bucket"; "how
are file sizes spread in /v1"; "distribution of object sizes under this path"; "size spread for
/v1/b1".
**Do-not-select examples.** "list files" → `api_v1_keys_listKeys`.
**Answering guidance.** Present as a distribution tied to the path.

---

## 7. Multi-tool reasoning

Select multiple tools when one tool cannot fully answer and each adds distinct data. Useful recipes:

- **Full cluster health** → `api_v1_clusterState` + `api_v1_datanodes` + `api_v1_pipelines` +
  `api_v1_task_status`.
- **Open keys: totals + sample** → `api_v1_keys_open_summary` + `api_v1_keys_open`.
- **Deletion backlog: totals + items** → `api_v1_keys_deletePending_summary` +
  `api_v1_keys_deletePending`.
- **Replication health sweep** → `api_v1_containers_unhealthy` + `api_v1_containers_missing`.
- **OM/SCM reconciliation** → `api_v1_containers_mismatch` + `api_v1_containers_mismatch_deleted`.
- **Capacity planning** → `api_v1_clusterState` + `api_v1_namespace_usage` + `api_v1_namespace_quota`.
- **Volume/bucket report** → `api_v1_volumes` + `api_v1_buckets`.
- **Total keys vs open keys** → `api_v1_clusterState` + `api_v1_keys_open_summary`.

**Do not chain tools when:** a single tool already returns the complete answer; chaining would be
speculative ("just in case"); or the extra data is unsupported. Prefer a summary tool over listing
all raw records when the user only asked "how many / how much".

---

## 8. Answer-behavior rules (after a tool runs)

- **Never claim a complete/cluster-wide answer when only a page/sample came back.** Say "sample",
  "first page", or "truncated" when the count is at the cap.
- **"random" requests:** the backend does not randomize — state the result is a sample from the
  first records returned, not a true random selection.
- **Empty results:** distinguish "no matching records were returned" (tool ran, found none) from
  "Recon has no tool for this" (unsupported).
- **Status/health:** lead with failures / abnormal states (missing containers, failed tasks, dead
  nodes), then the rest.
- **Lists:** show a manageable subset and mention the total or truncation when known.
- **IDs and paths:** preserve exact values, case, and separators (container IDs, key paths, UUIDs).
- **Sizes:** distinguish logical vs replicated where the tool provides both.
- Keep answers concise but always include the important caveats above. Do not dump raw
  implementation details unless they help the user.

---

## 9. Unsupported requests & fallback guidance

When no tool fits:
- **Do not hallucinate data** or invent tool names/parameters.
- **Do not claim Recon can't help if a close tool exists** — name the nearest supported area and
  offer it (e.g. "I can't list a container's replica timeline, but I can show unhealthy/missing
  containers and datanode health").
- **Ask a short clarification only when it would change the tool choice** (e.g. "open or committed
  keys?", "which volume/bucket?").
- **Casual/greeting/off-topic** → reply briefly and normally; do not force a tool.
- **Unsafe or out-of-scope actions** (mutations, deletes, config changes) → decline clearly; Recon
  tools here are read-only insights.

### Known unsupported areas (present in the old REST guide, but no tool exists)

- **Per-container replica history / timeline** (`/containers/{id}/replicaHistory`): not exposed.
  Nearest: `api_v1_containers_unhealthy` / `api_v1_containers_missing` (they include last-known
  replica info) and `api_v1_datanodes`.
- **Pending-block listing** (`/blocks/deletePending`): not exposed. Nearest:
  `api_v1_keys_deletePending[_summary]`.
- **Direct key↔container block mapping queries** ("which keys are in container N"): not exposed as a
  standalone tool. Nearest: `api_v1_keys_listKeys` (each key lists its block/container ids) or
  `api_v1_containers` (key counts per container).
- **ACL-only listings**: ACLs appear inside bucket records (`api_v1_buckets`) but there is no
  dedicated ACL tool.
- **Prometheus metrics** (formerly `api_v1_metrics_api`): removed; not available. For health use
  `api_v1_clusterState`, `api_v1_datanodes`, `api_v1_task_status`.

---

## 10. Regression test matrix

| User query | Expected tool | Expected parameters | Why | Should NOT choose |
|---|---|---|---|---|
| List keys under volume preprd bucket mygov | `api_v1_keys_listKeys` | `startPrefix=/preprd/mygov` | committed-key listing under a bucket | `api_v1_keys_open` |
| Give me a random list of FSO keys from volume preprd and bucket mygov | `api_v1_keys_listKeys` | `startPrefix=/preprd/mygov` | "FSO" = layout, no "open" word → committed | `api_v1_keys_open` |
| Give me open FSO keys from volume preprd and bucket mygov | `api_v1_keys_open` | `startPrefix=/preprd/mygov`, `includeFso=true` | "open" + FSO layout | `api_v1_keys_listKeys` |
| Give me a random sample of open keys from preprd/mygov | `api_v1_keys_open` | `startPrefix=/preprd/mygov`, `includeFso=true`, `includeNonFso=true` | open keys, layout unknown → both flags | `api_v1_keys_listKeys` |
| Show uncommitted keys | `api_v1_keys_open` | `includeFso=true`, `includeNonFso=true` | uncommitted = open | `api_v1_keys_listKeys` |
| Show active writes | `api_v1_keys_open` | `includeFso=true`, `includeNonFso=true` | active writes = open keys | `api_v1_keys_listKeys` |
| Show files under this FSO path /v1/b1/dir | `api_v1_keys_listKeys` | `startPrefix=/v1/b1/dir` | committed files under a path | `api_v1_keys_open` |
| EC keys larger than 100MB in /v1/b1 | `api_v1_keys_listKeys` | `startPrefix=/v1/b1`, `replicationType=EC`, `keySize=104857600` | filtered committed listing | `api_v1_namespace_usage` |
| How many open keys are there | `api_v1_keys_open_summary` | – | count only | `api_v1_keys_open` |
| Pending multipart uploads | `api_v1_keys_open_mpu_summary` | – | MPU-specific | `api_v1_keys_open` |
| Show me the status of OM tasks | `api_v1_task_status` | – | Recon tasks processing OM data | NO_SUITABLE_ENDPOINT |
| Are any Recon tasks failing? | `api_v1_task_status` | – | last-run status per task | – |
| When did Recon last sync with OM? | `api_v1_task_status` | – | sync/lag freshness | `api_v1_clusterState` |
| Which background tasks are running? | `api_v1_task_status` | – | running flag per task | – |
| Did the namespace summary task succeed? | `api_v1_task_status` | – | per-task last status | – |
| Is Recon caught up? | `api_v1_task_status` | – | sync freshness | `api_v1_clusterState` |
| Show unhealthy containers | `api_v1_containers_unhealthy` | – | all unhealthy states | `api_v1_containers_unhealthy_state` |
| Show missing containers | `api_v1_containers_missing` | – | explicit "missing" | `api_v1_containers_unhealthy_state` |
| Show under-replicated containers | `api_v1_containers_unhealthy_state` | `state=UNDER_REPLICATED` | named state | `api_v1_containers_unhealthy` |
| Show over-replicated containers | `api_v1_containers_unhealthy_state` | `state=OVER_REPLICATED` | named state | `api_v1_containers_unhealthy` |
| Show deleted containers | `api_v1_containers_deleted` | – | deleted in SCM | `api_v1_containers_missing` |
| Containers in OM but not SCM | `api_v1_containers_mismatch` | `missingIn=SCM` | missing from SCM side | `api_v1_containers_missing` |
| Containers deleted in SCM but still in OM | `api_v1_containers_mismatch_deleted` | – | stale OM remnants | `api_v1_containers_deleted` |
| Quasi-closed containers | `api_v1_containers_quasiClosed` | – | explicit quasi-closed | `api_v1_containers_unhealthy` |
| Export the unhealthy containers report | `api_v1_containers_unhealthy_export` | – | export job, not the list | `api_v1_containers_unhealthy` |
| List all containers | `api_v1_containers` | – | general inventory | `api_v1_containers_unhealthy` |
| Show datanode health | `api_v1_datanodes` | – | per-node health | `api_v1_clusterState` |
| Any dead or stale nodes? | `api_v1_datanodes` | – | node state field | – |
| Show pipelines | `api_v1_pipelines` | – | pipeline inventory | `api_v1_datanodes` |
| Who leads each pipeline? | `api_v1_pipelines` | – | leader per pipeline | `api_v1_datanodes` |
| How many pipelines exist? | `api_v1_pipelines` | – | pipeline count | `api_v1_clusterState` |
| Datanodes in pipeline P | `api_v1_pipelines` | – | members per pipeline | `api_v1_datanodes` |
| List all volumes | `api_v1_volumes` | – | volume inventory | `api_v1_buckets` |
| How many volumes exist? | `api_v1_volumes` | – | count volumes | `api_v1_clusterState` |
| List buckets in volume preprd | `api_v1_buckets` | `volume=preprd` | buckets within a volume | `api_v1_volumes` |
| Which buckets use FSO layout? | `api_v1_buckets` | – | layout field on buckets | `api_v1_keys_listKeys` |
| What is the namespace usage for this bucket? | `api_v1_namespace_usage` | `path=/v1/b1` | du-style totals | `api_v1_keys_listKeys` |
| Largest directories under /v1/b1 | `api_v1_namespace_usage` | `path=/v1/b1`, `sortSubPaths=true` | size breakdown, sorted | `api_v1_keys_listKeys` |
| How many keys under /v1/b1? | `api_v1_namespace_summary` | `path=/v1/b1` | object counts, not listing | `api_v1_keys_listKeys` |
| Quota usage for bucket b1 | `api_v1_namespace_quota` | `path=/v1/b1` | quota limit vs used | `api_v1_namespace_usage` |
| File size distribution under /v1/b1 | `api_v1_namespace_dist` | `path=/v1/b1` | per-path size histogram | `api_v1_utilization_fileCount` |
| How many small vs large files cluster-wide? | `api_v1_utilization_fileCount` | – | count distribution | `api_v1_keys_listKeys` |
| Container size distribution | `api_v1_utilization_containerCount` | – | container count histogram | `api_v1_containers` |
| Cluster overview / how is the cluster | `api_v1_clusterState` | – | overall snapshot | many sub-tools |
| How much storage is used? | `api_v1_clusterState` | – | capacity headline | `api_v1_namespace_usage` |
| List keys in the whole cluster | (ask to scope) / NO_SUITABLE_ENDPOINT | – | listKeys needs `/vol/bucket`; `/` is blocked | `api_v1_keys_listKeys` with `/` |
| Delete bucket b1 | (decline) | – | read-only insights; mutation unsupported | any tool |
| Show a container's replica timeline | (explain unsupported) | – | replica history not exposed | invent a tool |
| Which keys are in container 42? | (offer nearest) `api_v1_containers` or `api_v1_keys_listKeys` | – | direct mapping not exposed | invent a tool |
| Hi / hello | (no tool) | – | greeting | any tool |
| What can Recon tell me? | (no tool, answer directly) | – | documentation intent | any tool |
| Thanks! | (no tool) | – | casual | any tool |
| What does "under-replicated" mean? | (no tool, answer directly) | – | documentation intent | `api_v1_containers_unhealthy_state` |
| Tell me a joke | (no tool, brief reply) | – | off-topic | any tool |

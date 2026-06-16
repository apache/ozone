# Recon Tool Semantics Guide

> For LLM tool selection only. Tool names: api_v1_<path_with_underscores>.

## 1. Global Routing Rules (CRITICAL)

### Aggregation vs Enumeration (DU vs LS)
- User asks: total size, disk usage, space consumed, largest directories, namespace usage → use `api_v1_namespace_usage`.
- User asks: list files, find files, search keys, filter files by path/size/date → use `api_v1_keys_listKeys`.
- Never use listKeys to calculate namespace totals.
- Never use namespace_usage when the user wants individual files.

### Open vs Committed Keys
- Open, in-progress, uncommitted, stuck uploads, active writes → `api_v1_keys_open`.
- Counts only → `api_v1_keys_open_summary`.
- Multipart upload backlog → `api_v1_keys_open_mpu_summary`.
- Normal committed files → `api_v1_keys_listKeys`.

### Delete-Pending vs Deleted
- Keys waiting for cleanup → `api_v1_keys_deletePending`.
- Counts/sizes only → `api_v1_keys_deletePending_summary`.
- FSO directories waiting for cleanup → `api_v1_keys_deletePending_dirs`.
- Containers already deleted in SCM → `api_v1_containers_deleted`.

### Missing vs Unhealthy Containers
- Missing/lost containers → `api_v1_containers_missing`.
- All unhealthy states combined → `api_v1_containers_unhealthy`.
- Specific unhealthy class → `api_v1_containers_unhealthy_state`.
- Missing containers are a subset of unhealthy containers.

### Mismatch vs Missing
- Metadata disagreement between OM and SCM → `api_v1_containers_mismatch`.
- Physical/replication health issue → unhealthy or missing endpoints.
- Use parameter `missingIn=OM|SCM` to identify which system lacks the container.

### Summary vs Enumeration
Use summary endpoints when user asks:
- how many
- total count
- total size
- overall backlog
- cluster-wide statistics

Use enumeration endpoints when user asks:
- list
- show
- which
- find
- details

## 2. Tool Index

- api_v1_clusterState → Overall cluster health and capacity snapshot.
- api_v1_datanodes → DataNode inventory and health.
- api_v1_pipelines → Pipeline inventory and health.
- api_v1_containers → General container inventory.
- api_v1_containers_missing → Missing/lost containers.
- api_v1_containers_unhealthy → All unhealthy containers.
- api_v1_containers_unhealthy_state → Unhealthy containers filtered by state.
- api_v1_containers_deleted → Containers deleted in SCM.
- api_v1_containers_mismatch → OM/SCM metadata mismatches.
- api_v1_containers_mismatch_deleted → Deleted in SCM but still visible in OM.
- api_v1_containers_quasiClosed → Quasi-closed containers.
- api_v1_containers_unhealthy_export → Export unhealthy container dataset.
- api_v1_keys_open → Detailed open keys.
- api_v1_keys_open_summary → Open key totals.
- api_v1_keys_open_mpu_summary → Multipart upload summary.
- api_v1_keys_deletePending_summary → Delete backlog totals.
- api_v1_keys_deletePending → Detailed delete-pending keys.
- api_v1_keys_deletePending_dirs → Delete-pending directories.
- api_v1_keys_deletePending_dirs_summary → Directory deletion totals.
- api_v1_keys_listKeys → Committed file listing and filtering.
- api_v1_volumes → Volume inventory.
- api_v1_buckets → Bucket inventory and metadata.
- api_v1_task_status → Recon background task health.
- api_v1_utilization_fileCount → File-count distribution analytics.
- api_v1_utilization_containerCount → Container-count distribution analytics.
- api_v1_namespace_summary → Object summary for a path.
- api_v1_namespace_usage → Disk usage (DU).
- api_v1_namespace_quota → Quota usage and limits.
- api_v1_namespace_dist → File-size distribution.
- api_v1_metrics_api → Metrics for a specific API.

## 3. Multi-Tool Recipes

- Full cluster health → api_v1_clusterState + api_v1_datanodes + api_v1_pipelines + api_v1_task_status
- Storage consumed by a bucket → api_v1_namespace_usage + api_v1_namespace_quota
- Find large files in a path → api_v1_keys_listKeys + api_v1_namespace_usage
- Investigate open uploads → api_v1_keys_open_summary + api_v1_keys_open
- Investigate deletion backlog → api_v1_keys_deletePending_summary + api_v1_keys_deletePending
- Investigate FSO directory deletion backlog → api_v1_keys_deletePending_dirs_summary + api_v1_keys_deletePending_dirs
- Missing container investigation → api_v1_containers_missing + api_v1_datanodes
- Replication health investigation → api_v1_containers_unhealthy + api_v1_containers_unhealthy_state
- OM/SCM reconciliation audit → api_v1_containers_mismatch + api_v1_containers_mismatch_deleted
- Capacity planning → api_v1_clusterState + api_v1_namespace_usage + api_v1_namespace_quota
- Volume inventory report → api_v1_volumes + api_v1_buckets
- Bucket ownership and usage audit → api_v1_buckets + api_v1_namespace_usage
- Namespace diagnostics → api_v1_namespace_summary + api_v1_namespace_usage + api_v1_namespace_dist
- Recon health investigation → api_v1_task_status + api_v1_metrics_api

## 4. Tools by Domain

### Cluster & Nodes

#### api_v1_clusterState
Use for overall cluster status, capacity, health, and high-level overview.

Use when:
- "How healthy is the cluster?"
- "Give me a cluster summary."

Do not use for per-node details. Use datanodes instead.

#### api_v1_datanodes
Use for node inventory, node health, capacity, and node-specific issues.

Triggers:
- datanodes
- nodes
- hosts
- dead node
- stale node

Related:
- clusterState
- containers_missing

#### api_v1_pipelines
Use for pipeline status, counts, and replication topology diagnostics.

Triggers:
- pipelines
- replication pipelines
- unhealthy pipelines

### Containers

#### api_v1_containers
General container listing.

Use when:
- list containers
- count containers
- keys per container

Do not use for health issues.

#### api_v1_containers_missing
Missing or lost containers.

Examples:
- Which containers are missing?
- Show lost containers.
- Which containers disappeared?

Prefer over unhealthy_state(MISSING) when user explicitly says "missing containers".

#### api_v1_containers_unhealthy
All unhealthy states together.

States may include:
- MISSING
- UNDER_REPLICATED
- OVER_REPLICATED
- MIS_REPLICATED

Use when user asks broadly about unhealthy containers.

#### api_v1_containers_unhealthy_state
Filtered unhealthy containers.

Required:
- state

Valid values:
- MISSING
- UNDER_REPLICATED
- OVER_REPLICATED
- MIS_REPLICATED

Use when user explicitly names a state.

#### api_v1_containers_deleted
Containers already marked deleted in SCM.

Examples:
- Show deleted containers.
- Recently deleted containers.

#### api_v1_containers_mismatch
OM/SCM metadata mismatch.

Use when:
- inconsistent metadata
- exists in OM not SCM
- exists in SCM not OM

Parameter:
- missingIn=OM or SCM

#### api_v1_containers_mismatch_deleted
Deleted in SCM but still present in OM.

Use for:
- stale container metadata
- orphaned container entries

#### api_v1_containers_quasiClosed
<!-- inferred -->
Containers stuck in QUASI_CLOSED state.

Use when:
- quasi closed containers
- closing issues
- containers not fully closed

#### api_v1_containers_unhealthy_export
<!-- inferred -->
Export-oriented version of unhealthy container reporting.

Use for:
- export
- CSV/report generation
- bulk unhealthy review

### Keys

#### api_v1_keys_open
Detailed open/uncommitted keys.

Useful for:
- stuck uploads
- active writes
- unfinished files

Parameters:
- includeFso
- includeNonFso
- limit (max 1000)

#### api_v1_keys_open_summary
Totals only.

Examples:
- How many open keys?
- Total open-key size?

#### api_v1_keys_open_mpu_summary
Multipart upload backlog summary.

Examples:
- Pending MPUs
- Multipart upload consumption

#### api_v1_keys_deletePending
Detailed delete-pending keys.

Examples:
- Which keys are waiting for deletion?
- Largest deletion backlog items?

#### api_v1_keys_deletePending_summary
Totals for delete-pending keys.

#### api_v1_keys_deletePending_dirs
FSO directory deletion backlog.

Use when user mentions:
- directories
- folder cleanup
- FSO delete backlog

#### api_v1_keys_deletePending_dirs_summary
Aggregate statistics for delete-pending directories.

#### api_v1_keys_listKeys
Committed file listing (not open/uncommitted keys).

Use when:
- list files
- find files
- filter by path, replication type, creation date, or minimum size
- locate keys under a bucket or directory

Required parameter:
- startPrefix — at least /<volume>/<bucket>; never "/" alone

Optional filters:
- replicationType (e.g. RATIS, EC)
- creationDate (MM-dd-yyyy HH:mm:ss)
- keySize (minimum bytes)
- limit (max 1000)

Do NOT use for namespace disk-usage totals (api_v1_namespace_usage).
Do NOT use for open/in-progress writes (api_v1_keys_open).

### Volumes & Buckets

#### api_v1_volumes
Volume inventory.

Examples:
- List volumes.
- How many volumes exist?

#### api_v1_buckets
Bucket inventory and metadata.

Examples:
- Buckets in volume X
- Bucket owners
- FSO buckets
- OBS buckets
- Versioned buckets

Related:
- volumes
- namespace_usage

### Namespace

#### api_v1_namespace_summary
Path-level summary.

Use when:
- summarize path
- inspect namespace object

Prefer over namespace_usage when user wants metadata rather than disk consumption.

#### api_v1_namespace_usage
DU endpoint.

Use when:
- size
- disk usage
- largest directories
- storage consumed

Never use for file listings.

#### api_v1_namespace_quota
Quota consumption and limits.

Examples:
- quota exceeded
- remaining quota
- quota utilization

#### api_v1_namespace_dist
File-size distribution analytics.

Examples:
- file size histogram
- distribution of files

### Utilization

#### api_v1_utilization_fileCount
Distribution based on file counts.

Use for:
- busiest paths
- object-count analytics

#### api_v1_utilization_containerCount
Distribution based on container counts.

Use for:
- container density
- container allocation analysis

### Tasks

#### api_v1_task_status
Recon background task health.

Examples:
- Is Recon caught up?
- Task failures?
- Sync status?

### Metrics

#### api_v1_metrics_api
Metrics for a specific API.

Parameters:
- api
- query

Use when:
- endpoint performance
- API metrics
- request trends

## 5. Parameter Cheat Sheet

### Record Cap (No Pagination)
- The chatbot returns at most **1000 records per API call**.
- It does **not** paginate; never use `prevKey`.
- Use `limit` only to request fewer than 1000 records.
- For more data, narrow scope (`startPrefix`, filters) or use the Recon REST API directly.

### state
Used by:
- api_v1_containers_unhealthy_state

Valid values:
- MISSING
- UNDER_REPLICATED
- OVER_REPLICATED
- MIS_REPLICATED

### missingIn
Used by:
- api_v1_containers_mismatch

Values:
- OM → container exists in SCM but missing in OM.
- SCM → container exists in OM but missing in SCM.

### includeFso / includeNonFso
Used by open-key and delete-pending style endpoints.

Guidance:
- FSO-only questions → includeFso=true.
- Legacy/OBS questions → includeNonFso=true.
- General cluster-wide questions → enable both.

### startPrefix
Used by api_v1_keys_listKeys, api_v1_keys_open, api_v1_keys_deletePending, and namespace path tools.

Safety rules:
- Must start with at least /<volume>/<bucket>.
- Never use "/" alone (chatbot policy blocks full-cluster key scans).
- Prefer the most specific known path.
- Example: user says "bucket b1 in volume v1" → startPrefix=/v1/b1

### replicationType / creationDate / keySize
Used by api_v1_keys_listKeys only.

- replicationType — RATIS or EC filter
- creationDate — keys created on or after this timestamp
- keySize — minimum logical size in bytes

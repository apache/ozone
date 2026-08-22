---
title: Snapshot Trapped Deleted Bytes Accounting
summary: Per-snapshot accounting of logical data trapped in deleted tables due to snapshot references
date: 2026-07-22
jira: HDDS-15939
status: proposed
author: Sadanand Shenoy
---

<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Snapshot Trapped Deleted Bytes — Design Doc

## Table of Contents

1. [Motivation](#1-motivation)
2. [Background](#2-background)
3. [Goals and Non-Goals](#3-goals-and-non-goals)
4. [Terminology](#4-terminology)
5. [Current State](#5-current-state)
6. [Proposed Design](#6-proposed-design)
   * [6.1 SnapshotInfo fields](#61-snapshotinfo-fields)
   * [6.2 deleteDbKey ledger](#62-deletedbkey-ledger-minimal)
   * [6.3 Counter lifecycle](#63-counter-lifecycle)
   * [6.4 ExpandAndAccountDirService](#64-expandandaccountdirservice)
   * [6.5 Integration with existing services](#65-integration-with-existing-services)
7. [Flows](#7-flows)
8. [Race Conditions and Mitigations](#8-race-conditions-and-mitigations)
9. [Configuration](#9-configuration)
10. [API and Observability](#10-api-and-observability)
11. [Phased Implementation Plan](#11-phased-implementation-plan)
12. [Testing Strategy](#12-testing-strategy)
13. [Upgrade Notes](#13-upgrade-notes)

---

## 1. Motivation

When a key or directory is deleted in a bucket that has snapshots, the delete is not immediately
physical. Metadata moves to `deletedTable` / `deletedDirTable`, and blocks remain until background
services determine that no snapshot in the chain still references the object.

Operators and quota systems need to answer:

> **How much logical deleted data is trapped because of snapshot S?**

Today:

* `OmBucketInfo.snapshotUsedBytes` tracks **bucket-level** pending delete space (incremented on
  delete, decremented on purge).
* `SnapshotInfo.exclusiveSize` is computed opportunistically during KeyDeletingService deep clean and
  does not fully cover directory deletes or provide a complete trapped-delete breakdown.
* After snapshot create, `deletedTable` rows are **partitioned** into the new snapshot checkpoint and
  removed from the active object store (AOS). Per-snapshot trapped bytes are not recorded at create
  time.
* DirectoryDeletingService **does not expand** directories whose root exists in the pinning snapshot,
  so file bytes under pinned dirs are invisible to `deletedTable`-only accounting.

This design adds **per-snapshot trapped deleted metrics** with bounded OM cost and idempotent
accounting via a deleteDbKey-based ledger.

---

## 2. Background

### 2.1 Delete and snapshot chain lifecycle (simplified)

```
User deletes key
  → row in AOS deletedTable
  → bucket.usedBytes↓, bucket.snapshotUsedBytes↑

Snapshot S2 created
  → bucket-prefix rows copied into S2 checkpoint DB
  → stripped from AOS deletedTable / deletedDirTable

KeyDeletingService (KDS)
  → ReclaimableKeyFilter: if key not in previous snapshot → purge eligible
  → SCM deleteKeyBlocks → OMKeyPurgeRequest → row removed, snapshotUsedBytes↓

DirectoryDeletingService (DDS)
  → ReclaimableDirFilter: if dir root in previous snapshot → purgeDir=false
  → otherwise expand subtree → files to deletedTable → KDS purges blocks

SnapshotDeletingService (SDS)
  → on snapshot delete: move deletedTable/deletedDirTable rows to next snapshot or AOS
```

### 2.2 Reclaimable filter semantics

| Filter | `true` (reclaimable) means |
|--------|----------------------------|
| `ReclaimableKeyFilter` | Deleted key **not** referenced in immediate previous snapshot's key/file table |
| `ReclaimableDirFilter` | Deleted dir root **not** in immediate previous snapshot's directory table |
| `ReclaimableRenameEntryFilter` | Rename target **not** in previous snapshot |

`true` = eligible for purge/deep clean. It does **not** mean purge has completed.

---

## 3. Goals and Non-Goals

### Goals

1. Expose **per-snapshot** trapped deleted **file bytes** and **namespace** counts.
2. Count **directory roots** separately (`trappedDirNamespace`) without walking subtrees at snapshot
   create.
3. Account for file bytes under **pinned** (non-reclaimable) directories via a dedicated background
   service without blocking DirectoryDeletingService.
4. Prevent double counting using a **minimal deleteDbKey ledger** (small cache + DB; three fields only).
5. Decrement counters on **successful purge** (same durability boundary as `purgeSnapshotUsedBytes`).
6. Ship in **small phased PRs** behind feature flags.

### Non-Goals

1. Replacing `bucket.snapshotUsedBytes` as the **quota authority** (it remains the bucket total).
2. Synchronous subtree walks on the delete or snapshot-create Ratis path.
3. Removing `exclusiveSize` / `exclusiveSizeDeltaFromDirDeepCleaning` in the first release (may
   deprecate later).
4. Replacing `QuotaRepairTask` internals in v1 (only minimal trapped-counter repair additions are expected).

---

## 4. Terminology

| Term | Meaning |
|------|---------|
| **AOS** | Active object store — live OM metadata, not a snapshot checkpoint |
| **Store snapshot** | The snapshot whose DB holds a `deletedTable` / `deletedDirTable` row |
| **Trapped deleted bytes** | Logical data size of deleted objects not yet purged due to snapshot retention |
| **Deep clean** | Background pass that finishes scanning a snapshot's deleted tables (existing flags) |
| **Expand and account** | Read-only subtree walk that attributes file bytes without promoting to purge |

---

## 5. Current State

| Mechanism | What it measures | Gap |
|-----------|------------------|-----|
| `OmBucketInfo.snapshotUsedBytes` | Bucket total trapped delete bytes | No per-snapshot breakdown |
| `SnapshotInfo.referencedSize` | Estimated live bucket size at create | Not trapped deletes |
| `SnapshotInfo.exclusiveSize` | Keys exclusive to snapshot (KDS side effect) | Incomplete; dirs partial |
| `exclusiveSizeDeltaFromDirDeepCleaning` | Dir deep clean delta | Replaces not accumulates; pinned dirs skipped |
| Recon `DeletedKeysInsightHandler` | Full table scan | Off hot path; no per-snapshot trapped ownership model |

---

## 6. Proposed Design

### 6.1 SnapshotInfo fields

New fields on `SnapshotInfo` (Java + `OmClientProtocol.proto`):

| Field | Type | Set / incremented | Decremented |
|-------|------|-------------------|-------------|
| `trappedKeyBytes` | `long` | Snapshot create (`deletedTable` sum); ExpandAndAccountDirService; DDS promote to `deletedTable` (reclaimable dir) | Successful key purge |
| `trappedKeyNamespace` | `long` | Same | Successful key purge |
| `trappedDirNamespace` | `long` | Snapshot create (count of **root** rows in `deletedDirTable`); subtree dir credit when ExpandAndAccount walk completes | DDS removes root from `deletedDirTable` |

**Intentionally no `trappedDirBytes` at create** — dir rows carry no file payload.

`trappedKeyBytes` uses **replicated** logical size (same units as `bucket.snapshotUsedBytes` and KDS
`purgedBytes`) so create / promote increments reconcile with purge decrements.

**Displayed total trapped deleted file bytes for snapshot S:**

```
totalTrappedDeletedBytes(S) = S.trappedKeyBytes
```

`trappedDirNamespace` is namespace-only (pending dir roots), not byte size.

### 6.2 deleteDbKey ledger (minimal)

New OM table: `snapshotTrappedLedgerTable` (name TBD).

**Key:** `/{volumeId}/{bucketId}/{deleteDbKey}` (normalized row key based on `deletedTable` / `deletedDirTable`)

**Value:** three fields only — no bytes, namespace, or updateId stored in the ledger.

```protobuf
message SnapshotTrappedLedgerEntry {
  enum State {
    ACCOUNTED_KEY = 0;           // counted in trappedKeyBytes / trappedKeyNamespace
    ACCOUNTED_DIR_ROOT = 1;      // counted in trappedDirNamespace only
    DIR_EXPAND_ACCOUNTED = 2;    // dir root: subtree byte scan complete (skip re-walk)
    PURGED = 3;
  }
  required uint64 object_id = 1;
  required UUID snapshot_id = 2;   // snapshot that owns the trapped counter increment
  required State state = 3;
}
```

`snapshot_id` meaning:

* **File / key:** the snapshot whose `trappedKey*` was incremented (store snapshot at create,
  ExpandAndAccount, and DDS promote).
* **Dir root:** the store snapshot whose `trappedDirNamespace` was incremented at create.

Byte and namespace amounts live only on `SnapshotInfo` counters (and in the purge request sizes at
decrement time). The ledger answers only: *has this deleteDbKey been accounted, for which snapshot, and
in what phase of lifecycle?*

**Cache:** bounded in-memory map on OM leader mirroring the same three fields (`deleteDbKey` →
`{snapshotId, state}`). Write-through to DB on insert/CAS. No richer cached payload.

**Rules:**

* Increment snapshot counters **only** when `putIfAbsent` on ledger succeeds.
* Decrement **only** when CAS `ACCOUNTED_* → PURGED` (or `DIR_EXPAND_ACCOUNTED → PURGED` for dir
  root after purge) succeeds.
* One ledger row per `/{volumeId}/{bucketId}/{deleteDbKey}`; prevents double counting across create, expand, DDS promote, and purge.
* Purge decrement uses **purge payload sizes** (`BucketPurgeKeysSize` / dir purge), not ledger-stored
  bytes.
* `PURGED` rows are physically deleted from the ledger table by a background cleanup task or immediately upon purge.

### 6.3 Counter lifecycle

#### Increment paths

| Event | `trappedKeyBytes` | `trappedKeyNamespace` | `trappedDirNamespace` | Ledger |
|-------|-------------------|----------------------|----------------------|--------|
| Snapshot create: `deletedTable` row | += size (store snapshot) | += count | — | `ACCOUNTED_KEY` |
| Snapshot create: `deletedDirTable` root | — | — | += 1 | `ACCOUNTED_DIR_ROOT` |
| ExpandAndAccountDirService: file under non-reclaimable dir | += size (**store** snapshot) | += 1 | — | `ACCOUNTED_KEY` on `putIfAbsent` success |
| DDS promotes file to `deletedTable` | += size (**store** snapshot) on `putIfAbsent` success; **no increment** if ledger row already exists | += 1 on success; skip if exists | — | `ACCOUNTED_KEY` on first account only |
| ExpandAndAccountDirService subtree completed | — | — | += expanded subdir count (batched) | after CAS `ACCOUNTED_DIR_ROOT → DIR_EXPAND_ACCOUNTED` |

All three file increment paths use the same gate:
**`ledger.putIfAbsent(deleteDbKey, storeSnapshotId, ACCOUNTED_KEY)`** → increment `trappedKey*` only when the insert succeeds.

#### DDS promote to `deletedTable` (reclaimable directory expansion)

When DirectoryDeletingService deep-cleans a **reclaimable** deleted dir root, it promotes files from
`fileTable` into `deletedTable`. Those files were not in `deletedTable` at snapshot create (only the
dir root was). It can so happen that the DDS thread first picks up the dir before the Expand thread.

Per file promoted:

```
storeSnapshot = snapshot currently owning the delete DB

if ledger.putIfAbsent(deleteDbKey, storeSnapshotId, ACCOUNTED_KEY) succeeds:
    storeSnapshot.trappedKeyBytes      += replicatedSize
    storeSnapshot.trappedKeyNamespace += 1
    promote row to deletedTable
else:
    # Ledger row already exists (snapshot create or ExpandAndAccount got there first)
    promote row only — do not increment trappedKey*
```

| Prior ledger state | DDS promote behavior |
|------------------|----------------------|
| Absent | `putIfAbsent` → increment **store** snapshot `trappedKey*` → promote |
| `ACCOUNTED_KEY` (create / expand) | Promote only — prevents double count after expand → reclaimable transition |
| `PURGED` | Should not promote; treat as invariant violation |

Hook location: `OMDirectoriesPurgeResponseWithFSO` (or shared helper invoked when batching file rows
into `deletedTable`).

#### Decrement paths

| Event | Where | Condition |
|-------|-------|-----------|
| Key purge success | `OMKeyPurgeRequest` | Ledger CAS → PURGED; decrement **store** snapshot (via `fromSnapshotInfo`) |
| Dir root purged | `OMDirectoriesPurgeResponseWithFSO` | Root removed; `trappedDirNamespace -= 1` |
| Bucket total | existing `purgeSnapshotUsedBytes` | unchanged semantics |

**Important:** Decrement happens on **successful purge Ratis txn**, not when `ReclaimableKeyFilter` returns `true`. Purge is already snapshot-aware via `PurgeKeysRequest.snapshotTableKey` → `fromSnapshotInfo`.

#### AOS purge (no `fromSnapshotInfo`)

* Decrement `bucket.snapshotUsedBytes` only (existing).
* Do not decrement any `SnapshotInfo.trappedKey*` unless ledger attributes bytes to a store snapshot
  (Phase 5+).

### 6.4 ExpandAndAccountDirService

**Purpose:** Account for file bytes under directories that DDS cannot expand because
`ReclaimableDirFilter == false`.

**Trigger:** DDS enqueues a job when it encounters a non-reclaimable dir root (does not block DDS
iteration).

**Job input:**

```
{ storeSnapshotId | AOS,
  deletedDirDbKey,
  dirObjectId,
  expectedPreviousSnapshotId }
```

**Algorithm (batched, low priority):**

1. Re-validate snapshot chain (`expectedPreviousSnapshotId`).
2. Re-check `ReclaimableDirFilter`; if now reclaimable → exit (DDS owns expansion).
3. Read-only walk `fileTable` under dir root (and subdirs still in `directoryTable`) using the existing DDS traversal logic.
4. For each file: use `storeSnapshotId` from the job context.
5. `ledger.putIfAbsent(deleteDbKey, storeSnapshotId, ACCOUNTED_KEY)` → on success, increment store
   snapshot `trappedKey*` in the same OM DB batch.
6. If no unaccounted files remain under root → CAS dir root ledger
   `ACCOUNTED_DIR_ROOT → DIR_EXPAND_ACCOUNTED` (skip re-walk on subsequent runs) and credit
   `trappedDirNamespace` once for expanded subdirs under that root (batched).

**Does not:**

* Move rows to `deletedTable` (DDS still owns promotion when reclaimable).
* Call SCM or purge blocks.
* Update `trappedDirNamespace` before subtree walk completion.

**Ratis pressure mitigation:** accumulate counter deltas in memory; one batched `OMDirectoriesPurgeRequestWithFSO` or
inline `SnapshotInfo` update per task — **not** one transaction per file. The `putIfAbsent` and counter delta must use the same OM Ratis request and active DB batch to prevent failure windows.

### 6.5 Integration with existing services

```
┌─────────────────────────────────────────────────────────────────┐
│ Snapshot create (createOmSnapshotCheckpoint)                    │
│   sum deletedTable → trappedKey*                                │
│   count deletedDirTable roots → trappedDirNamespace             │
│   ledger putIfAbsent                                            │
│   (existing partition / strip from AOS)                         │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ DirectoryDeletingService                                        │
│   reclaimable dir  → expand subtree; promote files to           │
│                      deletedTable (ledger putIfAbsent per file) │
│   non-reclaimable  → enqueue ExpandAndAccountDirService         │
│   root purged      → trappedDirNamespace--                      │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ ExpandAndAccountDirService (new)                                │
│   read-only subtree account → trappedKey* on store snapshot       │
│   subtree complete        → trappedDirNamespace credit (subdirs)  │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ KeyDeletingService                                              │
│   reclaimable keys → SCM → OMKeyPurgeRequest                    │
│   trappedKey*-- on success (with ledger)                         │
│   deepClean / exclusiveSize unchanged in scope                  │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ SnapshotDeletingService                                         │
│   move tables S1→S2/AOS; update ledger snapshot_id on rows     │
│   transfer trapped* counters between store snapshots            │
│   all updates in one DB batch / one Ratis txn                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 7. Flows

### 7.1 File delete → snapshot → purge (happy path)

```
1. Delete key1 → AOS.deletedTable, snapshotUsedBytes↑
2. Create S2 → key1 row in S2.deletedTable, stripped from AOS
                 S2.trappedKeyBytes += size (ledger ACCOUNTED_KEY)
3. KDS on S2: key1 not in S1 → reclaimable
4. SCM + OMKeyPurgeRequest(fromSnapshot=S2)
                 S2.trappedKeyBytes -= size, snapshotUsedBytes↓, row deleted
```

### 7.2 Directory delete pinned by snapshot

```
1. S1 has /foo; user deletes /foo → deletedDirTable[/foo]
2. Create S2 → root moves to S2.deletedDirTable
                 S2.trappedDirNamespace = 1 (no file bytes yet)
3. DDS on S2: /foo in S1 → not reclaimable → enqueue ExpandAndAccount
4. ExpandAndAccount: files under /foo → S2.trappedKeyBytes += ... and credit subdirs on completion (same DB batch)
5. User deletes S1 → SDS moves tables; DDS expands (reclaimable); KDS purges
6. trappedKey* and trappedDirNamespace → 0 after full purge
```

### 7.3 Reclaimable directory — first account at DDS promote

```
1. User deletes /foo (reclaimable: no older snapshot pins the root)
   → deletedDirTable[/foo], trappedDirNamespace++ at next snapshot create
2. Files remain in fileTable until DDS runs
3. DDS expands /foo → for each file promoted to deletedTable:
     ledger.putIfAbsent(deleteDbKey, storeSnapshot, ACCOUNTED_KEY)
     on success → storeSnapshot.trappedKeyBytes += replicatedSize
4. KDS purges from deletedTable → trappedKey*-- on successful OMKeyPurgeRequest
```

If the dir was **non-reclaimable** first, ExpandAndAccount accounts files while still in `fileTable`;
when the dir later becomes reclaimable, DDS promote finds existing ledger rows and **skips**
increment (see §6.3).

### 7.4 Reclaimable filter true vs purge

```
ReclaimableKeyFilter == true  →  candidate only (in-memory)
SCM success                   →  included in PurgeKeysRequest
OMKeyPurgeRequest committed   →  trappedKey*--, ledger PURGED  ← durable boundary
```

---

## 8. Race Conditions and Mitigations

| Race | Risk | Mitigation |
|------|------|------------|
| Expand vs KDS purge same file | Double decrement or ghost increment | Ledger CAS on inc/dec; existence check before increment |
| Expand vs DDS promote | Double count | `putIfAbsent` on promote: increment only when ledger absent; promote-only when expand/create already ledgered. Coordination with DDS is required if running concurrently. |
| SDS move during expand job | Wrong store walk | `expectedPreviousSnapshotId` on job; abort/requeue |
| Dir becomes reclaimable before expand runs | Duplicate work | Re-check filter at job start; exit if reclaimable |
| Snapshot deleted mid-expand | Stale per-snapshot counter | Transfer row + ledger owner + trapped* in same SDS batch txn |
| Metric lag before expand | Under-reported trappedKeyBytes | Document; dir root state `DIR_EXPAND_ACCOUNTED` signals completeness |

**Bucket `snapshotUsedBytes`:** purge remains the reconciliation sink — one ledger entry, one purge
decrement.

---

## 9. Configuration

```properties
# Master switch (default false until stable)
ozone.om.snapshot.trapped.accounting.enabled=false

# ExpandAndAccountDirService
ozone.om.expand.and.account.dir.enabled=false
ozone.om.expand.and.account.dir.interval=60s
ozone.om.expand.and.account.dir.limit.per.task=1000

# Ledger cache
ozone.om.snapshot.trapped.ledger.cache.size=100000
```

All phases ship behind flags for safe incremental merge.

---

## 10. API and Observability

### Proto (`SnapshotInfo` in `OmClientProtocol.proto`)

```protobuf
optional uint64 trappedKeyBytes = 24;          // replicated size
optional uint64 trappedKeyNamespace = 25;
optional uint64 trappedDirNamespace = 26;
```

### CLI / `ozone sh snapshot info`

| Display | Source |
|---------|--------|
| Trapped deleted bytes | `trappedKeyBytes` |
| Trapped deleted keys | `trappedKeyNamespace` |
| Pending deleted dir roots | `trappedDirNamespace` |

### Metrics (Phase 11)

* `om_snapshot_trapped_key_bytes{snapshot=...}`
* `om_snapshot_trapped_dir_namespace{snapshot=...}`
* `om_expand_account_dir_jobs_total`

### Relationship to existing fields

| Field | Relationship |
|-------|--------------|
| `referencedSize` | Live data at create — orthogonal |
| `exclusiveSize` | Legacy deep-clean side effect — keep for reconciliation; do not sum with `trappedKeyBytes` |
| `snapshotUsedBytes` (bucket) | Superset across snapshots + AOS; authoritative for quota. Note: `trappedKeyBytes` can exceed `snapshotUsedBytes` temporarily because it includes files under pinned directories that are still in `fileTable` (not yet moved to `deletedTable` by DDS). |

---

## 11. Phased Implementation Plan

| Phase | Scope | Delivers |
|-------|-------|----------|
| **0** | Read existing flows | Understanding only |
| **1** | Schema: `trappedKey*`, `trappedDirNamespace` on `SnapshotInfo` + proto | Fields round-trip; display zeros |
| **2** | Snapshot create accounting (no ledger) | Sum `deletedTable`; count dir roots |
| **3** | KDS purge decrement on `fromSnapshotInfo` | `trappedKey*`-- next to `purgeSnapshotUsedBytes` |
| **4** | Ledger table; write on create only | `putIfAbsent` gates increment |
| **5** | KDS purge + ledger CAS | Idempotent decrement |
| **6** | DDS dir root purge → `trappedDirNamespace--` | Dir namespace lifecycle |
| **7** | ExpandAndAccountDirService (manual test trigger) | Pinned dir bytes |
| **8** | DDS promote ledger + enqueue non-reclaimable dirs | Reclaimable dir file accounting at promote; pinned dir enqueue |
| **9** | Dir root state `DIR_EXPAND_ACCOUNTED` | Skip re-walk |
| **10** | SDS move + ledger `snapshot_id` / counter transfer | Snapshot delete correctness |
| **11** | Metrics, docs, CLI polish | Operability |

**Checkpoint demos:**

* After **Phase 6:** trapped bytes for files in `deletedTable` end-to-end.
* After **Phase 8:** full design including pinned directories.

---

## 12. Testing Strategy

| Phase | Tests |
|-------|-------|
| 1 | `SnapshotInfo` codec / proto round-trip |
| 2 | `TestOMSnapshotCreateRequest`: file + dir create accounting |
| 3 | `TestKeyDeletingService`: purge decrements `trappedKeyBytes` on snapshot DB |
| 4–5 | Ledger unit tests; partial `RepeatedOmKeyInfo` purge |
| 6 | `TestDirectoryDeletingService`: reclaimable dir → `trappedDirNamespace` → 0; promote accounts `trappedKey*` when ledger absent |
| 7–8 | Integration: S1 pins dir, S2 create, expand accounts, S1 delete, full purge |
| 10 | `TestSnapshotDeletingServiceIntegrationTest` + counter transfer |

---

## 13. Upgrade Notes

* Proto field additions are backward compatible (`trappedKeyBytes`, `trappedKeyNamespace`,
  `trappedDirNamespace` default to 0).
* Feature stays disabled by default until explicitly enabled.
* Minimal `QuotaRepairTask` updates are needed to recompute and repair trapped counters from
  snapshot deleted tables and ledger state during repair runs.

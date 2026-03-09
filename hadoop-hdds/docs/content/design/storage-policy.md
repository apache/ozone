---
title: Storage Class
summary: New abstraction to configure replication methods.
date: 2020-06-08
jira: HDDS-3755
status: draft
author: Marton Ele
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

# Ozone Storage Tiering — MVP Requirements

Scoped subset of `01-user-requirements.md`. Four delivery stages: **MVP-1** (bucket policy + SCM placement), **MVP-2** (prefix policy + Mover), **MVP-3** (write-path hardening), **Future** (full feature set).

---

## Assumptions

- Bucket layout: **OBJECT_STORE** with **EC replication** (EC 3+2) only
- **FSO/Ratis buckets**: Storage tiering is **disabled** — setting a storage policy on an FSO or Ratis bucket is rejected. Writes to FSO/Ratis buckets continue using default placement (no StorageType passed to SCM). This avoids complexity with Ratis pipeline tiering and FSO directory inheritance in the MVP.
- Storage types: **DISK** (default) and **SSD/NVMe** only — no ARCHIVE
- Storage policies: **HOT** (SSD) and **WARM** (DISK, default)
- Policy management: **CLI only** — no S3 `x-amz-storage-class` integration
- Migration: **External Mover tool** (daily cron) — no internal OM TieringSatisfier
- Target cluster: 32 nodes — 16 SSD-only (12 SSDs each), 16 DISK-only (6 DISKs each)
- **Node topology note**: Heterogeneous nodes (both SSD + DISK on every DN) are preferred over dedicated nodes. With heterogeneous nodes, every DN qualifies for both SSD and DISK pipelines, so `BackgroundPipelineCreator` creates the same number of pipelines for each type from the full node pool. With dedicated nodes (SSD-only vs DISK-only), the node pool is split — fewer pipelines per type and on-demand creation (MVP-3) becomes more important.

---

## Implementation Overview

### MVP-1: Bucket Policy + SCM Placement (~830 lines)

1. **Policy model** — HOT/WARM enum in proto and Java
2. **Bucket policy** — store policy on bucket, accept via create/update
3. **Resolve at write time** — OM reads bucket policy → target StorageType
4. **Tell SCM** — OM passes StorageType to SCM's block allocation
5. **SCM filters pipelines** — pick pipeline where all nodes have the right type
6. **SCM creates typed pipelines** — proactively create SSD-only and DISK-only pipelines
7. **DN picks right volume** — create container on matching volume type
8. **Fallback** — no SSD pipeline → use DISK, log warning
9. **CLI** — `ozone sh bucket create/update --storage-policy HOT|WARM`

### MVP-2: Prefix Policy + Mover (~835 lines)

10. **Prefix policy** — store policy on key prefixes
11. **Extended resolution** — prefix → bucket → default
12. **Mover tool** — scan keys, rewrite mismatched ones to correct tier
13. **CLI** — `ozone sh prefix setStoragePolicy`, `ozone admin mover`

### MVP-3: Write-Path Hardening (~140 lines)

14. **On-demand pipeline creation** — create matching pipeline on-the-fly before falling back

---

## Scope Summary

| Requirement | Service | MVP-1 | MVP-2 | MVP-3 | Future | Lines |
|-------------|---------|-------|-------|-------|--------|-------|
| StoragePolicyProto enum, bucket/key fields, AllocateBlockRequest | Proto | **Yes** | | | | ~35 |
| Bucket policy model + handlers, resolution, pass StorageType to SCM | OM | **Yes** | | | | ~255 |
| Pipeline filtering + creation, PlacementPolicy filter, fallback | SCM | **Yes** | | | | ~240 |
| VolumeChoosingPolicy + KeyValueHandler StorageType threading | DN | **Yes** | | | | ~25 |
| bucket --storage-policy commands | CLI | **Yes** | | | | ~100 |
| Prefix proto fields | Proto | | **Yes** | | | ~20 |
| Prefix policy model + handlers, extended resolution | OM | | **Yes** | | | ~315 |
| prefix set/get/removeStoragePolicy commands | CLI | | **Yes** | | | ~50 |
| Mover tool (bulk migration + dry-run) | Client | | **Yes** | | | ~450 |
| CreateContainerCommand proto field | Proto | | | **Yes** | | ~10 |
| On-demand pipeline creation, error handling | SCM | | | **Yes** | | ~130 |
| FSO dirs, key-level policy, TieringSatisfier, fallback config | OM | | | | **Yes** | ~950+ |
| Full pipeline tiering, EC reconstruction, ContainerBalancer | SCM | | | | **Yes** | ~1500+ |
| Admin observability tools | CLI | | | | **Yes** | ~300+ |
| x-amz-storage-class integration | S3GW | | | | **Yes** | ~200+ |

---

## Detailed Scope Summary

| Requirement | Service | MVP-1 | MVP-2 | MVP-3 | Future | Lines |
|-------------|---------|-------|-------|-------|--------|-------|
| Proto: StoragePolicyProto enum + fields on Bucket, Key | Proto | **Yes** | | | | ~30 |
| OM: OzoneStoragePolicy enum + OmBucketInfo fields | OM | **Yes** | | | | ~150 |
| OM: Bucket create/update handlers for storagePolicy | OM | **Yes** | | | | ~80 |
| OM: Policy resolution (bucket → default) | OM | **Yes** | | | | ~15 |
| CLI: bucket --storage-policy | CLI | **Yes** | | | | ~100 |
| Proto: StorageType on AllocateBlockRequest | Proto | **Yes** | | | | ~5 |
| OM: Pass resolved StorageType to SCM allocateBlock | OM | **Yes** | | | | ~10 |
| SCM: BlockManagerImpl accepts StorageType, passes to provider | SCM | **Yes** | | | | ~10 |
| SCM: WritableContainerProvider filters pipelines by node StorageType | SCM | **Yes** | | | | ~30 |
| SCM: BackgroundPipelineCreator creates per-StorageType pipelines | SCM | **Yes** | | | | ~40 |
| SCM: PlacementPolicy StorageType filter in chooseDatanodes | SCM | **Yes** | | | | ~50 |
| SCM: Pipeline providers accept StorageType in node selection | SCM | **Yes** | | | | ~60 |
| SCM: Pipeline storageType attribute for tracking | SCM | **Yes** | | | | ~20 |
| SCM: Fallback to DISK when SSD unavailable + warn logging | SCM | **Yes** | | | | ~30 |
| DN: VolumeChoosingPolicy filters volumes by StorageType | DN | **Yes** | | | | ~20 |
| DN: KeyValueHandler threads StorageType to volume selection | DN | **Yes** | | | | ~5 |
| Proto: fields on Prefix | Proto | | **Yes** | | | ~20 |
| OM: OmPrefixInfo storagePolicy field | OM | | **Yes** | | | ~50 |
| OM: Prefix set/get/remove storagePolicy request handlers | OM | | **Yes** | | | ~250 |
| OM: Policy resolution extended (prefix → bucket → default) | OM | | **Yes** | | | ~15 |
| CLI: prefix set/get/removeStoragePolicy | CLI | | **Yes** | | | ~50 |
| Client: Mover tool (bulk migration + dry-run) | Client | | **Yes** | | | ~450 |
| SCM: WritableContainerProvider on-demand pipeline creation | SCM | | | **Yes** | | ~80 |
| Proto: StorageType on CreateContainerCommand | Proto | | | **Yes** | | ~10 |
| SCM/DN: Error handling, logging | SCM/DN | | | **Yes** | | ~50 |
| OM: FSO directory policy (tree-walk inheritance) | OM | | | | **Yes** | ~300+ |
| OM/Client: Key-level explicit policy | OM/Client | | | | **Yes** | ~100+ |
| S3GW: x-amz-storage-class integration | S3GW | | | | **Yes** | ~200+ |
| OM: TieringSatisfier (background service) | OM | | | | **Yes** | ~500+ |
| OM: Per-bucket fallback config | OM | | | | **Yes** | ~50+ |
| SCM: Full pipeline tiering (separate pools, lifecycle) | SCM | | | | **Yes** | ~1000+ |
| SCM: EC reconstruction StorageType awareness | SCM | | | | **Yes** | ~200+ |
| SCM: ContainerBalancer per-StorageType | SCM | | | | **Yes** | ~300+ |
| CLI: Admin observability tools | CLI/OM | | | | **Yes** | ~300+ |

---

# MVP-1: Bucket Policy + SCM Placement

Minimum viable deliverable. Bucket-level storage policy with SCM honoring StorageType at allocation time. New writes land on the correct tier. If SSD capacity is exhausted, falls back to DISK with a warning log.

~830 lines excluding tests. Low-medium risk.

---

## 1.1: Storage Policy Model

### Named Storage Policies
- **HOT** — SSD/NVMe
- **WARM** — DISK (default)

### Policy on Buckets
Set a default storage policy on a bucket via CLI. All keys in the bucket are placed on the corresponding storage tier.

### Dynamic Resolution
Resolved at runtime, not stamped on keys:
1. Bucket default
2. Server default (WARM)

Changing a bucket policy immediately changes the effective policy for all keys in it. Existing data remains on the old tier until migrated (MVP-2 Mover).

### Unset / Remove Policy
Remove a policy from a bucket → falls through to server default (WARM).

---

## 1.2: SCM Changes (Placement + Pipeline Creation)

Both new writes and the future Mover (MVP-2) call SCM's `allocateBlock()` to get blocks. Without SCM honoring StorageType, data always lands on whatever tier SCM happens to pick. MVP-1 includes SCM changes to make placement work.

### What Exists Today

SCM **already knows** which DataNodes have which storage types — DataNodes report per-volume storage types in every heartbeat via `StorageReportProto.storageType` (field 6). This data is stored in `DatanodeInfo.storageReports` and accessible via `SCMNodeStorageStatMap`. It's just never used for placement decisions.

### Why Proactive Pipeline Creation Is Required

On a cluster with 32 nodes — 16 SSD-only, 16 DISK-only — using EC 3+2 (5 nodes per pipeline), the probability that a randomly formed pipeline has all 5 members from SSD nodes is only **~2.2%** ((16/32)×(15/31)×(14/30)×(13/29)×(12/28)). With ~30 open pipelines, on average zero or one would qualify for HOT placement. Without proactive pipeline creation, writes cannot be placed on SSD.

With proactive creation, SCM intentionally forms SSD pipelines from SSD-capable nodes and DISK pipelines from DISK-capable nodes. Based on current cluster data (14 SSD-only nodes → 44 pipelines with EC 3+2), 16 SSD nodes would yield ~50 SSD pipelines and 16 DISK nodes ~50 DISK pipelines.

**Heterogeneous vs dedicated nodes**: If every DN has both SSD and DISK volumes (heterogeneous), all nodes qualify for both pipeline types. SCM creates the same number of pipelines for each type from the full node pool — no shortage. The 2.2% problem only applies to dedicated nodes (SSD-only vs DISK-only) where the node pool is split. With heterogeneous nodes, on-demand pipeline creation (MVP-3) becomes largely unnecessary.

### What MVP-1 Adds (~280 lines)

| Layer | Change | Lines |
|-------|--------|-------|
| **Proto** | Add `optional StorageTypeProto storageType` to `AllocateBlockRequest` | ~5 |
| **OM** | Pass resolved StorageType to SCM's `allocateBlock()` | ~10 |
| **SCM BlockManagerImpl** | Accept StorageType, pass to WritableContainerProvider | ~10 |
| **WritableContainerProvider** (Ratis + EC) | Filter open pipelines: keep only those where ALL member nodes have volumes of the requested StorageType | ~30 |
| **SCM BackgroundPipelineCreator** | Create per-StorageType pipelines (SSD pipelines from SSD nodes, DISK pipelines from DISK nodes) | ~40 |
| **SCM PlacementPolicy** | Add StorageType filter to `chooseDatanodes()` / `hasEnoughSpace()` — only select nodes with the requested type | ~50 |
| **SCM Pipeline providers** (Ratis + EC) | Accept StorageType filter in node selection when creating new pipelines | ~60 |
| **SCM Pipeline** | Add optional `storageType` attribute for tracking which type a pipeline serves | ~20 |
| **SCM Fallback** | SSD unavailable → fall back to DISK, log warning for monitoring | ~30 |
| **DN VolumeChoosingPolicy** | When StorageType is specified, filter candidate volumes by type before choosing | ~20 |
| **DN KeyValueHandler** | Thread StorageType from `CreateContainerCommand` to volume selection | ~5 |
| **Total** | | **~280** |

### Configuration

Proactive per-StorageType pipeline creation is controlled by a config flag:
```
ozone.scm.pipeline.creation.storage-type-aware.enabled = true  (default: false)
```
When disabled, `BackgroundPipelineCreator` behaves as today — no StorageType filtering. Useful for clusters that don't use tiering, during rollback, or on clusters with heterogeneous nodes (all DNs have both SSD and DISK) where random pipeline formation already produces pipelines that qualify for both types.

### How It Works

```
At startup / periodically (if storage-type-aware pipeline creation enabled):
  BackgroundPipelineCreator scans cluster storage types
    → Creates SSD pipelines using only SSD-capable nodes
    → Creates DISK pipelines using only DISK-capable nodes

On write:
  OM resolves policy (bucket → default) → StorageType = SSD
    ↓
  SCM allocateBlock(size, replicationConfig, storageType=SSD, ...)
    ↓
  Filter open pipelines: keep only those where ALL member nodes have SSD volumes
    ├─ Found → allocate block on that pipeline
    └─ Not found → FALL BACK to any open pipeline (DISK)
         → Log warning for monitoring
```

---

## 1.3: CLI

### Bucket Policy
```
ozone sh bucket create --storage-policy HOT|WARM <bucket-uri>
ozone sh bucket update --storage-policy HOT|WARM <bucket-uri>
ozone sh bucket info <bucket-uri>          # shows storage policy
```

---

## 1.4: Backward Compatibility

### Existing Data Unaffected
Keys without a policy resolve to WARM (DISK). No behavior change for clusters that don't set policies.

### Proto Backward Compat
All new fields `optional` with `UNSET = 0`. Old clients ignore new fields.

### Upgrade Path
Cold restart with new JARs. No DB migration — proto optional fields with `UNSET = 0` default handle old data. Layout versioning (`ozone admin om finalize`) can gate feature activation if needed.

---

## 1.5: How New Writes Work in MVP-1

- OM resolves effective policy at write time (bucket → default)
- OM passes resolved StorageType to SCM's `allocateBlock()`
- SCM filters open pipelines by node StorageType — `BackgroundPipelineCreator` ensures per-StorageType pipelines exist
- If a matching pipeline exists, the block lands on the correct tier
- If no matching pipeline exists (e.g., all SSD nodes full), SCM falls back to any open pipeline and logs a warning

MVP-1 is designed so that MVP-2 and MVP-3 are purely additive — bucket policy, proto fields, SCM placement, and CLI all remain unchanged.

---

# MVP-2: Prefix Policy + Mover

Builds on MVP-1. Adds prefix-level storage policy for finer-grained control within a bucket, and the Mover tool for bulk migration of existing data after policy changes.

~835 additional lines excluding tests. Low risk — OM metadata + external client tool.

---

## 2.1: Prefix Storage Policy

### Policy on Prefixes (Object Store)
Set a storage policy on a key prefix. Longest-prefix-match wins.

### Extended Resolution
Resolution chain extended to:
1. Prefix policy (longest match)
2. Bucket default
3. Server default (WARM)

Changing a prefix policy immediately changes the effective policy for all keys matching it. The Mover migrates existing mismatched data.

**Implementation note**: The resolution uses the same in-memory `PrefixManager` RadixTree that serves ACL inheritance. `resolveEffectiveStoragePolicy()` calls `prefixManager.getLongestPrefixPath()` independently — it does not depend on ACLs being enabled. The RadixTree is loaded at OM startup and maintained in memory, so this is a pure in-memory tree traversal with zero RocksDB I/O. Negligible performance impact.

### Unset / Remove Policy
Remove a policy from a prefix → falls through to bucket default → server default.

---

## 2.2: Mover Tool

### Bulk Migration
External CLI tool (`ozone admin mover`):
- Scans a bucket or prefix path
- For each key: resolves effective policy, compares to actual storage tier
- Moves mismatched keys via `rewriteKey()`
- Run daily via cron, or on-demand after a policy change

### Dry-Run Mode
`--dry-run` reports mismatches without moving data. Serves as the policy compliance check.

### Progress Reporting
Keys scanned, moved, skipped, failed, bytes migrated.

### Safety
Generation-based optimistic concurrency via `rewriteKey()`. Concurrent modification → move fails safely, retried on next run.

---

## 2.3: CLI

### Prefix Policy
```
ozone sh prefix setStoragePolicy --policy HOT|WARM <prefix-uri>
ozone sh prefix getStoragePolicy <prefix-uri>
ozone sh prefix removeStoragePolicy <prefix-uri>
```

### Mover
```
ozone admin mover --path o3://om/vol/bucket/             # entire bucket
ozone admin mover --path o3://om/vol/bucket/prefix/       # specific prefix
ozone admin mover --path o3://om/vol/bucket/ --dry-run    # report only
ozone admin mover --path o3://om/vol/bucket/ --threads 8  # parallel
```

---

# MVP-3: Write-Path Hardening

Builds on MVP-2. Adds on-demand pipeline creation when no matching pipeline exists (instead of falling back to DISK), and additional error handling. After MVP-3, writes almost never fall back to DISK unless the target storage type is genuinely full.

~140 additional lines excluding tests. Medium risk — extends SCM write path.

---

## 3.1: On-Demand Pipeline Creation

When no open pipeline matches the requested StorageType (e.g., all SSD pipelines are full/closed), `WritableContainerProvider` creates a new pipeline on-the-fly using only nodes that have the requested type, before falling back to DISK.

```
Filter open pipelines for SSD
  ├─ Found → allocate block
  └─ Not found → create new pipeline using only SSD-capable nodes
       ├─ Created → allocate block
       └─ Can't create (not enough SSD nodes) → FALL BACK to DISK (already in MVP-1)
```

---

## 3.2: Changes Required

| Layer | Change | Lines |
|-------|--------|-------|
| **WritableContainerProvider** (Ratis + EC) | On-demand pipeline creation for missing types | ~80 |
| **Proto** | Add `StorageType` to `CreateContainerCommand` | ~10 |
| **Error handling, logging** | | ~50 |
| **Total** | | **~140** |

---

# Future (Deferred)

Full feature set from `01-user-requirements.md` and `03-detailed-technical-plan.md`. Adds:

- **FSO/Ratis bucket support** — enable storage tiering for FSO layout and Ratis replication (MVP only supports OBS+EC)
- **FSO directory policy** — storage policy on directories with tree-walk inheritance (like HDFS)
- **Key-level explicit policy** — client sets policy per key at write time
- **S3 Gateway integration** — `x-amz-storage-class` header maps to storage policy
- **TieringSatisfier** — internal OM background service for continuous compliance (complements Mover)
- **Full pipeline tiering** — separate pipeline pools per storage type with lifecycle management
- **EC reconstruction awareness** — reconstruction targets filtered by StorageType
- **ContainerBalancer per-StorageType** — balance SSD→SSD and DISK→DISK separately
- **Per-bucket fallback config** — `allowFallbackStoragePolicy` per bucket instead of global
- **Admin observability** — `ozone admin storagepolicies list|check|usageinfo`
- **COLD/ARCHIVE tier** — exercise the COLD policy with ARCHIVE storage type

See `01-user-requirements.md` for full requirements and `03-detailed-technical-plan.md` for implementation details.

---

# Effort Summary

| Stage | Scope | Lines (excl. tests) | Risk | Deliverable |
|-------|-------|-------|------|-------------|
| **MVP-1** | Bucket policy + SCM placement + pipeline creation | ~830 | Low-medium | Bucket-level policy via CLI, per-StorageType pipelines, writes land on correct tier, fallback to DISK with warning log |
| **MVP-2** | Prefix policy + Mover | ~835 | Low | Prefix-level policy, Mover migrates misplaced data after policy changes |
| **MVP-3** | Write-path hardening | ~140 | Medium | On-demand pipeline creation — writes almost never fall back unless tier is genuinely full |
| **MVP-1 + MVP-2 + MVP-3** | Combined | **~1,805** | Medium | Full pilot-ready solution |
| **Future** | Full feature set | ~3,000+ | High | Production-grade, all layouts, S3, auto-satisfier |

MVP-1 is independently useful — bucket-level policy with correct write placement. MVP-2 adds prefix granularity and the Mover for bulk migration. MVP-3 adds resilience. All three together at ~1,805 lines.

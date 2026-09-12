---
title: Leader-Planned Execution — Migration Playbook
summary: Phase-0 prerequisites, hard-first phasing, full operation inventory, per-command process, and ZDU compatibility
date: 2026-06-12
jira: HDDS-11898
status: proposed
author: Abhishek Pal
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

# Migration Playbook

This document describes the phased migration strategy from the legacy
`validateAndUpdateCache` path to leader-planned execution. For the high-level
architecture, see the [overview](./overview.md).

## Table of Contents

1. [Phase-0 Prerequisites](#1-phase-0-prerequisites)
2. [Hard-First Phasing](#2-hard-first-phasing)
3. [Full Operation Inventory](#3-full-operation-inventory)
4. [Per-Command Migration Process](#4-per-command-migration-process)
5. [Zero-Downtime Upgrade (ZDU) Compatibility](#5-zero-downtime-upgrade-zdu-compatibility)

---

## 1. Phase-0 Prerequisites

Three prerequisites must land before any command migrates. They are inert
(zero behavior change) but gate every later phase.

**Prerequisite 1 — Legacy to ManagedIndex objectID retrofit:**

During the mixed-mode window, some commands execute on the legacy path
(objectID derived from Ratis log index) and others on the new path (objectID
from ManagedIndex). If the two counters are independent, a legacy operation and
a new operation can mint the same objectID — silent namespace corruption.

Fix: **both** paths draw from the shared `ManagedIndexService` counter during
mixed mode. The legacy `getObjectIdFromTxId` call sites are redirected to
`ManagedIndexService.next()` while keeping the encoding identical (same
`(epoch<<62)|(index<<8)` format, low 8 bits go dead-zero once the 256-window
is retired).

At first finalization the counter is seeded to `max(committed Ratis index) + 1`, ensuring the new-path range starts strictly above every Ratis-derived objectID ever issued — disjoint by construction.

**Prerequisite 2 — Dual-path applied-index durability:**

During mixed mode, both the legacy double-buffer path and the new patch-apply path advance the applied-index concurrently for different commands. A single, monotonic, crash-consistent notion of "last durably applied index" must be maintained that both writers advance correctly. Without this, a crash mid mixed-mode can replay a committed-but-unstamped patch (double-apply) or skip a stamped-but-unflushed legacy batch (lost write).

**Prerequisite 3 — `OMLayoutFeature` finalization gate:**

A new layout feature `LEADER_SIDE_EXECUTION` (next ordinal after `SNAPSHOT_DEFRAG`) gates the binary-level safety of the new path. Until finalization, the new proto envelope and `#MANAGED_INDEX` entry are additive and inert. A new leader must not replicate a patch an old follower cannot apply — finalization is the one-way, cluster-wide gate that ensures uniform binary support.

**These three are interlocked:** Prerequisite 1 makes mixed-mode objectIDs safe; Prerequisite 2 makes mixed-mode crash recovery safe; Prerequisite 3 makes mixed-mode binaries safe.

---

## 2. Hard-First Phasing

Commands are migrated hardest-first. The rationale is risk: if the easy,
high-visibility commands ship first, the hard bits (multi-step FSO, recursive
delete, commutative quota, snapshot Checkpoint ordering) can stall, leaving the
cluster permanently in mixed mode with showstoppers unsolved. Hard-first
front-loads the work whose feasibility is in doubt so that if the design fails,
it fails early while the legacy path is still fully intact.

| Phase | Scope | Depends on |
|-------|-------|-----------|
| **P-0** | Framework substrate (12 components) + 3 prerequisites. Inert. | — |
| **P-1** | Hardest single-step OBS: CreateKey, CommitKey, AllocateBlock, DeleteKey. Commutative quota and SCM allocation first bite here. | P-0 |
| **P-2** | Hardest multi-step FSO: CreateFile/CreateDirectory with implicit parents, FSO delete, recursive `rm -rf` + DirectoryDeletingService redesign. Showstoppers retired. | P-1 |
| **P-3** | Snapshot: CreateSnapshot (Checkpoint op), SnapshotPurge, snapshot moves. | P-2 |
| **P-4** | MPU: Initiate, CommitPart, Complete, Abort, AbortExpired. Large-value concern revisited. | P-3 |
| **P-5** | Batch/background: DeleteKeys, RenameKey/Keys, DeleteOpenKeys, PurgeKeys/Directories. | P-2 |
| **P-6** | Easy sweep: ACLs, tagging, SetTimes, secrets, tokens, tenant, snapshot properties, volume/bucket properties, prepare. | P-1 |
| **P-7** | Cleanup: remove double buffer and table cache, delete legacy path, finalize. | All |

P-5 and P-6 depend on the early hard phases (not the other way around) so
the easy work cannot be used to declare premature victory.

---

## 3. Full Operation Inventory

All ~47 write operations grouped by migration difficulty:

### Group C — migration-hard (17 operations, phases P-1 through P-4)

| Operation | Tables | Quota? | SCM? | Multi-step? | Phase |
|-----------|--------|--------|------|-------------|-------|
| CreateKey (OBS/FSO) | openKeyTable, directoryTable | usage | yes | FSO: implicit parents | P-1/P-2 |
| CommitKey (OBS/FSO) | keyTable, openKeyTable, deletedTable | usage | no | no | P-1/P-2 |
| AllocateBlock (OBS/FSO) | openKeyTable | no | yes | no | P-1/P-2 |
| DeleteKey (OBS/FSO) | keyTable, deletedTable | usage | no | FSO recursive | P-1/P-2 |
| CreateFile (FSO) | openKeyTable, directoryTable | usage | yes | yes (mkdir-p) | P-2 |
| CreateDirectory (FSO) | directoryTable | usage | no | yes (mkdir-p) | P-2 |
| PurgeDirectories | directoryTable, fileTable, deletedDirTable | usage (lazy) | no | yes (background) | P-2 |
| CreateSnapshot | snapshotInfoTable + RocksDB Checkpoint | no | no | no (barrier) | P-3 |
| SnapshotPurge | snapshotInfoTable | no | no | no | P-3 |
| SnapshotMoveDeletedKeys | deletedTable across snapshots | usage | no | no | P-3 |
| SnapshotMoveTableKeys | deletedTable across snapshots | usage | no | no | P-3 |
| InitiateMultiPartUpload | openKeyTable, multipartInfoTable | no | no | no | P-4 |
| CommitMultiPartUpload | multipartInfoTable, openKeyTable, deletedTable | usage | no | no | P-4 |
| CompleteMultiPartUpload | keyTable, openKeyTable, multipartInfoTable | usage | no | yes (assembly) | P-4 |
| AbortMultiPartUpload | openKeyTable, multipartInfoTable, deletedTable | usage | no | no | P-4 |
| AbortExpiredMultiPartUploads | openKeyTable, multipartInfoTable, deletedTable | usage | no | yes (batch) | P-4 |

### Group B — migration-core (8 operations, phases P-1/P-5)

| Operation | Tables | Quota? | Multi-step? | Phase |
|-----------|--------|--------|-------------|-------|
| DeleteKeys (batch) | keyTable, deletedTable | usage | yes (N keys) | P-5 |
| RenameKey | keyTable / directoryTable | no | no | P-5 |
| RenameKeys (batch) | keyTable | no | yes (N renames) | P-5 |
| DeleteOpenKeys | openKeyTable, deletedTable | usage | yes (batch) | P-5 |
| PurgeKeys | deletedTable | no | yes (batch) | P-5 |
| CreateBucket | bucketTable, volumeTable | usage (volume) | no | P-1 |
| DeleteBucket | bucketTable, volumeTable | usage (volume) | no | P-1 |
| SetBucketProperty | bucketTable | limit only | no | P-6 |

### Group A — migration-simple (~22 operations, phase P-6)

All share the shape: single column family, no quota usage, no SCM, single
transition, whole-row Put/Delete, idempotent on re-execution.

Includes: CreateVolume, SetVolumeProperty, DeleteVolume, all ACL operations
(Add/Remove/Set for volume/bucket/key/prefix), delegation token operations,
S3 secret operations, tenant operations, SetTimes, PutObjectTagging,
DeleteObjectTagging, RenameSnapshot, SetSnapshotProperty, Prepare,
CancelPrepare, FinalizeUpgrade, RecoverLease, QuotaRepair.

---

## 4. Per-Command Migration Process

Each command migration is a self-contained, independently revertible unit:

1. **One `PlannedRequest` subclass** — re-expresses the legacy
   `validateAndUpdateCache` body as plan-on-leader. The legacy subclass stays
   in the tree untouched as the fallback.
2. **A per-command runtime flag** — one config key (default: legacy/off),
   wired into the router that chooses legacy vs planned per command type.
   Default-off means landing the subclass changes nothing observable until an
   operator opts in.
3. **Tests** — linearizability harness scenarios, per-command unit tests,
   determinism test (leader-emitted patch applied on follower yields
   byte-identical DB state), and flag-routing test proving both paths produce
   identical on-disk results.
4. **Acceptance gate** — the phase does not advance until its tests are green,
   flag-routing equivalence holds, and (for performance-sensitive phases) the
   benchmark gate clears.

**Dual activation gate:** A command runs on the planned path only when BOTH
the `OMLayoutFeature.LEADER_SIDE_EXECUTION` is finalized AND the per-command
runtime flag is enabled. Finalization is the binary-safety gate; the flag is
the operational revert knob. Both must be true.

---

## 5. Zero-Downtime Upgrade (ZDU) Compatibility

The Ozone cluster supports Zero-Downtime Upgrade (ZDU) where nodes are
upgraded one at a time while the cluster stays online (see HDDS-14498). During a
rolling upgrade, different OMs may run different software versions at the same
time.

**How leader-planned execution works with ZDU:**

The planned execution path requires two gates:

1. **OMLayoutFeature.LEADER_SIDE_EXECUTION is finalized** — the binary-safety
   gate. Guarantees no un-upgraded follower receives a planned patch it cannot
   apply.
2. **Per-command runtime flag is enabled** — the operational revert knob.
   Defaults to legacy (off). Operators opt commands in after finalization.

The routing decision in `OzoneManagerStateMachine`:

```java
boolean isPlannedPath(Type cmdType) {
  if (!versionManager.isAllowed(OMLayoutFeature.LEADER_SIDE_EXECUTION)) {
    return false;  // pre-finalized: use legacy path
  }
  if (!perCommandFlagEnabled(cmdType)) {
    return false;  // flag not set: use legacy path
  }
  return plannedRequestCreators.containsKey(cmdType);
}
```

**Version lifecycle for removing the legacy path:**

| Version | Behavior |
|---------|----------|
| **Vn** (introduces planned execution) | Both paths exist. Pre-finalized: legacy. Post-finalized + flag on: planned. |
| **Vn+1** | Legacy path still present for pre-finalization and as the flag-off fallback. |
| **Vn+2** | Legacy path removed. ZDU from <= Vn to >= Vn+2 must pass through Vn+1. |

**Why this works safely:**

- The ZDU design guarantees all OMs run the same software version at
  finalization time.
- The new proto envelope and `#MANAGED_INDEX` entry are additive and inert
  until finalization — old software ignores them.
- The per-command flag defaults to legacy, so a freshly finalized cluster still
  runs legacy until an operator opts commands in.
- Finalization alone is not sufficient to activate the new path — this is what
  gives operators a no-downtime, no-downgrade revert.

**Non-rolling upgrade path:**

For clusters that accept downtime: stop all OMs, upgrade, start, finalize,
enable flags. The legacy path is never used post-finalization.

---
title: S3-compatible Object Versioning
summary: Bucket-level, S3-compatible object versioning with O(1) version writes and built-in reclamation
date: 2026-07-21
jira: HDDS-15728
status: accepted
author: Symious
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

# Summary

Add S3-compatible object versioning to Ozone: the full three-state bucket state
machine (Unversioned / Enabled / Suspended), per-key version chains with delete
markers and null versions, the S3 versioning APIs on the S3 Gateway, and built-in
version reclamation (`maxVersions`, background expiration) — with O(1) metadata
cost per version operation and zero regression on non-versioned paths.

# Status

Defined in the markdown header.

# Problem statement (Motivation / Abstract)

Amazon S3 provides bucket-level object versioning: a single key can retain multiple
versions, so users can recover objects that were accidentally overwritten or
deleted. A large part of the S3 ecosystem (backup software, data lake components,
DR tooling) depends on the versioning APIs (`PutBucketVersioning`,
`ListObjectVersions`, object operations with a `versionId`). Ozone exposes an
S3-compatible API through the S3 Gateway but does not support object versioning
today: the bucket-level `isVersionEnabled` boolean cannot express the Suspended
state, `OmKeyInfo.keyLocationVersions` tracks block locations within one record
rather than object versions, and the gateway has no versioning endpoints.

This proposal implements versioning with S3-compatible semantics, usable by
standard S3 clients (AWS CLI / SDKs) without modification. The metadata cost of a
version operation is decoupled from the number of versions (one extra small KV
write per operation), and reclamation controls are built into the feature itself
to avoid the unbounded version accumulation problems commonly seen on S3 (the S3
troubleshooting guide documents list degradation and throttling on keys with
millions of versions, and leaves the fix to user-configured Lifecycle rules that
are often forgotten).

# Non-goals

- **MFA delete** — depends on the AWS IAM/MFA device ecosystem; Ozone has no
  counterpart infrastructure. The `MfaDelete` field of `PutBucketVersioning`
  returns NotImplemented.
- **A full S3 Lifecycle rule engine** — only the minimal reclamation capabilities
  that versioning itself requires are included.
- **Version-aware cross-cluster replication.**
- **Versioning for FSO / LEGACY bucket layouts** — the first version supports
  OBJECT_STORE buckets only; enabling versioning on other layouts returns
  NotImplemented. Combining FSO's directory/rename semantics with per-key version
  chains is disproportionately complex, and S3 tooling scenarios essentially use
  the OBS layout. FSO support can be evaluated as an independent follow-up.
- **Coexistence with Ozone snapshots on the same bucket** — the version-aware
  reclamation the combination needs is deferred past the first version, so OM
  rejects the combination outright rather than leaving it to convention. The
  snapshot section below states the enforcement and what lifts it.

# Technical Description (Architecture and implementation details)

## Bucket state machine

```
Unversioned (default) ──enable──▶ Enabled ◀──enable── Suspended
                                     │                     ▲
                                     └──────suspend────────┘
        (Once Enabled, a bucket can never return to Unversioned)
```

A `BucketVersioningStatusProto` enum (`UNVERSIONED` / `VERSIONING_ENABLED` /
`VERSIONING_SUSPENDED`) is added as an optional field on `BucketInfo` and
`BucketArgs`. The legacy `isVersionEnabled` boolean is kept and maintained in
two-way sync (`ENABLED → true`, otherwise `false`; records without the enum are
interpreted via the boolean), so old and new clients/OMs coexist during rolling
upgrades. OM enforces the state machine on `SetBucketProperty`: transitions back
to `UNVERSIONED` are rejected with `INVALID_REQUEST`, preserving S3's
data-protection promise that no single state change can silently destroy
historical versions.

## Metadata layout: keyTable (current) + versionedKeyTable (noncurrent)

A new column family, **versionedKeyTable**, splits responsibilities with keyTable:

- **keyTable** (existing, semantics unchanged) always holds each key's **current
  version** — a regular object or a delete marker. Plain GET / HEAD / ListObjects
  read paths are unchanged.
- **versionedKeyTable** (new) holds all **noncurrent** versions (including
  noncurrent delete markers), each as a complete `OmKeyInfo`. The RocksDB key is

  ```
  /{volume}/{bucket}/{keyName}\x00{Long.MAX_VALUE - versionId}
  ```

  (fixed-width hex suffix; the separator is `0x00` rather than `/` because OBS key
  names contain `/` verbatim, which would interleave a key's versions with those of
  keys nested under it), so all versions of a key are physically adjacent and
  ordered newest to oldest: `ListObjectVersions` and version promotion are a
  single seek plus a sequential read. The table is registered in
  `OmMetadataManagerImpl.getTableBucketPrefix` alongside the other key tables, so
  bucket-prefixed iteration and SST filtering resolve its prefix.

`OmKeyInfo` gains three optional proto fields (old records deserialize
compatibly): `versionId` (int64, assigned once at version creation, then frozen),
`isDeleteMarker` (a marker is a record with this flag and no data blocks — no
datanode storage), and `isNullVersion` (the single overwritable "null version"
slot per key). Keys written before versioning was enabled are interpreted as null
versions on read — **zero migration**, matching S3's "enabling versioning does
not change existing objects".

Every keyTable ↔ versionedKeyTable update rides OM's existing atomic
multi-table WriteBatch commit (the same double-buffer pattern used today for
keyTable + deletedTable on overwrite): no new transaction mechanism and no
cross-table consistency problem. The new column family is introduced under the OM
layout feature / finalization framework (`OMLayoutFeature.OBJECT_VERSIONING`):
before finalization, requests carrying a versioning status are rejected.

## VersionId: a pluggable generator

`versionId` generation is abstracted behind a `VersionIdGenerator` interface,
chosen per cluster by class name (`ozone.om.versioning.version-id-generator`), so
a deployment can plug in its own. The generator is cluster-wide and may be changed
on a running cluster; it is not recorded in bucket metadata. Every generator must
satisfy, **for itself**: strictly increasing within a key (a later version's id is
always greater than every earlier version's id of that key); frozen once assigned;
`0` and `1` never handed out (`1` is the first-version sentinel).

That guarantee binds one generator, not a sequence of them, so the write path
enforces it at commit: a commit whose id does not come after the key's current
version is rejected (`INVALID_REQUEST`), and the operator deletes the key's
versions before writing under the new generator. The check costs no read in the
steady state — the current version holds the key's largest id, so an id above it
cannot be taken; only a record predating versioning, which carries no id to order
against, falls back to a versionedKeyTable lookup.

- **`TransactionIndexVersionIdGenerator` (default)** — the OM Ratis transaction
  index of the committing transaction, used directly. No allocator state of any
  kind. Externally encoded as an opaque URL-safe string. objectID's epoch bits are
  deliberately not applied: dropping them keeps versionIds small positive longs, so
  the versionedKeyTable ordering stays plain signed arithmetic, and uniqueness
  rests on the monotonicity of the Ratis log index — a log that was reset or rolled
  back is caught by the commit-time check rather than silently accepted.
- **`PinnedFirstVersionIdGenerator` (opt-in)** — only the first version is
  special: it takes the reserved sentinel `FIRST_VERSION_ID = 1`, below any usable
  transaction index, so it sorts oldest and can be referenced without listing the
  key's versions first. All later versions are the commit transaction's index; no
  persistent allocator. First versions are detected by "no current version in
  keyTable", reusing the lookup the write path performs anyway. Known trade-off: if
  every version of a key is permanently deleted and the key is recreated, the new
  first version takes the sentinel again; only deployments that accept this should
  configure this generator.

How a versionId is rendered on the wire — the opaque encoding, and whether the
pinned first version is presented as a fixed literal or derived from the keyName —
is part of the `?versionId=` read path (T4), not of generation.

The null version is not a special ID value but the `isNullVersion` attribute — it
carries a normally generated id like any other version, since a null created
between two versioned writes is not the oldest one;
`versionId=null` requests resolve to "locate this key's null slot".

## Request handling

- **PUT (Enabled)**, atomic in one WriteBatch: move the current version (if any)
  into versionedKeyTable; write the new record into keyTable as current.
- **PUT (Suspended)**: the new record takes the null slot and becomes the current
  version — if the null slot is already the current version, overwrite it in place;
  otherwise move the current version into versionedKeyTable, write the new record
  into keyTable as current, and delete the old null record (if any) from
  versionedKeyTable, blocks to deletedTable in both cases. Versions accumulated
  while Enabled are unaffected. **PUT (Unversioned)**: unchanged from today.
- **GET/HEAD**: without versionId, read keyTable; a current delete marker returns
  404 with `x-amz-delete-marker: true`. With versionId, check the current version
  first, then point-look-up versionedKeyTable; if the addressed version is a delete
  marker, current or not, the gateway returns 405 with `x-amz-delete-marker: true`
  and `Allow: DELETE`.
- **DELETE without versionId (Enabled)**: move current into versionedKeyTable and
  write a delete marker as the new current. (Suspended: the marker takes the null
  slot exactly as a suspended PUT does.) **DELETE ?versionId=x**: permanently
  delete that version (blocks to deletedTable); if it was current, trigger version
  promotion.
- **Version promotion** — the invariant is that keyTable always holds a key's
  current version. When a permanent delete removes the current version, one
  `seek` on the key's versionedKeyTable prefix yields the newest noncurrent
  version (reverse ordering), which is moved back into keyTable unchanged — a
  pure positional move; the record's content stays frozen. If no noncurrent
  version remains, the key disappears entirely. Executed in the same WriteBatch
  as the delete, under the bucket write lock. Deleting a delete marker this way
  is exactly the S3 "restore an object" flow.
- **ListObjects**: scans keyTable only, skipping keys whose current version is a
  marker — list scan volume is not amplified by versions (an advantage over S3's
  single-namespace implementation). **ListObjectVersions**: merges keyTable and
  versionedKeyTable in key order (both are key-name prefixed, so the merge is
  naturally ordered), with `key-marker` / `version-id-marker` pagination.

## Reclamation, quota, observability

Built-in bucket-level controls: `maxVersions` (default 100, 0 = unlimited; markers
count toward the limit), `noncurrentVersionExpiration` (opt-in), and
expired-delete-marker cleanup (enabled by default: when only a marker remains,
the whole key is removed) — all enforced by a new **VersionCleanupService**
following the `KeyDeletingService` pattern. Versions beyond `maxVersions` are
reclaimed oldest-first, asynchronously rather than inside the write transaction,
so the cost of a version write does not grow with the number of versions the key
already has. An earlier draft also offered a `REJECT` policy that failed the
write instead; it is dropped. S3 has no error code for the condition and no
client retries on it, so a default limit enforced that way would break unmodified
S3 tooling after `maxVersions` overwrites of one key — and reclaiming is what
S3's own lifecycle rules do. Expiration counts from the moment the version that
superseded this one was committed, as Lifecycle's `NoncurrentDays` does, not from
when the version itself was written. All versions count against the bucket space quota;
version count, marker count, and noncurrent bytes are exposed via
`ozone sh bucket info` and Recon. `maxVersions` is documented as an Ozone
extension over S3 semantics (comparable to Lifecycle `NewerNoncurrentVersions`).

## Interaction with Ozone snapshots

**The first version does not allow both features on the same bucket.** The
version-aware reclamation described in this section is deferred past it, and
without that lookup the gap below is a data-loss path rather than a rough edge,
so OM enforces the exclusion instead of documenting an expectation: snapshot
creation is rejected on an ENABLED or SUSPENDED bucket, and enabling versioning
or moving SUSPENDED → ENABLED is rejected while any snapshot exists in the
bucket's path chain, following linked buckets to their source. SUSPENDED is
covered along with ENABLED because a suspended bucket still holds noncurrent
versions. ENABLED → SUSPENDED stays allowed even with snapshots present: it only
reduces exposure, and a client must always be able to stop creating versions. So
is setting the status a bucket already has, since an S3 client re-sending
`PutBucketVersioning` must not fail against a bucket already in that state.
Neither check is latched — once the last snapshot is purged, versioning can be
enabled or resumed normally. Rejections return NOT_SUPPORTED_OPERATION with an
explanatory message.

The checks are exhaustive rather than best-effort, so a mixed bucket cannot
arise on a released cluster: versioning is gated on `OMLayoutFeature`
finalization, an OM that predates the feature cannot set a versioning status at
all, and the two checks are enforced when the transaction applies rather than in
preExecute, so Ratis orders them against each other and neither can slip past
the other. There is correspondingly **no migration path and no legacy mixed
state to be conservative about** — an earlier draft of this document assumed one.

The single exception is an OM config key, false by default and documented as
unsafe, which lifts both checks so that integration tests for the work described
below can be written against a real mixed bucket. On a cluster running with that
key, reclamation is *not* snapshot-aware and the data-loss path below is live;
that is the point of marking it unsafe. The rest of this section is the design
that lifts the restriction for good.

A snapshot checkpoints the entire OM RocksDB, so versionedKeyTable is captured
automatically along with the bucket's versioning status: a snapshot holds the
complete version history as of its creation. Snapshot creation and deletion need
no change — `SnapshotDeletingService` moves only deletedTable, deletedDirTable
and snapshotRenamedTable entries between snapshots, and permanently deleted
versions travel through deletedTable, the path already used when an overwrite
reclaims the previous blocks. Multiple versions of one key queued for deletion
share a deletedTable entry, which is a `RepeatedOmKeyInfo` list evaluated one
record at a time, so no new structure is required there either.

What does change is **block reclamation**. Snapshots share physical blocks with
the active object store, so a snapshot keeps its data only because
`KeyDeletingService` refuses to reclaim blocks that a previous snapshot still
references. That decision is made per deletedTable record by
`ReclaimableKeyFilter`, which resolves the key's path in the previous snapshot
and looks it up **in keyTable only**
(`KeyManagerImpl.getPreviousSnapshotOzoneKeyInfo`), then compares objectID and
block locations. With versioning a key's live records span two tables, so the
lookup misses: permanently deleting a noncurrent version resolves to whatever is
current in the previous snapshot — a different record with different blocks —
which reads as "not present in the previous snapshot", i.e. reclaimable. Blocks
a snapshot still points at would be deleted. objectID does not rescue the
comparison either way: `prepareFileInfo` keeps it stable across a key's versions,
so it matches while the block lists do not.

The fix is to make that lookup version-aware. A record carrying a `versionId`
resolves to the exact dbKey `/{volume}/{bucket}/{keyName}\x00{MAX - versionId}` in
the previous snapshot's versionedKeyTable, falling back to keyTable when that
version was current there. Because versionId is frozen at creation and never
reused, this is an exact point lookup that *replaces* — rather than extends — the
objectID-plus-block-list heuristic for versioned buckets. The same lookup feeds
`calculateExclusiveSize`, keeping snapshot exclusive-size reporting correct for
noncurrent versions.

This makes deletedTable the single choke point for version reclamation: every
path that removes a version permanently — `DELETE ?versionId=`, the suspended
null-slot overwrite, and VersionCleanupService — writes the record to deletedTable
and lets KeyDeletingService apply the snapshot-aware filter. None of them may
reclaim blocks directly.

Snapshot diff remains a **current-version diff**; versionedKeyTable is
deliberately not added to it. keyTable still holds exactly one record per key, so
diff cost and semantics do not change as versions accumulate. One rule does need
stating: **a key whose current version is a delete marker counts as absent**.
Without it a delete surfaces as a MODIFY (if the marker carries the key's
objectID) or as a DELETE plus a phantom CREATE (if it carries a new one), instead
of the DELETE users expect; restoring an object by removing its marker reports
CREATE correspondingly.

Reads inside a snapshot go through `OmSnapshot`, which implements
`IOmMetadataReader`, whose `lookupKey(OmKeyArgs)` has no version dimension.
`?versionId=` is plumbed through it so versions retained in a snapshot are
actually readable; `ListObjectVersions` against a snapshot is out of scope for
the first version.

## S3 Gateway and native API surface

New endpoints: `PutBucketVersioning`, `GetBucketVersioning`,
`ListObjectVersions`. Extended: GET/HEAD/DELETE with `?versionId=` (including the
literal `null`), `x-amz-version-id` / `x-amz-delete-marker` response headers,
per-entry version semantics in batch `DeleteObjects`, `CopyObject` versioning
behavior, and version-id headers on `PutObject` / `CompleteMultipartUpload`. The
native interfaces (`ozone sh`, `OzoneBucket` API) are extended in parallel so
versions are manageable outside the S3 path.

## Compatibility and upgrade

Wire protocol changes are limited to optional proto fields, so old records and
old clients are unaffected. The new column family and request surface are gated
by `OMLayoutFeature.OBJECT_VERSIONING`; before cluster finalization, enabling
versioning is rejected with `NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION`.
Snapshots taken before the feature is used are unaffected, and snapshot
interaction is covered above. Buckets without versioning behave exactly as today
(verified by benchmarks).

# Alternatives

- **Single-record layout** (versions embedded in the existing
  `keyLocationVersions` list): every PUT/DELETE becomes a full read-modify-write
  of one growing RocksDB value — write amplification linear in the version count
  (hundreds of KB per write at ~100 versions of a large key), degraded
  compaction, and no per-version object metadata (size / mtime / ETag) without
  rebuilding an object record inside the location structure. Rejected in favor of
  the two-table design, where each version operation writes one small KV (O(1))
  and each version is a complete `OmKeyInfo`.
- **A two-state model** (dropping Suspended): "disabled after being enabled" must
  retain historical versions anyway — otherwise a single state change silently
  destroys data — yet the bucket would present itself as never-versioned and
  `GetBucketVersioning` could not answer `Suspended`. Since the semantics are
  unavoidable, keep the standard three states and full S3 compatibility.
- **Reusing `objectID` as the version identity**: code verification showed the
  overwrite path (`OMKeyRequest.prepareFileInfo`) reuses the existing record via
  `toBuilder()`, so objectID is intentionally stable across overwrites — snapshot
  diff relies on it to distinguish "modified" from "deleted + recreated".
  **Reusing `updateID`**: hsync re-commits the same version repeatedly, so it
  drifts, while a version identity must be frozen at creation. Hence the
  dedicated `versionId` field.
- **S3-style random version IDs**: equivalent for reuse-prevention, but they
  destroy the temporal clustering of versionedKeyTable — version promotion would
  degrade from one seek to a scan-and-sort (with mtime tiebreak problems), and
  `GET ?versionId=` from a point lookup to a prefix scan. Unpredictability, the
  only remaining benefit, is provided by the external encoding layer instead.
- **A per-key sequential counter** for readable first versions: requires a
  persistent allocator and reuses IDs after full deletion of a key. Replaced by the
  pinned-first generator, which special-cases only the first version and needs no
  allocator.
- **A version-level snapshot diff** (reporting noncurrent versions created or
  removed between two snapshots): the diff is keyed by objectID, which a key's
  versions share, so it would need a different identity and a version dimension
  in the report; versionedKeyTable would also have to join
  `RocksDBCheckpointDiffer.COLUMN_FAMILIES_TO_TRACK_IN_DAG`, paying
  compaction-log and SST retention cost for information already reachable through
  the snapshot's own version listing. Deferred; the diff stays a current-version
  diff with an explicit delete-marker rule.
- **Pinning the generator into bucket metadata** (so a bucket keeps the generator
  it was created with): removes the risk of a mid-life generator change, but adds a
  proto field, a write path in both bucket create and set-property, and leaves the
  generator un-changeable even when an operator legitimately wants to switch.
  Replaced by the commit-time ordering check, which turns the risk into a loud,
  per-key failure with a clear remedy and costs no persistent state.

# Plan

Implemented as one umbrella Jira with eleven tasks (36 sub-tasks, each roughly one
PR), in dependency order `T1 → T2 → T3 → T4 → T5 → T6 → T7 → (T8 ∥ T9) → T10 → T11`
(reclamation and the snapshot exclusion both land before the S3 endpoints, so
versioning is never exposed without a way to reclaim versions, nor with snapshots
left unguarded). The snapshot interaction described above is deferred past this
first phase: none of the other tasks carry it, and T7 enforces the exclusion in OM
until it lands.

| Task | Scope |
|---|---|
| T1 Metadata foundation | proto three-state enum + legacy-boolean sync, set-property state machine, `OmKeyInfo` version fields, versionedKeyTable column family, layout feature gate |
| T2 VersionId generator framework | `VersionIdGenerator` interface with class-name configuration, transaction-index default, commit-time ordering check, pinned-first generator |
| T3 ENABLED write paths | PUT two-table update, DELETE marker insertion, quota accounting |
| T4 Read / permanent delete / promotion | `?versionId=` reads including null-slot addressing, reporting a delete-marker-addressed read as a condition distinct from not-found; permanent delete by versionId with quota accounting; version promotion |
| T5 SUSPENDED semantics | null-slot overwrite, null markers, zero-migration legacy keys |
| T6 Reclamation | VersionCleanupService, `maxVersions` trimming oldest-first, noncurrent expiration, expired marker cleanup |
| T7 Snapshot exclusion | OM-side rejection matrix for snapshot creation and versioning state transitions, applied to the source of a linked bucket and enforced at apply time so the two directions cannot race; dev-only opt-in config key |
| T8 S3 Gateway endpoints | bucket versioning endpoints, object `versionId` support, batch delete |
| T9 ListObjectVersions | OM merged listing, protocol plumbing, gateway `?versions` |
| T10 Quota and observability | quota edges + QuotaRepair, Recon / metrics |
| T11 Wrap-up | upgrade validation, robot tests, benchmarks, docs |

Testing follows three tracks: unit/integration tests per sub-task acceptance
criteria (state machine, two-table atomicity, promotion, null-slot semantics,
`maxVersions` boundaries, the full snapshot-exclusion rejection matrix); S3
compatibility via the smoketest/s3 robot suite and the versioning subset of
ceph/s3-tests; performance benchmarks asserting no regression with versioning off
and O(1) write latency with it on. The snapshot
integration tests belong with the snapshot work and are deferred with it.

Open questions tracked for implementation: whether an operator should be able to
cap `maxVersions` cluster-wide, over and above the per-bucket setting and the
cluster default; obfuscation of the external versionId encoding, and how a pinned first version
is rendered; whether changing `ozone.om.versioning.version-id-generator` should take
effect without an OM restart, and the operational procedure for keys already written
under the previous generator; `PutBucketVersioning(Suspended)` on a never-versioned
bucket (align via s3-tests); whether `ListObjectVersions` against a snapshot is
worth adding once the feature has landed; interaction with hsync/append writes
(appends apply to the current version and create no new one);
multipart uploads — the version is created at `CompleteMultipartUpload` commit and
parts stay invisible, but until that lands an MPU overwrite on a versioned bucket
neither reclaims the previous version's blocks nor records a version for them, so
those blocks leak; this has to be closed before multipart is declared supported.

# References

- [AWS S3 Versioning User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
- [AWS S3 troubleshooting: performance degradation with many versions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/troubleshooting-by-symptom.html)
- [ceph/s3-tests](https://github.com/ceph/s3-tests) — versioning test subset


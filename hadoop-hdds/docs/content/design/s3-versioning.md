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
  /{volume}/{bucket}/{keyName}/{Long.MAX_VALUE - versionId}
  ```

  (fixed-width hex suffix), so all versions of a key are physically adjacent and
  ordered newest to oldest: `ListObjectVersions` and version promotion are a
  single seek plus a sequential read.

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
`0` reserved for the null slot and `1` for the first-version sentinel.

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

The null version is not a special ID value but the `isNullVersion` attribute;
`versionId=null` requests resolve to "locate this key's null slot".

## Request handling

- **PUT (Enabled)**, atomic in one WriteBatch: move the current version (if any)
  into versionedKeyTable; write the new record into keyTable as current; if
  `maxVersions` is exceeded, trim the oldest noncurrent version in the same batch
  (its blocks go to deletedTable).
- **PUT (Suspended)**: locate the null slot (current, or an `isNullVersion`
  record in versionedKeyTable) and replace it; versions accumulated while Enabled
  are unaffected. **PUT (Unversioned)**: unchanged from today.
- **GET/HEAD**: without versionId, read keyTable; a current delete marker returns
  404 with `x-amz-delete-marker: true`. With versionId, check the current version
  first, then point-look-up versionedKeyTable.
- **DELETE without versionId (Enabled)**: move current into versionedKeyTable and
  write a delete marker as the new current. (Suspended: the marker overwrites the
  null slot.) **DELETE ?versionId=x**: permanently delete that version (blocks to
  deletedTable); if it was current, trigger version promotion.
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

Built-in bucket-level controls: `maxVersions` (default 100, 0 = unlimited;
synchronous trim inside the write transaction; markers count toward the limit),
`noncurrentVersionExpiration` (opt-in, enforced by a new
**VersionCleanupService** following the `KeyDeletingService` pattern), and
expired-delete-marker cleanup (enabled by default: when only a marker remains,
the whole key is removed). All versions count against the bucket space quota;
version count, marker count, and noncurrent bytes are exposed via
`ozone sh bucket info` and Recon. `maxVersions` is documented as an Ozone
extension over S3 semantics (comparable to Lifecycle `NewerNoncurrentVersions`).

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
versioning is rejected with `NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION`. Ozone
snapshots checkpoint the whole DB, so versionedKeyTable is included
automatically; snapshot diff must add it to its tracked table set. Buckets
without versioning behave exactly as today (verified by benchmarks).

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
- **Pinning the generator into bucket metadata** (so a bucket keeps the generator
  it was created with): removes the risk of a mid-life generator change, but adds a
  proto field, a write path in both bucket create and set-property, and leaves the
  generator un-changeable even when an operator legitimately wants to switch.
  Replaced by the commit-time ordering check, which turns the risk into a loud,
  per-key failure with a clear remedy and costs no persistent state.

# Plan

Implemented as one umbrella Jira with ten tasks (33 sub-tasks, each roughly one
PR), in dependency order `T1 → T2 → T3 → T4 → T5 → (T6 ∥ T7) → (T8 ∥ T9) → T10`:

| Task | Scope |
|---|---|
| T1 Metadata foundation | proto three-state enum + legacy-boolean sync, set-property state machine, `OmKeyInfo` version fields, versionedKeyTable column family, layout feature gate |
| T2 VersionId generator framework | `VersionIdGenerator` interface with class-name configuration, transaction-index default, commit-time ordering check, pinned-first generator |
| T3 ENABLED write paths | PUT two-table update, DELETE marker insertion, quota accounting |
| T4 Read / permanent delete / promotion | `?versionId=` reads, permanent delete, version promotion |
| T5 SUSPENDED semantics | null-slot overwrite, null markers, zero-migration legacy keys |
| T6 S3 Gateway endpoints | bucket versioning endpoints, object `versionId` support, batch delete |
| T7 ListObjectVersions | OM merged listing, protocol plumbing, gateway `?versions` |
| T8 Reclamation | `maxVersions` trim, VersionCleanupService, expired marker cleanup |
| T9 Quota and observability | quota edges + QuotaRepair, Recon / metrics |
| T10 Wrap-up | upgrade validation, snapshot diff adaptation, robot tests, benchmarks, docs |

Testing follows three tracks: unit/integration tests per sub-task acceptance
criteria (state machine, two-table atomicity, promotion, null-slot semantics,
`maxVersions` boundaries); S3 compatibility via the smoketest/s3 robot suite and
the versioning subset of ceph/s3-tests; performance benchmarks asserting no
regression with versioning off and O(1) write latency with it on.

Open questions tracked for implementation: the `maxVersions` default and cluster
cap; obfuscation of the external versionId encoding, and how a pinned first version
is rendered; whether changing `ozone.om.versioning.version-id-generator` should take
effect without an OM restart, and the operational procedure for keys already written
under the previous generator; `PutBucketVersioning(Suspended)` on a never-versioned
bucket (align via s3-tests); the snapshot diff change surface; interaction with
hsync/append writes (appends apply to the current version and create no new one);
multipart uploads (the version is created at `CompleteMultipartUpload` commit; parts
are invisible).

# References

- [AWS S3 Versioning User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
- [AWS S3 troubleshooting: performance degradation with many versions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/troubleshooting-by-symptom.html)
- [ceph/s3-tests](https://github.com/ceph/s3-tests) — versioning test subset


---
title: Server Side CopyKey with Shared Block Groups
summary: Metadata-only S3 CopyObject that shares the source key's blocks, and the reclaim rule that keeps it safe
date: 2026-08-28
status: draft
author: Lixucheng
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

# Server Side CopyKey with Shared Block Groups

## Summary

S3 `CopyObject` in Ozone reads every byte of the source key from the datanodes
through the S3 Gateway and writes it back as a new key, so copy latency is
proportional to object size. This document describes a proof of concept for an
OM-side `CopyKey` request that creates the destination key from the source key's
committed block list, moving no data at all, together with the reclaim rule that
prevents the shared blocks from being freed while either key is still alive.

The prior art is [HDDS-569](https://issues.apache.org/jira/browse/HDDS-569),
open since 2018, which proposed exactly this and states the copy "in ozone
world, this is just a metadata change". Its two attached patches only define
proto messages; no implementation was ever written, and the block reclaim
problem below was never addressed. That problem is the reason this needs a
design rather than a patch.

## The problem with sharing blocks

Committed blocks in Ozone are owned by exactly one key. There is no reference
counting anywhere in OM or SCM. Deleting a key moves its whole block list into
`deletedTable` in the same batch that removes the `keyTable` row
(`AbstractOMKeyDeleteResponse.addDeletionToBatch`), `KeyDeletingService` hands
those blocks to SCM, and the datanodes free the chunks. Nothing on that path
asks whether another key still needs them.

The one existing protection, `OMKeyRequest.filterOutBlocksStillInUse`, only
compares against the new version of the *same* key, and the comment above its
call site in `OMKeyCommitRequest` says plainly that shared blocks reaching
`deletedTable` cause data loss. Worse, a datanode asked to delete an already
deleted block succeeds quietly (`BlockDeletingTask` treats it as unreferenced),
so the failure is silent: the surviving key simply stops being readable.

Ozone does already tolerate several keys' worth of metadata pointing at one set
of blocks — hsync keeps an open-key twin alongside the committed key, snapshots
hold blocks a deleted key no longer needs, and completing a multipart upload
re-parents each part's blocks into the final key without copying data. None of
those is a general mechanism: the first two are identity checks on the same
logical key, and the third is a one-way ownership transfer.

## Design

### Lineage tag plus a per-group sharer count

Each key gains an optional `sharedBlockGroupId` (`KeyInfo` field 23). Zero, the
default for every existing key, means the key owns its blocks exclusively —
so no database needs backfilling. A non-zero value is the `objectID` of the
lineage root, the first key that was ever copied in that chain, and copies of
copies join the group their source already belongs to.

A new column family, `sharedBlockGroupTable`, maps that group id to the number
of keys currently sharing it. It holds **one row per copy lineage, not one per
block**: copying a key with ten thousand blocks costs a single row. A row exists
only while more than one key shares the group, so a count that falls back to one
deletes the row and the surviving key is indistinguishable from a key that was
never copied.

This is deliberately not a per-block reference count. A refcount would have to
be maintained by every producer of block releases — plain delete, bulk delete,
commit overwrite, multipart complete, FSO directory purge, open key cleanup, the
uncommitted-block pseudo key path — and missing one of them is silent data loss.
The design below changes **no producer at all**.

### Copy

`OMKeyCopyRequest` runs under the destination bucket's write lock and:

1. loads the source from `keyTable` and rejects anything ineligible (see below),
2. derives `G` from the source's tag or, on a first copy, from its `objectID`,
3. builds the destination `OmKeyInfo` from a fresh builder, copying the source's
   block list, replication config, ACLs and metadata but taking a new `objectID`
   and the request's `updateID`, destination name and modification time. It has
   to be a fresh builder rather than `sourceKeyInfo.toBuilder()`, because
   `WithObjectID.Builder.validate` refuses to change a non-zero `objectID`: an
   object's identity is immutable, and a copy is a new object that happens to
   point at the same blocks. The stored ETag is carried over because it
   describes content, which a copy reproduces exactly,
4. tags the source too, on its first copy, so its own deletion consults the
   count,
5. raises the group's sharer count, taking an absent row straight to two.

Everything lands in one `OMKeyCopyResponse` batch, so a key never becomes
visible without the count that protects its blocks.

Quota charges the copy its full logical size. The physical sharing is invisible
to quota, which stays internally consistent because deleting either key returns
the same amount. Bucket `usedBytes` can therefore exceed physical usage after
heavy copying; that is the same accounting philosophy snapshots already use, and
it must be documented for operators.

### Reclaim

The safety property lives at a single choke point. `KeyDeletingService` is the
only consumer that turns `deletedTable` entries into SCM block deletions, and
every producer already funnels the full `OmKeyInfo` into `deletedTable` — the
tag rides along for free because `OmUtils.prepareKeyForDelete` builds on
`toBuilder()`.

Before calling SCM, `processKeyDeletes` groups the reclaimable keys by tag and
reads one row per group:

- while a sharer outside this batch survives, every member of the batch is
  withheld from the SCM call and gets a synthetic success, exactly as empty keys
  already do, so its metadata is purged and its blocks are not;
- once the whole group dies together, one member releases the blocks and the
  rest are withheld, so SCM is told once;
- an absent row means the group is down to its last key, which releases its
  blocks through the ordinary path.

The matching count drops ride the final `PurgeKeysRequest` batch and are applied
by `OMKeyPurgeRequest` in the same Ratis transaction that purges the rows. The
ordering is deliberate: losing the last batch after rows were purged leaks
blocks, which an audit can reclaim, whereas decrementing first and then failing
to purge would let a retry decrement twice and release blocks a live key still
uses. **Every failure direction in this design degrades to a leak, never to data
loss.**

Counts are read on the leader outside Ratis, which is safe because a group's
count can only grow while at least one sharer is live in `keyTable`, and a copy
requires a live source. Once every sharer is in `deletedTable`, no increment is
possible.

The tag is excluded from snapshot diffs for free: `computeKeyInfoCompareSignature`
whitelists the fields that matter and skips the rest, which is correct — a key
that someone else copied has not changed.

## When a copy must fall back to reading and rewriting

These are hard constraints, not policy choices:

- **Encryption.** Block data is ciphertext under the source key's own DEK and
  IV, so a shared copy would have to carry the source's `FileEncryptionInfo`
  verbatim, which ties it to the source bucket's KMS key.
- **GDPR.** Erasure works by destroying the key's own secret; duplicating that
  secret into a second key sharing the same blocks defeats it.
- **Replication change.** The read path picks the EC or Ratis stream purely from
  the key's `ReplicationConfig`, so a mismatch is unreadable. A durability
  change such as THREE to ONE must also rewrite, to honour what was asked for
  and to keep quota math right.
- **Active hsync source.** The block list can still change under a live writer.
- **UploadPartCopy.** Whole-object sharing cannot express a byte range.

## Proof of concept scope

Implemented: the proto and `OmKeyInfo` tag, the column family, `OMKeyCopyRequest`
and `OMKeyCopyResponse` for `OBJECT_STORE` buckets, the reclaim rule end to end,
the client API, and an integration test in `TestKeyPurging` that copies a key,
deletes the source, and asserts the copy is still readable and the count row is
gone before deleting the copy.

`CopyKey` is gated on the `SERVER_SIDE_COPY` layout feature through
`@DisallowedUntilLayoutVersion`, so no copy and therefore no shared block can
exist until every OM in the cluster understands the tag. This is the
load-bearing guard rather than optional polish: to an older OM the tag is an
unknown proto field, so it would see a key whose blocks look exclusively owned
and release them while the other sharer is still alive.

Deliberately left out, and required before this could merge:

- Cross-bucket copy, which needs ordered multi-bucket locking.
- Overwriting an existing destination key, which needs the commit path's old
  version handling.
- FSO buckets, `OzoneManagerVersion` negotiation, an `ozone repair om` audit
  that rebuilds counts by scanning for tags, Recon awareness, S3 Gateway wiring
  with fallback, and metrics.

## Measurements

`BenchmarkCopyKey` in the integration-test module copies one key both ways on a
three-datanode `MiniOzoneCluster`, five iterations per size with the order
alternated between iterations and unique destination keys so that no block
reclamation runs inside a measurement. Medians:

| object size | read-and-rewrite copy | CopyKey | ratio |
|-------------|----------------------|---------|-------|
| 1 MiB       | 117 ms               | 7.5 ms  | 16x   |
| 16 MiB      | 889 ms               | 8.2 ms  | 108x  |
| 64 MiB      | 2 983 ms             | 6.7 ms  | 446x  |
| 256 MiB     | 11 100 ms            | 9.2 ms  | 1204x |

The shape is the result, not the ratios. CopyKey costs 6.7 to 9.2 ms with no
trend across a 256-fold size range, because it is one OM Ratis transaction no
matter how much data the key holds, while the byte copy grows linearly.

**The ratios above overstate what a real cluster would show.** Three datanodes,
OM, SCM and the client share one disk and one CPU here, so the byte copy only
reaches 18 to 23 MiB/s; a production cluster does several times better. Taking
100 MiB/s, which is roughly what S3 CopyObject delivers in practice, as the
byte-copy rate and keeping the measured 8 ms for CopyKey, the honest projection
is about 320x for a 256 MiB object, 1 280x for 1 GiB, and 6 400x at the 5 GiB
single-copy limit. Below a few MiB the gap closes, since both paths are then
dominated by round trips rather than data.

A second benchmark case reads both keys back and compares MD5 digests, so the
timings are known to describe copies that produced identical bytes.

## Independent quick wins

Two improvements need none of this machinery and are worth landing first:

- Self-copy with `x-amz-metadata-directive: REPLACE` currently rewrites the
  whole object to change a few metadata entries. As an in-place metadata update
  of the one existing row it creates no second reference to any block, so none
  of the problems above apply.
- `CopyObject` re-hashes every byte to compute an ETag the source metadata
  already holds.

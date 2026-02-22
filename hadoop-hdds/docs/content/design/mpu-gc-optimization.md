---
title: Multipart Upload GC Pressure Optimizations
summary: Change Multipart Upload Logic to improve OM GC Pressure
date: 2026-02-19
jira: HDDS-10611
status: proposed
author: Abhishek Pal, Rakesh Radhakrishnan
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

# Ozone MPU Optimization - Design Doc


## Table of Contents
1. [Motivation](#1-motivation)
2. [Proposal](#2-proposal)
  * [Split-table design (V2)](#split-table-design-v2)
  * [Comparison: V1 (legacy) vs V2](#comparison-v1-legacy-vs-v2)
  * [2.1 Data Layout Changes](#21-data-layout-changes)
  * [2.2 MPU Flow Changes](#22-mpu-flow-changes)
  * [2.3 Summary and Trade-offs](#23-summary-and-trade-offs)
3. [Upgrades](#3-upgrades)
4. [Industry Patterns](#4-industry-patterns-flattened-keys-in-lsmrocksdb-systems)
---

## 1. Motivation
Presently Ozone has several overheads when uploading large files via Multipart upload (MPU). This document presents a detailed design for optimizing the MPU storage layout to reduce these overheads.

### Problem with the current MPU schema
**Current design:**
* One row per MPU: `key = /{vol}/{bucket}/{key}/{uploadId}`
* Value = full `OmMultipartKeyInfo` with all parts inline.

**Implications:**
1. Each MPU part commit reads the full `OmMultipartKeyInfo`, deserializes it, adds one part, serializes it, and writes it back (HDDS-10611).
2. RocksDB WAL logs each full write → WAL growth (HDDS-8238).
3. GC pressure grows with the size of the object (HDDS-10611).

#### a) Deserialization overhead
| Operation     | Current                                                 |
|:--------------|:--------------------------------------------------------|
| Commit part N | Read + deserialize whole OmMultipartKeyInfo (N-1 parts) |

#### b) WAL overhead
Assuming one MPU part info object takes ~1.5KB.

| Scenario    | Current WAL                     |
|:------------|:--------------------------------|
| 1,000 parts | ~733 MB (1+2+...+1000) × 1.5 KB |

#### c) GC pressure
Current: Large short-lived objects per part commit.

#### Existing Storage Layout Overview
```protobuf
MultipartKeyInfo {
  uploadID : string
  creationTime : uint64
  type : ReplicationType
  factor : ReplicationFactor (optional)
  partKeyInfoList : repeated PartKeyInfo ← grows with each part
  objectID : uint64 (optional)
  updateID : uint64 (optional)
  parentID : uint64 (optional)
  ecReplicationConfig : optional
}
```

---

## 2. Proposal
The idea is to split the content of `MultipartInfoTable`. Part information will be stored separately in a flattened schema (one row per part) instead of one giant object.

### Split-table design (V2)
Split MPU metadata into:
* **Metadata table:** Lightweight per-MPU metadata (no part list).
* **Parts table:** One row per part (flat structure).

**New MultipartPartInfo Structure:**
```protobuf
message MultipartPartInfo {
  required string partName = 1;
  required uint32 partNumber = 2;
  required string volumeName = 3;
  required string bucketName = 4;
  required string keyName = 5;
  required uint64 dataSize = 6;
  required uint64 modificationTime = 7;
  repeated KeyLocationList keyLocationList = 8;
  repeated hadoop.hdds.KeyValue metadata = 9;
  optional FileEncryptionInfoProto fileEncryptionInfo = 10;
  optional FileChecksumProto fileChecksum = 11;
}
```

### Comparison: V1 (legacy) vs V2
| Metric              | Current (V1)                  | Split-Table (V2)                                 |
|:--------------------|:------------------------------|:-------------------------------------------------|
| **Commit part N**   | Read + deserialize whole list | Read Metadata (~200B) + write single PartKeyInfo |
| **1,000 parts WAL** | ~733 MB                       | ~1.5 MB (or ~600KB with optimized info)          |
| **GC Pressure**     | Large short-lived objects     | Small metadata + single-part objects             |

---

### 2.1 Data Layout Changes

#### 2.1.1 Chosen Approach: Reuse `multipartInfoTable` + add `multipartPartsTable`

Keep `multipartInfoTable` for MPU metadata, and store part rows in `multipartPartsTable`.

**Storage Layout:**
* **`multipartInfoTable` (RocksDB):**
  * V1: Key -> `OmMultipartKeyInfo` { parts inline }
  * V2: Key -> `OmMultipartKeyInfo` { empty list, `schemaVersion: 1` }
* **`multipartPartsTable` (RocksDB):**
  * Key type: `OmMultipartPartKey(uploadId, partNumber)`
  * Value type: `OmMultipartPartInfo`

**`multipartPartsTable` key codec (V2):**
* `OmMultipartPartKey` uses two logical fields:
  * `uploadId` (`String`)
  * `partNumber` (`int32`)
* Persisted key bytes are encoded as:
  * `uploadId(UTF-8 bytes)` + `0x00` + `partNumber(4-byte big-endian int)`
* Prefix scan for all parts in one upload uses:
  * `uploadId(UTF-8 bytes)` + `0x00`

```protobuf
message MultipartKeyInfo {
    required string uploadID = 1;
    required uint64 creationTime = 2;
    required hadoop.hdds.ReplicationType type = 3;
    optional hadoop.hdds.ReplicationFactor factor = 4;
    repeated PartKeyInfo partKeyInfoList = 5;
    optional uint64 objectID = 6;
    optional uint64 updateID = 7;
    optional uint64 parentID = 8;
    optional hadoop.hdds.ECReplicationConfig ecReplicationConfig = 9;
    optional uint32 schemaVersion = 10; // default 0
}
```

##### V1: `OmMultipartKeyInfo` (parts inline)
```
OmMultipartKeyInfo {
  uploadID
  creationTime
  type
  factor
  partKeyInfoList: [ PartKeyInfo, PartKeyInfo, ... ]   <- all parts inline
  objectID
  updateID
  parentID
  schemaVersion: 0 (or absent)
}
```

##### V2: `OmMultipartKeyInfo` (empty list + schemaVersion)
```
OmMultipartKeyInfo {
  uploadID
  creationTime
  type
  factor
  partKeyInfoList: []   <- empty
  objectID
  updateID
  parentID
  schemaVersion: 1
}
```

##### Example (for a 10-part MPU)

`multipartInfoTable`:
```
Key:   /vol1/bucket1/mp_file1/abc123-uuid-456

Value:
OmMultipartKeyInfo {
  uploadID: "abc123-uuid-456"
  creationTime: 1738742400000
  type: RATIS
  factor: THREE
  partKeyInfoList: []
  objectID: 1001
  updateID: 12345
  parentID: 0
  schemaVersion: 1
}
```

`multipartPartsTable` (logical keys):
```text
Key:   OmMultipartPartKey{uploadId="abc123-uuid-456", partNumber=1}
Value: OmMultipartPartInfo{partNumber=1, partName=".../part1", ...}

Key:   OmMultipartPartKey{uploadId="abc123-uuid-456", partNumber=2}
Value: OmMultipartPartInfo{partNumber=2, partName=".../part2", ...}
...
Key:   OmMultipartPartKey{uploadId="abc123-uuid-456", partNumber=10}
Value: OmMultipartPartInfo{partNumber=10, partName=".../part10", ...}
```

`multipartPartsTable` (encoded key bytes):
```text
uploadId = "abc123-uuid-456"
partNumber = 2

encodedKey = [61 62 63 31 32 33 2d 75 75 69 64 2d 34 35 36 00 00 00 00 02]
             [--------------uploadId UTF-8---------------][00][--int32 BE--]
```

#### 2.1.2 Alternative Approach: Add `multipartMetadataTable` + `multipartPartsTable`

Split metadata and introduce two new tables:
* **`multipartMetadataTable`**: lightweight per-MPU metadata (no part list).
* **`multipartPartsTable`**: one row per part (no aggregation).

```protobuf
message MultipartMetadataInfo {
  required string uploadID = 1;
  required uint64 creationTime = 2;
  required hadoop.hdds.ReplicationType type = 3;
  optional hadoop.hdds.ReplicationFactor factor = 4;
  optional hadoop.hdds.ECReplicationConfig ecReplicationConfig = 5;
  optional uint64 objectID = 6;
  optional uint64 updateID = 7;
  optional uint64 parentID = 8;
  optional uint32 schemaVersion = 9; // default 0
}
```

**Storage Layout Overview:**
* **`multipartInfoTable` (RocksDB):**
  * V1: `/vol/bucket/key/uploadId` -> `OmMultipartKeyInfo { partKeyInfoList: [...] }`
* **`multipartMetadataTable` (RocksDB):**
  * V2: `/vol/bucket/key/uploadId` -> `MultipartMetadata { schemaVersion: 1 }`
* **`multipartPartsTable` (RocksDB):**
  * Key: `OmMultipartPartKey(uploadId, partNumber)`
  * Value: `PartKeyInfo`-equivalent part payload

```protobuf
message MultipartMetadata {
  required string uploadID = 1;
  required uint64 creationTime = 2;
  required hadoop.hdds.ReplicationType type = 3;
  optional hadoop.hdds.ReplicationFactor factor = 4;
  optional uint64 objectID = 5;
  optional uint64 updateID = 6;
  optional uint64 parentID = 7;
  optional hadoop.hdds.ECReplicationConfig ecReplicationConfig = 8;
  optional uint32 schemaVersion = 9;
  // NO partKeyInfoList - moved to new table
}
```

Example:
```
Key: /vol1/bucket1/mp_file1/abc123-uuid-456

Value:
MultipartMetadata {
  uploadID: "abc123-uuid-456"
  creationTime: 1738742400000
  type: RATIS
  factor: THREE
  objectID: 1001
  updateID: 12345
  parentID: 0
  schemaVersion: 1
}
```

### 2.2 MPU Flow Changes

#### 2.2.1 Chosen Approach Flow Changes

##### Multipart Upload Initiate

**Old Flow**
* Create `multipartKey = /{vol}/{bucket}/{key}/{uploadId}`.
* Build `OmMultipartKeyInfo` (schema default/legacy, inline `partKeyInfoList` model).
* Write:
  * `openKeyTable[multipartKey] = OmKeyInfo`
  * `multipartInfoTable[multipartKey] = OmMultipartKeyInfo`

Example:
```text
multipartInfoTable[/vol1/b1/fileA/upload-001] ->
  OmMultipartKeyInfo{schemaVersion=0, partKeyInfoList=[]}
openKeyTable[/vol1/b1/fileA/upload-001] ->
  OmKeyInfo{key=fileA, objectID=9001}
```

**New Flow**
* Same keys/tables as old flow, but initiate sets `schemaVersion` explicitly:
  * `schemaVersion=1` when `OMLayoutFeature.MPU_PARTS_TABLE_SPLIT` is allowed.
  * `schemaVersion=0` otherwise.
* No part row is created at initiate time; part rows are created during commit-part.
* FSO response path (`S3InitiateMultipartUploadResponseWithFSO`) still writes parent directory entries, then open-file + multipart-info rows.
* Backward compatibility: write path selection is schema-based and layout-gated (see [3.1 Backward compatibility and layout gating](#31-backward-compatibility-and-layout-gating)).

Example:
```text
multipartInfoTable[/vol1/b1/fileA/upload-001] ->
  OmMultipartKeyInfo{schemaVersion=1, partKeyInfoList=[]}
```

##### Multipart Upload Commit Part

**Old Flow**
* Read `multipartInfoTable[multipartKey]`.
* Read current uploaded part blocks from `openKeyTable[getOpenKey(..., clientID)]`.
* Insert in inline map:
  * `oldPart = multipartKeyInfo.getPartKeyInfo(partNumber)`
  * `multipartKeyInfo.addPartKeyInfo(currentPart)`
* Delete committed one-shot open key for this part.
* Update quota based on overwrite delta.

Example:
```text
Before: partKeyInfoList=[{part=1,size=64MB},{part=2,size=32MB}]
Commit part 2 size=40MB
After:  partKeyInfoList=[{part=1,size=64MB},{part=2,size=40MB}]
```

**New Flow**
* Load `multipartKeyInfo` and validate layout gate:
  * if split feature is not allowed and `schemaVersion != 0`, fail early.
* Branch by schema:
  * `schemaVersion=0`: same old inline behavior.
  * `schemaVersion=1`:
    * create `multipartPartKey = OmMultipartPartKey(uploadId, partNumber)`,
    * write `multipartPartTable[multipartPartKey] = OmMultipartPartInfo{openKey, partName, partNumber, size, metadata, locations}`
    * keep current part open key in `openKeyTable` (needed later by list/complete/abort),
    * if overwriting an existing part row, delete old part open key and adjust quota.
* `multipartInfoTable[multipartKey]` is still updated for metadata/updateID.
* Backward compatibility: schema decides row format, and split behavior is blocked before finalization (see [3.1 Backward compatibility and layout gating](#31-backward-compatibility-and-layout-gating)).

Example (`schemaVersion=1`):
```text
multipartPartTable[OmMultipartPartKey{uploadId="upload-001", partNumber=2}] ->
  OmMultipartPartInfo{partNumber=2, openKey=/vol1/b1/fileA#client-77, size=40MB}
```

##### Multipart Upload Complete

**Old Flow**
* Read `multipartInfoTable[multipartKey]` and validate ordered requested parts.
* Validate each requested part from inline `partKeyInfoMap`.
* Build final key block list from selected parts.
* Write final key:
  * `keyTable[/vol1/b1/fileA] = OmKeyInfo{locations=p1+p2+...}`
* Delete MPU state:
  * `openKeyTable[multipartOpenKey]`
  * `multipartInfoTable[multipartKey]`
* Move unused parts to deleted table.

Example:
```text
Complete [1,2,3] -> keyTable[/vol1/b1/fileA] written, MPU rows removed
```

**New Flow**
* Validate layout gate first (same pattern as commit/abort when `schemaVersion != 0`).
* Load part materialization by schema:
  * `schemaVersion=0`: use inline `partKeyInfoMap`.
  * `schemaVersion=1`:
    * scan `multipartPartTable` with prefix `OmMultipartPartKey.prefix(uploadId)`,
    * rebuild `PartKeyInfo` view from `OmMultipartPartInfo`,
    * pull block locations from stored part metadata / open keys as needed.
* Perform same user-facing validation (order, existence, eTag/partName, min size).
* Commit final key to `keyTable`.
* Cleanup:
  * always delete `multipartInfoTable[multipartKey]` and `openKeyTable[multipartOpenKey]`,
  * for split schema also delete all matching `multipartPartTable` rows and their tracked part open keys.
* Backward compatibility: completion transparently supports both persisted schemas and enforces layout gating for split rows (see [3.1 Backward compatibility and layout gating](#31-backward-compatibility-and-layout-gating)).

Example (`schemaVersion=1` cleanup):
```text
Delete:
  multipartPartTable[OmMultipartPartKey{uploadId="upload-001", partNumber=1}]
  multipartPartTable[OmMultipartPartKey{uploadId="upload-001", partNumber=2}]
  ...
  openKeyTable[part-open-key-1], openKeyTable[part-open-key-2], ...
```

##### Multipart Upload Abort

**Old Flow**
* Read `multipartInfoTable[multipartKey]`.
* Release bucket used-bytes from inline `partKeyInfoMap`.
* Delete `openKeyTable[multipartOpenKey]` and `multipartInfoTable[multipartKey]`.
* Move part key infos to deleted table.

Example:
```text
Abort upload-001 -> delete MPU metadata/open rows and tombstone parts
```

**New Flow**
* Validate layout gate (reject split behavior before finalization if schema indicates split).
* Branch by schema for quota and cleanup:
  * `schemaVersion=0`: same legacy inline part iteration.
  * `schemaVersion=1`:
    * iterate `multipartPartTable` by prefix (`OmMultipartPartKey.prefix(uploadId)`),
    * compute released quota from part rows,
    * for each part: move corresponding open key to deleted table, delete part open key, delete part row.
* Delete `multipartInfoTable[multipartKey]` and `multipartOpenKey` entry.
* Backward compatibility: abort handles both old inline and new split rows in the same codepath (see [3.1 Backward compatibility and layout gating](#31-backward-compatibility-and-layout-gating)).

Example (`schemaVersion=1`):
```text
multipartPartTable[OmMultipartPartKey{uploadId="upload-001", partNumber=3}] -> deleted
openKeyTable[/vol1/b1/fileA#client-90] -> deleted
deletedTable[objectId:/vol1/b1/fileA/upload-001] -> appended
```

##### Multipart Upload List Parts

**Old Flow**
* API path:
  * S3G `MultipartKeyHandler.listParts(...)`
  * `OzoneManager.listParts(...)`
  * `KeyManagerImpl.listParts(...)`
* Read `multipartInfoTable[multipartKey]`.
* Iterate inline `partKeyInfoMap` in part-number order; apply marker + `maxParts`.
* If no part entries exist yet, read replication from `openKeyTable[multipartKey]`.

Example:
```text
parts=[1,2,3,4], marker=1, maxParts=2 => return [2,3], nextMarker=3, truncated=true
```

**New Flow**
* Same API path and response contract.
* Branch by schema in `KeyManagerImpl.listParts`:
  * `schemaVersion=0`: iterate inline `partKeyInfoMap`.
  * `schemaVersion=1`:
    * scan `multipartPartTable` by `OmMultipartPartKey.prefix(uploadId)`,
    * resolve each part's `openKey` from `openKeyTable`,
    * build `PartKeyInfo` view on the fly and paginate.
* Replication fallback remains from MPU open key when no part is returned.

Example (`schemaVersion=1` read materialization):
```text
multipartPartTable[OmMultipartPartKey{uploadId="upload-001", partNumber=2}] ->
  openKey=/vol1/b1/fileA#c2
openKeyTable[/vol1/b1/fileA#c2] -> KeyInfo{size=40MB, eTag=...}
listParts response partNumber=2, size=40MB, eTag=...
```

#### 2.2.2 Alternative Approach Flow

##### Multipart Upload Initiate

**Old Flow**
* Write `openKeyTable` + `multipartInfoTable` (`OmMultipartKeyInfo`), with legacy inline-part model.

**New Flow**
* Write `openKeyTable` + `multipartMetadataTable` (new metadata object, no part list).
* `multipartInfoTable` is no longer used for new V1 MPU writes in this approach.
* `schemaVersion` (or equivalent layout marker) is stored in `multipartMetadataTable` row.

Example:
```text
multipartMetadataTable[/vol1/b1/fileA/upload-001] ->
  MultipartMetadata{schemaVersion=1, replication=RATIS/THREE, objectID=9001}
openKeyTable[/vol1/b1/fileA/upload-001] ->
  OmKeyInfo{key=fileA, locations=[empty]}
```

##### Multipart Upload Commit Part

**Old Flow**
* Read/modify/write one `multipartInfoTable` row by updating inline part list.

**New Flow**
* Read `multipartMetadataTable` only for MPU metadata + validation context.
* Write one row per part into `multipartPartsTable`:
  * key: `OmMultipartPartKey(uploadId, partNumber)`,
  * value: `PartKeyInfo` / equivalent flattened part payload.
* Avoid rewriting a large aggregate MPU value for each part.

Example:
```text
multipartPartsTable[OmMultipartPartKey{uploadId="upload-001", partNumber=2}] ->
  PartKeyInfo{partNumber=2, partName=..., keyInfo={blocks,size,etag}}
```

##### Multipart Upload Complete

**Old Flow**
* Read parts from inline `partKeyInfoList` in `multipartInfoTable`.
* Commit final key, delete MPU row and MPU open row.

**New Flow**
* Read MPU metadata from `multipartMetadataTable`.
* Scan `multipartPartsTable` prefix to gather candidate parts; validate request list/order/eTags.
* Build final key and commit to `keyTable`.
* Cleanup:
  * delete `multipartMetadataTable[multipartKey]`,
  * delete all `multipartPartsTable` rows for that upload,
  * delete MPU open key and part open keys.

##### Multipart Upload Abort

**Old Flow**
* Iterate inline `partKeyInfoList` to release quota and move parts to deleted table.

**New Flow**
* Read `multipartMetadataTable` for replication + MPU identity.
* Iterate `multipartPartsTable` by prefix for quota release and delete-table movement.
* Delete metadata row + all part rows + corresponding open keys.

##### Multipart Upload List Parts

**Old Flow**
* `listParts` reads `multipartInfoTable` and paginates inline `partKeyInfoList`.

**New Flow**
* `listParts` reads `multipartMetadataTable` for MPU existence/metadata.
* Materialize part listing from `multipartPartsTable` prefix scan and paginate by part number marker.
* If needed, join with `openKeyTable` for additional per-part runtime fields.

##### Compatibility and Upgrade Guard
* Same compatibility strategy as section [3.1 Backward compatibility and layout gating](#31-backward-compatibility-and-layout-gating):
  * legacy rows continue on old path,
  * new writes only when layout feature is finalized,
  * reject unsupported split-layout mutations pre-finalize.

### 2.3 Summary and Trade-offs
* **Approach-1:** Minimal change, same value type, uses `schemaVersion` flag.
* **Approach-2:** Dedicated metadata table, cleanest separation, requires broader refactor.

#### Pros and Cons

|                      | Pros                                                                                                                                                                                                   | Cons                                                                                                                                                                                         |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Chosen Approach      | * Minimal migration risk<br/> * Reuses existing `OmMultipartKeyInfo` message<br/>* Easiest incremental rollout with `schemaVersion` gating.<br/> * Lower implementation impact request/response paths. | * Carries complexity for mixed code (same table serving legacy + split metadata modes).<br/> * Still coupled to `OmMultipartKeyInfo`.<br/> * More conditional logic over time.               |
| Alternative Approach | * Clean separation of concerns (`multipartMetadataTable` vs `multipartPartsTable`)<br/> * Clearer long-term model and easier mental mapping.<br/> * Avoids overloading legacy value type.              | * Requires wider code changes (new codecs/table wiring/request/response updates).<br/> * Higher migration and compatibility test scope.<br/> * More rollout complexity than chosen approach. |

## 3. Upgrades
Add a new feature in `OMLayoutFeature`:
```java
MPU_PARTS_TABLE_SPLIT(10, "Split multipart table into separate table for parts and key");
```

### 3.1 Backward compatibility and layout gating

Backward compatibility is handled by combining `schemaVersion` with layout-feature checks.

- **New MPU initiate writes**
  - `schemaVersion` is set to `1` only when `MPU_PARTS_TABLE_SPLIT` is allowed.
  - Otherwise initiate writes `schemaVersion=0` and stays on legacy inline part behavior.

- **Existing MPU rows**
  - `schemaVersion=0` rows continue to use legacy inline-part read/write paths.
  - `schemaVersion=1` rows use split-table paths (`multipartPartsTable` + tracked open keys).

- **Pre-finalize protection**
  - Mutating split-table operations (commit part / complete / abort) check:
    - if feature is not allowed and `schemaVersion != 0`, reject with `NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION`.
  - This prevents accidental split-layout writes/updates before finalization.

- **Read compatibility**
  - `listParts` supports both schemas:
    - schema 0 -> read inline `partKeyInfoMap`,
    - schema 1 -> materialize parts from `multipartPartsTable` + `openKeyTable`.

In short: old MPU entries keep working unchanged, new entries only use split layout when cluster layout allows it, and write paths are guarded to avoid unsafe transitions.

## 4. Industry Patterns: Flattened Keys in LSM/RocksDB Systems

Using flattened keys (for example, `baseKey + sortable suffix`) is a common design in RocksDB-backed systems.
The MPU `multipartPartsTable` layout follows the same principle by using one row per part keyed by
`OmMultipartPartKey(uploadId, partNumber)` with byte-level ordering from codec serialization.

### 4.1 Why this is common

RocksDB is optimized for:
- point lookups by key,
- prefix/range scans over lexicographically ordered keys,
- append-like write patterns with small values.

Flattened schemas map naturally to this model:
- each logical sub-record (part/version) becomes an independent KV row,
- updates rewrite only one small row instead of a large aggregate object,
- range scans can fetch all rows for one logical entity via a shared prefix.

### 4.2 MVCC systems as examples

In MVCC-oriented systems such as CockroachDB and TiKV, a common pattern is:
- encode the user key as a prefix,
- encode version dimension (timestamp / sequence / commit-ts) in key suffix,
- use key ordering to make version reads and range scans efficient.

High-level shape:
```text
<logical-key>/<version-or-ts>
```

Example (illustrative):
```text
user:42@1699000001
user:42@1699000005
user:42@1699000010
```

The idea is conceptually similar to MPU part storage:
```text
OmMultipartPartKey{uploadId="abc123-uuid-456", partNumber=1}
OmMultipartPartKey{uploadId="abc123-uuid-456", partNumber=2}
OmMultipartPartKey{uploadId="abc123-uuid-456", partNumber=3}
```

Both designs rely on ordered keys to make grouped reads/writes efficient.

### 4.3 Relevance to MPU optimization

For MPU, flattened part rows provide:
- lower write amplification per part commit (single-row updates),
- lower object allocation pressure in OM (no repeated large list rebuild),
- straightforward cleanup by prefix scan during complete/abort,
- better operational visibility (`one row = one part`).

This is why the split schema is not just an optimization for this code path, but also a storage-layout pattern that aligns well with how LSM/RocksDB systems are typically modeled.

---
title: Multipart Upload GC Pressure Optimizations
summary: Change Multipart Upload Logic to improve OM GC Pressure
date: 2026-02-19
jira: HDDS-10611
status: implemented
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
* [Backward Compatibility](#backward-compatibility)
* [Split-table design (V1)](#split-table-design-v1)
* [Comparison: V0 (legacy) vs V1](#comparison-v0-legacy-vs-v1)
* [2.1 Approach-1: Reuse multipartInfoTable with empty part list](#21-approach-1--reuse-multipartinfotable-with-empty-part-list)
* [2.2 Approach-2: Introduce new multipartMetadataTable](#22-approach-2--introduce-new-multipartmetadatatable)
* [2.3 Summary](#23-summary)
* [2.4 Chosen Approach: Approach-1](#24-chosen-approach-approach-1)
3. [Upgrades](#3-upgrades)
4. [Benchmarking and Performance](#4-benchmarking-and-performance)
5. [Open Questions](#5-open-questions)

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

### Split-table design (V1)
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

### Comparison: V0 (legacy) vs V1
| Metric              | Current (V0)                  | Split-Table (V1)                                 |
|:--------------------|:------------------------------|:-------------------------------------------------|
| **Commit part N**   | Read + deserialize whole list | Read Metadata (~200B) + write single PartKeyInfo |
| **1,000 parts WAL** | ~733 MB                       | ~1.5 MB (or ~600KB with optimized info)          |
| **GC Pressure**     | Large short-lived objects     | Small metadata + single-part objects             |

---

### 2.1. Approach-1 : Reuse multipartInfoTable with empty part list
Reuse the existing table but introduce a new `multipartPartsTable`.

**Storage Layout:**
* **multipartInfoTable (RocksDB):**
  * V0: Key → `OmMultipartKeyInfo` { parts inline }
  * V1: Key → `OmMultipartKeyInfo` { empty list, schemaVersion: 1 }
* **multipartPartsTable (RocksDB) [V1 only]:**
  * `/uploadId/part00001` → `PartKeyInfo`
  * `/uploadId/part00002` → `PartKeyInfo`
  * `/uploadId/part00003` → `PartKeyInfo`


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

#### V0: OmMultipartKeyInfo (parts inline)
```
OmMultipartKeyInfo {
  uploadID
  creationTime
  type
  factor
  partKeyInfoList: [ PartKeyInfo, PartKeyInfo, ... ]   ← all parts inline
  objectID
  updateID
  parentID
  schemaVersion: 0 (or absent)
}
```
##### V1: OmMultipartKeyInfo (empty list + schemaVersion)
```
OmMultipartKeyInfo {
  uploadID
  creationTime
  type
  factor
  partKeyInfoList: []   ← empty
  objectID
  updateID
  parentID
  schemaVersion: 1
}
```

#### Example (for a 10 part MPU)

---
#### MultipartInfoTable :
```
Key:   `/vol1/bucket1/mp_file1/abc123-uuid-456`

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

#### MultipartPartsTable – 10 rows:

```text
Key:   /vol1/bucket1/mp_file1/abc123-uuid-456/part00001
Value: PartKeyInfo { partName: ".../part1", partNumber: 1, partKeyInfo: KeyInfo{blocks, size,...} }

Key:   /vol1/bucket1/mp_file1/abc123-uuid-456/part00002
Value: PartKeyInfo { partName: ".../part2", partNumber: 2, partKeyInfo: KeyInfo{...} }
...
...
Key:   /vol1/bucket1/mp_file1/abc123-uuid-456/part00010
Value: PartKeyInfo { partName: ".../part10", partNumber: 10, partKeyInfo: KeyInfo{...} }
```

### 2.2. Approach-2 : Introduce new multipartMetadataTable

Split metadata and introduce two new tables:
- **multipartMetadataTable**: lightweight per-MPU metadata (no part list).
- **multipartPartsTable**: one row per part (no aggregation).

Below is the new metadata table info object structure:
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

#### Storage Layout Overview

* **multipartInfoTable (RocksDB):**
  * V0: `/vol/bucket/key/uploadId` → `OmMultipartKeyInfo { partKeyInfoList: [...] }`


* **multipartMetadataTable (RocksDB)**
  * V1: `/vol/bucket/key/uploadId` → `MultipartMetadata { schemaVersion: 1 }`


* **multipartPartsTable (RocksDB) [v1 only]**
  * `/vol/bucket/key/uploadId/part00001`  → `PartKeyInfo` 
  * `/vol/bucket/key/uploadId/part00002`  → `PartKeyInfo` 
  * `/vol/bucket/key/uploadId/part00003`  → `PartKeyInfo`
  * `...`

#### multipartMetadataInfo Table – 1 row
**V1: OmMultipartMetadataInfo (metadata only)**
```text
OmMultipartMetadataInfo {
  uploadID
  creationTime
  type (ReplicationType)
  factor (ReplicationFactor)
  objectID
  updateID
  parentID
  ecReplicationConfig
  schemaVersion: 1
}
```

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

#### Example:

---
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

---

### 2.3. Summary
* **Approach-1:** Minimal change, same value type, uses `schemaVersion` flag.
* **Approach-2:** Dedicated table, cleanest separation, requires new lookup logic.

----
### 2.4. Chosen Approach: Approach-1
We have chosen **Approach-1: Reuse multipartInfoTable with empty part list**
as the preferred implementation for MPU optimization (V1).

This approach is favored because it introduces minimal changes to the existing `OmMultipartKeyInfo` protobuf structure.
<br>
By simply introducing an optional `schemaVersion` field and ensuring the partKeyInfoList is empty for V1 entries,
we maintain backward compatibility.

The key advantages are:
* **Minimal Protobuf Change**: Older clients and processes can still read the multipartInfoTable entries without issue,
    as the **core structure remains the same**.
* **Compatibility**: Older uploads (V0) **remain fully functional**, and new uploads (V1) can be distinguished by
    the schemaVersion. This significantly reduces the risk of breaking existing functionality.
* **Simplicity**: The transition logic between V0 and V1 is straightforward, primarily checking the
    `schemaVersion` field upon read.
---

## 3. Upgrades
Add a new feature in `OMLayoutFeature`:
```java
MPU_PARTS_TABLE_SPLIT(10, "Split multipart table into separate table for parts and key");
```
`schemaVersion` is set to `1` only when the upgrade is finalized.

For each of the four S3MultipartUpload request types we need to ensure the check that split table layout feature
is allowed and only then we can set the schema as `version:1`
<br>
This ensures that no new writes are happening if the split table is not supported -
specially in cases where in pre-finalize the client may try to write a new MPU key.

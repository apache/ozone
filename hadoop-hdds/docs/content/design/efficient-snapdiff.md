---
title: Snapshot Diff Optimization
summary: Describe proposal for an optimized snapshot diff that uses mostly sequential reads and batch puts
date: 2025-05-22
jira: HDDS-9154
status: draft
author: Saketa Chalamchala
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

## 1. Introduction
This document outlines the technical design, architectural choices, and algorithmic improvements to optimize Ozone's Snapshot Diff feature. The design addresses performance bottlenecks in both the **Full Diff** and **DAG-based Diff** paths. The primary goals are to reduce random I/O, minimize CPU overhead from deserialization, and streamline the classification of differences.

 ## Goals
 - Reduce random I/O.
 - Minimize CPU cost of deserializing KeyInfo and DirectoryInfo for comparisons.
 - Keep baseline diff semantics for CREATE/DELETE/RENAME/MODIFY where possible.

---

## 2. Core Design Choices & Optimizations

### 2.1. Sequential Reads & Table Iterators
**Baseline Issue:** Baseline full diff enumerates keys via SST readers (plus per-key `db.get` lookups), and the DAG-based diff relies heavily on random point lookups (`db.get()`) against the snapshot RocksDB instances to fetch the old and new states of keys identified in the delta SST files. For buckets with millions of keys, this random I/O degrades performance and thrashes the OS page cache.
**Optimized Design:** The optimization shifts mostly to sequential reads. For the Full Diff path, it uses native RocksDB **Table Iterators** to scan the entire `directoryTable` and `fileTable` sequentially. For the DAG-based path, it uses a **K-way Merge Iterator** over the delta SST files to sequentially extract the latest visible versions without needing to query the main snapshot DBs. This sequential I/O pattern maximizes disk throughput and cache efficiency.

### 2.2. Lightweight Parsing
**Baseline Issue:** The baseline implementation fully deserializes `OmKeyInfo` and `OmDirectoryInfo` protobuf messages to compare objects, which is extremely CPU and memory intensive when scanning millions of keys.
**Optimized Design:** Introduces a lightweight `SnapshotDiffValueParser` that reads the raw protobuf byte stream directly. It extracts only the required fields (like `updateID`, `parentID`, `name` and compare signature fields) without instantiating full Java objects. It dynamically builds a compare signature by hashing only meaningful fields (content-change: latest block layout, size, `fileChecksum` and metadata-change: ACLs, metadata, tags), skipping volatile fields like `modificationTime` or `creationTime` to identify modified entries.

#### Pseudo-code: Selective Parsing and Signature
```java
ParsedObjectInfo parseRequiredKeyInfo(byte[] raw, boolean meaningfulOnly) {
  ParsedObjectInfo parsed = new ParsedObjectInfo();
  CodedInputStream input = CodedInputStream.newInstance(raw);
  while (!input.isAtEnd()) {
    int tag = input.readTag();
    switch (WireFormat.getTagFieldNumber(tag)) {
    case KEYINFO_OBJECT_ID_FIELD:
      parsed.setObjectId(input.readUInt64());
      break;
    case KEYINFO_PARENT_ID_FIELD:
      parsed.setParentId(input.readUInt64());
      break;
    case KEYINFO_KEY_NAME_FIELD:
      parsed.setName(input.readString());
      break;
    case KEYINFO_UPDATE_ID_FIELD:
      parsed.setUpdateId(input.readUInt64());
      break;
    default:
      input.skipField(tag);
      break;
    }
  }
  return parsed;
}

ParsedObjectInfo parseSignatureKeyInfo(byte[] raw, boolean meaningfulOnly) {
  ParsedObjectInfo parsed = new ParsedObjectInfo();
  CodedInputStream input = CodedInputStream.newInstance(raw);
  while (!input.isAtEnd()) {
    int tag = input.readTag();
    switch (WireFormat.getTagFieldNumber(tag)) {
    case KEYINFO_METADATA_FIELD:
    case KEYINFO_ACLS_FIELD:
    case KEYINFO_TAGS_FIELD:
    case KEYINFO_FILE_CHECKSUM_FIELD:
        updateSignature(tag, input, parsed);
      break;
    case KEYINFO_BLOCK_LOCATIONS_FIELD:
        updateSignature(extractLatestBlockInfo(tag, input), parsed);
    default:
      input.skipField(tag);
      break;
    }
  }
  return parsed;
}
```

### 2.3. Sequence/UpdateID Gating
**Baseline Issue:** The baseline performs full object comparisons including timestamps to detect modifications, which is susceptible to clock skew and is computationally expensive.
**Optimized Design:** Use snapshot-specific gates that align with the transactional guarantees of the deployment mode.
- **Full diff (w/ OM HA only):** `updateID > fromSnapshot.lastTransactionInfo.txIndex`. This compares two OM/Ratis log indices.
- **DAG diff:** Extend raw SST iterators to expose internal sequence numbers, gate with `entry.sequence > fromSnapshot.dbTxSequenceNumber`.

### 2.4. Deferred Classification & Path Resolution
**Baseline Issue:** Baseline builds the diff key set first and then classifies entries during `generateDiffReport`, which requires resolving paths for all candidates. This causes unnecessary path lookups for entries that might ultimately be ignored.
**Optimized Design:** Diff classification is strictly deferred to the final **Merge Join** stage. Path resolution is also deferred until an entry is definitively classified as a diff. This prevents wasting I/O and CPU on resolving paths for entries that might ultimately be ignored or unchanged.

### 2.5. Batch Puts to Snapshot Diff DB
**Baseline Issue:** Writing intermediate lists and final diff reports often relies on individual RocksDB `put` operations, incurring high JNI overhead.
**Optimized Design:** The design advocates for using RocksDB `WriteBatch` operations. By batching writes to the `snap-diff-report-table` and intermediate `PersistentList`/`PersistentMap` structures, we significantly improve write throughput and reduce disk sync overhead.

### 2.6. Delete Report Consistency
**Baseline Issue:** With baseline full diff, deleting a directory emits `DELETE` entries for the directory but reports sub-directories and sub-files inconsistently depending on how far deep cleaning of the `toSnapshot` progressed. In DAG-based diff, only the deleted directory and any sub-directory/sub-file that was explicitly deleted before the top-level directory are reported. For the same snapshots, diff output can vary based on timing (before vs after deep cleaning) or mode (full diff vs DAG-based diff).
**Optimized Design:** Only top level deleted directories are reported. This keeps diff results stable regardless of snapshot deep cleaning and which diff path was used.

### 2.7. Dependency Ordered Reporting
**Baseline Issue:** With baselines, diff report entries are ordered by diff type, `DELETES` are reported first followed by `RENAMES, CREATES, MODIFIES` in order. When the report is replayed this order does not safely cover all scenarios.

For example, 
* Snapshot 1 has file `A/B` and directory `C`. 
* Snapshot 2 renames `A/B` to `C/B` and deletes directory `A`. 
* The diff entries are `RENAME A/B -> C/B` and `DELETE A`. 
If deletes are replayed first, `A/B` is removed before the rename and the rename fails. The correct replay order is `RENAME A/B -> C/B` followed by `DELETE A`.

**Optimized Design:** Ensure the report can be replayed safely by ordering entries based on their dependencies rather than their diff type.

**Dependency Rules:**
1. Parents must appear before children for `CREATE/RENAME/MODIFY`.
2. Children must appear before parents for `DELETE`.
3. If a rename or create targets a path that is being deleted, the delete must come first.
4. If a rename frees a source path that is re-created in the same diff, the rename must come first.

**Building the dependency graph:**
- Each diff entry becomes a node in a directed graph.
- Add edges using the rules above:
  - For hierarchy ordering, add edges from parent to child for `CREATE/RENAME/MODIFY`.
  - For deletes, use the same parent-child edges but emit them in reverse order later.
  - For path conflicts, add edges from the delete node to the rename/create node that reuses the deleted path, and from rename to create if the rename frees a path that is re-created.

**Emitting entries using the graph:**
- Run Kahn's algorithm on the graph to produce a topological order for `CREATE/RENAME/MODIFY`.
- Emit all `CREATE/RENAME/MODIFY` entries in that order (parents before children, and conflict edges respected).
- Emit `DELETE` entries in reverse topological order (children before parents) so deletes do not remove parents before their children.

**Note on OBS Buckets:** Since OBS buckets lack a directory hierarchy, dependency ordering simplifies to path-conflict rules (Rules 3 and 4), ensuring renames and deletes occur in the correct sequence to avoid collisions or missing sources.

---

## 3. Data Structures and Algorithms
 
- **oldList/newList maps**: `PersistentMap<byte[], byte[]>` keyed by `objectId`, storing `EntryValue` (`parentId`, `name`, `isDir`, `signature`).
- **Directory path lookup**:
  - **Persisted BFS**: RocksDB CFs for edges storing `(parentID, objectID) -> name` and resolved paths `objectID -> fullPath`, with an LRU cache for hot path lookups.
- **DiffCandidateSet**: `Set<objectIds>/Set<keys>` captured by snapshot-specific gating rules mentioned in Section 2.3
- **SHA-256 Hashing:** Used to generate compact, fixed-size compare signatures for object metadata.
- **Delete retention sets (full diff only):** `deletedDirSet` and `deletedRootSet` to suppress redundant deletes.
- **Dependency ordering graph:** adjacency list of `objectId -> children`, in-degree map, and a queue of zero in-degree nodes for Kahn's algorithm.
- **Raw SST iterators**: `ManagedRawSSTFileIterator` yielding `(userKey, sequence, type, value)` tuples including tombstones used during DAG based diff delta SST scan.
- **K-way merge heap**: Min-heap ordered by `(userKey ASC, sequence DESC)` to dedupe to the latest visible version per userKey. It guarantees $O(N \log K)$ time complexity for $N$ keys across $K$ SST files, ensuring sequential disk I/O.

---

## 4. Optimized DAG-Based Diff Implementation Stages

The DAG-based diff optimizes the process by only looking at SST files that changed between snapshots. It identifies the set of SST files that differ between `fromSnapshot` and `toSnapshot` using the `RocksDBCheckpointDiffer` (compaction DAG).

### Stage 1: Sequential Read Flow + Batched Point Lookups + Directory Scans
**Baseline Issue:** Baseline reads these delta files and then performs random reads against the snapshot DBs to find the old/new state of the keys, causing severe I/O bottlenecks.
**Optimized Design (in order):**
1. **Sequential scan of `toSnapshot` diff SSTs:** Use native iterators (`ManagedRawSSTFileIterator`) and a K-way merge to scan the delta SSTs **only in `toSnapshot`**. This yields the latest visible versions for changed keys and populates `newList` (for non-tombstones) plus the `DiffCandidateSet` (all tombstones + all keys with `entry.sequence > fromSnapshot.dbTxSequenceNumber`).
2. **Full table scan of `toSnapshot.directoryTable` (FSO only):** Use `tableIterator` to scan all directory entries sequentially and populate `jobId-to-edges`.
3. **Full table scan of `fromSnapshot.directoryTable` (FSO only):** Use `tableIterator` to scan all directory entries sequentially.
   * Populate `jobId-from-edges` with `(parentId, objectId) -> name`.
   * For directory objectIds that are in the `DiffCandidateSet`, populate `oldList` (build signatures using the value read from the table).
4. **Batch point lookups of `fromSnapshot.file/keyTable`:** Use `multiGet` for keys in the `DiffCandidateSet` that correspond to files and populate `oldList`.

### Stage 2: Merge Join & Classification
A synchronized sequential iteration (merge join) is performed over the `oldList` and `newList` based on `objectID`. Since `oldList` and `newList` are backed by RocksDB the iteration is ordered by the key `objectID`.
*   **Only in `newList`** → `CREATE`
*   **Only in `oldList`** → `DELETE`
*   **In both lists**:
    *   If `parentId` or `name` differs → `RENAME`
    *   If signatures differ → `MODIFY`
    *   Else → ignore

### Stage 3: Deferred BFS with Early Stop + Dependency ordering + Final Write
1. **Run persisted BFS (FSO only)** to resolve paths only for diff entries:
   * Resolve CREATE + RENAME paths from `jobId-to-edges`.
   * Resolve RENAME + MODIFY + DELETE paths from `jobId-from-edges`.
   * Stop once all diff entries are resolved or the entire directory tree is traversed.
   * Remove entries with unresolvable paths from diff lists
3. **Write dependency ordered report to table**
   * Build a dependency graph described in Section 2.7 using `parentId` for resolved entries.
   * Write the topologically sorted report to reportTable.

---

## 5. Optimized Full Diff Implementation Stages

The Full Diff path is used when compaction DAGs are unavailable or a full recalculation is forced.

### Stage 1: Sequential Table Scanning & Filtering
Instead of random lookups, the optimization uses native RocksDB **Table Iterators** to sequentially scan the `directoryTable` and `fileTable` of both snapshots while deferring path resolution until after classification.

**1. `toSnapshot` Directory Scan (FSO only):**
*   Iterates sequentially through the `toSnapshot`'s `directoryTable`.
*   Extracts `updateID` using the lightweight parser. If `updateID <= fromSnapshot.lastTransactionInfo.txIndex`, the entry is unchanged (not created/renamed/modified) and is skipped. Otherwise, its compare signature is built and it is added to the `newList` and recorded in the `DiffCandidateSet`.
*   **Graph Construction:** Regardless of whether the entry is a candidate, the `parentID` and `name` are extracted to build the foundational edges of the `toSnapshot` directory structure graph. This is done by writing `(parentID, objectID) -> name` entries into a temporary RocksDB Column Family (`jobId-to-edges`).

**2. `fromSnapshot` Directory Scan (FSO only):**
*   Iterates sequentially through the `fromSnapshot`'s `directoryTable`.
*   Only processes entries whose `objectID` is in `DiffCandidateSet` during the `toSnapshot` scan. Adds these to the `oldList`.
*   **Graph Construction:** Extracts `parentID` and `name` for all entries to build the `fromSnapshot` directory structure graph by writing to another temporary Column Family (`jobId-from-edges`).

**3. `toSnapshot` Key Scan:**
*   Iterates sequentially through the `toSnapshot`'s `key/fileTable`.
*   Applies the same `updateID` gating logic: skips if `updateID <= fromSnapshot.lastTransactionInfo.txIndex`.
*   Builds the compare signature and adds to `newList`, recording these entries in the `DiffCandidateSet`. No parentID/path checks are performed at this stage.

**4. `fromSnapshot` Key Scan:**
*   Iterates sequentially through the `fromSnapshot`'s `key/fileTable`.
*   Only builds compare signature for entries whose `objectID` was marked in `DiffCandidateSet` during the `toSnapshot` file scan. Adds these to the `oldList`.


### Stage 2: Merge Join & Classification
Same as Stage 2 of DAG based diff implementation.
 

### Stage 3: Top level delete retention (FSO only)
After merge join, 
* Build `deletedDirSet` for deleted directories.
* Compute `deletedRootSet` by removing any directory whose parent is also deleted
* Only report delete entries for the directories in `deletedRootSet`


### Stage 4: Deferred BFS with Early Stop + Dependency ordering + Final Write
Same as Stage 3 of DAG based diff implementation.

---

## 6. Comparison with Baseline & Trade-offs

| Feature | Baseline Implementation | Optimized Implementation                                      |
| :--- | :--- |:--------------------------------------------------------------|
| **Object Parsing** | Full Protobuf Deserialization (Heavy CPU/GC). | `SnapshotDiffValueParser` (Lightweight byte-stream parsing).  |
| **Modification Detection** | Full object equality. | Key `sequence`/`updateID` gating + selective field hashing.   |
| **DAG Diff I/O Pattern** | Random point lookups (`db.get()`) for delta keys. | Sequential reads with K-way merge of SST files.               |
| **Classification Timing** | During report generation. | Deferred until merge join.                                    |
| **Path Resolution** | During report generation for all candidates. | Deferred to diff entries only.                                |
| **Delete Handling** | Emits deletes of descendants inconsistently. | Retains only top level directory deletes, dependency ordered. |
| **Report Ordering** | Naive ordering based on Diff Type. | Dependency ordered with Kahn's algorithm.                     |

### Trade-offs
1.  **Reliance on `updateID` in full diff:** The optimized snapdiff's speed in Full Diff relies heavily on `updateID`. If Ozone has bugs where `updateID` is not bumped during a meaningful metadata change (e.g., parent directory `modificationTime` updates during a child rename), the optimization will miss the modification. Baseline catches this via full comparison, albeit much slower.
2.  **K-way Merge Memory Overhead:** While the DAG optimization drastically reduces random I/O, maintaining a Priority Queue for K-way merging requires slightly more active memory and CPU comparison logic than simple iteration, though this is vastly outweighed by the I/O savings.
3.  **Signature Collisions:** Hash-based comparison assumes no SHA-256 collisions. While statistically negligible, baseline's exact object equality has zero collision risk.
4.  **Dependency Ordering Overhead:** Building and topologically sorting the dependency graph adds some CPU and memory overhead, especially for large delete sets.

## 7. Conclusion
The optimized implementation represents a shift from a compute-and-I/O-heavy approach to a streamlined, sequential, and deferred-evaluation model. By utilizing `SnapshotDiffValueParser` and entry `sequence`/`updateID` gating, CPU cycles and Garbage Collection pauses are drastically reduced. By replacing random reads in the DAG diff with a sequential K-way merge, disk I/O bottlenecks are eliminated. Deferred path resolution, batch RocksDB puts, and dependency ordered output ensure that resources are only spent on actual differences and replay remains consistent. Despite trade-offs around `updateID` reliance and graph ordering overhead, the optimization provides a scalable and accurate snapshot diff engine suitable for massive buckets.

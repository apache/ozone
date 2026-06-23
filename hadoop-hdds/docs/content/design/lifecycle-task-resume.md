/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

# Design for Resumable Lifecycle Scans(HDDS-8342)

## Problem Statement:

The `HDDS-8342` branch introduces the `KeyLifecycleService`, a background service running on the Ozone Manager (OM) Leader to enforce bucket lifecycle rules (expiration, moving to trash, and aborting incomplete multipart uploads).
The entire bucket is scanned in a single `call()` execution. If the OM restarts, crashes, or a leader transfer occurs, the scan state is lost. The new leader must restart the scan from the beginning. 
For buckets with billions of keys, the scan may never complete if leader transfers happen periodically.

## Design: Persisting Bucket Scan Pointers

To solve the resumability issues, we need to persist the scan progress (the "pointer") to the OM DB. This ensures that a new OM leader can resume from where the previous leader left off.

### 2.1 Data Structure for the Scan Pointer

Define a new Protobuf message `LifecycleScanState` to capture the exact position of the scan.

```protobuf
message LifecycleScanState {
    optional string bucketKey = 1;  // e.g., /volume/bucket
    optional uint64 bucketObjID = 2;  // bucket's object ID, in case the bucket is deleted and recreated with same name
    optional uint64 lifecycleConfigurationUpdateID = 3;  // lifecycle configuration update ID, in case the bucket is updated with new rules
    optional uint64 scanStartTime = 4;  // Epoch time when this full scan started
    optional uint64 scanEndTime = 5;  // Epoch time when this full scan is completed
    optional string lastScannedKey = 6;  // the last scanned key in the bucket(for both OBS and FSO)
    optional string lastScannedDir = 7;  // the last scanned dir path, e.g /dir1/dir2/dir3
    optional string lastScannedDirKey = 8; // the last scanned dir key in directoryTable, e.g /0/1/3/dir3
    optional string lastScannedMpuKey = 9;
}
```

### OM DB Schema Updates
Add a new table `lifecycleScanStateTable` to `OMMetadataManager` to store the scan states:
- **Table Name:** `lifecycleScanStateTable`
- **Key:** `bucketKey` (String, e.g., `/volumeName/bucketName`)
- **Value:** `LifecycleScanState`

### When to Persist the Pointer
Persisting the pointer for every key would overwhelm Ratis and RocksDB. We should checkpoint periodically:

1. **Piggybacking on Deletes:** Add an optional `LifecycleScanState` field to `DeleteKeysRequest`. When the OM state machine applies the deletion, it atomically updates the `lifecycleScanStateTable` with the new pointer. This guarantees exactly-once semantics for the scan pointer relative to deletions.
2. **Move to trash**: Since there is no `RenameKeysRequest`, rename has be called multiple times for a batch of keys. We introduce a new OM request `SaveLifecycleScanStateRequest`. After a batch of keys are moved to trash, call `SaveLifecycleScanStateRequest` explicitly to persist the state.
3. **Periodic Standalone Checkpoints:** If no keys are expired (e.g., scanning millions of valid keys), we still need to save progress. The `LifecycleActionTask` will send this request periodically (e.g., every 100,000 keys iterated, or every 1 minute of execution time).
3. **End of Scan:** When the scan for a bucket finishes, a `SaveLifecycleScanStateRequest` is sent to mark state as completed by recording the completion time.

### How to Resume the Scan
When `KeyLifecycleService` schedules a `LifecycleActionTask` for a bucket, it first reads the `LifecycleScanState` from the `lifecycleScanStateTable`.

- **OBS/Legacy Resumption:** 
  The iterator for `keyTable` is initialized to seek to `lastScannedKey` instead of the bucket prefix.
  ```java
  TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr = keyTable.iterator(bucketPrefix);
  if (state.getLastScannedKey() != null) {
      keyTblItr.seek(state.getLastScannedKey());
      // skip the exact match since it was already processed
  }
  ```

- **FSO Resumption:**
  Since FSO bucket is iterated via a Depth-First Search (DFS) way, any directory that is after the `lastScannedDir` in the traversal path can be skipped. 

- **MPU Resumption:**
  // TODO: implement MPU resumption
  The `multipartInfoTable` iterator seeks to `lastScannedMpuKey` and continues.

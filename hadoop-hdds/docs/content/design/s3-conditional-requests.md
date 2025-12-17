---
title: "S3 Conditional Requests"
summary: Design to support S3 conditional requests for atomic operations.
date: 2025-11-20
jira: HDDS-13117
status: draft
author: Chu Cheng Li
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

# S3 Conditional Requests Design

## Background

AWS S3 supports conditional requests using HTTP conditional headers, enabling atomic operations, cache optimization, and preventing race conditions. This includes:

- **Conditional Writes** (PutObject): `If-Match` and `If-None-Match` headers for atomic operations
- **Conditional Reads** (GetObject, HeadObject): `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` for cache validation
- **Conditional Copy** (CopyObject): Conditions on both source and destination objects

### Current State

- HDDS-10656 implemented atomic rewrite using `expectedDataGeneration`
- OM HA uses single Raft group with single applier thread (Ratis StateMachineUpdater)
- S3 gateway doesn't expose conditional headers to OM layer

## Use Cases

### Conditional Writes

- **Atomic key rewrites**: Prevent race conditions when updating existing objects
- **Create-only semantics**: Prevent accidental overwrites (`If-None-Match: *`)
- **Optimistic locking**: Enable concurrent access with conflict detection
- **Leader election**: Implement distributed coordination using S3 as backing store

### Conditional Reads

- **Bandwidth optimization**: Avoid downloading unchanged objects (304 Not Modified)
- **HTTP caching**: Support standard browser/CDN caching semantics
- **Conditional processing**: Only process objects that meet specific criteria

### Conditional Copy

- **Atomic copy operations**: Copy only if source/destination meets specific conditions
- **Prevent overwrite**: Copy only if destination doesn't exist

## Specification

### AWS S3 Conditional Write Specification

#### If-None-Match Header

```
If-None-Match: "*"
```

- Succeeds only if object does NOT exist
- Returns `412 Precondition Failed` if object exists
- Primary use case: Create-only semantics

#### If-Match Header

```
If-Match: "<etag>"
```

- Succeeds only if object EXISTS and ETag matches
- Returns `412 Precondition Failed` if object doesn't exist or ETag mismatches
- Primary use case: Atomic updates (compare-and-swap)

#### Restrictions

- Cannot use both headers together in same request
- No additional charges for failed conditional requests

### AWS S3 Conditional Read Specification

TODO

### AWS S3 Conditional Copy Specification

TODO

## Implementation

### AWS S3 Conditional Write Implementation

The implementation aims to minimize Redundant RPCs (RTT) while ensuring strict atomicity for conditional operations.

- **If-None-Match** utilizes the atomic "Create-If-Not-Exists" capability ([HDDS-13963](https://issues.apache.org/jira/browse/HDDS-13963 "null")).
- **If-Match** optimizes the happy path by pushing ETag validation directly into the Ozone Manager's write path, avoiding preliminary read operations.

#### If-None-Match Implementation

This implementation ensures strict create-only semantics by utilizing a specific generation ID marker.

In `OzoneConsts.java`, add the `-1` as a constant for readability:
```java
/**
 * Special value for expectedDataGeneration to indicate "Create-If-Not-Exists" semantics.
 * When used with If-None-Match conditional requests, this ensures atomicity:
 * if a concurrent write commits between Create and Commit phases, the commit
 * fails the validation check, preserving strict create-if-not-exists semantics.
 */
public static final long EXPECTED_DATA_GENERATION_CREATE_IF_NOT_EXISTS = -1L;
```

##### S3 Gateway Layer

1. Parse `If-None-Match: *`.
2. Set `existingKeyGeneration = OzoneConsts.EXPECTED_DATA_GENERATION_CREATE_IF_NOT_EXISTS`.
3. Call `RpcClient.rewriteKey()`.

##### OM Create Phase

1. OM receives request with `expectedDataGeneration == OzoneConsts.EXPECTED_DATA_GENERATION_CREATE_IF_NOT_EXISTS`.
2. **Pre-check**: If key is already in the OpenKeyTable or KeyTable, throw `KEY_ALREADY_EXISTS`.
3. If not exists, proceed to create the open key entry.

##### OM Commit Phase (Atomicity)

1. During the commit phase (or strict atomic create), the OM validates that the key still does not exist.
2. If a concurrent client created the key between the Create and Commit phases, the transaction fails with `KET_GENERATION_MISMATCH`.

##### Race Condition Handling

Using `OzoneConsts.EXPECTED_DATA_GENERATION_CREATE_IF_NOT_EXISTS = -1` ensures atomicity. If a concurrent write (Client B) commits between Client A's Create and Commit,
Client A's commit fails the `CREATE IF NOT EXISTS` validation check, preserving strict create-if-not-exists semantics.

> **Note**: This ability will be added along with [HDDS-13963](https://issues.apache.org/jira/browse/HDDS-13963) (Atomic Create-If-Not-Exists).

#### If-Match Implementation

To optimize performance and reduce latency, we avoid a pre-flight check (GetS3KeyDetails) and instead validate the ETag during the OM Write operation.
This requires adding an optional `expectedETag` field to `KeyArgs`. This approach optimizes the "happy path" (successful match) by removing an extra network round trip.
For failing requests, they still incur the cost of a write RPC and Raft log entry, but this is acceptable under optimistic concurrency control assumptions.

##### S3 Gateway Layer

1. Parse `If-Match: "<etag>"` header.
2. Populate `KeyArgs` with the parsed `expectedETag`.
3. Send the write request (CreateKey) to OM.

##### OM Create Phase

Validation is performed within the `validateAndUpdateCache` method to ensure atomicity within the Ratis state machine application.

1. **Locking**: The OM acquires the write lock for the bucket/key.
2. **Key Lookup**: Retrieve the existing key from `KeyTable`.
3. **Validation**:
    - **Key Not Found**: If the key does not exist, throw `KEY_NOT_FOUND` (maps to S3 412).
    - **No ETag Metadata**: If the existing key (e.g., uploaded via OFS) does not have an ETag property, throw `ETAG_NOT_AVAILABLE` (maps to S3 412). The precondition cannot be evaluated, so we must fail rather than silently proceed.
    - **ETag Mismatch**: Compare `existingKey.ETag` with `expectedETag`. If they do not match, throw `ETAG_MISMATCH` (maps to S3 412).
4. **Extract Generation**: If ETag matches, extract `existingKey.updateID`.
5. **Create Open Key**: Create open key entry with `expectedDataGeneration = existingKey.updateID`.

##### OM Commit Phase

The commit phase reuses the existing atomic-rewrite validation logic from HDDS-10656:

1. Read open key entry (contains `expectedDataGeneration` set during create phase).
2. Read current committed key from `KeyTable`.
3. Validate `currentKey.updateID == openKey.expectedDataGeneration`.
4. If match, commit succeeds. If mismatch (concurrent modification), throw `KEY_NOT_FOUND` (maps to S3 412).

This approach ensures end-to-end atomicity: even if another client modifies the key between Create and Commit phases, the commit will fail.

#### Error Mapping

|   |   |   |   |
|---|---|---|---|
|**OM Error**|**S3 Status**|**S3 Error Code**|**Scenario**|
|`KEY_ALREADY_EXISTS`|412|PreconditionFailed|If-None-Match failed (key exists)|
|`KEY_NOT_FOUND`|412|PreconditionFailed|If-Match failed (key missing or concurrent modification)|
|`ETAG_NOT_AVAILABLE`|412|PreconditionFailed|If-Match failed (key has no ETag, e.g., created via OFS)|
|`ETAG_MISMATCH`|412|PreconditionFailed|If-Match failed (ETag mismatch)|

## AWS S3 Conditional Read Implementation

TODO

## AWS S3 Conditional Copy Implementation

TODO

## References

- [AWS S3 Conditional Requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)
- [RFC 7232 - HTTP Conditional Requests](https://tools.ietf.org/html/rfc7232)
- [HDDS-10656 - Atomic Rewrite Key](https://issues.apache.org/jira/browse/HDDS-10656)
- [HDDS-13963 - Atomic Create-If-Not-Exists](https://issues.apache.org/jira/browse/HDDS-13963)
- [Leader Election with S3 Conditional Writes](https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/)
- [An MVCC-like columnar table on S3 with constant-time deletes](https://simonwillison.net/2025/Oct/11/mvcc-s3/)

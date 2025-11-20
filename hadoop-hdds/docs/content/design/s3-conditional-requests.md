---
title: "S3 Conditional Requests"
summary: Design to support S3 conditional requests for atomic operations.
date: 2025-11-20
jira: HDDS-13117
status: draft
author: Chu Cheng Li
---

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

## AWS S3 Conditional Write

### Specification

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

### Implementation

#### Architecture Overview

#### If-None-Match Implementation

##### S3 Gateway Layer

1. Parse `If-None-Match: *`.
2. Set `existingKeyGeneration = -1`.
3. Call `RpcClient.rewriteKey()`.

##### OM Create Phase

1. Validate `expectedDataGeneration == -1`.
2. If key exists → throw `KEY_ALREADY_EXISTS`.
3. Store `-1` in open key metadata.

##### OM Commit Phase

1. Check `expectedDataGeneration == -1` from open key.
2. If key now exists (race condition) → throw `KEY_ALREADY_EXISTS`.
3. Commit key.

##### Race Condition Handling

Using `-1` ensures atomicity. If a concurrent write (Client B) commits between Client A's Create and Commit, Client A's commit fails the `-1` validation check (key now exists), preserving strict create-if-not-exists semantics.

#### If-Match Implementation

Leverages existing `expectedDataGeneration` from HDDS-10656:

##### S3 Gateway Layer

1. Parse `If-Match: "<etag>"` header
2. Look up existing key via `getS3KeyDetails()`
3. Validate ETag matches, else throw `PRECOND_FAILED` (412)
4. Extract `expectedGeneration` from existing key
5. Pass `expectedGeneration` to RpcClient

##### OM Create Phase

1. Receive `expectedDataGeneration` parameter
2. Look up current key and validate exists
3. Extract current key's `updateID` value
4. Create open key with `expectedDataGeneration = updateID`
5. Return stream to S3 gateway

##### OM Commit Phase

1. Read open key (contains `expectedDataGeneration`)
2. Read current committed key
3. Validate `current.updateID == openKey.expectedDataGeneration`
4. Commit if match, reject if mismatch (existing HDDS-10656 logic)

#### Error Mapping

| OM Error | S3 Status | S3 Error Code | Scenario |
|----------|-----------|---------------|----------|
| `KEY_ALREADY_EXISTS` | 412 | PreconditionFailed | If-None-Match failed |
| `KEY_NOT_FOUND` | 412 | PreconditionFailed | If-Match failed (key missing) |
| `ETAG_MISMATCH` | 412 | PreconditionFailed | If-Match failed (ETag mismatch) |
| `GENERATION_MISMATCH` | 412 | PreconditionFailed | If-Match failed (concurrent modification) |

## AWS S3 Conditional Read

TODO

## AWS S3 Conditional Copy

TODO

## References

- [AWS S3 Conditional Requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)
- [RFC 7232 - HTTP Conditional Requests](https://tools.ietf.org/html/rfc7232)
- [HDDS-10656 - Atomic Rewrite Key](https://issues.apache.org/jira/browse/HDDS-10656)
- [Leader Election with S3 Conditional Writes](https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/)

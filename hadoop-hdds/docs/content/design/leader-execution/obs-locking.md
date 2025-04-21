---
title: Ozone Granular locking for OBS bucket
summary: Granular locking for OBS bucket
date: 2025-01-06
jira: HDDS-11898
status: draft
author: Sumit Agrawal 
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

# OBS locking

OBS case just involves volume, bucket and key. So this is more simplified in terms of locking.

There will be:
1. Volume Lock: locking for volume
2. Bucket Lock: locking for bucket
3. Key Lock: Locking for key

**Note**: Multiple keys locking (like delete multiple keys or rename operation), lock needs to be taken in order to avoid deadlock.

### Striped locking (java builtin class)
Locking class: com.google.common.util.concurrent.Striped

This reduces needs of maintaining list of lock object for all keys, and make use of set of locks pre-allocated.
Lock object is obtained from these lock set based on hash index of key.

Striped locking ordering: Striped class have method to provide locks in order, `bulkGet()`. This internally,
- Index is obtained from hash of key and lock object is ordered based on index and returned the ordered lock object.
- locks to be taken in same order as returned by `bulkGet()`

## Bucket and volume locking as required for concurrency for obs key handling

### Volume Operation
All operation over volume has taken write lock to avoid,
- delete volume which may add entry if parallel operation add entry with updated volume info to rocksdb.
- bucket creation which takes read lock to retrieve latest information from volume as present like acls
- bucket deletion which takes read lock

| API Name     | Locking Key       | Notes                                                                                          |
|--------------|-------------------|------------------------------------------------------------------------------------------------|
| CreateVolume | Volume Write lock | volume level write lock to avoid parallel volume creation and deletion                         |
| DeleteVolume | Volume Write lock | volume level write lock to avoid parallel volume creation and deletion                         |
| SetProperty  | Volume Write lock | volume property update like owner and quota, avoid parallel operation like delete volume       |
| SetAcl       | Volume Write lock | volume acl updated, avoid parallel operation like delete volume and bucket creation / deletion |
| AddAcl       | Volume Write lock | volume acl updated, avoid parallel operation like delete volume and bucket creation / deletion |
| RemoveAcl    | Volume Write lock | volume acl updated, avoid parallel operation like delete volume and bucket creation / deletion |

### Bucket Operation

| API Name     | Locking Key                    | Notes                                                                                                                           |
|--------------|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| CreateBucket | Volume Read, Bucket Write lock | creates bucket, volume level read lock to avoid parallel volume deletion                                                        |
| DeleteBucket | Volume Read, Bucket Write lock | creates bucket, volume level read lock to avoid parallel volume deletion                                                        |
| SetProperty  | Bucket Write lock              | bucket property update like owner, replication, quota and so on, lock to avoid key operation which depends on bucket properties |
| SetAcl       | Bucket Write lock              | bucket acl updated, block key operation                                                                                         |
| AddAcl       | Bucket Write lock              | bucket acl updated, block key operation                                                                                         |
| RemoveAcl    | Bucket Write lock              | bucket acl updated, block key operation                                                                                         |

## OBS operation
Bucket read lock will be there default. This is to ensure:
- key operation uses updated bucket acl, quota and other properties
- key does not becomes dandling when parallel bucket is deleted

Note: Volume lock is not required as key depends on bucket only to retrieve information and bucket as parent.

For key operations in OBS buckets, the following concurrency control is proposed:

| API Name                | Locking Key                               | Notes                                                                                                       |
|-------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| CreateKey               | Bucket Read Lock, `No Lock` for key       | Key can be created parallel by client in open key table, so do not need key lock                            |
| CommitKey               | Bucket Read, Key Write lock               | Avoid parallel key commit by different client, otherwise it can leave dangling blocks on overwrite          |
| InitiateMultiPartUpload | Bucket Read Lock, `No Lock` for key       | Key can be created parallel by client with different uploadId in open key table, so do not need key lock    |
| CommitMultiPartUpload   | WriteLock: PartKey Name                   | Avoid same part commit in parallel, else it can leave dangling blocks on overwrite                          |
| CompleteMultiPartUpload | Bucket Read, Key Write lock               | Avoid parallel multi-part upload complete with different uploadId, else overwrite can cause dangling blocks |
| AbortMultiPartUpload    | Bucket Read, Key Write lock               | Need to avoid other operation in parallel like commit part                                                  |
| DeleteKey               | Bucket Read, Key Write lock               | Avoid create and delete in parallel                                                                         |
| DeleteKeys              | Bucket Read, Key Write with ordered lock  | Avoid create and delete in parallel, ordered key locks to avoid deadlock                                    |
| RenameKey               | Bucket Read, Keys Write with ordered lock | lock in order to delete key from original location and move to new location                                 |
| SetAcl                  | Bucket Read, Key Write lock               | Avoid parallel delete key                                                                                   |
| AddAcl                  | Bucket Read, Key Write lock               | Avoid parallel delete key                                                                                   |
| RemoveAcl               | Bucket Read, Key Write lock               | Avoid parallel delete key                                                                                   |
| AllocateBlock           | Bucket Read, Key Write lock               | Need lock key to avoid wrong update of key, if same client does parallel allocate                           |
| SetTimes                | Bucket Read, Key Write lock               | Avoid parallel delete key                                                                                   |

Batch Operation:
1. deleteKeys: batch will be divided to multiple threads in Execution Pool to run parallel calling DeleteKey
2. RenameKeys: This is `depreciated`, but for compatibility, will be divided to multiple threads in Execution Pool to run parallel calling RenameKey

For batch operation, atomicity is not guaranteed for above api, and same is behavior for s3 perspective.

## Lock operation timeout
Currently, lock is taken within a scope of a method, i.e.` validateAndUpdateCache()` of implementation method for handling request.

But as per new change, lock is taken involving remote call,
- take the lock for the key involved in request
- prepare db changes for the request
- send to all nodes to update db (ie involve remote handling by follower nodes)
- on response, release the lock

This adds risk for infinite locking if there is some error in network or request handling. So a timeout is required for lock over a key.

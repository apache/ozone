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

Like for key commit operation, it needs,
1. Volume: `<No lock required similar to existing>`
1. Bucket: Read lock
2. Key: write lock

There will be:
1. BucketStripLock: locking bucket operation
2. KeyStripLock: Locking key operation

**Note**: Multiple keys locking (like delete multiple keys or rename operation), lock needs to be taken in order, i.e. using StrippedLocking order to avoid dead lock.

Stripped locking ordering:
- Strip lock is obtained over a hash bucket.
- All keys needs to be ordered with hash bucket
- And then need take lock in sequence order

## OBS operation
Bucket read lock will be there default.

For key operations in OBS buckets, the following concurrency control is proposed:

| API Name                | Locking Key                           | Notes                                                                                                     |
|-------------------------|---------------------------------------|-----------------------------------------------------------------------------------------------------------|
| CreateKey               | `No Lock` (Only bucket read lock)     | Key can be created parallel by client in open key table and all are exclusive to each other               |
| CommitKey               | WriteLock: Key Name                   | Only one key can be committed at a time with the same name: Without locking OM can leave dangling blocks  |
| InitiateMultiPartUpload | `No Lock` (Only bucket read lock)     | no lock is required as key will be created with upload Id and can be parallel                             |
| CommitMultiPartUpload   | WriteLock: PartKey Name               | Only one part can be committed at a time with the same name: Without locking OM can leave dangling blocks |
| CompleteMultiPartUpload | WriteLock: Key Name                   | Only one key can be completed at a time with the same name: Without locking OM can leave dangling blocks  |
| AbortMultiPartUpload    | WriteLock: Key Name                   | Need avoid abort and commit parallel                                                                      |
| DeleteKey               | WriteLock: Key Name                   | Only one key can be deleted at a time with the same name: Without locking write to DB can fail            |
| RenameKey               | WriteLock: sort(Key Name1, Key Name 2) | Only one key can be renamed at a time with the same name: Without locking OM can leave dangling blocks    |
| SetAcl                  | WriteLock: Key Name                   | Only one key can be updated at a time with the same name                                                  |
| AddAcl                  | WriteLock: Key Name                   | Only one key can be updated at a time with the same name                                                  |
| RemoveAcl               | WriteLock: Key Name                   | Only one key can be updated at a time with the same name                                                  |
| AllocateBlock           | WriteLock: Key Name                   | Only one key can be updated at a time with the same name                                                  |
| SetTimes                | WriteLock: Key Name                   | Only one key can be updated at a time with the same name                                                  |

Batch Operation:
1. deleteKeys: batch will be divided to multiple threads in Execution Pool to run parallel calling DeleteKey
2. RenameKeys: This is `depreciated`, but for compatibility, will be divided to multiple threads in Execution Pool to run parallel calling RenameKey

For batch operation, atomicity is not guranteed for above api, and same is behavior for s3 perspective.

## Bucket and volume locking as required for concurrency for obs key handling

### Volume Operation

| API Name     | Locking Key       | Notes                                                                                                                   |
|--------------|-------------------|-------------------------------------------------------------------------------------------------------------------------|
| CreateVolume | Volume Write lock | volume level write lock to avoid parallel volume creation and deletion                                                  |
| DeleteVolume | Volume Write lock | volume level write lock to avoid parallel volume creation and deletion                                                  |
| SetProperty  | Volume Write lock | volume property update like owner and quota, lock to avoid parallel operation over volume |
| SetAcl       | Volume Write lock | volume acl updated avoid parallel operation over volume                                                              |
| AddAcl       | Volume Write lock | volume acl updated avoid parallel operation over volume                                                                   |
| RemoveAcl    | Volume Write lock | volume acl updated avoid parallel operation over volume                                                                   |

### Bucket Operation

| API Name     | Locking Key                       | Notes                                                                                                                    |
|--------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| CreateBucket | Volume Read and Bucket Write lock | volume level write lock to avoid parallel volume creation and deletion                                                   |
| DeleteBucket | Bucket Write lock                 | volume level write lock to avoid parallel volume creation and deletion                                                   |
| SetProperty  | Bucket Write lock                 | volume property update like owner and quota, lock to avoid any operation happening inside volume at bucket and key level |
| SetAcl       | Bucket Write lock                 | bucket acl updated blocking any operation over bucket and key                                                    |
| AddAcl       | Bucket Write lock                 | bucket acl updated blocking any operation over bucket and key                                                    |
| RemoveAcl    | Bucket Write lock                 | bucket acl updated blocking any operation over bucket and key                                                    |

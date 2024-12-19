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

| API Name                | Locking Key                            | Notes                                                                                                     |
|-------------------------|----------------------------------------|-----------------------------------------------------------------------------------------------------------|
| CreateKey               | `No Lock`                              | Key can be created parallel by client in open key table and all are exclusive to each other               |
| CommitKey               | WriteLock: Key Name                    | Only one key can be committed at a time with the same name: Without locking OM can leave dangling blocks  |
| InitiateMultiPartUpload | `No Lock`                              | no lock is required as key will be created with upload Id and can be parallel                             |
| CommitMultiPartUpload   | WriteLock: PartKey Name                | Only one part can be committed at a time with the same name: Without locking OM can leave dangling blocks |
| CompleteMultiPartUpload | WriteLock: Key Name                    | Only one key can be completed at a time with the same name: Without locking OM can leave dangling blocks  |
| AbortMultiPartUpload    | `No Lock`                              | lock is not required in discarding multiple part upload                                                   |
| DeleteKey               | WriteLock: Key Name                    | Only one key can be deleted at a time with the same name: Without locking write to DB can fail            |
| RenameKey               | WriteLock: sort(Key Name1, Key Name 2) | Only one key can be renamed at a time with the same name: Without locking OM can leave dangling blocks    |
| SetAcl                  | WriteLock: Key Name                    | Only one key can be updated at a time with the same name                                                  |
| AddAcl                  | WriteLock: Key Name                    | Only one key can be updated at a time with the same name                                                  |
| RemoveAcl               | WriteLock: Key Name                    | Only one key can be updated at a time with the same name                                                  |
| AllocateBlock           | WriteLock: Key Name                    | Only one key can be updated at a time with the same name                                                  |
| SetTimes                | WriteLock: Key Name                    | Only one key can be updated at a time with the same name                                                  |

Batch Operation:
1. deleteKeys: batch will be divided to multiple threads in Execution Pool to run parallel calling DeleteKey
2. RenameKeys: This is `depreciated`, but for compatibility, will be divided to multiple threads in Execution Pool to run parallel calling RenameKey

For batch operation, atomicity is not guranteed for above api, and same is behavior for s3 perspective.

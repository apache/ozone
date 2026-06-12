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

package org.apache.hadoop.ozone.om.lock;

import java.util.concurrent.locks.ReadWriteLock;

import com.google.common.util.concurrent.Striped;

/**
 * Granular locking for FSO key operations in the planned execution path.
 *
 * <p>FSO keys are identified by (volumeId, bucketId, parentObjectId, fileName).
 * The lock granularity is at the parent-directory level: operations under the
 * same parent are serialized via a striped write lock keyed on parentObjectId.
 *
 * <p>CreateKey: bucket read lock + parent-directory write lock.
 * <p>CommitKey: bucket read lock + parent-directory write lock.
 * <p>Lock ordering: bucket read lock first, then parent-directory write lock.
 *
 * <p>The parent-directory lock serializes operations that modify the same
 * directory's children (file creation, rename into/out of directory, etc.)
 * while allowing concurrent operations in different directories.
 */
public final class FSOKeyLockManager {

  private static final int DEFAULT_STRIPE_COUNT = 1024;

  private final Striped<ReadWriteLock> parentDirStripedLocks;
  private final IOzoneManagerLock omLock;
  private final int stripeCount;

  public FSOKeyLockManager(IOzoneManagerLock omLock) {
    this(omLock, DEFAULT_STRIPE_COUNT);
  }

  public FSOKeyLockManager(IOzoneManagerLock omLock, int stripeCount) {
    this.omLock = omLock;
    this.stripeCount = stripeCount;
    this.parentDirStripedLocks = Striped.readWriteLock(stripeCount);
  }

  public void acquireBucketReadLock(String volumeName, String bucketName) {
    omLock.acquireReadLock(OzoneManagerLock.LeveledResource.BUCKET_LOCK,
        volumeName, bucketName);
  }

  public void releaseBucketReadLock(String volumeName, String bucketName) {
    omLock.releaseReadLock(OzoneManagerLock.LeveledResource.BUCKET_LOCK,
        volumeName, bucketName);
  }

  /**
   * Acquires a write lock on the parent directory identified by its objectId.
   * All file/key operations under the same parent directory will be serialized.
   *
   * @param volumeId the volume object ID
   * @param bucketId the bucket object ID
   * @param parentObjectId the parent directory's object ID
   */
  public void acquireParentDirWriteLock(long volumeId, long bucketId,
      long parentObjectId) {
    String lockKey = volumeId + "/" + bucketId + "/" + parentObjectId;
    parentDirStripedLocks.get(lockKey).writeLock().lock();
  }

  public void releaseParentDirWriteLock(long volumeId, long bucketId,
      long parentObjectId) {
    String lockKey = volumeId + "/" + bucketId + "/" + parentObjectId;
    parentDirStripedLocks.get(lockKey).writeLock().unlock();
  }

  /**
   * Acquires a read lock on the parent directory. Use this when the operation
   * only reads children (e.g., listing) and doesn't modify the directory.
   */
  public void acquireParentDirReadLock(long volumeId, long bucketId,
      long parentObjectId) {
    String lockKey = volumeId + "/" + bucketId + "/" + parentObjectId;
    parentDirStripedLocks.get(lockKey).readLock().lock();
  }

  public void releaseParentDirReadLock(long volumeId, long bucketId,
      long parentObjectId) {
    String lockKey = volumeId + "/" + bucketId + "/" + parentObjectId;
    parentDirStripedLocks.get(lockKey).readLock().unlock();
  }

  public int getStripeCount() {
    return stripeCount;
  }
}

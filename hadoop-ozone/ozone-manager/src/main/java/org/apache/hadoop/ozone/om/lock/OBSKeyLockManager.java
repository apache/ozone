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
 * Granular locking for OBS key operations in the planned execution path.
 *
 * <p>CreateKey: bucket read lock only (openKeyTable entries unique per clientID).
 * <p>CommitKey: bucket read lock + striped key write lock.
 * <p>Lock ordering: bucket read lock first, then key write lock.
 */
public final class OBSKeyLockManager {

  private static final int DEFAULT_STRIPE_COUNT = 1024;

  private final Striped<ReadWriteLock> keyStripedLocks;
  private final IOzoneManagerLock omLock;
  private final int stripeCount;

  public OBSKeyLockManager(IOzoneManagerLock omLock) {
    this(omLock, DEFAULT_STRIPE_COUNT);
  }

  public OBSKeyLockManager(IOzoneManagerLock omLock, int stripeCount) {
    this.omLock = omLock;
    this.stripeCount = stripeCount;
    this.keyStripedLocks = Striped.readWriteLock(stripeCount);
  }

  public void acquireBucketReadLock(String volumeName, String bucketName) {
    omLock.acquireReadLock(OzoneManagerLock.LeveledResource.BUCKET_LOCK,
        volumeName, bucketName);
  }

  public void releaseBucketReadLock(String volumeName, String bucketName) {
    omLock.releaseReadLock(OzoneManagerLock.LeveledResource.BUCKET_LOCK,
        volumeName, bucketName);
  }

  public void acquireKeyWriteLock(String volumeName, String bucketName, String keyName) {
    String lockKey = volumeName + "/" + bucketName + "/" + keyName;
    keyStripedLocks.get(lockKey).writeLock().lock();
  }

  public void releaseKeyWriteLock(String volumeName, String bucketName, String keyName) {
    String lockKey = volumeName + "/" + bucketName + "/" + keyName;
    keyStripedLocks.get(lockKey).writeLock().unlock();
  }

  public int getStripeCount() {
    return stripeCount;
  }
}

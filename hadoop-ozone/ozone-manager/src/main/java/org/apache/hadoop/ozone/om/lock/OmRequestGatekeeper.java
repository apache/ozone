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

import com.google.common.util.concurrent.Striped;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.util.Time;

/**
 * Manage locking of volume, bucket, keys and others.
 */
public class OmRequestGatekeeper {
  private static final long LOCK_TIMEOUT_DEFAULT = 10 * 60 * 1000;
  private static final int NUM_VOLUME_STRIPES = 1024;
  private static final int NUM_BUCKET_STRIPES = 1024;
  private static final int NUM_KEY_STRIPES = 4096;

  private final Striped<ReadWriteLock> volumeLocks;
  private final Striped<ReadWriteLock> bucketLocks;
  private final Striped<ReadWriteLock> keyLocks;

  public OmRequestGatekeeper() {
    volumeLocks = SimpleStriped.readWriteLock(NUM_VOLUME_STRIPES, false);
    bucketLocks = SimpleStriped.readWriteLock(NUM_BUCKET_STRIPES, false);
    keyLocks = SimpleStriped.readWriteLock(NUM_KEY_STRIPES, false);
  }

  public OmLockObject lock(OmLockInfo lockInfo) throws IOException {
    OmLockObject omLockObject = new OmLockObject(lockInfo);
    List<Lock> locks = omLockObject.getLocks();
    long startTime = Time.monotonicNowNanos();
    Optional<OmLockInfo.LockInfo> optionalVolumeLock = lockInfo.getVolumeLock();
    Optional<OmLockInfo.LockInfo> optionalBucketLock = lockInfo.getBucketLock();
    Optional<Set<OmLockInfo.LockInfo>> optionalKeyLocks = lockInfo.getKeyLocks();

    if (optionalVolumeLock.isPresent()) {
      OmLockInfo.LockInfo volumeLockInfo = optionalVolumeLock.get();
      if (volumeLockInfo.isWriteLock()) {
        omLockObject.setLockStatType(OmLockStats.Type.WRITE);
        locks.add(volumeLocks.get(volumeLockInfo.getName()).writeLock());
      } else {
        locks.add(volumeLocks.get(volumeLockInfo.getName()).readLock());
      }
    }

    if (optionalBucketLock.isPresent()) {
      OmLockInfo.LockInfo bucketLockInfo = optionalBucketLock.get();
      if (bucketLockInfo.isWriteLock()) {
        omLockObject.setLockStatType(OmLockStats.Type.WRITE);
        locks.add(bucketLocks.get(bucketLockInfo.getName()).writeLock());
      } else {
        locks.add(bucketLocks.get(bucketLockInfo.getName()).readLock());
      }
    }

    if (optionalKeyLocks.isPresent()) {
      for (ReadWriteLock keyLock: keyLocks.bulkGet(optionalKeyLocks.get())) {
        omLockObject.setLockStatType(OmLockStats.Type.WRITE);
        locks.add(keyLock.writeLock());
      }
    }

    try {
      acquireLocks(locks);
      lockStatsBegin(omLockObject.getLockStats(), Time.monotonicNowNanos(), startTime);
    } catch (InterruptedException e) {
      locks.clear();
      Thread.currentThread().interrupt();
      throw new OMException("waiting for locks is interrupted", OMException.ResultCodes.INTERNAL_ERROR);
    } catch (TimeoutException e) {
      locks.clear();
      throw new OMException("Unable to get locks, timeout occurred", OMException.ResultCodes.TIMEOUT);
    }
    return omLockObject;
  }

  /*
  Optional: If we want more diagnostic info on the type of lock that failed to be acquired (volume, bucket, or key),
  We can make the parameter a list of objects that wrap the Lock with information about its type.
  Note that logging the specific volume, bucket or keys this lock was trying to acquire is not helpful and
  misleading because collisions within the stripe lock might mean we are blocked on a request for a completely
  different part of the namespace.
  Obtaining the thread ID that we were waiting on would be more useful, but there is no easy way to do that.
   */
  private void acquireLocks(List<Lock> locks) throws TimeoutException, InterruptedException {
    List<Lock> acquiredLocks = new ArrayList<>(locks.size());
    for (Lock lock: locks) {
      if (lock.tryLock(LOCK_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS)) {
        try {
          acquiredLocks.add(lock);
        } catch (Throwable e) {
          // We acquired this lock but were unable to add it to our acquired locks list.
          lock.unlock();
          releaseLocks(acquiredLocks);
          throw e;
        }
      } else {
        releaseLocks(acquiredLocks);
        throw new TimeoutException("Failed to acquire lock after the given timeout.");
      }
    }
  }

  public void unlock(OmLockObject lockObject) {
    releaseLocks(lockObject.getLocks());
    lockStatsEnd(lockObject.getLockStats(), lockObject.getLockStatType());
    lockObject.getLocks().clear();
  }

  private void releaseLocks(List<Lock> locks) {
    ListIterator<Lock> reverseIterator = locks.listIterator(locks.size());
    while (reverseIterator.hasPrevious()) {
      Lock lock = reverseIterator.previous();
      lock.unlock();
    }
  }

  private static void lockStatsBegin(OmLockStats lockStats, long endTime, long startTime) {
    lockStats.add(endTime - startTime, OmLockStats.Type.WAIT);
    lockStats.setLockStartTime(endTime);
  }

  private static void lockStatsEnd(OmLockStats lockStats, OmLockStats.Type type) {
    if (lockStats.getLockStartTime() > 0) {
      lockStats.add(Time.monotonicNowNanos() - lockStats.getLockStartTime(), type);
    }
  }

  /**
   * Lock information after taking locks.
   */
  public static class OmLockObject {
    private final OmLockInfo omLockInfo;
    private final List<Lock> locks = new ArrayList<>();
    private final OmLockStats lockStats = new OmLockStats();
    private OmLockStats.Type lockStatType = OmLockStats.Type.READ;

    public OmLockObject(OmLockInfo lockInfoProvider) {
      this.omLockInfo = lockInfoProvider;
    }

    public List<Lock> getLocks() {
      return locks;
    }

    public OmLockStats getLockStats() {
      return lockStats;
    }

    public OmLockStats.Type getLockStatType() {
      return lockStatType;
    }

    public void setLockStatType(OmLockStats.Type lockStatType) {
      this.lockStatType = lockStatType;
    }

    public OmLockInfo getOmLockInfo() {
      return omLockInfo;
    }
  }

  /**
   * lock stats.
   */
  public static class OmLockStats {
    private long lockStartTime;
    private long waitLockNanos;
    private long readLockNanos;
    private long writeLockNanos;

    public long getLockStartTime() {
      return lockStartTime;
    }

    public void setLockStartTime(long lockStartTime) {
      this.lockStartTime = lockStartTime;
    }

    public long getWaitLockNanos() {
      return waitLockNanos;
    }

    public long getReadLockNanos() {
      return readLockNanos;
    }

    public long getWriteLockNanos() {
      return writeLockNanos;
    }

    void add(long timeNanos, Type type) {
      switch (type) {
      case WAIT:
        waitLockNanos += timeNanos;
        break;
      case READ:
        readLockNanos += timeNanos;
        break;
      case WRITE:
        writeLockNanos += timeNanos;
        break;
      default:
      }
    }

    /**
     * lock time stat type.
     */
    public enum Type {
      WAIT,
      READ,
      WRITE
    }
  }
}

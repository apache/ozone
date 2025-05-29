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
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
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
    long startTime = Time.monotonicNowNanos();
    Optional<OmLockInfo.LockInfo> optionalVolumeLock = lockInfo.getVolumeLock();
    Optional<OmLockInfo.LockInfo> optionalBucketLock = lockInfo.getBucketLock();
    Optional<Set<OmLockInfo.LockInfo>> optionalKeyLocks = lockInfo.getKeyLocks();
    List<Lock> locks = new ArrayList<>();

    if (optionalVolumeLock.isPresent()) {
      OmLockInfo.LockInfo volumeLockInfo = optionalVolumeLock.get();
      if (volumeLockInfo.isWriteLock()) {
        omLockObject.setReadStatsType(false);
        locks.add(volumeLocks.get(volumeLockInfo.getName()).writeLock());
      } else {
        locks.add(volumeLocks.get(volumeLockInfo.getName()).readLock());
      }
    }

    if (optionalBucketLock.isPresent()) {
      OmLockInfo.LockInfo bucketLockInfo = optionalBucketLock.get();
      if (bucketLockInfo.isWriteLock()) {
        omLockObject.setReadStatsType(false);
        locks.add(bucketLocks.get(bucketLockInfo.getName()).writeLock());
      } else {
        locks.add(bucketLocks.get(bucketLockInfo.getName()).readLock());
      }
    }

    if (optionalKeyLocks.isPresent()) {
      for (ReadWriteLock keyLock: keyLocks.bulkGet(optionalKeyLocks.get())) {
        omLockObject.setReadStatsType(false);
        locks.add(keyLock.writeLock());
      }
    }

    try {
      acquireLocks(locks, omLockObject.getLocks());
      lockStatsBegin(omLockObject.getLockStats(), Time.monotonicNowNanos(), startTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OMException("Waiting for locks is interrupted, " + lockInfo, OMException.ResultCodes.INTERNAL_ERROR);
    } catch (TimeoutException e) {
      throw new OMException("Timeout occurred for locks " + lockInfo, OMException.ResultCodes.TIMEOUT);
    }
    return omLockObject;
  }

  private void acquireLocks(List<Lock> locks, Stack<Lock> acquiredLocks) throws TimeoutException, InterruptedException {
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

  private void releaseLocks(Stack<Lock> locks) {
    while (!locks.empty()) {
      locks.pop().unlock();
    }
  }

  private static void lockStatsBegin(OmLockStats lockStats, long endTime, long startTime) {
    lockStats.addWaitLockNanos(endTime - startTime);
    lockStats.setLockStartTime(endTime);
  }

  private static void lockStatsEnd(OmLockStats lockStats, boolean readStatsType) {
    if (lockStats.getLockStartTime() > 0) {
      if (readStatsType) {
        lockStats.addReadLockNanos(Time.monotonicNowNanos() - lockStats.getLockStartTime());
      } else {
        lockStats.addWriteLockNanos(Time.monotonicNowNanos() - lockStats.getLockStartTime());
      }
    }
  }

  /**
   * Lock information after taking locks, and to be used to release locks.
   */
  public static class OmLockObject implements AutoCloseable {
    private final OmLockInfo omLockInfo;
    private final Stack<Lock> locks = new Stack<>();
    private final OmLockStats lockStats = new OmLockStats();
    private boolean readStatsType = true;

    public OmLockObject(OmLockInfo lockInfoProvider) {
      this.omLockInfo = lockInfoProvider;
    }

    public Stack<Lock> getLocks() {
      return locks;
    }

    public OmLockStats getLockStats() {
      return lockStats;
    }

    public void setReadStatsType(boolean readStatsType) {
      this.readStatsType = readStatsType;
    }

    public boolean getReadStatsType() {
      return readStatsType;
    }

    public OmLockInfo getOmLockInfo() {
      return omLockInfo;
    }

    @Override
    public void close() throws IOException {
      while (!locks.empty()) {
        locks.pop().unlock();
      }
      lockStatsEnd(lockStats, readStatsType);
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

    void addWaitLockNanos(long timeNanos) {
      waitLockNanos += timeNanos;
    }

    void addReadLockNanos(long timeNanos) {
      readLockNanos += timeNanos;
    }

    void addWriteLockNanos(long timeNanos) {
      writeLockNanos += timeNanos;
    }
  }
}

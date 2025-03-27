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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * Manage locking of volume, bucket, keys and others.
 */
public class OmLockOperation {
  private static final long LOCK_TIMEOUT_DEFAULT = 10 * 60 * 1000;

  private final WrappedStripedLock keyLocking;
  private final WrappedStripedLock bucketLocking;
  private final WrappedStripedLock volumeLocking;

  public OmLockOperation() {
    keyLocking = new WrappedStripedLock(102400, LOCK_TIMEOUT_DEFAULT, false);
    bucketLocking = new WrappedStripedLock(1024, LOCK_TIMEOUT_DEFAULT, false);
    volumeLocking = new WrappedStripedLock(1024, LOCK_TIMEOUT_DEFAULT, false);
  }

  public OmLockObject lock(OmLockInfo.LockInfoProvider lockInfo) throws IOException {
    OmLockObject omLockObject = new OmLockObject(lockInfo);
    long startTime = Time.monotonicNowNanos();
    try {
      OmLockInfo.LockLevel level = lockInfo.getLevel();
      switch (level) {
      case VOLUME:
        volumeLock((OmLockInfo.VolumeLockInfo) lockInfo, omLockObject);
        break;
      case BUCKET:
        bucketLock((OmLockInfo.BucketLockInfo) lockInfo, omLockObject);
        break;
      case KEY:
        keyLock((OmLockInfo.KeyLockInfo) lockInfo, omLockObject);
        break;
      case MULTI_KEY:
        multiKeyLock((OmLockInfo.MultiKeyLockInfo) lockInfo, omLockObject);
        break;
      default:
        throw new OMException("Unsupported lock level", OMException.ResultCodes.INTERNAL_ERROR);
      }
      lockStatsBegin(omLockObject.getLockStats(), Time.monotonicNowNanos(), startTime);
    } catch (IOException e) {
      unlock(omLockObject);
      throw e;
    }
    return omLockObject;
  }

  private void volumeLock(OmLockInfo.VolumeLockInfo lockInfo, OmLockObject omLockObject) throws IOException {
    List<Lock> locks = omLockObject.getLocks();
    if (lockInfo.getAction() == OmLockInfo.LockAction.READ) {
      locks.add(getLock(volumeLocking::readLock, lockInfo.getVolumeName()));
    } else if (lockInfo.getAction() == OmLockInfo.LockAction.WRITE) {
      locks.add(getLock(volumeLocking::writeLock, lockInfo.getVolumeName()));
      omLockObject.setLockStatType(OmLockStats.Type.WRITE);
    }
  }

  private void bucketLock(OmLockInfo.BucketLockInfo lockInfo, OmLockObject omLockObject) throws IOException {
    if (null != lockInfo.getVolumeLockInfo()) {
      volumeLock(lockInfo.getVolumeLockInfo(), omLockObject);
    }
    List<Lock> locks = omLockObject.getLocks();
    if (lockInfo.getAction() == OmLockInfo.LockAction.READ) {
      locks.add(getLock(bucketLocking::readLock, lockInfo.getBucketName()));
    } else if (lockInfo.getAction() == OmLockInfo.LockAction.WRITE) {
      locks.add(getLock(bucketLocking::writeLock, lockInfo.getBucketName()));
      omLockObject.setLockStatType(OmLockStats.Type.WRITE);
    }
  }

  private void keyLock(OmLockInfo.KeyLockInfo lockInfo, OmLockObject omLockObject) throws IOException {
    bucketLock(lockInfo.getBucketLockInfo(), omLockObject);
    List<Lock> locks = omLockObject.getLocks();
    if (lockInfo.getAction() == OmLockInfo.LockAction.READ) {
      locks.add(getLock(keyLocking::readLock, lockInfo.getKey()));
    } else if (lockInfo.getAction() == OmLockInfo.LockAction.WRITE) {
      locks.add(getLock(keyLocking::writeLock, lockInfo.getKey()));
      omLockObject.setLockStatType(OmLockStats.Type.WRITE);
    }
  }

  private void multiKeyLock(OmLockInfo.MultiKeyLockInfo lockInfo, OmLockObject omLockObject) throws IOException {
    bucketLock(lockInfo.getBucketLockInfo(), omLockObject);
    List<Lock> locks = omLockObject.getLocks();
    if (lockInfo.getAction() == OmLockInfo.LockAction.READ) {
      getLock(keyLocking::readLock, lockInfo.getKeyList(), locks);
    } else if (lockInfo.getAction() == OmLockInfo.LockAction.WRITE) {
      getLock(keyLocking::writeLock, lockInfo.getKeyList(), locks);
      omLockObject.setLockStatType(OmLockStats.Type.WRITE);
    }
  }

  private static Lock getLock(
      CheckedFunction<String, Lock, InterruptedException> lockFunction, String name) throws OMException {
    try {
      Lock lockObj = lockFunction.apply(name);
      if (lockObj == null) {
        throw new OMException("Unable to get lock for " + name + ", timeout occurred", OMException.ResultCodes.TIMEOUT);
      }
      return lockObj;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OMException("waiting for lock is interrupted for " + name, OMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  private static void getLock(
      CheckedBiFunction<List<String>, List<Lock>, Boolean, InterruptedException> lockFunction,
      List<String> lockKeys, List<Lock> lockList) throws OMException {
    try {
      if (!lockFunction.apply(lockKeys, lockList)) {
        throw new OMException("Unable to get locks, timeout occurred", OMException.ResultCodes.TIMEOUT);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OMException("waiting for locks is interrupted", OMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  public void unlock(OmLockObject lockObject) {
    Collections.reverse(lockObject.getLocks());
    lockObject.getLocks().forEach(Lock::unlock);
    lockStatsEnd(lockObject.getLockStats(), lockObject.getLockStatType());
    lockObject.getLocks().clear();
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
   * Lock information.
   */
  public static class OmLockObject {
    private final OmLockInfo.LockInfoProvider lockInfoProvider;
    private final List<Lock> locks = new ArrayList<>();
    private final OmLockStats lockStats = new OmLockStats();
    private OmLockStats.Type lockStatType = OmLockStats.Type.READ;

    public OmLockObject(OmLockInfo.LockInfoProvider lockInfoProvider) {
      this.lockInfoProvider = lockInfoProvider;
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

    public OmLockInfo.LockInfoProvider getLockInfoProvider() {
      return lockInfoProvider;
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

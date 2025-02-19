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
 * Manage locking of volume, bucket and keys.
 */
public class OmLockOpr {
  private static final long LOCK_TIMEOUT_DEFAULT = 10 * 60 * 1000;

  private final WrappedStripedLock keyLocking;
  private final WrappedStripedLock bucketLocking;
  private final WrappedStripedLock volumeLocking;

  public OmLockOpr() {
    keyLocking = new WrappedStripedLock(102400, LOCK_TIMEOUT_DEFAULT, false);
    bucketLocking = new WrappedStripedLock(1024, LOCK_TIMEOUT_DEFAULT, false);
    volumeLocking = new WrappedStripedLock(1024, LOCK_TIMEOUT_DEFAULT, false);
  }

  public OmLockInfo volumeReadLock(String volumeName) throws IOException {
    return lockOneKey(volumeLocking::readLock, volumeName, OmLockInfo.LockOpType.WAIT);
  }

  public OmLockInfo volumeWriteLock(String volumeName) throws IOException {
    return lockOneKey(volumeLocking::writeLock, volumeName, OmLockInfo.LockOpType.WAIT);
  }

  public OmLockInfo volBucketRWLock(String volumeName, String bucketName) throws IOException {
    OmLockInfo omLockInfo = new OmLockInfo();
    List<Lock> locks = omLockInfo.getLocks();
    long startTime = Time.monotonicNowNanos();
    try {
      locks.add(getLock(volumeLocking::readLock, volumeName));
      locks.add(getLock(bucketLocking::writeLock, bucketName));
      long endTime = Time.monotonicNowNanos();
      omLockInfo.add(endTime - startTime, OmLockInfo.LockOpType.WAIT);
      omLockInfo.setLockTakenTime(endTime);
      return omLockInfo;
    } catch (IOException ex) {
      writeUnlock(omLockInfo);
      throw ex;
    }
  }

  public OmLockInfo bucketWriteLock(String bucketName) throws IOException {
    return lockOneKey(bucketLocking::writeLock, bucketName, OmLockInfo.LockOpType.WAIT);
  }

  public OmLockInfo bucketReadLock(String bucketName) throws IOException {
    return lockOneKey(bucketLocking::readLock, bucketName, OmLockInfo.LockOpType.WAIT);
  }

  private OmLockInfo lockOneKey(
      CheckedFunction<String, Lock, InterruptedException> lockFunction, String name, OmLockInfo.LockOpType type)
      throws IOException {
    OmLockInfo omLockInfo = new OmLockInfo();
    List<Lock> locks = omLockInfo.getLocks();
    long startTime = Time.monotonicNowNanos();
    locks.add(getLock(lockFunction, name));
    long endTime = Time.monotonicNowNanos();
    omLockInfo.add(endTime - startTime, type);
    omLockInfo.setLockTakenTime(endTime);
    return omLockInfo;
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

  public OmLockInfo obsLock(String bucketName, String keyName) throws IOException {
    OmLockInfo omLockInfo = new OmLockInfo();
    List<Lock> locks = omLockInfo.getLocks();
    // bucket read lock
    long startTime = Time.monotonicNowNanos();
    try {
      locks.add(getLock(bucketLocking::readLock, bucketName));
      // key lock with bucket uniqueness as same key can be present across bucket
      locks.add(getLock(keyLocking::writeLock, bucketName + "/" + keyName));
      long endTime = Time.monotonicNowNanos();
      omLockInfo.add(endTime - startTime, OmLockInfo.LockOpType.WAIT);
      omLockInfo.setLockTakenTime(endTime);
      return omLockInfo;
    } catch (IOException ex) {
      writeUnlock(omLockInfo);
      throw ex;
    }
  }

  public OmLockInfo obsLock(String bucketName, List<String> keyList) throws IOException {
    OmLockInfo omLockInfo = new OmLockInfo();
    List<Lock> locks = omLockInfo.getLocks();
    // bucket read lock
    long startTime = Time.monotonicNowNanos();
    try {
      locks.add(getLock(bucketLocking::readLock, bucketName));
      // key lock with bucket uniqueness as same key can be present across bucket
      List<String> prefixedBucketKeyList = new ArrayList<>();
      keyList.forEach(e -> prefixedBucketKeyList.add(bucketName + "/" + e));
      getLock(keyLocking::writeLock, prefixedBucketKeyList, locks);
      long endTime = Time.monotonicNowNanos();
      omLockInfo.add(endTime - startTime, OmLockInfo.LockOpType.WAIT);
      omLockInfo.setLockTakenTime(endTime);
      return omLockInfo;
    } catch (IOException ex) {
      writeUnlock(omLockInfo);
      throw ex;
    }
  }

  public void writeUnlock(OmLockInfo lockInfo) {
    unlock(lockInfo, OmLockInfo.LockOpType.WRITE);
  }

  public void readUnlock(OmLockInfo lockInfo) {
    unlock(lockInfo, OmLockInfo.LockOpType.READ);
  }

  private void unlock(OmLockInfo lockInfo, OmLockInfo.LockOpType type) {
    Collections.reverse(lockInfo.getLocks());
    lockInfo.getLocks().forEach(Lock::unlock);
    if (lockInfo.getLockTakenTime() > 0) {
      lockInfo.add(Time.monotonicNowNanos() - lockInfo.getLockTakenTime(), type);
    }
    lockInfo.getLocks().clear();
  }

  /**
   * Lock information.
   */
  public static class OmLockInfo {
    private String key;
    private long lockTakenTime;
    private long waitLockNanos;
    private long readLockNanos;
    private long writeLockNanos;
    private List<Lock> locks = new ArrayList<>();

    public void setKey(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
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

    public List<Lock> getLocks() {
      return locks;
    }

    public long getLockTakenTime() {
      return lockTakenTime;
    }

    public void setLockTakenTime(long lockTakenTime) {
      this.lockTakenTime = lockTakenTime;
    }

    void add(long timeNanos, LockOpType lockOpType) {
      switch (lockOpType) {
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

    @Override
    public String toString() {
      return "OMLockDetails{" +
          "key=" + key +
          ", waitLockNanos=" + waitLockNanos +
          ", readLockNanos=" + readLockNanos +
          ", writeLockNanos=" + writeLockNanos +
          '}';
    }

    enum LockOpType {
      WAIT,
      READ,
      WRITE
    }
  }
}

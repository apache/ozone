/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.lock;

import com.google.common.util.concurrent.Striped;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * key locking.
 */
public class KeyLock {
  private static final Logger LOG = LoggerFactory.getLogger(KeyLock.class);
  private static final long LOCK_TIMEOUT_DEFAULT = 10 * 60 * 1000;
  private final Striped<ReadWriteLock> fileStripedLock;
  private final long lockTimeout;

  public KeyLock(int stripLockSize) {
    this(stripLockSize, LOCK_TIMEOUT_DEFAULT);
  }

  public KeyLock(int stripLockSize, long timeout) {
    fileStripedLock = Striped.readWriteLock(stripLockSize);
    lockTimeout = timeout;
  }

  public List<Lock> lock(List<String> keyList) throws IOException {
    List<Lock> locks = new ArrayList<>();
    boolean isSuccess = false;
    try {
      Iterable<ReadWriteLock> readWriteLocks = fileStripedLock.bulkGet(keyList);
      for (ReadWriteLock rwLock : readWriteLocks) {
        Lock lockObj = rwLock.writeLock();
        boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
        if (!b) {
          LOG.error("Key write lock is failed for {} after wait of {}ms", this, lockTimeout);
          throw new OMException("Unable to get write lock after " + lockTimeout + "ms"
              + ", read lock info: " + rwLock.readLock(),
              OMException.ResultCodes.TIMEOUT);
        }
        locks.add(lockObj);
      }
      isSuccess = true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OMException("Unable to get write lock as interrupted", OMException.ResultCodes.INTERNAL_ERROR);
    } finally {
      if (!isSuccess) {
        Collections.reverse(locks);
        locks.forEach(Lock::unlock);
        locks.clear();
      }
    }
    return locks;
  }

  public Lock lock(String key) throws IOException {
    LOG.debug("Key {} is locked for instance {} {}", key, this, fileStripedLock.get(key));
    try {
      Lock lockObj = fileStripedLock.get(key).writeLock();
      boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
      if (!b) {
        LOG.error("Key {} lock is failed for {} after wait of {}ms", key, this, lockTimeout);
        throw new OMException("Unable to get write lock for " + key + " after " + lockTimeout + "ms"
            + ", read lock info: " + fileStripedLock.get(key).readLock(),
            OMException.ResultCodes.TIMEOUT);
      }
      return lockObj;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OMException("Unable to get read lock for " + key + " is interrupted",
          OMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  public List<Lock> readLock(List<String> keyList) throws OMException {
    List<Lock> locks = new ArrayList<>();
    boolean isSuccess = false;
    try {
      Iterable<ReadWriteLock> readWriteLocks = fileStripedLock.bulkGet(keyList);
      for (ReadWriteLock rwLock : readWriteLocks) {
        Lock lockObj = rwLock.readLock();
        boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
        if (!b) {
          LOG.error("Key read lock is failed for {} after wait of {}ms", this, lockTimeout);
          throw new OMException("Unable to get read lock after " + lockTimeout + "ms"
              + ", write lock info: " + rwLock.writeLock(),
              OMException.ResultCodes.TIMEOUT);
        }
        locks.add(lockObj);
      }
      isSuccess = true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OMException("Unable to get read lock as interrupted", OMException.ResultCodes.INTERNAL_ERROR);
    } finally {
      if (!isSuccess) {
        Collections.reverse(locks);
        locks.forEach(Lock::unlock);
        locks.clear();
      }
    }
    return locks;
  }

  public Lock readLock(String key) throws OMException {
    try {
      LOG.debug("Key {} is read locked for instance {} {}", key, this, fileStripedLock.get(key));
      Lock lockObj = fileStripedLock.get(key).readLock();
      boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
      if (!b) {
        throw new OMException("Unable to get read lock for " + key + " after " + lockTimeout + "ms",
            OMException.ResultCodes.TIMEOUT);
      }
      return lockObj;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OMException("Unable to get read lock for " + key + " is interrupted",
          OMException.ResultCodes.INTERNAL_ERROR);
    }
  }
}

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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locking class that wraps Striped locking with timeout and logging. It also provides ordering while multiple locks.
 */
public class WrappedStripedLock {
  private static final Logger LOG = LoggerFactory.getLogger(WrappedStripedLock.class);
  private final Striped<ReadWriteLock> fileStripedLock;
  private final long lockTimeout;

  public WrappedStripedLock(int stripLockSize, long timeout, boolean fair) {
    fileStripedLock = SimpleStriped.readWriteLock(stripLockSize, fair);
    lockTimeout = timeout;
  }

  /**
   * lock the list of keys in order.
   * Sample code for lock and unlock handling:
   * <code>
   *   try {
   *     if (!wrappedStripedLock.lock(keyList, locks)) {
   *       // timeout occurred, release lock if any in reverse order
   *       Collections.reverse(locks);
   *       locks.forEach(Lock::unlock);
   *     }
   *     // perform business logic
   *   } finally {
   *     // to be released in reverse order
   *     Collections.reverse(locks);
   *     locks.forEach(Lock::unlock);
   *   }
   * </code>
   * 
   * @param keyList key list which needs to be locked
   * @param locks successful lock object returned which will be used to release lock
   * @return boolean true if success, else false
   * @throws InterruptedException exception on interrupt
   */
  public boolean writeLock(List<String> keyList, List<Lock> locks) throws InterruptedException {
    try {
      Iterable<ReadWriteLock> readWriteLocks = fileStripedLock.bulkGet(keyList);
      for (ReadWriteLock rwLock : readWriteLocks) {
        Lock lockObj = rwLock.writeLock();
        boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
        if (!b) {
          LOG.error("Write lock for keys are failed for the instance {} after wait of {}ms, read lock info: {}", this,
              lockTimeout, rwLock.readLock());
          return false;
        }
        locks.add(lockObj);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Write lock for keys are interrupted for the instance {}", this);
      throw e;
    }
    return true;
  }

  /**
   * lock single key.
   * @param key object for which lock to be taken
   * @return lock object to be used to release lock, null if unable to take lock due to timeout
   * @throws InterruptedException exception on interrupt
   */
  public Lock writeLock(String key) throws InterruptedException {
    LOG.debug("Key {} is locked for instance {} {}", key, this, fileStripedLock.get(key));
    try {
      Lock lockObj = fileStripedLock.get(key).writeLock();
      boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
      if (!b) {
        LOG.error("Write lock for the key is failed for the instance {} after wait of {}ms, read lock info: {}", this,
            lockTimeout, fileStripedLock.get(key).readLock());
        return null;
      }
      return lockObj;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Write lock for the key is interrupted for the instance {}", this);
      throw e;
    }
  }

  /**
   * lock the list of keys in order.
   * Sample code for lock and unlock handling:
   * <code>
   *   try {
   *     if (!wrappedStripedLock.lock(keyList, locks)) {
   *       // timeout occurred, release lock if any in reverse order
   *       Collections.reverse(locks);
   *       locks.forEach(Lock::unlock);
   *     }
   *     // perform business logic
   *   } finally {
   *     // to be released in reverse order
   *     Collections.reverse(locks);
   *     locks.forEach(Lock::unlock);
   *   }
   * </code>
   *
   * @param keyList key list which needs to be locked
   * @param locks successful lock object returned which will be used to release lock
   * @return boolean true if success, else false
   * @throws InterruptedException exception on interrupt
   */
  public boolean readLock(List<String> keyList, List<Lock> locks) throws InterruptedException {
    try {
      Iterable<ReadWriteLock> readWriteLocks = fileStripedLock.bulkGet(keyList);
      for (ReadWriteLock rwLock : readWriteLocks) {
        Lock lockObj = rwLock.readLock();
        boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
        if (!b) {
          LOG.error("Read lock for keys are failed for the instance {} after wait of {}ms, write lock info: {}", this,
              lockTimeout, rwLock.writeLock());
          return false;
        }
        locks.add(lockObj);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Read lock for keys are interrupted for the instance {}", this);
      throw e;
    }
    return true;
  }

  /**
   * read lock single key.
   * @param key object for which lock to be taken
   * @return lock object to be used to release lock, null if unable to take lock due to timeout
   * @throws InterruptedException exception on interrupt
   */
  public Lock readLock(String key) throws InterruptedException {
    try {
      LOG.debug("Key {} is read locked for instance {} {}", key, this, fileStripedLock.get(key));
      Lock lockObj = fileStripedLock.get(key).readLock();
      boolean b = lockObj.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
      if (!b) {
        LOG.error("Read lock for the key is failed for the instance {} after wait of {}ms, write lock info: {}", this,
            lockTimeout, fileStripedLock.get(key).readLock());
        return null;
      }
      return lockObj;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Read lock for the key is interrupted for the instance {}", this);
      throw e;
    }
  }
}

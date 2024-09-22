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
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * key locking.
 */
public class KeyLocking {
  private static final Logger LOG = LoggerFactory.getLogger(KeyLocking.class);
  private static final int DEFAULT_FILE_LOCK_STRIPED_SIZE = 10240;
  private static final long LOCK_TIMEOUT = 10 * 60 * 1000;
  private Striped<ReadWriteLock> fileStripedLock = Striped.readWriteLock(DEFAULT_FILE_LOCK_STRIPED_SIZE);
  private AtomicLong writeLockCount = new AtomicLong();
  private AtomicLong readLockCount = new AtomicLong();
  private AtomicLong failedLockCount = new AtomicLong();
  private AtomicLong failedUnlockCount = new AtomicLong();

  public void lock(List<String> keyList) throws IOException {
    for (String key : keyList) {
      lock(key);
    }
  }

  public void unlock(List<String> keyList) {
    ListIterator<String> itr = keyList.listIterator(keyList.size());
    while (itr.hasPrevious()) {
      unlock(itr.previous());
    }
  }

  public void lock(String key) throws IOException {
    LOG.debug("Key {} is locked for instance {} {}", key, this, fileStripedLock.get(key));
    try {
      boolean b = fileStripedLock.get(key).writeLock().tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      if (!b) {
        LOG.error("Key {} lock is failed after wait of {}ms", key, LOCK_TIMEOUT);
        failedLockCount.incrementAndGet();
        throw new OMException("Unable to get write lock for " + key + " after " + LOCK_TIMEOUT + "ms"
            + ", lock info: " + fileStripedLock.get(key).readLock(),
            OMException.ResultCodes.TIMEOUT);
      }
      writeLockCount.incrementAndGet();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void unlock(String key) {
    LOG.debug("Key {} is un-locked for instance {} {}", key, this, fileStripedLock.get(key));
    try {
      fileStripedLock.get(key).writeLock().unlock();
    } catch (Throwable th) {
      LOG.error("Key {} un-lock is failed", key, th);
      failedUnlockCount.incrementAndGet();
      throw th;
    }
    writeLockCount.decrementAndGet();
  }

  public void readLock(List<String> keyList) throws OMException {
    for (String key : keyList) {
      readLock(key);
    }
  }

  public void readUnlock(List<String> keyList) {
    ListIterator<String> itr = keyList.listIterator(keyList.size());
    while (itr.hasPrevious()) {
      readUnlock(itr.previous());
    }
  }
  public void readLock(String key) throws OMException {
    try {
      LOG.debug("Key {} is read locked for instance {} {}", key, this, fileStripedLock.get(key));
      boolean b = fileStripedLock.get(key).readLock().tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      if (!b) {
        failedLockCount.incrementAndGet();
        throw new OMException("Unable to get read lock for " + key + " after " + LOCK_TIMEOUT + "ms",
            OMException.ResultCodes.TIMEOUT);
      }
      readLockCount.incrementAndGet();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void readUnlock(String key) {
    try {
      LOG.debug("Key {} is read un-locked for instance {} {}", key, this, fileStripedLock.get(key));
      fileStripedLock.get(key).readLock().unlock();
    } catch (Throwable th) {
      LOG.error("Key {} read un-lock is failed", key, th);
      failedUnlockCount.incrementAndGet();
      throw th;
    }
    readLockCount.decrementAndGet();
  }

  @Override
  public String toString() {
    return String.format("Lock status: Write lock count %d, Read lock Count %d, failed lock count %d" +
        ", failed un-lock count %d", writeLockCount.get(), readLockCount.get(), failedLockCount.get(),
        failedUnlockCount.get());
  }
}

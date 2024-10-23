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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * Prefix locking for FSO.
 */
public class FSOPrefixLocking {
  private static final int LRU_CACH_MIN_SIZE = 1000;
  private static final int LRU_TRY_REMOVE_PERCENT = 10;
  private static final long LOCK_TIMEOUT = 10 * 60 * 1000;
  private final ScheduledExecutorService executorService;
  private final AtomicLong writeLockCnt = new AtomicLong();
  private final AtomicLong readLockCnt = new AtomicLong();

  private LockInfo lockInfo = new LockInfo(null, null);
  private NavigableSet<LockInfo> lru = new ConcurrentSkipListSet<>();

  public FSOPrefixLocking(String threadPrefix) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadPrefix + "FullTableCache-Cleanup-%d").build();
    executorService = Executors.newScheduledThreadPool(1, threadFactory);
    executorService.scheduleWithFixedDelay(() -> cleanupTask(), 1000L, 1000L, TimeUnit.MILLISECONDS);
  }
  
  public void stop() {
    executorService.shutdown();
  }
  
  private void cleanupTask() {
    // most recent will be leaf node
    int size = lru.size();
    if (size > LRU_CACH_MIN_SIZE) {
      int count = size / LRU_TRY_REMOVE_PERCENT;
      for (int i = 0; i < count; ++i) {
        LockInfo tmpLockInfo = lru.pollLast();
        // try remove, if not successful, add back
        try {
          boolean b = tmpLockInfo.removeSelf();
          if (!b) {
            lru.add(tmpLockInfo);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  
  public List<LockInfo> readLock(List<String> keyElements) throws OMException {
    List<LockInfo> acquiredLocks = new ArrayList<>(keyElements.size());
    LockInfo tmpLockInfo = lockInfo;
    for (String path : keyElements) {
      tmpLockInfo = tmpLockInfo.addPath(path);
      tmpLockInfo.readLock();
      acquiredLocks.add(tmpLockInfo);
      readLockCnt.incrementAndGet();
    }
    return acquiredLocks;
  }

  public void readUnlock(List<LockInfo> acquiredLocks) {
    ListIterator<LockInfo> li = acquiredLocks.listIterator(acquiredLocks.size());
    while (li.hasPrevious()) {
      LockInfo previous = li.previous();
      previous.readUnlock();
      updateLru(previous);
      readLockCnt.decrementAndGet();
    }
  }

  public LockInfo writeLock(LockInfo prvLockInfo, String name) throws IOException {
    if (null == prvLockInfo) {
      prvLockInfo = lockInfo;
    }
    prvLockInfo = prvLockInfo.addPath(name);
    prvLockInfo.writeLock();
    writeLockCnt.incrementAndGet();
    return prvLockInfo;
  }

  public void writeUnlock(LockInfo curLockInfo) {
    curLockInfo.writeUnlock();
    updateLru(curLockInfo);
    writeLockCnt.decrementAndGet();
  }

  private void updateLru(LockInfo previous) {
    // update is done in unlock as this is more applicable for lru cleanup as lock is released
    lru.remove(previous);
    lru.add(previous);
  }

  @Override
  public String toString() {
    return String.format("Lock status: Write lock count %d, Read lock Count %d", writeLockCnt.get(), readLockCnt.get());
  }
  /**
   * lock info about lock tree.
   */
  public static class LockInfo implements Comparable {
    private String key;
    private Map<String, LockInfo> lockMap = new ConcurrentHashMap<>();
    private ReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private LockInfo parent;
    public LockInfo(LockInfo p, String k) {
      parent = p;
      this.key = k;
    }
    public LockInfo get(String path) {
      return lockMap.get(path);
    }
    public LockInfo addPath(String path) {
      // concurrentHashMap computeIfAbsent is atomic
      LockInfo lockInfo = lockMap.computeIfAbsent(path, k -> new LockInfo(this, k));
      return lockInfo;
    }
    public void readLock() throws OMException {
      try {
        boolean b = rwLock.readLock().tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
        if (!b) {
          throw new OMException("Unable to get read lock for " + key + " after " + LOCK_TIMEOUT + "ms",
              OMException.ResultCodes.TIMEOUT);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    public void readUnlock() {
      rwLock.readLock().unlock();
    }
    public void writeLock() throws IOException {
      try {
        boolean b = writeLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
        if (!b) {
          throw new OMException("Unable to get write lock for " + key + " after " + LOCK_TIMEOUT + "ms",
              OMException.ResultCodes.TIMEOUT);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    public void writeUnlock() {
      rwLock.writeLock().unlock();
    }
    private boolean writeLock(long time, TimeUnit timeUnit) throws InterruptedException {
      return rwLock.writeLock().tryLock(time, timeUnit);
    }
    private void remove(String path) {
      lockMap.remove(path);
    }
    public boolean removeSelf() throws InterruptedException {
      boolean selfLock = false;
      boolean parentLock = false;
      try {
        selfLock = writeLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
        if (selfLock) {
          parentLock = parent.writeLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
          if (parentLock) {
            parent.remove(key);
          }
        }
      } finally {
        if (selfLock) {
          writeUnlock();
        }
        if (parentLock) {
          parent.writeUnlock();
        }
      }
      return parentLock;
    }

    public LockInfo getParent() {
      return parent;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof LockInfo)) {
        return false;
      }
      if (this == other) {
        return true;
      }
      return this.compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public int compareTo(Object o) {
      if (!(o instanceof LockInfo)) {
        return -1;
      }
      int rst = Integer.compare(parent.hashCode(), ((LockInfo) o).parent.hashCode());
      if (rst == 0) {
        return key.compareTo(((LockInfo) o).key);
      }
      return rst;
    }
  }
}

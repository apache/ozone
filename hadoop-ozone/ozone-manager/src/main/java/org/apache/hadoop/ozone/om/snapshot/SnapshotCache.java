/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;

/**
 * Thread-safe custom unbounded LRU cache to manage open snapshot DB instances.
 */
public class SnapshotCache implements ReferenceCountedCallback<OmSnapshot>, AutoCloseable {

  static final Logger LOG = LoggerFactory.getLogger(SnapshotCache.class);

  // Snapshot cache internal hash map.
  // Key:   SnapshotId
  // Value: OmSnapshot instance, each holds a DB instance handle inside
  // TODO: [SNAPSHOT] Consider wrapping SoftReference<> around IOmMetadataReader
  private final ConcurrentHashMap<UUID, ReferenceCounted<OmSnapshot>> dbMap;
  private final CacheLoader<UUID, OmSnapshot> cacheLoader;
  // Soft-limit of the total number of snapshot DB instances allowed to be
  // opened on the OM.
  private final int cacheSizeLimit;
  private final Set<UUID> pendingEvictionQueue;
  private final Scheduler scheduler;
  private static final String SNAPSHOT_CACHE_CLEANUP_SERVICE =
      "SnapshotCacheCleanupService";

  private final OMMetrics omMetrics;
  private final ReadWriteLock lock;
  private final Lock readLock;
  private final Lock writeLock;
  private int lockCnt;
  private final Condition lockReleased;
  private final Condition dbClosed;
  private final long lockTimeout;
  private final Map<Long, Map<UUID, Long>> snapshotRefThreadIds;
  private final AtomicBoolean closed;

  public SnapshotCache(CacheLoader<UUID, OmSnapshot> cacheLoader, int cacheSizeLimit, OMMetrics omMetrics,
                       long cleanupInterval, long lockTimeout) {
    this.dbMap = new ConcurrentHashMap<>();
    this.cacheLoader = cacheLoader;
    this.cacheSizeLimit = cacheSizeLimit;
    this.omMetrics = omMetrics;
    this.pendingEvictionQueue = ConcurrentHashMap.newKeySet();
    this.lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.snapshotRefThreadIds = new ConcurrentHashMap<>();
    this.lockTimeout = lockTimeout;
    this.lockCnt = 0;
    this.lockReleased = this.readLock.newCondition();
    this.dbClosed = this.writeLock.newCondition();
    this.closed = new AtomicBoolean(false);
    if (cleanupInterval > 0) {
      this.scheduler = new Scheduler(SNAPSHOT_CACHE_CLEANUP_SERVICE,
          true, 1);
      this.scheduler.scheduleWithFixedDelay(this::cleanup, cleanupInterval,
          cleanupInterval, TimeUnit.MILLISECONDS);
    } else {
      this.scheduler = null;
    }
  }

  @VisibleForTesting
  ConcurrentHashMap<UUID, ReferenceCounted<OmSnapshot>> getDbMap() {
    return dbMap;
  }

  /**
   * @return number of DB instances currently held in cache.
   */
  public int size() {
    return dbMap.size();
  }

  /**
   * Immediately invalidate an entry.
   * @param key SnapshotId
   */
  public void invalidate(UUID key) {
    dbMap.compute(key, (k, v) -> {
      if (v == null) {
        LOG.warn("SnapshotId: '{}' does not exist in snapshot cache.", k);
      } else {
        readLock.lock();
        try {
          if (lockCnt > 0) {
            for (Long tid : snapshotRefThreadIds.keySet()) {
              snapshotRefThreadIds.computeIfPresent(tid, (threadId, map) -> {
                map.computeIfPresent(key, (uuid, val) -> null);
                return map.isEmpty() ? null : map;
              });
            }
          }
          v.get().close();
        } catch (IOException e) {
          throw new IllegalStateException("Failed to close snapshotId: " + key, e);
        } finally {
          readLock.unlock();
        }
        omMetrics.decNumSnapshotCacheSize();
      }
      return null;
    });
  }

  /**
   * Immediately invalidate all entries and close their DB instances in cache.
   */
  public void invalidateAll() {
    for (UUID key : dbMap.keySet()) {
      invalidate(key);
    }
  }

  @Override
  public void close() {
    closed.set(true);
    invalidateAll();
    if (this.scheduler != null) {
      this.scheduler.close();
    }
  }

  /**
   * Decreases the lock count. When the count reaches zero all new threads would be able to get a handle of snapshot.
   */
  private Runnable decrementLockCount() {
    lockCnt -= 1;
    if (lockCnt <= 0) {
      LOG.warn("Invalid negative lock count : {}. Setting it to 0", lockCnt);
      lockCnt = 0;
    }

    if (lockCnt == 0) {
      snapshotRefThreadIds.clear();
    }
    return () -> {
      readLock.lock();
      try {
        if (lockCnt == 0) {
          lockReleased.signalAll();
        }
      } finally {
        readLock.unlock();
      }
    };
  }

  /**
   * Releases a lock on the cache.
   */
  public void releaseLock() {
    writeLock.lock();
    Runnable callback = decrementLockCount();
    try {
      decrementLockCount();
    } finally {
      writeLock.unlock();
    }
    callback.run();
  }

  /**
   * Acquires lock on the cache within max amount time.
   * @param timeout Max time to wait to acquire lock.
   * @return true if lock is acquired otherwise false.
   * @throws InterruptedException
   */
  public boolean tryAcquire(long timeout) throws InterruptedException {
    long endTime = System.currentTimeMillis() + timeout;
    if (timeout <= 0) {
      endTime = Long.MAX_VALUE;
      timeout = Long.MAX_VALUE;
    }
    if (writeLock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
      Runnable rollbackCallback = null;
      try {
        lockCnt += 1;
        if (lockCnt == 1) {
          snapshotRefThreadIds.clear();
          dbMap.values().stream()
              .flatMap(referenceCounted ->
                  referenceCounted.getThreadCntMap().entrySet().stream().map(entry -> Pair.of(entry, referenceCounted)))
              .forEach(entry -> updateThreadCnt(entry.getKey().getKey(), entry.getValue().get().getSnapshotID(),
                  entry.getKey().getValue()));
        }
        while (!snapshotRefThreadIds.isEmpty()) {
          long currentTime = System.currentTimeMillis();
          if (currentTime >= endTime) {
            // If and release acquired lock
            rollbackCallback = decrementLockCount();
            break;
          }
          dbClosed.await(Math.min(endTime - currentTime, lockTimeout), TimeUnit.MILLISECONDS);
        }
      } finally {
        writeLock.unlock();
      }
      if (rollbackCallback != null) {
        rollbackCallback.run();
        return false;
      }
      invalidateAll();
      return true;
    }
    return false;
  }

  private void updateThreadCnt(long threadId, UUID key, long cnt) {
    snapshotRefThreadIds.compute(threadId, (tid, countMap) -> {
      if (countMap == null) {
        if (cnt <= 0) {
          return null;
        }
        countMap = new ConcurrentHashMap<>();
      }
      countMap.compute(key, (snapId, count) -> {
        if (count == null) {
          count = 0L;
        }
        count += cnt;
        return count > 0 ? count : null;
      });
      return countMap.isEmpty() ? null : countMap;
    });
  }

  /**
   * Waits for lock to be released. This function doesn't wait for the lock if the thread already has a few snapshots
   * open. It only waits if the thread is reading it's first snapshot.
   * @param threadId
   * @throws InterruptedException
   */
  private void waitForLock(long threadId) throws IOException {
    if (snapshotRefThreadIds.computeIfPresent(threadId, (k, v) -> v) != null) {
      while (lockCnt > 0) {
        try {
          lockReleased.await(lockTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException("Error while waiting for locks to be released.", e);
        }

      }
    }
  }

  /**
   * State the reason the current thread is getting the OmSnapshot instance.
   * Unused for now.
   */
  public enum Reason {
    FS_API_READ,
    SNAP_DIFF_READ,
    DEEP_CLEAN_WRITE,
    GARBAGE_COLLECTION_WRITE
  }

  /**
   * Get or load OmSnapshot. Shall be close()d after use.
   * TODO: [SNAPSHOT] Can add reason enum to param list later.
   * @param key SnapshotId
   * @return an OmSnapshot instance, or null on error
   */
  public ReferenceCounted<OmSnapshot> get(UUID key) throws IOException {
    // Warn if actual cache size exceeds the soft limit already.
    if (size() > cacheSizeLimit) {
      LOG.warn("Snapshot cache size ({}) exceeds configured soft-limit ({}).",
          size(), cacheSizeLimit);
    }
    // Atomic operation to initialize the OmSnapshot instance (once) if the key
    // does not exist, and increment the reference count on the instance.
    ReferenceCounted<OmSnapshot> rcOmSnapshot;
    readLock.lock();
    try {
      long threadId = Thread.currentThread().getId();
      waitForLock(threadId);
      rcOmSnapshot =
          dbMap.compute(key, (k, v) -> {
            if (v == null) {
              if (closed.get()) {
                return v;
              }
              LOG.info("Loading SnapshotId: '{}'", k);
              try {
                v = new ReferenceCounted<>(cacheLoader.load(key), false, this);
              } catch (OMException omEx) {
                // Return null if the snapshot is no longer active
                if (!omEx.getResult().equals(FILE_NOT_FOUND)) {
                  throw new IllegalStateException(omEx);
                }
              } catch (IOException ioEx) {
                // Failed to load snapshot DB
                throw new IllegalStateException(ioEx);
              } catch (Exception ex) {
                // Unexpected and unknown exception thrown from CacheLoader#load
                throw new IllegalStateException(ex);
              }
              omMetrics.incNumSnapshotCacheSize();
            }
            if (v != null) {
              // When RC OmSnapshot is successfully loaded
              v.incrementRefCount();
              if (lockCnt > 0) {
                updateThreadCnt(threadId, key, 1);
              }
            }
            return v;
          });
    } finally {
      readLock.unlock();
    }
    if (rcOmSnapshot == null) {
      if (closed.get()) {
        throw new IOException("Unable to open snapshot with SnapshotId: '" + key + "' since snapshot cache is closed.");
      }
      // The only exception that would fall through the loader logic above
      // is OMException with FILE_NOT_FOUND.
      throw new OMException("SnapshotId: '" + key + "' not found, or the snapshot is no longer active.",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }
    return rcOmSnapshot;
  }

  /**
   * Release the reference count on the OmSnapshot instance.
   * @param key SnapshotId
   */
  public void release(UUID key) {
    ReferenceCounted<OmSnapshot> val = dbMap.get(key);
    if (val == null) {
      throw new IllegalArgumentException("Key '" + key + "' does not " +
          "exist in cache.");
    }
    val.decrementRefCount();
  }


  /**
   * If cache size exceeds soft limit, attempt to clean up and close the
     instances that has zero reference count.
   */
  @VisibleForTesting
  void cleanup() {
    if (dbMap.size() > cacheSizeLimit) {
      for (UUID evictionKey : pendingEvictionQueue) {
        dbMap.compute(evictionKey, (k, v) -> {
          pendingEvictionQueue.remove(k);
          if (v == null) {
            throw new IllegalStateException("SnapshotId '" + k + "' does not exist in cache. The RocksDB " +
                "instance of the Snapshot may not be closed properly.");
          }

          if (v.getTotalRefCount() > 0) {
            LOG.debug("SnapshotId {} is still being referenced ({}), skipping its clean up.", k, v.getTotalRefCount());
            return v;
          } else {
            LOG.debug("Closing SnapshotId {}. It is not being referenced anymore.", k);
            // Close the instance, which also closes its DB handle.
            try {
              v.get().close();
            } catch (IOException ex) {
              throw new IllegalStateException("Error while closing snapshot DB.", ex);
            }
            omMetrics.decNumSnapshotCacheSize();
            return null;
          }
        });
      }
    }
  }

  /**
   * Callback method used to enqueue or dequeue ReferenceCounted from
   * pendingEvictionList.
   * @param referenceCounted ReferenceCounted object
   */
  @Override
  public void callback(ReferenceCounted<OmSnapshot> referenceCounted) {
    long threadId = Thread.currentThread().getId();
    UUID snapshotId = referenceCounted.get().getSnapshotID();
    // Remove snapshotRef from the thread count map.
    writeLock.lock();
    try {
      if (lockCnt > 0) {
        updateThreadCnt(threadId, snapshotId, -1);
      }
      dbClosed.signal();
    } finally {
      writeLock.unlock();
    }

    if (referenceCounted.getTotalRefCount() == 0L) {
      // Reference count reaches zero, add to pendingEvictionList
      pendingEvictionQueue.add(snapshotId);
    }
  }
}

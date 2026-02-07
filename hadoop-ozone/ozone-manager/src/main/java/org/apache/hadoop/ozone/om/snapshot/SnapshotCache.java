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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_LOCK;
import static org.apache.hadoop.ozone.om.lock.OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED;
import static org.apache.hadoop.ozone.om.lock.OMLockDetails.EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COLUMN_FAMILIES_TO_TRACK;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.ratis.util.BatchLogger;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe custom unbounded LRU cache to manage open snapshot DB instances.
 */
public class SnapshotCache implements ReferenceCountedCallback, AutoCloseable {

  static final Logger LOG = LoggerFactory.getLogger(SnapshotCache.class);
  private static final long CACHE_WARNING_THROTTLE_INTERVAL_MS = 60_000L;

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
  private final IOzoneManagerLock lock;
  private static final String SNAPSHOT_CACHE_CLEANUP_SERVICE =
      "SnapshotCacheCleanupService";
  private final boolean compactNonSnapshotDiffTables;

  private final OMMetrics omMetrics;

  private enum BatchLogKey implements BatchLogger.Key {
    SNAPSHOT_CACHE_SIZE_EXCEEDED;

    @Override
    public TimeDuration getBatchDuration() {
      return TimeDuration.valueOf(CACHE_WARNING_THROTTLE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
  }

  private boolean shouldCompactTable(String tableName) {
    return !COLUMN_FAMILIES_TO_TRACK.contains(tableName);
  }

  /**
   * Compacts the RocksDB tables in the given snapshot that are not part of the snapshot diff DAG.
   * This operation is performed outside of the main snapshot operations to avoid blocking reads.
   * Only tables that are not tracked in the DAG (determined by {@link #shouldCompactTable}) will be compacted.
   *
   * @param snapshot The OmSnapshot instance whose tables need to be compacted
   * @throws IOException if there is an error accessing the metadata manager
   */
  private void compactSnapshotDB(OmSnapshot snapshot) throws IOException {
    if (!compactNonSnapshotDiffTables) {
      return;
    }
    OMMetadataManager metadataManager = snapshot.getMetadataManager();
    for (Table<?, ?> table : metadataManager.getStore().listTables()) {
      if (shouldCompactTable(table.getName())) {
        try {
          metadataManager.getStore().compactTable(table.getName());
        } catch (IOException e) {
          LOG.warn("Failed to compact table {} in snapshot {}: {}",
              table.getName(), snapshot.getSnapshotID(), e.getMessage());
        }
      }
    }
  }

  public SnapshotCache(CacheLoader<UUID, OmSnapshot> cacheLoader, int cacheSizeLimit, OMMetrics omMetrics,
                       long cleanupInterval, boolean compactNonSnapshotDiffTables, IOzoneManagerLock lock) {
    this.dbMap = new ConcurrentHashMap<>();
    this.cacheLoader = cacheLoader;
    this.cacheSizeLimit = cacheSizeLimit;
    this.omMetrics = omMetrics;
    this.lock = lock;
    this.pendingEvictionQueue = ConcurrentHashMap.newKeySet();
    this.compactNonSnapshotDiffTables = compactNonSnapshotDiffTables;
    if (cleanupInterval > 0) {
      this.scheduler = new Scheduler(SNAPSHOT_CACHE_CLEANUP_SERVICE,
          true, 1);
      this.scheduler.scheduleWithFixedDelay(() -> this.cleanup(false), cleanupInterval,
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
        LOG.debug("SnapshotId: '{}' does not exist in snapshot cache.", k);
      } else {
        try {
          v.get().close();
        } catch (IOException e) {
          throw new IllegalStateException("Failed to close snapshotId: " + key, e);
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
    invalidateAll();
    if (this.scheduler != null) {
      this.scheduler.close();
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
   * Get or load OmSnapshot. Shall be close()d after use. This would acquire a read lock on the Snapshot Database
   * during the entire lifecycle of the returned OmSnapshot instance.
   * TODO: [SNAPSHOT] Can add reason enum to param list later.
   * @param key SnapshotId
   * @return an OmSnapshot instance, or null on error
   */
  public UncheckedAutoCloseableSupplier<OmSnapshot> get(UUID key) throws IOException {
    // Warn if actual cache size exceeds the soft limit already.
    if (size() > cacheSizeLimit) {
      BatchLogger.print(
          BatchLogKey.SNAPSHOT_CACHE_SIZE_EXCEEDED, // The unique key for this log type
          "CacheSizeWarning", // A specific name for this log message
          suffix -> LOG.warn("Snapshot cache size ({}) exceeds configured soft-limit ({}).{}",
              size(), cacheSizeLimit, suffix)
      );
    }
    OMLockDetails lockDetails = lock.acquireReadLock(SNAPSHOT_DB_LOCK, key.toString());
    if (!lockDetails.isLockAcquired()) {
      throw new OMException("Unable to acquire readlock on snapshot db with key " + key,
          OMException.ResultCodes.INTERNAL_ERROR);
    }
    try {
      // Atomic operation to initialize the OmSnapshot instance (once) if the key
      // does not exist, and increment the reference count on the instance.
      ReferenceCounted<OmSnapshot> rcOmSnapshot = dbMap.compute(key, (k, v) -> {
        if (v == null) {
          LOG.info("Loading SnapshotId: '{}'", k);
          try {
            v = new ReferenceCounted<>(cacheLoader.load(key), false, this);
          } catch (OMException omEx) {
            // Return null if the snapshot is no longer active
            if (!omEx.getResult().equals(OMException.ResultCodes.FILE_NOT_FOUND)) {
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
        }
        return v;
      });
      if (rcOmSnapshot == null) {
        throw new OMException("SnapshotId: '" + key + "' not found, or the snapshot is no longer active.",
            OMException.ResultCodes.FILE_NOT_FOUND);
      }

      return new UncheckedAutoCloseableSupplier<OmSnapshot>() {
        private final AtomicReference<Boolean> closed = new AtomicReference<>(false);
        @Override
        public OmSnapshot get() {
          return rcOmSnapshot.get();
        }

        @Override
        public void close() {
          closed.updateAndGet(alreadyClosed -> {
            if (!alreadyClosed) {
              rcOmSnapshot.decrementRefCount();
              lock.releaseReadLock(SNAPSHOT_DB_LOCK, key.toString());
            }
            return true;
          });
        }
      };
    } catch (Throwable e) {
      // Release the read lock irrespective of the exception thrown.
      lock.releaseReadLock(SNAPSHOT_DB_LOCK, key.toString());
      throw e;
    }
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
   * Acquires a write lock on the snapshot database and returns an auto-closeable supplier
   * for lock details. The lock ensures that the operations accessing the snapshot database
   * are performed in a thread-safe manner. The returned supplier automatically releases the
   * lock when closed, preventing potential resource contention or deadlocks.
   */
  public UncheckedAutoCloseableSupplier<OMLockDetails> lock() {
    return lock(() -> lock.acquireResourceWriteLock(SNAPSHOT_DB_LOCK),
        () -> lock.releaseResourceWriteLock(SNAPSHOT_DB_LOCK), () -> {
        cleanup(true);
        return dbMap.isEmpty();
      });
  }

  /**
   * Acquires a write lock on a specific snapshot database and returns an auto-closeable supplier for lock details.
   * The lock ensures that the operations accessing the snapshot database are performed in a thread safe manner. The
   * returned supplier automatically releases the lock acquired when closed, preventing potential resource
   * contention or deadlocks.
   */
  public UncheckedAutoCloseableSupplier<OMLockDetails> lock(UUID snapshotId) {
    return lock(() -> lock.acquireWriteLock(SNAPSHOT_DB_LOCK, snapshotId.toString()),
        () -> lock.releaseWriteLock(SNAPSHOT_DB_LOCK, snapshotId.toString()),
        () -> {
        cleanup(snapshotId, false);
        return !dbMap.containsKey(snapshotId);
      });
  }

  private OMLockDetails getEmptyOmLockDetails(OMLockDetails lockDetails) {
    return lockDetails.isLockAcquired() ? EMPTY_DETAILS_LOCK_ACQUIRED : EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  private UncheckedAutoCloseableSupplier<OMLockDetails> lock(Supplier<OMLockDetails> lockFunction,
      Supplier<OMLockDetails> unlockFunction, Supplier<Boolean> cleanupFunction) {
    Supplier<OMLockDetails> emptyLockFunction = () -> getEmptyOmLockDetails(lockFunction.get());
    Supplier<OMLockDetails> emptyUnlockFunction = () -> getEmptyOmLockDetails(unlockFunction.get());

    AtomicReference<OMLockDetails> lockDetails = new AtomicReference<>(emptyLockFunction.get());
    if (lockDetails.get().isLockAcquired()) {
      if (!cleanupFunction.get()) {
        lockDetails.set(emptyUnlockFunction.get());
      }
    }

    return new UncheckedAutoCloseableSupplier<OMLockDetails>() {

      @Override
      public void close() {
        lockDetails.updateAndGet((prevLock) -> {
          if (prevLock != null && prevLock.isLockAcquired()) {
            return emptyUnlockFunction.get();
          }
          return prevLock;
        });
      }

      @Override
      public OMLockDetails get() {
        return lockDetails.get();
      }
    };
  }

  /**
   * If cache size exceeds soft limit, attempt to clean up and close the
     instances that has zero reference count.
   */
  private synchronized Void cleanup(boolean force) {
    if (force || dbMap.size() > cacheSizeLimit) {
      for (UUID evictionKey : pendingEvictionQueue) {
        cleanup(evictionKey, true);
      }
    }
    return null;
  }

  private synchronized Void cleanup(UUID evictionKey, boolean expectKeyToBePresent) {
    ReferenceCounted<OmSnapshot> snapshot = dbMap.get(evictionKey);

    if (!expectKeyToBePresent && snapshot == null) {
      return null;
    }

    if (snapshot != null && snapshot.getTotalRefCount() == 0) {
      try {
        compactSnapshotDB(snapshot.get());
      } catch (IOException e) {
        LOG.warn("Failed to compact snapshot DB for snapshotId {}: {}",
            evictionKey, e.getMessage());
      }
    }

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
    return null;
  }

  /**
   * Callback method used to enqueue or dequeue ReferenceCounted from
   * pendingEvictionList.
   * @param referenceCounted ReferenceCounted object
   */
  @Override
  public void callback(ReferenceCounted referenceCounted) {
    if (referenceCounted.getTotalRefCount() == 0L) {
      // Reference count reaches zero, add to pendingEvictionList
      pendingEvictionQueue.add(((OmSnapshot) referenceCounted.get())
          .getSnapshotID());
    }
  }

  long totalRefCount(UUID key) {
    return dbMap.containsKey(key) ? dbMap.get(key).getTotalRefCount() : 0;
  }
}

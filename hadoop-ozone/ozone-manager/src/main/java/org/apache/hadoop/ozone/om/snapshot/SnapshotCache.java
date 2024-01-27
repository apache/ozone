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
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;

/**
 * Thread-safe custom unbounded LRU cache to manage open snapshot DB instances.
 */
public class SnapshotCache {

  static final Logger LOG = LoggerFactory.getLogger(SnapshotCache.class);

  // Snapshot cache internal hash map.
  // Key:   DB snapshot table key
  // Value: OmSnapshot instance, each holds a DB instance handle inside
  // TODO: [SNAPSHOT] Consider wrapping SoftReference<> around IOmMetadataReader
  private final ConcurrentHashMap<String, ReferenceCounted<IOmMetadataReader, SnapshotCache>> dbMap;

  private final OmSnapshotManager omSnapshotManager;
  private final CacheLoader<String, OmSnapshot> cacheLoader;
  // Soft-limit of the total number of snapshot DB instances allowed to be
  // opened on the OM.
  private final int cacheSizeLimit;

  public SnapshotCache(
      OmSnapshotManager omSnapshotManager,
      CacheLoader<String, OmSnapshot> cacheLoader,
      int cacheSizeLimit) {
    this.dbMap = new ConcurrentHashMap<>();
    this.omSnapshotManager = omSnapshotManager;
    this.cacheLoader = cacheLoader;
    this.cacheSizeLimit = cacheSizeLimit;
  }

  @VisibleForTesting
  ConcurrentHashMap<String, ReferenceCounted<IOmMetadataReader, SnapshotCache>> getDbMap() {
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
   * @param key DB snapshot table key
   */
  public void invalidate(String key) throws IOException {
    dbMap.compute(key, (k, v) -> {
      if (v == null) {
        LOG.warn("Key: '{}' does not exist in cache.", k);
      } else {
        try {
          ((OmSnapshot) v.get()).close();
        } catch (IOException e) {
          throw new IllegalStateException("Failed to close snapshot: " + key, e);
        }
      }
      return null;
    });
  }

  /**
   * Immediately invalidate all entries and close their DB instances in cache.
   */
  public void invalidateAll() {
    Iterator<Map.Entry<String, ReferenceCounted<IOmMetadataReader, SnapshotCache>>>
        it = dbMap.entrySet().iterator();

    while (it.hasNext()) {
      Map.Entry<String, ReferenceCounted<IOmMetadataReader, SnapshotCache>> entry = it.next();
      OmSnapshot omSnapshot = (OmSnapshot) entry.getValue().get();
      try {
        // TODO: If wrapped with SoftReference<>, omSnapshot could be null?
        omSnapshot.close();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to close snapshot", e);
      }
      it.remove();
    }
  }

  /**
   * State the reason the current thread is getting the OmSnapshot instance.
   * Unused for now.
   */
  public enum Reason {
    FS_API_READ,
    SNAPDIFF_READ,
    DEEP_CLEAN_WRITE,
    GARBAGE_COLLECTION_WRITE
  }

  public ReferenceCounted<IOmMetadataReader, SnapshotCache> get(String key) throws IOException {
    return get(key, false);
  }

  /**
   * Get or load OmSnapshot. Shall be close()d after use.
   * TODO: [SNAPSHOT] Can add reason enum to param list later.
   * @param key snapshot table key
   * @return an OmSnapshot instance, or null on error
   */
  public ReferenceCounted<IOmMetadataReader, SnapshotCache> get(String key, boolean skipActiveCheck)
      throws IOException {
    // Atomic operation to initialize the OmSnapshot instance (once) if the key
    // does not exist, and increment the reference count on the instance.
    ReferenceCounted<IOmMetadataReader, SnapshotCache> rcOmSnapshot =
        dbMap.compute(key, (k, v) -> {
          if (v == null) {
            LOG.info("Loading snapshot. Table key: {}", k);
            try {
              v = new ReferenceCounted<>(cacheLoader.load(k), false, this);
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
          }
          if (v != null) {
            // When RC OmSnapshot is successfully loaded
            v.incrementRefCount();
          }
          return v;
        });

    if (rcOmSnapshot == null) {
      // The only exception that would fall through the loader logic above
      // is OMException with FILE_NOT_FOUND.
      throw new OMException("Snapshot table key '" + key + "' not found, "
          + "or the snapshot is no longer active",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }

    // If the snapshot is already loaded in cache, the check inside the loader
    // above is ignored. But we would still want to reject all get()s except
    // when called from SDT (and some) if the snapshot is not active anymore.
    if (!skipActiveCheck && !omSnapshotManager.isSnapshotStatus(key, SNAPSHOT_ACTIVE)) {
      // Ref count was incremented. Need to decrement on exception here.
      rcOmSnapshot.decrementRefCount();
      throw new OMException("Unable to load snapshot. " +
          "Snapshot with table key '" + key + "' is no longer active",
          FILE_NOT_FOUND);
    }

    // Check if any entries can be cleaned up.
    // At this point, cache size might temporarily exceed cacheSizeLimit
    // even if there are entries that can be evicted, which is fine since it
    // is a soft limit.
    cleanup();

    return rcOmSnapshot;
  }

  /**
   * Release the reference count on the OmSnapshot instance.
   * @param key snapshot table key
   */
  public void release(String key) {
    dbMap.compute(key, (k, v) -> {
      if (v == null) {
        throw new IllegalArgumentException("Key '" + key + "' does not exist in cache.");
      } else {
        v.decrementRefCount();
      }
      return v;
    });

    // The cache size might have already exceeded the soft limit
    // Thus triggering cleanup() to check and evict if applicable
    cleanup();
  }

  /**
   * Alternatively, can release with OmSnapshot instance directly.
   * @param omSnapshot OmSnapshot
   */
  public void release(OmSnapshot omSnapshot) {
    final String snapshotTableKey = omSnapshot.getSnapshotTableKey();
    release(snapshotTableKey);
  }

  /**
   * Wrapper for cleanupInternal() that is synchronized to prevent multiple
   * threads from interleaving into the cleanup method.
   */
  private synchronized void cleanup() {
    if (dbMap.size() > cacheSizeLimit) {
      cleanupInternal();
    }
  }

  /**
   * If cache size exceeds soft limit, attempt to clean up and close the
   * instances that has zero reference count.
   * TODO: [SNAPSHOT] Add new ozone debug CLI command to trigger this directly.
   */
  private void cleanupInternal() {
    for (Map.Entry<String, ReferenceCounted<IOmMetadataReader, SnapshotCache>> entry : dbMap.entrySet()) {
      dbMap.compute(entry.getKey(), (k, v) -> {
        if (v == null) {
          throw new IllegalStateException("Key '" + k + "' does not exist in cache. The RocksDB " +
              "instance of the Snapshot may not be closed properly.");
        }

        if (v.getTotalRefCount() > 0) {
          LOG.debug("Snapshot {} is still being referenced ({}), skipping its clean up",
              k, v.getTotalRefCount());
          return v;
        } else {
          LOG.debug("Closing Snapshot {}. It is not being referenced anymore.", k);
          // Close the instance, which also closes its DB handle.
          try {
            ((OmSnapshot) v.get()).close();
          } catch (IOException ex) {
            throw new IllegalStateException("Error while closing snapshot DB", ex);
          }
          return null;
        }
      });
    }
  }
}

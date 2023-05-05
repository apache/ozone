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
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
  // TODO: Wrap SoftReference<> around the value?
  private final ConcurrentHashMap<String, OmSnapshot> dbMap;

  // Linked hash set that holds OmSnapshot instances whose reference count
  // has reached zero. Those entries are eligible to be evicted and closed.
  // Sorted in last used order. Least-recently-used entry placed at the beginning.
  private final LinkedHashSet<OmSnapshot> instancesEligibleForClosure;
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
    this.instancesEligibleForClosure = new LinkedHashSet<>();
    this.omSnapshotManager = omSnapshotManager;
    this.cacheLoader = cacheLoader;
    this.cacheSizeLimit = cacheSizeLimit;
  }

  @VisibleForTesting
  ConcurrentHashMap<String, OmSnapshot> getDbMap() {
    return dbMap;
  }

  @VisibleForTesting
  LinkedHashSet<OmSnapshot> getInstancesEligibleForClosure() {
    return instancesEligibleForClosure;
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
    dbMap.computeIfPresent(key, (k, v) -> {
      instancesEligibleForClosure.remove(v);
      try {
        v.close();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to close snapshot: " + key, e);
      }
      // Remove the entry from map by returning null
      return null;
    });
  }

  /**
   * Immediately invalidate all entries and close their DB instances in cache.
   */
  public void invalidateAll() throws IOException {
    Iterator<Map.Entry<String, OmSnapshot>> it = dbMap.entrySet().iterator();

    while (it.hasNext()) {
      Map.Entry<String, OmSnapshot> entry = it.next();
      instancesEligibleForClosure.remove(entry.getValue());
      // TODO: If wrapped with SoftReference<>, entry.getValue() could be null?
      entry.getValue().close();
      it.remove();
    }
  }

  /**
   * State the reason the current thread is getting the OmSnapshot instance.
   */
  public enum Reason {
    FS_API_READ,
    SNAPDIFF_READ,
    DEEP_CLEAN_WRITE,
    GARBAGE_COLLECTION_WRITE
  }

  /**
   * Get or load OmSnapshot.
   * Must call release() when OmSnapshot is no longer used.
   * TODO: [SNAPSHOT] Can add reason enum to param list later.
   * @param key snapshot table key
   * @return an OmSnapshot instance, or null on error
   */
  public OmSnapshot get(String key) throws IOException {
    // Atomic operation to initialize the OmSnapshot instance (once) if the key
    // does not exist.
    dbMap.computeIfAbsent(key, k -> {
      LOG.info("Loading snapshot. Table key: {}", k);
      try {
        return cacheLoader.load(k);
      } catch (OMException omEx) {
        // Return null if the snapshot is no longer active
        if (!omEx.getResult().equals(OMException.ResultCodes.FILE_NOT_FOUND)) {
          throw new IllegalStateException(omEx);
        }
      } catch (IOException ioEx) {
        // Failed to load snapshot DB
        throw new IllegalStateException(ioEx);
      } catch (Exception ex) {
        // Unexpected and unknown exception thrown from Guava CacheLoader#load
        throw new IllegalStateException(ex);
      }
      // Do not put the value in the map on exception
      return null;
    });

    OmSnapshot omSnapshot = dbMap.get(key);
    // dbMap.get() returns non-null only if:
    // 1. snapshot DB is successfully loaded right above; or
    // 2. previous cache get calls initializes that.

    if (omSnapshot == null) {
      // The only exception that would fall through the loader logic above
      // is OMException with FILE_NOT_FOUND.
      throw new OMException("Snapshot table key '" + key + "' not found, "
          + "or the snapshot is no longer active",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }

    // If the snapshot is already loaded in cache, the check inside the loader
    // above is ignored. But we would still want to reject all get()s except
    // when called from SDT (and some) if the snapshot is not active any more.
    if (!omSnapshotManager.isSnapshotStatus(key, SNAPSHOT_ACTIVE) &&
        !SnapshotUtils.isCalledFromSnapshotDeletingService()) {
      throw new OMException("Unable to load snapshot. " +
          "Snapshot with table key '" + key + "' is no longer active",
          FILE_NOT_FOUND);
    }

    // Increment the reference count on the instance.
    omSnapshot.incrementRefCount();

    // Remove instance from clean up list.
    // TODO: [SNAPSHOT] Check thread safety with release
    instancesEligibleForClosure.remove(omSnapshot);

    // Check if any entries can be cleaned up.
    // At this point, cache size might temporarily exceed cacheSizeLimit
    // even if there are entries that can be evicted, which is fine since it
    // is a soft limit.
    cleanup();

    return omSnapshot;
  }

  /**
   * Release the reference count on the OmSnapshot instance.
   * @param key snapshot table key
   */
  public void release(String key) {
    OmSnapshot omSnapshot = dbMap.get(key);
    Preconditions.checkNotNull(omSnapshot,
        "Can't release reference count on a non-existent key");

    if (omSnapshot.decrementRefCount() == 0L) {
      // Eligible to be closed, add it to the list.
      instancesEligibleForClosure.add(omSnapshot);
      cleanup();
    }
  }

  /**
   * Alternatively, can release with OmSnapshot instance directly.
   * @param omSnapshot OmSnapshot
   */
  public void release(OmSnapshot omSnapshot) {
    final String key = omSnapshot.getSnapshotTableKey();
    release(key);
  }

  /**
   * If cache size exceeds soft limit, attempt to clean up and close the
   * instances that has zero reference count.
   * TODO: [SNAPSHOT] Add new ozone debug CLI command to trigger this directly.
   */
  private void cleanup() {
    long numOfInstToClose = (long) dbMap.size() - cacheSizeLimit;
    while (instancesEligibleForClosure.size() > 0 && numOfInstToClose > 0L) {
      // Get the first instance in the clean up list
      OmSnapshot omSnapshot = instancesEligibleForClosure.iterator().next();
      LOG.debug("Evicting OmSnapshot instance {} with table key {}",
          omSnapshot, omSnapshot.getSnapshotTableKey());
      // Sanity check
      Preconditions.checkState(omSnapshot.getRefCountTotal() == 0L,
          "Illegal state: OmSnapshot reference count non-zero ("
              + omSnapshot.getRefCountTotal() + ") but shows up in the "
              + "clean up list");

      final String key = omSnapshot.getSnapshotTableKey();
      final OmSnapshot result = dbMap.remove(key);
      // Sanity check
      Preconditions.checkState(omSnapshot == result,
          "Cache map entry removal failure. The cache is in an inconsistent "
              + "state. Expected OmSnapshot instance: " + omSnapshot
              + ", actual: " + result);

      instancesEligibleForClosure.remove(result);

      // Close the instance, which also closes its DB handle.
      try {
        omSnapshot.close();
      } catch (IOException ex) {
        throw new IllegalStateException("Error while closing snapshot DB", ex);
      }

      --numOfInstToClose;
    }

    // Print warning message if actual cache size is exceeding the soft limit
    // even after the cleanup procedure above.
    if ((long) dbMap.size() > cacheSizeLimit) {
      LOG.warn("Current snapshot cache size ({}) is exceeding configured "
          + "soft-limit ({}) after possible evictions.",
          dbMap.size(), cacheSizeLimit);

      Preconditions.checkState(instancesEligibleForClosure.size() == 0);
    }
  }

  /**
   * Check cache consistency.
   * @return true if the cache internal structure is consistent to the best of
   * its knowledge, false if found to be inconsistent and details logged.
   */
  public boolean isConsistent() {
    // Using dbMap as the source of truth in this cache, whether dbMap entries
    // are in OM DB's snapshotInfoTable is out of the scope of this check.

    // TODO: [SNAPSHOT]
    // 1. Objects in instancesEligibleForClosure must have ref count equals 0
    // 2. Objects in instancesEligibleForClosure should still be in dbMap
    // 3. instancesEligibleForClosure must be empty if cache size exceeds limit

    return true;
  }

}

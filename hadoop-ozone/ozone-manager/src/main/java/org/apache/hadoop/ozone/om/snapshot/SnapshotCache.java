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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;

/**
 * Thread-safe custom unbounded LRU cache to manage open snapshot DB instances.
 */
public class SnapshotCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotCache.class);

  // Snapshot cache internal hash map.
  // Key:   DB snapshot table key
  // Value: OmSnapshot instance, each holds a DB instance handle inside
  private final ConcurrentHashMap<String, OmSnapshot> dbMap;
  // Linked hash set that holds OmSnapshot instances whose reference count
  // has reached zero.
  // Sorted in last used order. Least-recently-used entry at the beginning.
  private final LinkedHashSet<OmSnapshot> instancesEligibleForClosure;
  private final OmSnapshotManager omSnapshotManager;
  private final CacheLoader<String, OmSnapshot> cacheLoader;
  // Soft-limit of the total number of snapshot DB instances allowed to be open
  // on the leader OM.
  private final int cacheSize;

  public SnapshotCache(
      OmSnapshotManager omSnapshotManager,
      CacheLoader<String, OmSnapshot> cacheLoader,
      int cacheSize) {
    this.dbMap = new ConcurrentHashMap<>();
    this.instancesEligibleForClosure = new LinkedHashSet<>();
    this.omSnapshotManager = omSnapshotManager;
    this.cacheLoader = cacheLoader;
    this.cacheSize = cacheSize;
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
    // when called from SDT if the snapshot is not active any more.
    if (!omSnapshotManager.isSnapshotStatus(key, SNAPSHOT_ACTIVE) &&
        !OmSnapshotManager.isCalledFromSnapshotDeletingService()) {
      throw new OMException("Unable to load snapshot. " +
          "Snapshot with table key '" + key + "' is no longer active",
          FILE_NOT_FOUND);
    }

    // Increment the reference count on the instance.
    omSnapshot.incrementRefCount();

    // Remove instance from clean up list.
    // TODO: [SNAPSHOT] Check thread safety with release
    instancesEligibleForClosure.remove(omSnapshot);

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
    long numOfInstToClose = (long) dbMap.size() - cacheSize;
    while (numOfInstToClose > 0L) {
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
    if ((long) dbMap.size() > cacheSize) {
      LOG.warn("Current snapshot cache size ({}) is exceeding configured "
          + "soft-limit ({}) after possible evictions.",
          dbMap.size(), cacheSize);

      Preconditions.checkState(instancesEligibleForClosure.size() == 0);
    }
  }

}

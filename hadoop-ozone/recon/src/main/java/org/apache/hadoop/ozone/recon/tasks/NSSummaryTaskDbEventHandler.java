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

package org.apache.hadoop.ozone.recon.tasks;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for holding all NSSummaryTask methods
 * related to DB operations so that they can commonly be
 * used in NSSummaryTaskWithFSO and NSSummaryTaskWithLegacy.
 */
public class NSSummaryTaskDbEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskDbEventHandler.class);
  // Size ~ 32k works well; tune as needed.
  private static final int DB_CACHE_CAPACITY = 32_768;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private ReconOMMetadataManager reconOMMetadataManager;

  // Small, hot LRU to avoid hammering the DB for the same parents/dirs.
  private LinkedHashMap<Long, NSSummary> dbReadCache;

  public NSSummaryTaskDbEventHandler(ReconNamespaceSummaryManager
                                         reconNamespaceSummaryManager,
                                     ReconOMMetadataManager
                                         reconOMMetadataManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.dbReadCache =
        new LinkedHashMap<Long, NSSummary>(DB_CACHE_CAPACITY, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<Long, NSSummary> e) {
            return size() > DB_CACHE_CAPACITY;
          }
        };
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public ReconOMMetadataManager getReconOMMetadataManager() {
    return reconOMMetadataManager;
  }

  private void updateNSSummariesToDB(Map<Long, NSSummary> nsSummaryMap, Collection<Long> objectIdsToBeDeleted)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<Long, NSSummary> entry : nsSummaryMap.entrySet()) {
        try {
          reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation, entry.getKey(), entry.getValue());
        } catch (IOException e) {
          LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
          throw e;
        }
      }
      for (Long objectId : objectIdsToBeDeleted) {
        try {
          reconNamespaceSummaryManager.batchDeleteNSSummaries(rdbBatchOperation, objectId);
        } catch (IOException e) {
          LOG.error("Unable to delete Namespace Summary data from Recon DB.", e);
          throw e;
        }
      }
      reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
    }
    LOG.debug("Successfully updated Namespace Summary data in Recon DB.");
  }

  protected void handlePutKeyEvent(OmKeyInfo keyInfo, Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    final long parentId = keyInfo.getParentObjectID();
    final long size = keyInfo.getDataSize();
    final long repl = keyInfo.getReplicatedSize();

    NSSummary ns = getOrLoadSummary(nsSummaryMap, parentId);

    // Totals (this directory holds totals of all descendants)
    ns.incFilesAndBytes(1, size);

    long curRepl = ns.getReplicatedSizeOfFiles();
    if (curRepl < 0) {
      curRepl = 0;
      ns.setReplicatedSizeOfFiles(0);
    }
    ns.setReplicatedSizeOfFiles(curRepl + Math.max(0L, repl));

    // Buckets count immediate files only
    ns.incBucket(ReconUtils.getFileSizeBinIndex(size));

    // Propagate totals to all ancestors (buckets are NOT propagated)
    propagateSizeUpwards(parentId, size, Math.max(0L, repl), 1, nsSummaryMap);
  }

  protected void handlePutDirEvent(OmDirectoryInfo directoryInfo, Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    final long parentId = directoryInfo.getParentObjectID();
    final long dirId = directoryInfo.getObjectID();

    // Snapshot existing totals (if directory already existed)
    NSSummary existing = maybeGetSummary(nsSummaryMap, dirId);
    final long existedSize = existing != null ? existing.getSizeOfFiles() : 0L;
    final int existedFiles = existing != null ? existing.getNumOfFiles() : 0;
    long existedRepl = existing != null ? existing.getReplicatedSizeOfFiles() : 0L;
    if (existedRepl < 0) {
      existedRepl = 0;
    }

    // Ensure current directory summary exists in this batch and set metadata
    NSSummary self = getOrLoadSummary(nsSummaryMap, dirId);
    self.setParentId(parentId);
    self.setDirName(directoryInfo.getName());

    // Parent summary: add child link
    NSSummary parent = getOrLoadSummary(nsSummaryMap, parentId);
    parent.addChildDir(dirId);

    // If the directory already had content, propagate its totals upward
    if (existedSize > 0 || existedFiles > 0) {
      propagateSizeUpwards(dirId, existedSize, existedRepl, existedFiles, nsSummaryMap);
    }
  }

  protected void handleDeleteKeyEvent(OmKeyInfo keyInfo, Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    final long parentId = keyInfo.getParentObjectID();
    final long size = keyInfo.getDataSize();
    final long repl = keyInfo.getReplicatedSize();

    NSSummary ns = maybeGetSummary(nsSummaryMap, parentId);
    if (ns == null) {
      LOG.error("The namespace table is not correctly populated for parentId={}.", parentId);
      return;
    }

    // Totals with clamping
    ns.decFilesAndBytes(1, size); // clamps to >= 0 inside

    long curRepl = ns.getReplicatedSizeOfFiles();
    if (curRepl < 0) {
      curRepl = 0;
    }
    long newRepl = curRepl - Math.max(0L, repl);
    ns.setReplicatedSizeOfFiles(clampNonNegativeLong(newRepl));

    // Buckets: immediate files only
    ns.decBucket(ReconUtils.getFileSizeBinIndex(size));

    // Propagate negative deltas up the chain
    propagateSizeUpwards(parentId, -size, -Math.max(0L, repl), -1, nsSummaryMap);
  }

  protected void handleDeleteDirEvent(OmDirectoryInfo directoryInfo, Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    final long dirId = directoryInfo.getObjectID();
    final long parentId = directoryInfo.getParentObjectID();

    // Parent summary must exist to unlink child
    NSSummary parent = maybeGetSummary(nsSummaryMap, parentId);
    if (parent == null) {
      LOG.error("The namespace table is not correctly populated for parentId={} (deleteDir).", parentId);
      return;
    }
    parent.removeChildDir(dirId);

    // If the directory existed, propagate its totals upward as negative deltas
    NSSummary deleted = maybeGetSummary(nsSummaryMap, dirId);
    if (deleted != null) {
      long repl = deleted.getReplicatedSizeOfFiles();
      if (repl < 0) {
        repl = 0;
      }

      propagateSizeUpwards(dirId, -deleted.getSizeOfFiles(), -repl, -deleted.getNumOfFiles(), nsSummaryMap);

      // Unlink the directory (no parent)
      deleted.setParentId(0);
    }
  }

  protected boolean flushAndCommitNSToDB(Map<Long, NSSummary> nsSummaryMap) {
    try {
      updateNSSummariesToDB(nsSummaryMap, Collections.emptyList());
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return false;
    } finally {
      nsSummaryMap.clear();
    }
    return true;
  }

  /**
   * Flush and commit updated NSSummary to DB. This includes deleted objects of OM metadata also.
   *
   * @param nsSummaryMap         Map of objectId to NSSummary
   * @param objectIdsToBeDeleted list of objectids to be deleted
   * @return true if successful, false otherwise
   */
  protected boolean flushAndCommitUpdatedNSToDB(Map<Long, NSSummary> nsSummaryMap,
                                                Collection<Long> objectIdsToBeDeleted) {
    try {
      updateNSSummariesToDB(nsSummaryMap, objectIdsToBeDeleted);
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB. batchSize={}", nsSummaryMap.size(), e);
      return false;
    } finally {
      nsSummaryMap.clear();
    }
    return true;
  }

  /**
   * Propagates size and count changes upwards through the parent chain.
   * This ensures that when files are added/deleted, all ancestor directories
   * reflect the total changes in their sizeOfFiles and numOfFiles fields.
   */
  /**
   * Propagate totals (size, replicated size, file count) to all ancestors.
   * NOTE: buckets represent immediate files only and are NOT propagated.
   */
  protected void propagateSizeUpwards(long objectId,
                                      long sizeChange,
                                      long replicatedSizeChange,
                                      int countChange,
                                      Map<Long, NSSummary> nsSummaryMap) throws IOException {
    long current = objectId;

    // Walk up the chain iteratively (avoid deep recursion)
    while (true) {
      NSSummary cur = maybeGetSummary(nsSummaryMap, current);
      if (cur == null) {
        break; // no more parents
      }
      long parentId = cur.getParentId();
      if (parentId == 0) {
        break;
      }

      NSSummary parent = getOrLoadSummary(nsSummaryMap, parentId);

      // Update totals with clamping
      long newSize = parent.getSizeOfFiles() + sizeChange;
      parent.setSizeOfFiles(clampNonNegativeLong(newSize));

      long parentRepl = parent.getReplicatedSizeOfFiles();
      if (parentRepl < 0) {
        parentRepl = 0;
      }
      long newRepl = parentRepl + replicatedSizeChange;
      parent.setReplicatedSizeOfFiles(clampNonNegativeLong(newRepl));

      int newCount = parent.getNumOfFiles() + countChange;
      parent.setNumOfFiles(clampNonNegativeInt(newCount));

      // Move up
      current = parentId;
    }
  }

  // ---------- Helpers: read-through LRU + safe loaders ----------

  /**
   * Get summary from batch map or DB/LRU; creates empty if missing, and ensures it is in the map.
   */
  private NSSummary getOrLoadSummary(Map<Long, NSSummary> map, long id) throws IOException {
    NSSummary s = map.get(id);
    if (s != null) {
      return s;
    }

    // LRU first
    synchronized (dbReadCache) {
      s = dbReadCache.get(id);
    }
    if (s == null) {
      s = reconNamespaceSummaryManager.getNSSummary(id);
      if (s == null) {
        s = new NSSummary();
      }
      synchronized (dbReadCache) {
        dbReadCache.put(id, s);
      }
    }

    // Ensure presence in this batch map
    NSSummary prev = map.get(id);
    if (prev == null) {
      map.put(id, s);
    }
    return prev != null ? prev : s;
  }

  /**
   * Like getOrLoadSummary but DOES NOT create a new empty summary if DB has none.
   */
  private NSSummary maybeGetSummary(Map<Long, NSSummary> map, long id) throws IOException {
    NSSummary s = map.get(id);
    if (s != null) {
      return s;
    }

    synchronized (dbReadCache) {
      s = dbReadCache.get(id);
    }
    if (s == null) {
      s = reconNamespaceSummaryManager.getNSSummary(id);
      if (s != null) {
        synchronized (dbReadCache) {
          dbReadCache.put(id, s);
        }
      }
    }
    if (s != null && map.get(id) == null) {
      map.put(id, s);
    }
    return s;
  }

  /**
   * Clamp helpers to avoid underflow if events are out-of-order.
   */
  private static int clampNonNegativeInt(int v) {
    return v < 0 ? 0 : v;
  }

  private static long clampNonNegativeLong(long v) {
    return v < 0L ? 0L : v;
  }

}

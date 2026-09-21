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

package org.apache.hadoop.ozone.om.snapshot.diff;

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.DELETE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.RENAME;
import static org.apache.hadoop.ozone.om.snapshot.diff.SnapDiffJobStore.DEFAULT_BATCH_SIZE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone.getDiffReportEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stages 2–4 of the optimized full snapshot diff pipeline: merge join and
 * classification, top-level delete retention, ancestor backtrack path resolution,
 * dependency ordering, and batched report write.
 *
 * <p>Classified rows are persisted in per-type column families as minimal parent-id
 * payloads encoded by {@link SnapDiffJobStore}. Only directory delete/rename object ids
 * are held in heap during processing besides path-resolution LRU state.
 */
public final class MergeJoinSnapDiffWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MergeJoinSnapDiffWriter.class);

  private static final DiffType[] FSO_RESOLVE_ORDER =
      {MODIFY, RENAME, CREATE};

  private MergeJoinSnapDiffWriter() {
  }

  public static Pair<Long, String> writeReport(SnapshotDiffManager manager, SnapDiffJobStore store,
      long bucketObjectId, boolean isFso) throws IOException {
    return writeReport(manager, store, bucketObjectId, isFso, false);
  }

  /**
   * {@code dependencyOrderingEnabled} applies to FSO buckets only; OBS entries are always
   * resolved and written directly to the report table.
   */
  public static Pair<Long, String> writeReport(SnapshotDiffManager manager,
      SnapDiffJobStore store, long bucketObjectId, boolean isFso,
      boolean dependencyOrderingEnabled) throws IOException {
    store.beginReportWrite();
    if (isFso) {
      // Classified CFs hold only objectId + parentId(s). Paths are always resolved before emission.
      classifyMergeJoin(store, true);
      store.dropListColumnFamilies();

      SnapDiffPathResolver fromResolver = store.newFromPathResolver(bucketObjectId);
      SnapDiffPathResolver toResolver = store.newToPathResolver(bucketObjectId);
      boolean[] dependencyOrderingEnabledRef = {dependencyOrderingEnabled};
      int nodeCount = filterAndResolveTopLevelDeletes(manager, store, bucketObjectId,
          fromResolver, dependencyOrderingEnabledRef);
      store.dropDirectoryIdColumnFamilies();
      store.dropClassifiedColumnFamily(DELETE);

      // Remaining classified entries are resolved and emitted straight to the report table,
      // if dependency ordering is disabled.
      // Otherwise, entries are emitted into {jobId}-dependency-nodes for ordering.
      nodeCount = resolveDiffPaths(store, fromResolver, toResolver, dependencyOrderingEnabledRef,
          nodeCount);
      for (DiffType diffType : FSO_RESOLVE_ORDER) {
        store.dropClassifiedColumnFamily(diffType);
      }
      store.dropEdgeColumnFamilies();

      if (dependencyOrderingEnabledRef[0]) {
        // Classified entries are ordered and emitted straight to the report table.
        orderAndWriteReport(store, nodeCount);
      }
      store.dropDependencyGraphColumnFamilies();
      store.markTemporaryColumnFamiliesDropped();

    } else {
      classifyMergeJoin(store, false);
      // check if dependency graph is required.
      store.dropListColumnFamilies();
    }
    return store.finishReportWrite();
  }

  private static void classifyMergeJoin(SnapDiffJobStore store, boolean isFso) throws IOException {
    try (SnapDiffJobStore.ListIterator newHead = store.newListIterator();
         SnapDiffJobStore.ListIterator oldHead = store.oldListIterator()) {
      Map.Entry<Long, byte[]> newEntry = newHead.hasNext() ? newHead.next() : null;
      Map.Entry<Long, byte[]> oldEntry = oldHead.hasNext() ? oldHead.next() : null;

      while (newEntry != null || oldEntry != null) {
        if (oldEntry == null || (newEntry != null && newEntry.getKey() < oldEntry.getKey())) {
          emitCreate(store, newEntry, isFso);
          newEntry = newHead.hasNext() ? newHead.next() : null;
        } else if (newEntry == null || newEntry.getKey() > oldEntry.getKey()) {
          emitDelete(store, oldEntry, isFso);
          oldEntry = oldHead.hasNext() ? oldHead.next() : null;
        } else {
          emitBothPresent(store, newEntry, oldEntry, isFso);
          newEntry = newHead.hasNext() ? newHead.next() : null;
          oldEntry = oldHead.hasNext() ? oldHead.next() : null;
        }
      }
    } catch (RocksDatabaseException e) {
      throw new IOException(e);
    }
    store.flushWrites();
  }

  private static void emitCreate(SnapDiffJobStore store, Map.Entry<Long, byte[]> newEntry,
      boolean isFso) throws IOException {
    if (store.isPresentMarker(newEntry.getValue())) {
      return;
    }
    EntryValue value = EntryValue.fromBytes(newEntry.getValue());
    if (isFso) {
      store.putClassified(CREATE, newEntry.getKey(),
          SnapDiffJobStore.encodeClassifiedParent(value.getParentId()));
    } else {
      store.putReportEntry(getDiffReportEntry(CREATE, value.getName()));
    }

  }

  private static void emitDelete(SnapDiffJobStore store, Map.Entry<Long, byte[]> oldEntry,
      boolean isFso) throws IOException {
    EntryValue value = EntryValue.fromBytes(oldEntry.getValue());
    if (isFso) {
      if (value.isDir()) {
        store.addDeletedDirectoryId(oldEntry.getKey());
      }
      store.putClassified(DELETE, oldEntry.getKey(),
          SnapDiffJobStore.encodeClassifiedParent(value.getParentId()));
    } else {
      store.putReportEntry(getDiffReportEntry(DELETE, value.getName()));
    }

  }

  private static void emitBothPresent(SnapDiffJobStore store,
      Map.Entry<Long, byte[]> newEntry, Map.Entry<Long, byte[]> oldEntry, boolean isFso)
      throws IOException {
    byte[] newBytes = newEntry.getValue();
    if (store.isPresentMarker(newBytes)) {
      return;
    }
    EntryValue newValue = EntryValue.fromBytes(newBytes);
    EntryValue oldValue = EntryValue.fromBytes(oldEntry.getValue());
    if (newValue.isDir() != oldValue.isDir()) {
      LOG.error("SnapDiff job {} objectId {} has isDir mismatch (new={}, old={})",
          store.getJobId(), newEntry.getKey(), newValue.isDir(), oldValue.isDir());
      throw new IOException(String.format(
          "Stage 1 invariant violation for job %s objectId %d: isDir mismatch",
          store.getJobId(), newEntry.getKey()));
    }
    boolean pathDiffers = newValue.getParentId() != oldValue.getParentId()
        || !newValue.getName().equals(oldValue.getName());
    boolean contentDiffers = !Arrays.equals(newValue.getSignature(), oldValue.getSignature());
    if (pathDiffers) {
      if (isFso) {
        store.putClassified(RENAME, newEntry.getKey(), SnapDiffJobStore.encodeClassifiedRename(
            oldValue.getParentId(), newValue.getParentId()));
        // Check should be on oldId so that it is read as this entry from old snapshot has been renamed.
        // Even in top-level deletes the ID here is checked against old snapshot namespace.
        if (newValue.isDir()) {
          store.addRenamedDirectoryId(newEntry.getKey());
        }
      } else {
        store.putReportEntry(getDiffReportEntry(RENAME, oldValue.getName(), newValue.getName()));
      }
    }
    if (contentDiffers) {
      if (isFso) {
        store.putClassified(MODIFY, oldEntry.getKey(),
            SnapDiffJobStore.encodeClassifiedParent(oldValue.getParentId()));
      } else {
        store.putReportEntry(getDiffReportEntry(MODIFY, oldValue.getName()));
      }

    }
  }

  private static int filterAndResolveTopLevelDeletes(SnapshotDiffManager manager,
      SnapDiffJobStore store, long bucketObjectId, SnapDiffPathResolver fromResolver,
      boolean[] dependencyOrderingEnabled) throws IOException {
    Map<Long, Boolean> ancestorMemo = newAncestorMemo(store);
    int nodeIndex = 0;
    List<Map.Entry<Long, byte[]>> deleteBatch = new ArrayList<>(DEFAULT_BATCH_SIZE);
    try (SnapDiffJobStore.ClassifiedIterator deleteIter = store.classifiedIterator(DELETE)) {
      while (deleteIter.hasNext()) {
        deleteBatch.add(deleteIter.next());
        if (deleteBatch.size() == DEFAULT_BATCH_SIZE) {
          nodeIndex = filterAndResolveTopLevelDeleteBatch(manager, store, bucketObjectId,
              fromResolver, dependencyOrderingEnabled, ancestorMemo, nodeIndex, deleteBatch);
          deleteBatch.clear();
        }
      }
    } catch (RocksDatabaseException e) {
      throw new IOException(e);
    }
    if (!deleteBatch.isEmpty()) {
      nodeIndex = filterAndResolveTopLevelDeleteBatch(manager, store, bucketObjectId,
          fromResolver, dependencyOrderingEnabled, ancestorMemo, nodeIndex, deleteBatch);
    }
    store.flushWrites();
    return dependencyOrderingEnabled[0] ? nodeIndex : 0;
  }

  private static Map<Long, Boolean> newAncestorMemo(SnapDiffJobStore store) {
    final int capacity = (int) Math.min(store.getMaxInMemoryEntries(), Integer.MAX_VALUE);
    return new LinkedHashMap<Long, Boolean>(capacity, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
        return size() > capacity;
      }
    };
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static int filterAndResolveTopLevelDeleteBatch(SnapshotDiffManager manager,
      SnapDiffJobStore store, long bucketObjectId, SnapDiffPathResolver fromResolver,
      boolean[] dependencyOrderingEnabled, Map<Long, Boolean> ancestorMemo, int nodeIndex,
      List<Map.Entry<Long, byte[]>> deleteBatch) throws IOException {
    List<Long> parentObjectIds = new ArrayList<>(deleteBatch.size());
    for (Map.Entry<Long, byte[]> row : deleteBatch) {
      parentObjectIds.add(SnapDiffJobStore.decodeClassifiedParent(row.getValue()));
    }
    boolean[] hasDeletedAncestor = manager.hasDeletedAncestors(parentObjectIds,
        store::isDeletedDirectoryId, store::isRenamedDirectoryId, store::multiGetFromParentIds,
        bucketObjectId, ancestorMemo);
    List<Map.Entry<Long, byte[]>> survivorRows = new ArrayList<>();
    List<Long> survivorObjectIds = new ArrayList<>();
    for (int i = 0; i < deleteBatch.size(); i++) {
      if (hasDeletedAncestor[i]) {
        continue;
      }
      Map.Entry<Long, byte[]> row = deleteBatch.get(i);
      survivorRows.add(row);
      survivorObjectIds.add(row.getKey());
    }
    if (survivorRows.isEmpty()) {
      return nodeIndex;
    }
    List<DiffReportEntry> reportEntries = resolveReportEntries(DELETE, survivorObjectIds,
        fromResolver, null);
    return writeReportEntriesForRows(store, DELETE, survivorRows, reportEntries,
        dependencyOrderingEnabled, nodeIndex);
  }

  private static int resolveDiffPaths(SnapDiffJobStore store,
      SnapDiffPathResolver fromResolver, SnapDiffPathResolver toResolver,
      boolean[] dependencyOrderingEnabled, int nodeIndex) throws IOException {
    for (DiffType diffType : FSO_RESOLVE_ORDER) {
      List<Map.Entry<Long, byte[]>> batch = new ArrayList<>(DEFAULT_BATCH_SIZE);
      try (SnapDiffJobStore.ClassifiedIterator iter = store.classifiedIterator(diffType)) {
        while (iter.hasNext()) {
          batch.add(iter.next());
          if (batch.size() == DEFAULT_BATCH_SIZE) {
            nodeIndex = resolveDiffPathBatch(store, diffType, fromResolver, toResolver,
                dependencyOrderingEnabled, nodeIndex, batch);
            batch.clear();
          }
        }
      } catch (RocksDatabaseException e) {
        throw new IOException(e);
      }
      if (!batch.isEmpty()) {
        nodeIndex = resolveDiffPathBatch(store, diffType, fromResolver, toResolver,
            dependencyOrderingEnabled, nodeIndex, batch);
      }
    }
    store.flushWrites();
    return dependencyOrderingEnabled[0] ? nodeIndex : 0;
  }

  private static int resolveDiffPathBatch(SnapDiffJobStore store, DiffType diffType,
      SnapDiffPathResolver fromResolver, SnapDiffPathResolver toResolver,
      boolean[] dependencyOrderingEnabled, int nodeIndex,
      List<Map.Entry<Long, byte[]>> batch) throws IOException {
    List<Long> objectIds = new ArrayList<>(batch.size());
    for (Map.Entry<Long, byte[]> row : batch) {
      objectIds.add(row.getKey());
    }
    List<DiffReportEntry> reportEntries = resolveReportEntries(diffType, objectIds, fromResolver,
        toResolver);
    return writeReportEntriesForRows(store, diffType, batch, reportEntries,
        dependencyOrderingEnabled, nodeIndex);
  }

  private static List<DiffReportEntry> resolveReportEntries(DiffType diffType,
      List<Long> objectIds, SnapDiffPathResolver fromResolver,
      SnapDiffPathResolver toResolver) throws IOException {
    List<DiffReportEntry> reportEntries = new ArrayList<>(objectIds.size());
    switch (diffType) {
    case CREATE:
      List<String> toPaths = toResolver.resolvePaths(objectIds);
      for (String toPath : toPaths) {
        reportEntries.add(toPath != null ? getDiffReportEntry(CREATE, toPath) : null);
      }
      break;
    case DELETE:
    case MODIFY:
      List<String> fromPaths = fromResolver.resolvePaths(objectIds);
      for (String fromPath : fromPaths) {
        reportEntries.add(fromPath != null ? getDiffReportEntry(diffType, fromPath) : null);
      }
      break;
    case RENAME:
      List<String> sourcePaths = fromResolver.resolvePaths(objectIds);
      List<String> targetPaths = toResolver.resolvePaths(objectIds);
      for (int i = 0; i < objectIds.size(); i++) {
        String sourcePath = sourcePaths.get(i);
        String targetPath = targetPaths.get(i);
        if (sourcePath != null && targetPath != null) {
          reportEntries.add(getDiffReportEntry(RENAME, sourcePath, targetPath));
        } else {
          reportEntries.add(null);
        }
      }
      break;
    default:
      throw new IllegalArgumentException("Unsupported diff type: " + diffType);
    }
    return reportEntries;
  }

  private static int writeReportEntriesForRows(SnapDiffJobStore store, DiffType diffType,
      List<Map.Entry<Long, byte[]>> rows, List<DiffReportEntry> reportEntries,
      boolean[] dependencyOrderingEnabled, int nodeIndex) throws IOException {
    if (!dependencyOrderingEnabled[0]) {
      writeDirectReportEntries(store, diffType, rows, reportEntries);
      return 0;
    }

    List<SnapDiffDependencyEntry> dependencyEntries = new ArrayList<>();
    for (int i = 0; i < rows.size(); i++) {
      DiffReportEntry reportEntry = reportEntries.get(i);
      if (reportEntry == null) {
        LOG.debug("SnapDiff job {}: dropping {} report entry for unresolvable objectId: {}",
            store.getJobId(), diffType.name(), rows.get(i).getKey());
        continue;
      }
      dependencyEntries.add(buildDependencyEntry(rows.get(i).getKey(), rows.get(i).getValue(),
          reportEntry));
    }
    if (dependencyEntries.isEmpty()) {
      return nodeIndex;
    }
    if (nodeIndex + dependencyEntries.size() > store.getMaxInMemoryEntries()) {
      LOG.error("SnapDiff job {}: dependency node count exceeds limit of {} for job; "
              + "falling back to direct report write",
          store.getJobId(), store.getMaxInMemoryEntries());
      if (nodeIndex > 0) {
        flushUnorderedDependencyNodes(store, nodeIndex);
      }
      dependencyOrderingEnabled[0] = false;
      writeDirectReportEntries(store, diffType, rows, reportEntries);
      return 0;
    }
    store.putDependencyNodes(nodeIndex, dependencyEntries);
    return nodeIndex + dependencyEntries.size();
  }

  private static void writeDirectReportEntries(SnapDiffJobStore store, DiffType diffType,
      List<Map.Entry<Long, byte[]>> rows, List<DiffReportEntry> reportEntries) throws IOException {
    List<DiffReportEntry> directReportEntries = new ArrayList<>();
    for (int i = 0; i < rows.size(); i++) {
      DiffReportEntry reportEntry = reportEntries.get(i);
      if (reportEntry == null) {
        LOG.debug("SnapDiff job {}: dropping {} report entry for unresolvable objectId: {}",
            store.getJobId(), diffType.name(), rows.get(i).getKey());
        continue;
      }
      directReportEntries.add(reportEntry);
    }
    if (!directReportEntries.isEmpty()) {
      store.putReportEntries(directReportEntries);
    }
  }

  private static void flushUnorderedDependencyNodes(SnapDiffJobStore store, int nodeCount)
      throws IOException {
    for (int position = 0; position < nodeCount; position += DEFAULT_BATCH_SIZE) {
      int end = Math.min(position + DEFAULT_BATCH_SIZE, nodeCount);
      List<Integer> nodeBatch = new ArrayList<>(end - position);
      for (int batchPosition = position; batchPosition < end; batchPosition++) {
        nodeBatch.add(batchPosition);
      }
      List<DiffReportEntry> reportEntries = store.multiGetDependencyReportEntries(nodeBatch);
      store.putReportEntries(reportEntries);
    }
  }

  private static SnapDiffDependencyEntry buildDependencyEntry(long objectId, byte[] classifiedValue,
      DiffReportEntry reportEntry) {
    long sourceParentId;
    long targetParentId;
    if (reportEntry.getType() == RENAME) {
      sourceParentId = SnapDiffJobStore.decodeClassifiedRenameSourceParent(classifiedValue);
      targetParentId = SnapDiffJobStore.decodeClassifiedRenameTargetParent(classifiedValue);
    } else {
      sourceParentId = SnapDiffJobStore.decodeClassifiedParent(classifiedValue);
      targetParentId = sourceParentId;
    }
    return new SnapDiffDependencyEntry(objectId, sourceParentId, targetParentId, reportEntry);
  }

  private static void orderAndWriteReport(SnapDiffJobStore store, int nodeCount)
      throws IOException {
    try (SnapDiffJobStore.DependencyNodeIterator dependencyEntries = store.dependencyNodeIterator()) {
      buildOrderedNodeIds(store.getJobId(), store, dependencyEntries);
    }
    for (int position = 0; position < nodeCount; position += DEFAULT_BATCH_SIZE) {
      int end = Math.min(position + DEFAULT_BATCH_SIZE, nodeCount);
      List<Integer> nodeBatch = new ArrayList<>(end - position);
      for (int batchPosition = position; batchPosition < end; batchPosition++) {
        nodeBatch.add(store.getDepOrderNodeIndex(batchPosition));
      }
      List<DiffReportEntry> reportEntries = store.multiGetDependencyReportEntries(nodeBatch);
      store.putReportEntries(reportEntries);
    }
  }

  private static List<Integer> buildOrderedNodeIds(String jobId,
      SnapDiffJobStore store, java.util.Iterator<SnapDiffDependencyEntry> dependencyEntries)
      throws IOException {
    SnapDiffDependencyGraph graph = new SnapDiffDependencyGraph(dependencyEntries);
    try {
      List<Integer> orderedNodeIds = graph.getOrderedNodeIds();
      graph.persistToStore(store, orderedNodeIds);
      store.flushWrites();
      return orderedNodeIds;
    } catch (IllegalStateException e) {
      if (e.getMessage() != null && e.getMessage().contains("Cycle detected")) {
        LOG.error("SnapDiff job {} dependency graph cycle: {}", jobId, e.getMessage());
        throw new IOException("Dependency graph cycle for job " + jobId, e);
      }
      throw e;
    }
  }
}

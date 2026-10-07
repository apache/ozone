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
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone.getDiffReportEntry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
 * Merge join and classification, top-level delete retention, ancestor backtrack path resolution,
 * dependency ordering, and batched report write of diff entries.
 *
 */
public final class MergeJoinSnapDiffWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MergeJoinSnapDiffWriter.class);

  private static final DiffType[] FSO_RESOLVE_ORDER =
      {MODIFY, RENAME, CREATE};

  private MergeJoinSnapDiffWriter() {
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
      // Classified CFs hold only objectId + (parentId, name). Paths are always resolved before emission.
      classifyMergeJoin(store, true);
      store.dropListColumnFamilies();

      SnapDiffPathResolver fromResolver = store.newFromPathResolver(bucketObjectId);
      boolean[] dependencyOrderingEnabledRef = {dependencyOrderingEnabled};
      int nodeCount = filterAndResolveTopLevelDeletes(manager, store, bucketObjectId,
          fromResolver, dependencyOrderingEnabledRef);
      store.dropDirectoryIdColumnFamilies();
      store.dropClassifiedColumnFamily(DELETE);

      // Remaining classified entries are resolved and emitted straight to the report table,
      // if dependency ordering is disabled.
      // Otherwise, entries are emitted into {jobId}-dependency-nodes for ordering.
      SnapDiffPathResolver toResolver = store.newToPathResolver(bucketObjectId);
      nodeCount = resolveDiffPaths(store, fromResolver, toResolver, dependencyOrderingEnabledRef,
          nodeCount);
      fromResolver.clearPathCache();
      toResolver.clearPathCache();
      for (DiffType diffType : FSO_RESOLVE_ORDER) {
        store.dropClassifiedColumnFamily(diffType);
      }
      store.dropEdgeColumnFamilies();

      if (dependencyOrderingEnabledRef[0]) {
        // Classified entries are ordered and emitted straight to the report table.
        orderAndWriteReport(store, nodeCount);
      }
      store.dropDependencyGraphColumnFamilies();

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
          SnapDiffJobStore.encodeEdgeLink(value.getParentId(), nameBytes(value.getName())));
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
          SnapDiffJobStore.encodeEdgeLink(value.getParentId(), nameBytes(value.getName())));
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
        store.putClassified(RENAME, oldEntry.getKey(), SnapDiffJobStore.encodeClassifiedRename(
            oldValue.getParentId(), nameBytes(oldValue.getName()),
            newValue.getParentId(), nameBytes(newValue.getName())));
        // Check should be on oldId so that it is read as this entry from old snapshot has been renamed.
        // Even in top-level deletes the ID here is checked against old snapshot namespace.
        if (oldValue.isDir()) {
          store.addRenamedDirectoryId(oldEntry.getKey());
        }
      } else {
        store.putReportEntry(getDiffReportEntry(RENAME, oldValue.getName(), newValue.getName()));
      }
    }
    if (contentDiffers) {
      if (isFso) {
        store.putClassified(MODIFY, oldEntry.getKey(), SnapDiffJobStore.encodeEdgeLink(
            oldValue.getParentId(), nameBytes(oldValue.getName())));
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
    int writeBatchSize = store.getWriteBatchSize();
    List<Map.Entry<Long, byte[]>> deleteBatch = new ArrayList<>(writeBatchSize);
    try (SnapDiffJobStore.ClassifiedIterator deleteIter = store.classifiedIterator(DELETE)) {
      while (deleteIter.hasNext()) {
        deleteBatch.add(deleteIter.next());
        if (deleteBatch.size() == writeBatchSize) {
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
    ancestorMemo.clear();
    return dependencyOrderingEnabled[0] ? nodeIndex : 0;
  }

  private static Map<Long, Boolean> newAncestorMemo(SnapDiffJobStore store) {
    final int capacity = store.ancestorMemoCapacity();
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
      parentObjectIds.add(SnapDiffJobStore.decodeEdgeLinkParentId(row.getValue()));
    }
    boolean[] hasDeletedAncestor = manager.hasDeletedAncestors(parentObjectIds,
        store::isDeletedDirectoryId, store::isRenamedDirectoryId, store::multiGetFromParentIds,
        bucketObjectId, ancestorMemo);
    List<Long> survivorObjectIds = new ArrayList<>();
    List<String> survivorNames = new ArrayList<>();
    int survivorIndex = 0;
    for (int i = 0; i < deleteBatch.size(); i++) {
      if (hasDeletedAncestor[i]) {
        continue;
      }
      Map.Entry<Long, byte[]> row = deleteBatch.get(i);
      survivorObjectIds.add(row.getKey());
      survivorNames.add(SnapDiffJobStore.decodeEdgeLinkName(row.getValue()));
      if (survivorIndex != i) {
        parentObjectIds.set(survivorIndex, parentObjectIds.get(i));
      }
      survivorIndex++;
    }
    if (survivorIndex == 0) {
      return nodeIndex;
    }
    parentObjectIds.subList(survivorIndex, parentObjectIds.size()).clear();
    List<DiffReportEntry> reportEntries = resolveReportEntries(DELETE, parentObjectIds, null, survivorNames,
        null, fromResolver, null);
    return writeReportEntriesForRows(store, DELETE, survivorObjectIds, parentObjectIds, null,
        reportEntries, dependencyOrderingEnabled, nodeIndex);
  }

  private static int resolveDiffPaths(SnapDiffJobStore store,
      SnapDiffPathResolver fromResolver, SnapDiffPathResolver toResolver,
      boolean[] dependencyOrderingEnabled, int nodeIndex) throws IOException {
    int writeBatchSize = store.getWriteBatchSize();
    for (DiffType diffType : FSO_RESOLVE_ORDER) {
      List<Map.Entry<Long, byte[]>> batch = new ArrayList<>(writeBatchSize);
      try (SnapDiffJobStore.ClassifiedIterator iter = store.classifiedIterator(diffType)) {
        while (iter.hasNext()) {
          batch.add(iter.next());
          if (batch.size() == writeBatchSize) {
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
    List<Long> sourceParentIds = new ArrayList<>(batch.size());
    List<Long> targetParentIds = new ArrayList<>(batch.size());
    List<String> sourceNames = new ArrayList<>(batch.size());
    List<String> targetNames = new ArrayList<>(batch.size());
    decodeClassifiedBatch(diffType, batch, objectIds, sourceParentIds,
        targetParentIds, sourceNames, targetNames);
    List<DiffReportEntry> reportEntries = resolveReportEntries(diffType, sourceParentIds, targetParentIds,
        sourceNames, targetNames, fromResolver, toResolver);
    return writeReportEntriesForRows(store, diffType, objectIds, sourceParentIds,
        targetParentIds, reportEntries, dependencyOrderingEnabled, nodeIndex);
  }

  private static void decodeClassifiedBatch(DiffType diffType, List<Map.Entry<Long, byte[]>> batch,
      List<Long> objectIds, List<Long> sourceParentIds, List<Long> targetParentIds,
      List<String> sourceNames, List<String> targetNames) {
    for (Map.Entry<Long, byte[]> row : batch) {
      objectIds.add(row.getKey());
      byte[] value = row.getValue();
      if (diffType == RENAME) {
        byte[] sourceLink = SnapDiffJobStore.decodeClassifiedRenameSourceLink(value);
        byte[] targetLink = SnapDiffJobStore.decodeClassifiedRenameTargetLink(value);
        sourceParentIds.add(SnapDiffJobStore.decodeEdgeLinkParentId(sourceLink));
        targetParentIds.add(SnapDiffJobStore.decodeEdgeLinkParentId(targetLink));
        sourceNames.add(SnapDiffJobStore.decodeEdgeLinkName(sourceLink));
        targetNames.add(SnapDiffJobStore.decodeEdgeLinkName(targetLink));
      } else {
        sourceParentIds.add(SnapDiffJobStore.decodeEdgeLinkParentId(value));
        sourceNames.add(SnapDiffJobStore.decodeEdgeLinkName(value));
      }
    }
  }

  private static List<DiffReportEntry> resolveReportEntries(DiffType diffType,
      List<Long> sourceParentIds, List<Long> targetParentIds,
      List<String> sourceNames, List<String> targetNames,
      SnapDiffPathResolver fromResolver, SnapDiffPathResolver toResolver) throws IOException {
    switch (diffType) {
    case CREATE:
      return resolvePathsFromParentAndName(toResolver, sourceParentIds, sourceNames, CREATE);
    case DELETE:
    case MODIFY:
      return resolvePathsFromParentAndName(fromResolver, sourceParentIds, sourceNames, diffType);
    case RENAME:
      List<String> sourceParentPaths = fromResolver.resolvePaths(sourceParentIds);
      List<String> targetParentPaths = toResolver.resolvePaths(targetParentIds);
      List<DiffReportEntry> reportEntries = new ArrayList<>(sourceNames.size());
      for (int i = 0; i < sourceNames.size(); i++) {
        String sourcePath = buildChildPath(sourceParentPaths.get(i), sourceNames.get(i));
        String targetPath = buildChildPath(targetParentPaths.get(i), targetNames.get(i));
        if (sourcePath != null && targetPath != null) {
          reportEntries.add(getDiffReportEntry(RENAME, sourcePath, targetPath));
        } else {
          reportEntries.add(null);
        }
      }
      return reportEntries;
    default:
      throw new IllegalArgumentException("Unsupported diff type: " + diffType);
    }
  }

  private static List<DiffReportEntry> resolvePathsFromParentAndName(
      SnapDiffPathResolver resolver, List<Long> parentIds, List<String> names, DiffType diffType)
      throws IOException {
    List<String> parentPaths = resolver.resolvePaths(parentIds);
    List<DiffReportEntry> reportEntries = new ArrayList<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      String path = buildChildPath(parentPaths.get(i), names.get(i));
      reportEntries.add(path != null ? getDiffReportEntry(diffType, path) : null);
    }
    return reportEntries;
  }

  private static String buildChildPath(String parentPath, String name) {
    if (parentPath == null) {
      return null;
    }
    return parentPath.isEmpty() ? name : parentPath + OM_KEY_PREFIX + name;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static int writeReportEntriesForRows(SnapDiffJobStore store, DiffType diffType,
      List<Long> objectIds, List<Long> sourceParentIds, List<Long> targetParentIds,
      List<DiffReportEntry> reportEntries, boolean[] dependencyOrderingEnabled, int nodeIndex) throws IOException {
    for (int i = 0; i < objectIds.size(); i++) {
      DiffReportEntry reportEntry = reportEntries.get(i);
      if (reportEntry == null) {
        LOG.debug("SnapDiff job {}: dropping {} report entry for unresolvable objectId: {}",
            store.getJobId(), diffType.name(), objectIds.get(i));
        continue;
      }
      nodeIndex++;
      if (!dependencyOrderingEnabled[0]) {
        store.putReportEntry(reportEntry);
      } else {
        if (nodeIndex > store.getMaxInMemoryEntries()) {
          LOG.error("SnapDiff job {}: dependency node count exceeds limit of {} for job; "
                  + "falling back to direct report write",
              store.getJobId(), store.getMaxInMemoryEntries());
          flushUnorderedDependencyNodes(store, nodeIndex);
          dependencyOrderingEnabled[0] = false;
          store.putReportEntry(reportEntry);
        } else {
          long sourceParentId;
          long targetParentId;
          if (diffType == RENAME) {
            sourceParentId = sourceParentIds.get(i);
            targetParentId = targetParentIds.get(i);
          } else {
            sourceParentId = sourceParentIds.get(i);
            targetParentId = sourceParentId;
          }
          store.putDependencyNode(nodeIndex,
              buildDependencyEntry(objectIds.get(i), sourceParentId, targetParentId, reportEntry));
        }
      }
    }
    return nodeIndex;
  }

  private static void flushUnorderedDependencyNodes(SnapDiffJobStore store, int nodeIndex)
      throws IOException {
    if (nodeIndex <= 1) {
      return;
    }
    store.flushWrites();
    int writeBatchSize = store.getWriteBatchSize();
    for (int position = 1; position < nodeIndex; position += writeBatchSize) {
      int end = Math.min(position + writeBatchSize, nodeIndex);
      List<Integer> nodeBatch = new ArrayList<>(end - position);
      for (int batchPosition = position; batchPosition < end; batchPosition++) {
        nodeBatch.add(batchPosition);
      }
      List<DiffReportEntry> reportEntries = store.multiGetDependencyReportEntries(nodeBatch);
      store.putReportEntries(reportEntries);
    }
  }

  private static SnapDiffDependencyEntry buildDependencyEntry(long objectId, long sourceParentId,
      long targetParentId, DiffReportEntry reportEntry) {
    return new SnapDiffDependencyEntry(objectId, sourceParentId, targetParentId, reportEntry);
  }

  private static void orderAndWriteReport(SnapDiffJobStore store, int nodeCount)
      throws IOException {
    List<Integer> orderedNodeIds;
    try (SnapDiffJobStore.DependencyNodeIterator dependencyEntries = store.dependencyNodeIterator()) {
      orderedNodeIds = buildOrderedNodeIds(store.getJobId(), dependencyEntries);
    }
    int writeBatchSize = store.getWriteBatchSize();
    for (int position = 0; position < nodeCount; position += writeBatchSize) {
      int end = Math.min(position + writeBatchSize, nodeCount);
      List<Integer> nodeBatch = new ArrayList<>(end - position);
      for (int batchPosition = position; batchPosition < end; batchPosition++) {
        nodeBatch.add(orderedNodeIds.get(batchPosition) + 1);
      }
      List<DiffReportEntry> reportEntries = store.multiGetDependencyReportEntries(nodeBatch);
      store.putOrderedReportEntries(reportEntries);
    }
  }

  private static List<Integer> buildOrderedNodeIds(String jobId,
      java.util.Iterator<SnapDiffDependencyEntry> dependencyEntries) throws IOException {
    SnapDiffDependencyGraph graph = new SnapDiffDependencyGraph(dependencyEntries);
    try {
      return graph.getOrderedNodeIds();
    } catch (IllegalStateException e) {
      if (e.getMessage() != null && e.getMessage().contains("Cycle detected")) {
        LOG.error("SnapDiff job {} dependency graph cycle: {}", jobId, e.getMessage());
        throw new IOException("Dependency graph cycle for job " + jobId, e);
      }
      throw e;
    }
  }

  private static byte[] nameBytes(String name) {
    return name.getBytes(StandardCharsets.UTF_8);
  }
}

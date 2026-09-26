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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_IN_MEMORY_ENTRIES_PER_JOB_DEFAULT;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager.getReportKeyForIndex;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.dropColumnFamilyHandle;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Owns the per-job temporary RocksDB column families and batched writes shared
 * between optimized snapshot diff pipeline stages.
 *
 * <p>Diff-candidate {@code objectId}s are held in memory while their count is at
 * most {@code maxInMemoryEntries}; larger sets spill to a temporary column family.
 * Deleted and renamed directory object ids collected during merge join use the same
 * in-memory limit and spill policy.
 *
 * <p>All RocksDB puts use raw {@code byte[]} keys and values (the JNI boundary). Callers
 * serialize {@link EntryValue} via {@link EntryValue#toBytes()} before writing to
 * {@code newList}/{@code oldList}.
 *
 * <p>Temporary column families disable RocksDB auto-compactions and are dropped once a pipeline
 * stage no longer reads them. Individual key deletes are avoided so short-lived CFs do not
 * accumulate tombstones before drop.
 *
 * <p>This initial version supports the full diff sequential reader ({@link FullDiffSequentialReader}).
 * DAG diff support extends this store in HDDS-15393.
 */
public final class SnapDiffJobStore implements AutoCloseable {

  /** Default RocksDB {@code WriteBatch} commit size for job-store puts. */
  public static final int DEFAULT_BATCH_SIZE = 1000;

  /** Reverse edge value header: parentId (8 BE) + nameLen (4 BE). */
  static final int EDGE_LINK_HEADER_BYTES = Long.BYTES + Integer.BYTES;

  /** Classified RENAME value length: sourceParentId + targetParentId. */
  private static final int CLASSIFIED_RENAME_BYTES = 2 * Long.BYTES;

  private static final String NEW_LIST_SUFFIX = "-new-list";
  private static final String OLD_LIST_SUFFIX = "-old-list";
  private static final String CAND_IDS_SUFFIX = "-cand-ids";
  private static final String TO_EDGES_SUFFIX = "-to-edges";
  private static final String FROM_EDGES_SUFFIX = "-from-edges";
  private static final String CLASSIFIED_CREATE_SUFFIX = "-cls-create";
  private static final String CLASSIFIED_DELETE_SUFFIX = "-cls-delete";
  private static final String CLASSIFIED_MODIFY_SUFFIX = "-cls-modify";
  private static final String CLASSIFIED_RENAME_SUFFIX = "-cls-rename";
  private static final String DEPENDENCY_NODE_SUFFIX = "-dependency-nodes";
  private static final String DEP_ADJ_OFF_SUFFIX = "-dep-adj-off";
  private static final String DEP_ADJ_TGT_SUFFIX = "-dep-adj-tgt";
  private static final String DEP_IN_DEG_SUFFIX = "-dep-in-deg";
  private static final String DEP_ORDER_SUFFIX = "-dep-order";
  private static final String DELETED_DIR_IDS_SUFFIX = "-deleted-dir-ids";
  private static final String RENAMED_DIR_IDS_SUFFIX = "-renamed-dir-ids";
  private static final String REPORT_SUFFIX = "-report";

  private final ManagedRocksDB db;
  private final String jobId;
  private final boolean fso;
  private final CodecRegistry codecRegistry;
  private ColumnFamilyHandle reportCfh;
  private final boolean ownsReportColumnFamily;
  private final byte[] presentMarker;
  private final ManagedColumnFamilyOptions familyOptions;
  private final ManagedColumnFamilyOptions tempColumnFamilyOptions;
  private final long maxInMemoryEntries;

  private ColumnFamilyHandle newListCf;
  private ColumnFamilyHandle oldListCf;
  private ColumnFamilyHandle toEdgesCf;
  private ColumnFamilyHandle fromEdgesCf;
  private ColumnFamilyHandle classifiedCreateCf;
  private ColumnFamilyHandle classifiedDeleteCf;
  private ColumnFamilyHandle classifiedModifyCf;
  private ColumnFamilyHandle classifiedRenameCf;
  private ColumnFamilyHandle dependencyNodesCf;
  private ColumnFamilyHandle depAdjOffCf;
  private ColumnFamilyHandle depAdjTgtCf;
  private ColumnFamilyHandle depInDegCf;
  private ColumnFamilyHandle depOrderCf;

  private String diffCandCfName;
  private Set<Long> diffCandidates;
  private ColumnFamilyHandle diffCandidatesCf;
  private boolean diffCandidatesSpilled;

  private Set<Long> deletedDirectoryIds;
  private Set<Long> renamedDirectoryIds;
  private String deletedDirectoryIdsCfName;
  private String renamedDirectoryIdsCfName;
  private ColumnFamilyHandle deletedDirectoryIdsCf;
  private ColumnFamilyHandle renamedDirectoryIdsCf;
  private boolean deletedDirectoryIdsSpilled;
  private boolean renamedDirectoryIdsSpilled;

  private final ManagedWriteBatch writeBatch;
  private final ManagedWriteOptions writeOptions;
  private final int writeBatchSize;
  private int pendingOps;

  private long reportIndex;
  private String largestReportKey;
  private boolean reportWriteStarted;
  private boolean temporaryColumnFamiliesDropped;

  /** Reusable big-endian key buffers; safe because RocksDB copies keys on put/get. */
  private final byte[] objectIdKeyBuffer = new byte[Long.BYTES];
  private final byte[] intKeyBuffer = new byte[Integer.BYTES];

  /** Full diff: shared new/old lists plus FSO edge column families. */
  public enum Mode {
    FULL
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private SnapDiffJobStore(ManagedRocksDB db, String jobId, CodecRegistry codecRegistry, boolean fso,
      int writeBatchSize, ManagedColumnFamilyOptions familyOptions, long maxInMemoryEntries,
      @Nullable ColumnFamilyHandle reportCfh) throws IOException {
    this.db = db;
    this.jobId = jobId;
    this.fso = fso;
    this.codecRegistry = codecRegistry;
    this.reportCfh = reportCfh;
    this.ownsReportColumnFamily = reportCfh == null;
    this.writeBatchSize = writeBatchSize;
    this.familyOptions = familyOptions;
    this.tempColumnFamilyOptions = new ManagedColumnFamilyOptions(familyOptions);
    this.tempColumnFamilyOptions.setDisableAutoCompactions(true);
    this.maxInMemoryEntries = maxInMemoryEntries;
    this.presentMarker = codecRegistry.asRawData(Boolean.TRUE);
    this.writeBatch = new ManagedWriteBatch();
    this.writeOptions = new ManagedWriteOptions();
    this.pendingOps = 0;
    this.diffCandidates = new HashSet<>();
    this.deletedDirectoryIds = new HashSet<>();
    this.renamedDirectoryIds = new HashSet<>();
  }

  public static SnapDiffJobStore open(@Nonnull ManagedRocksDB db,
      @Nonnull CodecRegistry codecRegistry,
      @Nonnull ManagedColumnFamilyOptions familyOptions,
      @Nonnull String jobId,
      boolean fso,
      @Nonnull Mode mode) throws IOException {
    return open(db, codecRegistry, familyOptions, jobId, fso, mode, DEFAULT_BATCH_SIZE,
        OZONE_OM_SNAPSHOT_DIFF_MAX_IN_MEMORY_ENTRIES_PER_JOB_DEFAULT);
  }

  public static SnapDiffJobStore open(@Nonnull ManagedRocksDB db,
      @Nonnull CodecRegistry codecRegistry,
      @Nonnull ManagedColumnFamilyOptions familyOptions,
      @Nonnull String jobId,
      boolean fso,
      @Nonnull Mode mode,
      int writeBatchSize) throws IOException {
    return open(db, codecRegistry, familyOptions, jobId, fso, mode, writeBatchSize,
        OZONE_OM_SNAPSHOT_DIFF_MAX_IN_MEMORY_ENTRIES_PER_JOB_DEFAULT);
  }

  @SuppressWarnings("parameternumber")
  public static SnapDiffJobStore open(@Nonnull ManagedRocksDB db,
      @Nonnull CodecRegistry codecRegistry,
      @Nonnull ManagedColumnFamilyOptions familyOptions,
      @Nonnull String jobId,
      boolean fso,
      @Nonnull Mode mode,
      int writeBatchSize,
      long maxInMemoryEntries) throws IOException {
    return open(db, codecRegistry, familyOptions, jobId, fso, mode, writeBatchSize,
        maxInMemoryEntries, null);
  }

  @SuppressWarnings("parameternumber")
  public static SnapDiffJobStore open(@Nonnull ManagedRocksDB db,
      @Nonnull CodecRegistry codecRegistry,
      @Nonnull ManagedColumnFamilyOptions familyOptions,
      @Nonnull String jobId,
      boolean fso,
      @Nonnull Mode mode,
      int writeBatchSize,
      long maxInMemoryEntries,
      @Nullable ColumnFamilyHandle reportCfh) throws IOException {
    if (mode != Mode.FULL) {
      throw new IllegalArgumentException("Unsupported mode: " + mode);
    }
    SnapDiffJobStore store = new SnapDiffJobStore(db, jobId, codecRegistry, fso, writeBatchSize,
        familyOptions, maxInMemoryEntries, reportCfh);
    try {
      store.initColumnFamilies(familyOptions);
      return store;
    } catch (RocksDBException e) {
      store.closeQuietly();
      throw new IOException("Failed to open SnapDiff job store for job " + jobId, e);
    }
  }

  long getMaxInMemoryEntries() {
    return maxInMemoryEntries;
  }

  /** Marks job-scoped column families already dropped by the merge-join writer. */
  void markTemporaryColumnFamiliesDropped() {
    temporaryColumnFamiliesDropped = true;
  }

  /** Returns report entries written during a test run. */
  List<DiffReportEntry> readReportEntriesForTest() throws IOException {
    if (reportCfh == null) {
      return Collections.emptyList();
    }
    List<DiffReportEntry> entries = new ArrayList<>();
    try (ManagedRocksIterator iterator = new ManagedRocksIterator(db.get().newIterator(reportCfh))) {
      for (iterator.get().seekToFirst(); iterator.get().isValid(); iterator.get().next()) {
        entries.add(codecRegistry.asObject(iterator.get().value(), DiffReportEntry.class));
      }
    }
    return entries;
  }

  /** Writes a present-marker for {@code objectId} in {@code newList}. */
  public void putNewListPresentMarker(long objectId) throws IOException {
    batchPut(newListCf, objectIdKeyBuffer(objectId), presentMarker);
  }

  /** Writes a full diff-candidate {@link EntryValue} for {@code objectId} in {@code newList}. */
  public void putNewList(long objectId, byte[] entryValue) throws IOException {
    batchPut(newListCf, objectIdKeyBuffer(objectId), entryValue);
  }

  public void putOldList(long objectId, byte[] entryValue) throws IOException {
    batchPut(oldListCf, objectIdKeyBuffer(objectId), entryValue);
  }

  /** Writes {@code objectId -> (parentId, name)} on the to-side reverse edge index. */
  public void putToEdge(long parentId, long objectId, byte[] name) throws IOException {
    requireFso();
    batchPut(toEdgesCf, objectIdKeyBuffer(objectId), encodeEdgeLink(parentId, name));
  }

  /** Writes {@code objectId -> (parentId, name)} on the from-side reverse edge index. */
  public void putFromEdge(long parentId, long objectId, byte[] name) throws IOException {
    requireFso();
    batchPut(fromEdgesCf, objectIdKeyBuffer(objectId), encodeEdgeLink(parentId, name));
  }

  public byte[] getNewList(long objectId) throws IOException {
    return get(newListCf, objectIdKeyBuffer(objectId));
  }

  public byte[] getOldList(long objectId) throws IOException {
    return get(oldListCf, objectIdKeyBuffer(objectId));
  }

  public String getJobId() {
    return jobId;
  }

  /** Returns whether the stored new-list value is a present-marker (unchanged). */
  public boolean isPresentMarker(byte[] value) {
    return value != null && Arrays.equals(value, presentMarker);
  }

  /** Ascending merge-join iterator over {@code newList}. */
  public ListIterator newListIterator() throws RocksDatabaseException {
    return new ListIterator(newListCf);
  }

  /** Ascending merge-join iterator over {@code oldList}. */
  public ListIterator oldListIterator() throws RocksDatabaseException {
    return new ListIterator(oldListCf);
  }

  /** Builds a from-snapshot path resolver over the reverse edge index. */
  SnapDiffPathResolver newFromPathResolver(long bucketObjectId) {
    requireFso();
    return new SnapDiffPathResolver(db, fromEdgesCf, bucketObjectId);
  }

  /** Builds a to-snapshot path resolver over the reverse edge index. */
  SnapDiffPathResolver newToPathResolver(long bucketObjectId) {
    requireFso();
    return new SnapDiffPathResolver(db, toEdgesCf, bucketObjectId);
  }

  List<byte[]> multiGetFromEdgeValues(List<Long> objectIds) throws IOException {
    if (objectIds.isEmpty()) {
      return Collections.emptyList();
    }
    List<byte[]> keys = new ArrayList<>(objectIds.size());
    for (Long objectId : objectIds) {
      keys.add(objectIdKey(objectId));
    }
    List<ColumnFamilyHandle> cfs = Collections.nCopies(objectIds.size(), fromEdgesCf);
    try {
      return db.get().multiGetAsList(cfs, keys);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  public List<Long> multiGetFromParentIds(List<Long> objectIds) throws IOException {
    List<byte[]> values = multiGetFromEdgeValues(objectIds);
    List<Long> parentIds = new ArrayList<>(values.size());
    for (byte[] value : values) {
      parentIds.add(value == null ? null : decodeEdgeLinkParentId(value));
    }
    return parentIds;
  }

  /** Writes a classified entry keyed by {@code objectId}; diff type is implied by the CF. */
  public void putClassified(DiffType diffType, long objectId, byte[] value) throws IOException {
    batchPut(classifiedColumnFamily(diffType), objectIdKeyBuffer(objectId), value);
  }

  /** Drops one classified column family after its entries have been consumed. */
  void dropClassifiedColumnFamily(DiffType diffType) {
    switch (diffType) {
    case CREATE:
      classifiedCreateCf = dropAndClose(classifiedCreateCf);
      break;
    case DELETE:
      classifiedDeleteCf = dropAndClose(classifiedDeleteCf);
      break;
    case MODIFY:
      classifiedModifyCf = dropAndClose(classifiedModifyCf);
      break;
    case RENAME:
      classifiedRenameCf = dropAndClose(classifiedRenameCf);
      break;
    default:
      throw new IllegalArgumentException("Unsupported diff type: " + diffType);
    }
  }

  /** Drops merge-join list column families after classification. */
  void dropListColumnFamilies() {
    newListCf = dropAndClose(newListCf);
    oldListCf = dropAndClose(oldListCf);
  }

  /** Drops FSO reverse-edge column families after path resolution. */
  void dropEdgeColumnFamilies() {
    toEdgesCf = dropAndClose(toEdgesCf);
    fromEdgesCf = dropAndClose(fromEdgesCf);
  }

  /** Drops persisted dependency-graph column families. */
  void dropDependencyGraphColumnFamilies() {
    dependencyNodesCf = dropAndClose(dependencyNodesCf);
    depAdjOffCf = dropAndClose(depAdjOffCf);
    depAdjTgtCf = dropAndClose(depAdjTgtCf);
    depInDegCf = dropAndClose(depInDegCf);
    depOrderCf = dropAndClose(depOrderCf);
  }

  /** Ascending iterator over one classified column family. */
  public ClassifiedIterator classifiedIterator(DiffType diffType) throws RocksDatabaseException {
    return new ClassifiedIterator(classifiedColumnFamily(diffType));
  }

  /** Writes dependency node entries starting at {@code startNodeIndex}. */
  public void putDependencyNodes(int startNodeIndex, List<SnapDiffDependencyEntry> entries)
      throws IOException {
    for (int i = 0; i < entries.size(); i++) {
      batchPut(dependencyNodesCf, intKeyBuffer(startNodeIndex + i), entries.get(i).toResolvedBytes());
    }
  }

  /** Ascending iterator over the dependency-node column family. */
  public DependencyNodeIterator dependencyNodeIterator() throws RocksDatabaseException {
    return new DependencyNodeIterator(dependencyNodesCf);
  }

  public List<DiffReportEntry> multiGetDependencyReportEntries(List<Integer> nodeIndices)
      throws IOException {
    if (nodeIndices.isEmpty()) {
      return Collections.emptyList();
    }
    List<byte[]> keys = new ArrayList<>(nodeIndices.size());
    for (Integer nodeIndex : nodeIndices) {
      keys.add(intValueBuffer(nodeIndex));
    }
    List<ColumnFamilyHandle> cfs = Collections.nCopies(nodeIndices.size(), dependencyNodesCf);
    try {
      List<byte[]> values = db.get().multiGetAsList(cfs, keys);
      List<DiffReportEntry> reportEntries = new ArrayList<>(values.size());
      for (int i = 0; i < values.size(); i++) {
        byte[] value = values.get(i);
        if (value == null) {
          throw new IOException("Missing dependency node at index " + nodeIndices.get(i));
        }
        reportEntries.add(SnapDiffDependencyEntry.fromResolvedBytes(value).getReportEntry());
      }
      return reportEntries;
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  /** Persists CSR adjacency offset for {@code nodeIndex}. Key: 4-byte BE index. */
  public void putDepAdjOffset(int nodeIndex, int offset) throws IOException {
    batchPut(depAdjOffCf, intKeyBuffer(nodeIndex), intValueBuffer(offset));
  }

  /** Persists one CSR adjacency target. Key: 4-byte BE edge index. */
  public void putDepAdjTarget(int edgeIndex, int targetNodeIndex) throws IOException {
    batchPut(depAdjTgtCf, intKeyBuffer(edgeIndex), intValueBuffer(targetNodeIndex));
  }

  /** Persists in-degree for {@code nodeIndex}. Key: 4-byte BE index. */
  public void putDepInDegree(int nodeIndex, int inDegree) throws IOException {
    batchPut(depInDegCf, intKeyBuffer(nodeIndex), intValueBuffer(inDegree));
  }

  /** Persists topological order position {@code position -> nodeIndex}. */
  public void putDepOrder(int position, int nodeIndex) throws IOException {
    batchPut(depOrderCf, intKeyBuffer(position), intValueBuffer(nodeIndex));
  }

  /** Reads one node index from the persisted topological order. */
  public int getDepOrderNodeIndex(int position) throws IOException {
    byte[] value = get(depOrderCf, intKeyBuffer(position));
    if (value == null || value.length < Integer.BYTES) {
      throw new IOException("Missing dependency order entry at position " + position);
    }
    return decodeInt(value, 0);
  }

  /**
   * Records a to-side diff candidate {@code objectId}. Retained in memory until
   * {@code maxInMemoryEntries} is reached, then spilled to a temporary column family.
   */
  public void addDiffCandidate(long objectId) throws IOException {
    if (diffCandidatesSpilled) {
      batchPut(diffCandidatesCf, objectIdKeyBuffer(objectId), presentMarker);
      return;
    }
    if (diffCandidates.size() >= maxInMemoryEntries) {
      spillDiffCandidates();
    }
    if (diffCandidatesSpilled) {
      batchPut(diffCandidatesCf, objectIdKeyBuffer(objectId), presentMarker);
    } else {
      diffCandidates.add(objectId);
    }
  }

  /** Returns whether {@code objectId} was gated in as a to-side diff candidate. */
  public boolean isDiffCandidate(long objectId) throws IOException {
    if (diffCandidatesSpilled) {
      return get(diffCandidatesCf, objectIdKeyBuffer(objectId)) != null;
    }
    return diffCandidates.contains(objectId);
  }

  /** Clears the diff-candidate set after a from-side scan consumes it. */
  public void clearDiffCandidates() throws IOException {
    diffCandidates.clear();
    if (diffCandidatesSpilled) {
      diffCandidatesCf = dropAndClose(diffCandidatesCf);
      diffCandidatesSpilled = false;
    }
  }

  /** Returns the current in-memory diff-candidate count (for tests and limit wiring). */
  public int getDiffCandidateCount() {
    return diffCandidates.size();
  }

  /** Records a deleted directory {@code objectId} for ancestor filtering. */
  public void addDeletedDirectoryId(long objectId) throws IOException {
    if (deletedDirectoryIdsSpilled) {
      batchPut(deletedDirectoryIdsCf, objectIdKeyBuffer(objectId), presentMarker);
      return;
    }
    if (deletedDirectoryIds.size() >= maxInMemoryEntries) {
      spillDeletedDirectoryIds();
    }
    if (deletedDirectoryIdsSpilled) {
      batchPut(deletedDirectoryIdsCf, objectIdKeyBuffer(objectId), presentMarker);
    } else {
      deletedDirectoryIds.add(objectId);
    }
  }

  /** Records a renamed directory {@code objectId} for ancestor filtering. */
  public void addRenamedDirectoryId(long objectId) throws IOException {
    if (renamedDirectoryIdsSpilled) {
      batchPut(renamedDirectoryIdsCf, objectIdKeyBuffer(objectId), presentMarker);
      return;
    }
    if (renamedDirectoryIds.size() >= maxInMemoryEntries) {
      spillRenamedDirectoryIds();
    }
    if (renamedDirectoryIdsSpilled) {
      batchPut(renamedDirectoryIdsCf, objectIdKeyBuffer(objectId), presentMarker);
    } else {
      renamedDirectoryIds.add(objectId);
    }
  }

  /** Returns whether {@code objectId} is a deleted directory in this diff. */
  public boolean isDeletedDirectoryId(long objectId) throws IOException {
    if (deletedDirectoryIdsSpilled) {
      return get(deletedDirectoryIdsCf, objectIdKeyBuffer(objectId)) != null;
    }
    return deletedDirectoryIds.contains(objectId);
  }

  /** Returns whether {@code objectId} is a renamed directory in this diff. */
  public boolean isRenamedDirectoryId(long objectId) throws IOException {
    if (renamedDirectoryIdsSpilled) {
      return get(renamedDirectoryIdsCf, objectIdKeyBuffer(objectId)) != null;
    }
    return renamedDirectoryIds.contains(objectId);
  }

  /** Drops directory-id sets after delete filtering completes. */
  void dropDirectoryIdColumnFamilies() {
    deletedDirectoryIds.clear();
    deletedDirectoryIdsCf = dropAndClose(deletedDirectoryIdsCf);
    deletedDirectoryIdsSpilled = false;
    renamedDirectoryIds.clear();
    renamedDirectoryIdsCf = dropAndClose(renamedDirectoryIdsCf);
    renamedDirectoryIdsSpilled = false;
  }

  public void flushWrites() throws IOException {
    if (pendingOps == 0) {
      return;
    }
    try {
      db.get().write(writeOptions, writeBatch);
    } catch (RocksDBException e) {
      throw new IOException("Failed to flush SnapDiff job store write batch", e);
    }
    writeBatch.clear();
    pendingOps = 0;
  }

  /**
   * Starts batched emission of resolved rows to the snap diff report table.
   */
  public void beginReportWrite() {
    if (reportCfh == null) {
      throw new IllegalStateException("Snap diff report column family not configured for job store");
    }
    if (reportWriteStarted) {
      throw new IllegalStateException("Snap diff report write already started for job " + jobId);
    }
    reportWriteStarted = true;
    reportIndex = 0;
    largestReportKey = "";
  }

  /**
   * Appends one resolved {@link DiffReportEntry} to the snap diff report table.
   */
  public void putReportEntry(DiffReportEntry entry) throws IOException {
    if (!reportWriteStarted) {
      throw new IllegalStateException("Snap diff report write not started for job " + jobId);
    }
    appendReportEntry(entry);
  }

  /**
   * Appends a batch of resolved {@link DiffReportEntry}s to the snap diff report table.
   */
  public void putReportEntries(List<DiffReportEntry> entries) throws IOException {
    if (!reportWriteStarted) {
      throw new IllegalStateException("Snap diff report write not started for job " + jobId);
    }
    for (DiffReportEntry entry : entries) {
      appendReportEntry(entry);
    }
  }

  /**
   * Flushes pending report rows and returns the entry count and largest report key.
   */
  public Pair<Long, String> finishReportWrite() throws IOException {
    if (!reportWriteStarted) {
      throw new IllegalStateException("Snap diff report write not started for job " + jobId);
    }
    flushWrites();
    reportWriteStarted = false;
    return Pair.of(reportIndex, largestReportKey);
  }

  private byte[] objectIdKeyBuffer(long objectId) {
    encodeLong(objectIdKeyBuffer, 0, objectId);
    return objectIdKeyBuffer;
  }

  private byte[] intKeyBuffer(int value) {
    encodeInt(intKeyBuffer, 0, value);
    return intKeyBuffer;
  }

  private byte[] intValueBuffer(int value) {
    byte[] buffer = new byte[Integer.BYTES];
    encodeInt(buffer, 0, value);
    return buffer;
  }

  private void appendReportEntry(DiffReportEntry entry) throws IOException {
    String jobReportKey = getReportKeyForIndex(jobId, entry.getType(), reportIndex++);
    batchPut(reportCfh, codecRegistry.asRawData(jobReportKey), codecRegistry.asRawData(entry));
    if (jobReportKey.compareTo(largestReportKey) > 0) {
      largestReportKey = jobReportKey;
    }
  }

  private ColumnFamilyHandle classifiedColumnFamily(DiffType diffType) {
    ColumnFamilyHandle cf;
    switch (diffType) {
    case CREATE:
      cf = classifiedCreateCf;
      break;
    case DELETE:
      cf = classifiedDeleteCf;
      break;
    case MODIFY:
      cf = classifiedModifyCf;
      break;
    case RENAME:
      cf = classifiedRenameCf;
      break;
    default:
      throw new IllegalArgumentException("Unsupported diff type: " + diffType);
    }
    if (cf == null) {
      throw new IllegalStateException("Classified column family already dropped: " + diffType);
    }
    return cf;
  }

  private void initColumnFamilies(ManagedColumnFamilyOptions options)
      throws RocksDBException {
    newListCf = createColumnFamily(this.jobId + NEW_LIST_SUFFIX);
    oldListCf = createColumnFamily(this.jobId + OLD_LIST_SUFFIX);
    diffCandCfName = this.jobId + CAND_IDS_SUFFIX;
    deletedDirectoryIdsCfName = this.jobId + DELETED_DIR_IDS_SUFFIX;
    renamedDirectoryIdsCfName = this.jobId + RENAMED_DIR_IDS_SUFFIX;
    classifiedCreateCf = createColumnFamily(this.jobId + CLASSIFIED_CREATE_SUFFIX);
    classifiedDeleteCf = createColumnFamily(this.jobId + CLASSIFIED_DELETE_SUFFIX);
    classifiedModifyCf = createColumnFamily(this.jobId + CLASSIFIED_MODIFY_SUFFIX);
    classifiedRenameCf = createColumnFamily(this.jobId + CLASSIFIED_RENAME_SUFFIX);
    dependencyNodesCf = createColumnFamily(this.jobId + DEPENDENCY_NODE_SUFFIX);
    depAdjOffCf = createColumnFamily(this.jobId + DEP_ADJ_OFF_SUFFIX);
    depAdjTgtCf = createColumnFamily(this.jobId + DEP_ADJ_TGT_SUFFIX);
    depInDegCf = createColumnFamily(this.jobId + DEP_IN_DEG_SUFFIX);
    depOrderCf = createColumnFamily(this.jobId + DEP_ORDER_SUFFIX);
    if (fso) {
      toEdgesCf = createColumnFamily(this.jobId + TO_EDGES_SUFFIX);
      fromEdgesCf = createColumnFamily(this.jobId + FROM_EDGES_SUFFIX);
    }
    if (ownsReportColumnFamily) {
      reportCfh = createColumnFamily(this.jobId + REPORT_SUFFIX);
    }
  }

  private void spillDiffCandidates() throws IOException {
    try {
      diffCandidatesCf = createColumnFamily(diffCandCfName);
    } catch (RocksDBException e) {
      throw new IOException("Failed to create diff candidate column family " + diffCandCfName, e);
    }
    for (Long objectId : diffCandidates) {
      batchPut(diffCandidatesCf, objectIdKeyBuffer(objectId), presentMarker);
    }
    diffCandidates.clear();
    flushWrites();
    diffCandidatesSpilled = true;
  }

  private void spillDeletedDirectoryIds() throws IOException {
    try {
      deletedDirectoryIdsCf = createColumnFamily(deletedDirectoryIdsCfName);
    } catch (RocksDBException e) {
      throw new IOException("Failed to create deleted directory id column family "
          + deletedDirectoryIdsCfName, e);
    }
    for (Long objectId : deletedDirectoryIds) {
      batchPut(deletedDirectoryIdsCf, objectIdKeyBuffer(objectId), presentMarker);
    }
    deletedDirectoryIds.clear();
    flushWrites();
    deletedDirectoryIdsSpilled = true;
  }

  private void spillRenamedDirectoryIds() throws IOException {
    try {
      renamedDirectoryIdsCf = createColumnFamily(renamedDirectoryIdsCfName);
    } catch (RocksDBException e) {
      throw new IOException("Failed to create renamed directory id column family "
          + renamedDirectoryIdsCfName, e);
    }
    for (Long objectId : renamedDirectoryIds) {
      batchPut(renamedDirectoryIdsCf, objectIdKeyBuffer(objectId), presentMarker);
    }
    renamedDirectoryIds.clear();
    flushWrites();
    renamedDirectoryIdsSpilled = true;
  }

  private void batchPut(ColumnFamilyHandle cf, byte[] key, byte[] value) throws IOException {
    try {
      writeBatch.put(cf, key, value);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    pendingOps++;
    if (pendingOps >= writeBatchSize) {
      flushWrites();
    }
  }

  private byte[] get(ColumnFamilyHandle cf, byte[] key) throws IOException {
    if (cf == null) {
      return null;
    }
    try {
      return db.get().get(cf, key);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  private ColumnFamilyHandle createColumnFamily(String name) throws RocksDBException {
    return db.get().createColumnFamily(
        new ColumnFamilyDescriptor(StringUtils.string2Bytes(name), tempColumnFamilyOptions));
  }

  private void requireFso() {
    if (!fso) {
      throw new IllegalStateException("Directory edge column families require an FSO bucket");
    }
  }

  @Override
  public void close() throws IOException {
    flushWrites();
    writeBatch.close();
    writeOptions.close();
    if (temporaryColumnFamiliesDropped) {
      tempColumnFamilyOptions.close();
      return;
    }
    newListCf = dropAndClose(newListCf);
    oldListCf = dropAndClose(oldListCf);
    diffCandidatesCf = dropAndClose(diffCandidatesCf);
    deletedDirectoryIdsCf = dropAndClose(deletedDirectoryIdsCf);
    renamedDirectoryIdsCf = dropAndClose(renamedDirectoryIdsCf);
    toEdgesCf = dropAndClose(toEdgesCf);
    fromEdgesCf = dropAndClose(fromEdgesCf);
    classifiedCreateCf = dropAndClose(classifiedCreateCf);
    classifiedDeleteCf = dropAndClose(classifiedDeleteCf);
    classifiedModifyCf = dropAndClose(classifiedModifyCf);
    classifiedRenameCf = dropAndClose(classifiedRenameCf);
    dependencyNodesCf = dropAndClose(dependencyNodesCf);
    depAdjOffCf = dropAndClose(depAdjOffCf);
    depAdjTgtCf = dropAndClose(depAdjTgtCf);
    depInDegCf = dropAndClose(depInDegCf);
    depOrderCf = dropAndClose(depOrderCf);
    if (ownsReportColumnFamily) {
      reportCfh = dropAndClose(reportCfh);
    }
    tempColumnFamilyOptions.close();
  }

  private void closeQuietly() {
    try {
      close();
    } catch (IOException ignored) {
      // best effort while handling a failed open
    }
  }

  private ColumnFamilyHandle dropAndClose(ColumnFamilyHandle handle) {
    if (handle == null) {
      return null;
    }
    dropColumnFamilyHandle(db, handle);
    handle.close();
    return null;
  }

  /** Big-endian 8-byte objectId key used by {@code newList}/{@code oldList}. */
  static byte[] objectIdKey(long objectId) {
    byte[] key = new byte[Long.BYTES];
    encodeLong(key, 0, objectId);
    return key;
  }

  static long decodeObjectId(byte[] key) {
    if (key.length < Long.BYTES) {
      throw new IllegalArgumentException("objectId key too short: " + key.length);
    }
    return decodeLong(key, 0);
  }

  /** Classified CREATE/DELETE/MODIFY value: {@code | parentId (8 BE) |}. */
  static byte[] encodeClassifiedParent(long parentId) {
    byte[] value = new byte[Long.BYTES];
    encodeLong(value, 0, parentId);
    return value;
  }

  /** Classified RENAME value: {@code | sourceParentId (8 BE) | targetParentId (8 BE) |}. */
  static byte[] encodeClassifiedRename(long sourceParentId, long targetParentId) {
    byte[] value = new byte[CLASSIFIED_RENAME_BYTES];
    encodeLong(value, 0, sourceParentId);
    encodeLong(value, Long.BYTES, targetParentId);
    return value;
  }

  static long decodeClassifiedParent(byte[] bytes) {
    if (bytes.length < Long.BYTES) {
      throw new IllegalArgumentException("Classified parent value too short: " + bytes.length);
    }
    return decodeLong(bytes, 0);
  }

  static long decodeClassifiedRenameSourceParent(byte[] bytes) {
    if (bytes.length < CLASSIFIED_RENAME_BYTES) {
      throw new IllegalArgumentException("Classified rename value too short: " + bytes.length);
    }
    return decodeLong(bytes, 0);
  }

  static long decodeClassifiedRenameTargetParent(byte[] bytes) {
    if (bytes.length < CLASSIFIED_RENAME_BYTES) {
      throw new IllegalArgumentException("Classified rename value too short: " + bytes.length);
    }
    return decodeLong(bytes, Long.BYTES);
  }

  /** Reverse edge value: {@code | parentId (8 BE) | nameLen (4 BE) | name |}. */
  static byte[] encodeEdgeLink(long parentId, byte[] nameBytes) {
    byte[] value = new byte[EDGE_LINK_HEADER_BYTES + nameBytes.length];
    encodeLong(value, 0, parentId);
    encodeInt(value, Long.BYTES, nameBytes.length);
    System.arraycopy(nameBytes, 0, value, EDGE_LINK_HEADER_BYTES, nameBytes.length);
    return value;
  }

  static long decodeEdgeLinkParentId(byte[] bytes) {
    if (bytes == null || bytes.length < EDGE_LINK_HEADER_BYTES) {
      throw new IllegalArgumentException("Edge link value too short: "
          + (bytes == null ? 0 : bytes.length));
    }
    return decodeObjectId(bytes);
  }

  static String decodeEdgeLinkName(byte[] bytes) {
    return new String(decodeEdgeLinkNameBytes(bytes), StandardCharsets.UTF_8);
  }

  static void encodeLong(byte[] buffer, int offset, long value) {
    for (int shift = Long.SIZE - 8; shift >= 0; shift -= 8) {
      buffer[offset++] = (byte) (value >>> shift);
    }
  }

  static void encodeInt(byte[] buffer, int offset, int value) {
    buffer[offset++] = (byte) (value >>> 24);
    buffer[offset++] = (byte) (value >>> 16);
    buffer[offset++] = (byte) (value >>> 8);
    buffer[offset] = (byte) value;
  }

  static int decodeInt(byte[] buffer, int offset) {
    return ((buffer[offset] & 0xFF) << 24)
        | ((buffer[offset + 1] & 0xFF) << 16)
        | ((buffer[offset + 2] & 0xFF) << 8)
        | (buffer[offset + 3] & 0xFF);
  }

  static long decodeLong(byte[] buffer, int offset) {
    long value = 0L;
    for (int i = 0; i < Long.BYTES; i++) {
      value = (value << 8) | (buffer[offset + i] & 0xFF);
    }
    return value;
  }

  private static byte[] decodeEdgeLinkNameBytes(byte[] bytes) {
    if (bytes == null || bytes.length < EDGE_LINK_HEADER_BYTES) {
      throw new IllegalArgumentException("Edge link value too short: "
          + (bytes == null ? 0 : bytes.length));
    }
    int nameLen = decodeInt(bytes, Long.BYTES);
    if (nameLen < 0 || nameLen > bytes.length - EDGE_LINK_HEADER_BYTES) {
      throw new IllegalArgumentException("Invalid edge link name length: " + nameLen);
    }
    byte[] nameBytes = new byte[nameLen];
    System.arraycopy(bytes, EDGE_LINK_HEADER_BYTES, nameBytes, 0, nameLen);
    return nameBytes;
  }

  /** Iterator over one list column family for merge join. */
  public final class ListIterator implements AutoCloseable, Iterator<Map.Entry<Long, byte[]>> {
    private final ManagedRocksIterator iterator;
    private Map.Entry<Long, byte[]> next;

    private ListIterator(ColumnFamilyHandle cf) throws RocksDatabaseException {
      this.iterator = new ManagedRocksIterator(db.get().newIterator(cf));
      iterator.get().seekToFirst();
      advance();
    }

    private void advance() {
      if (iterator.get().isValid()) {
        byte[] key = iterator.get().key();
        next = new AbstractMap.SimpleImmutableEntry<>(decodeObjectId(key), iterator.get().value());
        iterator.get().next();
      } else {
        next = null;
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Map.Entry<Long, byte[]> next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<Long, byte[]> current = next;
      advance();
      return current;
    }

    @Override
    public void close() {
      iterator.close();
    }
  }

  /** Iterator over a classified column family (objectId key, parent value). */
  public final class ClassifiedIterator implements AutoCloseable, Iterator<Map.Entry<Long, byte[]>> {
    private final ManagedRocksIterator iterator;
    private Map.Entry<Long, byte[]> next;

    private ClassifiedIterator(ColumnFamilyHandle cf) throws RocksDatabaseException {
      this.iterator = new ManagedRocksIterator(db.get().newIterator(cf));
      iterator.get().seekToFirst();
      advance();
    }

    private void advance() {
      if (iterator.get().isValid()) {
        byte[] key = iterator.get().key();
        next = new AbstractMap.SimpleImmutableEntry<>(decodeObjectId(key), iterator.get().value());
        iterator.get().next();
      } else {
        next = null;
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Map.Entry<Long, byte[]> next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<Long, byte[]> current = next;
      advance();
      return current;
    }

    @Override
    public void close() {
      iterator.close();
    }
  }

  /** Iterator over dependency-node entries (ordered by node index). */
  public final class DependencyNodeIterator implements AutoCloseable,
      Iterator<SnapDiffDependencyEntry> {
    private final ManagedRocksIterator iterator;
    private SnapDiffDependencyEntry next;

    private DependencyNodeIterator(ColumnFamilyHandle cf) throws RocksDatabaseException {
      this.iterator = new ManagedRocksIterator(db.get().newIterator(cf));
      iterator.get().seekToFirst();
      advance();
    }

    private void advance() {
      if (iterator.get().isValid()) {
        next = SnapDiffDependencyEntry.fromResolvedBytes(iterator.get().value());
        iterator.get().next();
      } else {
        next = null;
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public SnapDiffDependencyEntry next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      SnapDiffDependencyEntry current = next;
      advance();
      return current;
    }

    @Override
    public void close() {
      iterator.close();
    }
  }
}

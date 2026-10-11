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

import static org.apache.commons.lang3.StringUtils.leftPad;
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
  private static final String DELETED_DIR_IDS_SUFFIX = "-deleted-dir-ids";
  private static final String RENAMED_DIR_IDS_SUFFIX = "-renamed-dir-ids";

  private final ManagedRocksDB db;
  private final String jobId;
  private final boolean fso;
  private final CodecRegistry codecRegistry;
  private final ColumnFamilyHandle reportCfh;
  private final byte[] presentMarker;
  private final ManagedColumnFamilyOptions familyOptions;
  private final long maxInMemoryEntries;
  private final List<ColumnFamilyHandle> temporaryColumnFamilies = new ArrayList<>();

  private ColumnFamilyHandle newListCf;
  private ColumnFamilyHandle oldListCf;
  private ColumnFamilyHandle toEdgesCf;
  private ColumnFamilyHandle fromEdgesCf;
  private ColumnFamilyHandle classifiedCreateCf;
  private ColumnFamilyHandle classifiedDeleteCf;
  private ColumnFamilyHandle classifiedModifyCf;
  private ColumnFamilyHandle classifiedRenameCf;
  private ColumnFamilyHandle dependencyNodesCf;

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

  /** Reusable big-endian key buffers; safe because RocksDB copies keys on put/get. */
  private final byte[] objectIdKeyBuffer = new byte[Long.BYTES];
  private final byte[] intKeyBuffer = new byte[Integer.BYTES];

  /** Full diff: shared new/old lists plus FSO edge column families. */
  public enum Mode {
    FULL,
    DAG
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private SnapDiffJobStore(ManagedRocksDB db, String jobId, CodecRegistry codecRegistry, boolean fso,
      int writeBatchSize, ManagedColumnFamilyOptions familyOptions, long maxInMemoryEntries,
      @Nonnull ColumnFamilyHandle reportCfh) throws IOException {
    this.db = db;
    this.jobId = jobId;
    this.fso = fso;
    this.codecRegistry = codecRegistry;
    this.reportCfh = reportCfh;
    this.writeBatchSize = writeBatchSize;
    this.familyOptions = familyOptions;
    this.familyOptions.setDisableAutoCompactions(true);
    this.maxInMemoryEntries = maxInMemoryEntries;
    this.presentMarker = codecRegistry.asRawData(Boolean.TRUE);
    this.writeBatch = new ManagedWriteBatch();
    this.writeOptions = new ManagedWriteOptions();
    this.pendingOps = 0;
    this.diffCandidates = new HashSet<>();
    this.deletedDirectoryIds = new HashSet<>();
    this.renamedDirectoryIds = new HashSet<>();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static SnapDiffJobStore open(@Nonnull ManagedRocksDB db,
      @Nonnull CodecRegistry codecRegistry,
      @Nonnull ManagedColumnFamilyOptions familyOptions,
      @Nonnull String jobId,
      boolean fso,
      @Nonnull ColumnFamilyHandle reportCfh,
      @Nullable Integer writeBatchSize,
      @Nullable Long maxInMemoryEntries) throws IOException {
    int resolvedWriteBatchSize = writeBatchSize != null ? writeBatchSize : DEFAULT_BATCH_SIZE;
    long resolvedMaxInMemoryEntries = maxInMemoryEntries != null ? maxInMemoryEntries
        : OZONE_OM_SNAPSHOT_DIFF_MAX_IN_MEMORY_ENTRIES_PER_JOB_DEFAULT;
    SnapDiffJobStore store = new SnapDiffJobStore(db, jobId, codecRegistry, fso, resolvedWriteBatchSize,
        familyOptions, resolvedMaxInMemoryEntries, reportCfh);
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

  int getWriteBatchSize() {
    return writeBatchSize;
  }

  /** Returns report entries written during a test run. */
  List<DiffReportEntry> readReportEntriesForTest() throws IOException {
    List<DiffReportEntry> entries = new ArrayList<>();
    try (ManagedRocksIterator iterator = new ManagedRocksIterator(db.get().newIterator(reportCfh))) {
      iterator.get().seek(codecRegistry.asRawData(jobId));
      while (iterator.get().isValid()) {
        String key = codecRegistry.asObject(iterator.get().key(), String.class);
        if (!key.startsWith(jobId)) {
          break;
        }
        entries.add(codecRegistry.asObject(iterator.get().value(), DiffReportEntry.class));
        iterator.get().next();
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
    return new SnapDiffPathResolver(db, fromEdgesCf, bucketObjectId, pathResolverLruCapacity());
  }

  /** Builds a to-snapshot path resolver over the reverse edge index. */
  SnapDiffPathResolver newToPathResolver(long bucketObjectId) {
    requireFso();
    return new SnapDiffPathResolver(db, toEdgesCf, bucketObjectId, pathResolverLruCapacity());
  }

  private int pathResolverLruCapacity() {
    return (int) maxInMemoryEntries / 2;
  }

  int ancestorMemoCapacity() {
    return Math.max(4096, pathResolverLruCapacity() / 2);
  }

  List<byte[]> multiGetFromEdgeValues(List<Long> objectIds) throws IOException {
    if (objectIds.isEmpty()) {
      return Collections.emptyList();
    }
    List<byte[]> keys = new ArrayList<>(objectIds.size());
    for (Long objectId : objectIds) {
      keys.add(objectIdKey(objectId));
    }
    return multiGet(db, fromEdgesCf, keys);
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
      dropAndClose(classifiedCreateCf);
      break;
    case DELETE:
      dropAndClose(classifiedDeleteCf);
      break;
    case MODIFY:
      dropAndClose(classifiedModifyCf);
      break;
    case RENAME:
      dropAndClose(classifiedRenameCf);
      break;
    default:
      throw new IllegalArgumentException("Unsupported diff type: " + diffType);
    }
  }

  /** Drops merge-join list column families after classification. */
  void dropListColumnFamilies() {
    dropAndClose(newListCf);
    dropAndClose(oldListCf);
  }

  /** Drops FSO reverse-edge column families after path resolution. */
  void dropEdgeColumnFamilies() {
    dropAndClose(toEdgesCf);
    dropAndClose(fromEdgesCf);
  }

  /** Drops persisted dependency-graph column families. */
  void dropDependencyGraphColumnFamilies() {
    dropAndClose(dependencyNodesCf);
  }

  /** Ascending iterator over one classified column family. */
  public ClassifiedIterator classifiedIterator(DiffType diffType) throws RocksDatabaseException {
    return new ClassifiedIterator(classifiedColumnFamily(diffType));
  }

  /** Writes dependency node entries starting at {@code startNodeIndex}. */
  public void putDependencyNode(int nodeIndex, SnapDiffDependencyEntry entry)
      throws IOException {
    batchPut(dependencyNodesCf, intKeyBuffer(nodeIndex), entry.toResolvedBytes());
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
      keys.add(intKey(nodeIndex));
    }
    List<byte[]> values = multiGet(db, dependencyNodesCf, keys);
    List<DiffReportEntry> reportEntries = new ArrayList<>(values.size());
    for (int i = 0; i < values.size(); i++) {
      byte[] value = values.get(i);
      if (value == null) {
        throw new IOException("Missing dependency node at index " + nodeIndices.get(i));
      }
      reportEntries.add(SnapDiffDependencyEntry.fromResolvedBytes(value).getReportEntry());
    }
    return reportEntries;
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
      dropAndClose(diffCandidatesCf);
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
    dropAndClose(deletedDirectoryIdsCf);
    deletedDirectoryIdsSpilled = false;
    renamedDirectoryIds.clear();
    dropAndClose(renamedDirectoryIdsCf);
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
   * Appends a batch of resolved and ordered {@link DiffReportEntry}s to the snap diff report table.
   */
  public void putOrderedReportEntries(List<DiffReportEntry> entries) throws IOException {
    if (!reportWriteStarted) {
      throw new IllegalStateException("Snap diff report write not started for job " + jobId);
    }
    for (DiffReportEntry entry : entries) {
      String jobReportKey = getReportKeyForIndex(jobId, leftPad(String.valueOf(reportIndex++), 21, '0'));
      putReportEntry(jobReportKey, entry);
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

  private void appendReportEntry(DiffReportEntry entry) throws IOException {
    String jobReportKey = getReportKeyForIndex(jobId, entry.getType(), reportIndex++);
    putReportEntry(jobReportKey, entry);
  }

  private void putReportEntry(String jobReportKey, DiffReportEntry entry) throws IOException {
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
    if (fso) {
      toEdgesCf = createColumnFamily(this.jobId + TO_EDGES_SUFFIX);
      fromEdgesCf = createColumnFamily(this.jobId + FROM_EDGES_SUFFIX);
    }
  }

  void spillDiffCandidates() throws IOException {
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
    ColumnFamilyHandle handle = db.get().createColumnFamily(
        new ColumnFamilyDescriptor(StringUtils.string2Bytes(name), familyOptions));
    temporaryColumnFamilies.add(handle);
    return handle;
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
    while (!temporaryColumnFamilies.isEmpty()) {
      dropAndClose(temporaryColumnFamilies.get(0));
    }
  }

  private void closeQuietly() {
    try {
      close();
    } catch (IOException ignored) {
      // best effort while handling a failed open
    }
  }

  private void dropAndClose(ColumnFamilyHandle handle) {
    if (handle == null) {
      return;
    }
    temporaryColumnFamilies.remove(handle);
    dropColumnFamilyHandle(db, handle);
    handle.close();
  }

  /** Big-endian 8-byte objectId key used by {@code newList}/{@code oldList}. */
  static byte[] objectIdKey(long objectId) {
    byte[] key = new byte[Long.BYTES];
    encodeLong(key, 0, objectId);
    return key;
  }

  /** Big-endian 4-byte int key for multi-get lookups. */
  static byte[] intKey(int value) {
    byte[] key = new byte[Integer.BYTES];
    encodeInt(key, 0, value);
    return key;
  }

  static List<byte[]> multiGet(ManagedRocksDB db, ColumnFamilyHandle cf, List<byte[]> keys)
      throws IOException {
    if (keys.isEmpty()) {
      return Collections.emptyList();
    }
    List<ColumnFamilyHandle> columnFamilies = Collections.nCopies(keys.size(), cf);
    try {
      List<byte[]> values = db.get().multiGetAsList(columnFamilies, keys);
      if (values.size() != keys.size()) {
        throw new IOException("RocksDB multiGet returned " + values.size()
            + " values for " + keys.size() + " keys");
      }
      return values;
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  static long decodeObjectId(byte[] key) {
    if (key.length < Long.BYTES) {
      throw new IllegalArgumentException("objectId key too short: " + key.length);
    }
    return decodeLong(key, 0);
  }

  /**
   * Classified RENAME value: source edge link followed by target edge link, each in
   * the same wire format as {@link #encodeEdgeLink(long, byte[])}.
   */
  static byte[] encodeClassifiedRename(long sourceParentId, byte[] sourceName,
      long targetParentId, byte[] targetName) {
    byte[] sourceLink = encodeEdgeLink(sourceParentId, sourceName);
    byte[] targetLink = encodeEdgeLink(targetParentId, targetName);
    byte[] value = new byte[sourceLink.length + targetLink.length];
    System.arraycopy(sourceLink, 0, value, 0, sourceLink.length);
    System.arraycopy(targetLink, 0, value, sourceLink.length, targetLink.length);
    return value;
  }

  static byte[] decodeClassifiedRenameSourceLink(byte[] bytes) {
    int sourceLen = edgeLinkByteLength(bytes, 0);
    return Arrays.copyOfRange(bytes, 0, sourceLen);
  }

  static byte[] decodeClassifiedRenameTargetLink(byte[] bytes) {
    int sourceLen = edgeLinkByteLength(bytes, 0);
    if (bytes.length <= sourceLen) {
      throw new IllegalArgumentException("Classified rename value missing target link");
    }
    return Arrays.copyOfRange(bytes, sourceLen, bytes.length);
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

  private static int edgeLinkByteLength(byte[] bytes, int offset) {
    if (bytes == null || bytes.length - offset < EDGE_LINK_HEADER_BYTES) {
      throw new IllegalArgumentException("Edge link value too short: "
          + (bytes == null ? 0 : bytes.length - offset));
    }
    int nameLen = decodeInt(bytes, offset + Long.BYTES);
    if (nameLen < 0 || nameLen > bytes.length - offset - EDGE_LINK_HEADER_BYTES) {
      throw new IllegalArgumentException("Invalid edge link name length: " + nameLen);
    }
    return EDGE_LINK_HEADER_BYTES + nameLen;
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

    private ListIterator(ColumnFamilyHandle cf) {
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

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
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.dropColumnFamilyHandle;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Owns the per-job temporary RocksDB column families and batched writes shared
 * between optimized snapshot diff pipeline stages.
 *
 * <p>Diff-candidate {@code objectId}s are held in memory while their count is at
 * most {@code maxInMemoryEntries}; larger sets spill to a temporary column family.
 *
 * <p>All RocksDB puts use raw {@code byte[]} keys and values (the JNI boundary). Callers
 * serialize {@link EntryValue} via {@link EntryValue#toBytes()} before writing to
 * {@code newList}/{@code oldList}.
 *
 * <p>This initial version supports the full diff sequential reader ({@link FullDiffSequentialReader}).
 * DAG diff support extends this store in HDDS-15393.
 */
public final class SnapDiffJobStore implements AutoCloseable {

  /** Default RocksDB {@code WriteBatch} commit size for job-store puts. */
  public static final int DEFAULT_WRITE_BATCH_SIZE = 1000;

  private static final String NEW_LIST_SUFFIX = "-new-list";
  private static final String OLD_LIST_SUFFIX = "-old-list";
  private static final String CAND_IDS_SUFFIX = "-cand-ids";
  private static final String TO_EDGES_SUFFIX = "-to-edges";
  private static final String FROM_EDGES_SUFFIX = "-from-edges";

  private final ManagedRocksDB db;
  private final boolean fso;
  private final byte[] presentMarker;
  private final ManagedColumnFamilyOptions familyOptions;
  private final long maxInMemoryEntries;

  private ColumnFamilyHandle newListCf;
  private ColumnFamilyHandle oldListCf;
  private ColumnFamilyHandle toEdgesCf;
  private ColumnFamilyHandle fromEdgesCf;

  private String diffCandCfName;
  private Set<Long> diffCandidates;
  private ColumnFamilyHandle diffCandidatesCf;
  private boolean diffCandidatesSpilled;

  private final ManagedWriteBatch writeBatch;
  private final ManagedWriteOptions writeOptions;
  private final int writeBatchSize;
  private int pendingOps;

  /** Reusable big-endian key buffers; safe because RocksDB copies keys on put/get. */
  private final byte[] objectIdKeyBuffer = new byte[Long.BYTES];
  private final byte[] edgeKeyBuffer = new byte[2 * Long.BYTES];

  /** Full diff: shared new/old lists plus FSO edge column families. */
  public enum Mode {
    FULL
  }

  private SnapDiffJobStore(ManagedRocksDB db, CodecRegistry codecRegistry, boolean fso,
      int writeBatchSize, ManagedColumnFamilyOptions familyOptions, long maxInMemoryEntries)
      throws IOException {
    this.db = db;
    this.fso = fso;
    this.writeBatchSize = writeBatchSize;
    this.familyOptions = familyOptions;
    this.maxInMemoryEntries = maxInMemoryEntries;
    this.presentMarker = codecRegistry.asRawData(Boolean.TRUE);
    this.writeBatch = new ManagedWriteBatch();
    this.writeOptions = new ManagedWriteOptions();
    this.pendingOps = 0;
    this.diffCandidates = new HashSet<>();
  }

  public static SnapDiffJobStore open(@Nonnull ManagedRocksDB db,
      @Nonnull CodecRegistry codecRegistry,
      @Nonnull ManagedColumnFamilyOptions familyOptions,
      @Nonnull String jobId,
      boolean fso,
      @Nonnull Mode mode) throws IOException {
    return open(db, codecRegistry, familyOptions, jobId, fso, mode, DEFAULT_WRITE_BATCH_SIZE,
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
    if (mode != Mode.FULL) {
      throw new IllegalArgumentException("Unsupported mode: " + mode);
    }
    SnapDiffJobStore store = new SnapDiffJobStore(db, codecRegistry, fso, writeBatchSize,
        familyOptions, maxInMemoryEntries);
    try {
      store.initColumnFamilies(familyOptions, jobId);
      return store;
    } catch (RocksDBException e) {
      store.closeQuietly();
      throw new IOException("Failed to open SnapDiff job store for job " + jobId, e);
    }
  }

  public boolean isFso() {
    return fso;
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

  public void putToEdge(long parentId, long objectId, byte[] name) throws IOException {
    requireFso();
    batchPut(toEdgesCf, edgeKeyBuffer(parentId, objectId), name);
  }

  public void putFromEdge(long parentId, long objectId, byte[] name) throws IOException {
    requireFso();
    batchPut(fromEdgesCf, edgeKeyBuffer(parentId, objectId), name);
  }

  public byte[] getNewList(long objectId) throws IOException {
    return get(newListCf, objectIdKeyBuffer(objectId));
  }

  public byte[] getOldList(long objectId) throws IOException {
    return get(oldListCf, objectIdKeyBuffer(objectId));
  }

  public boolean hasNewListEntry(long objectId) throws IOException {
    return getNewList(objectId) != null;
  }

  public boolean isNewListCandidate(long objectId) throws IOException {
    byte[] value = getNewList(objectId);
    return value != null && !Arrays.equals(value, presentMarker);
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

  boolean areDiffCandidatesSpilled() {
    return diffCandidatesSpilled;
  }

  public byte[] getToEdgeName(long parentId, long objectId) throws IOException {
    requireFso();
    return get(toEdgesCf, edgeKeyBuffer(parentId, objectId));
  }

  public byte[] getFromEdgeName(long parentId, long objectId) throws IOException {
    requireFso();
    return get(fromEdgesCf, edgeKeyBuffer(parentId, objectId));
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

  private byte[] objectIdKeyBuffer(long objectId) {
    encodeLong(objectIdKeyBuffer, 0, objectId);
    return objectIdKeyBuffer;
  }

  private byte[] edgeKeyBuffer(long parentId, long objectId) {
    encodeLong(edgeKeyBuffer, 0, parentId);
    encodeLong(edgeKeyBuffer, Long.BYTES, objectId);
    return edgeKeyBuffer;
  }

  private static void encodeLong(byte[] buffer, int offset, long value) {
    for (int shift = Long.SIZE - 8; shift >= 0; shift -= 8) {
      buffer[offset++] = (byte) (value >>> shift);
    }
  }

  private void initColumnFamilies(ManagedColumnFamilyOptions options, String jobId)
      throws RocksDBException {
    newListCf = createColumnFamily(jobId + NEW_LIST_SUFFIX, options);
    oldListCf = createColumnFamily(jobId + OLD_LIST_SUFFIX, options);
    diffCandCfName = jobId + CAND_IDS_SUFFIX;
    if (fso) {
      toEdgesCf = createColumnFamily(jobId + TO_EDGES_SUFFIX, options);
      fromEdgesCf = createColumnFamily(jobId + FROM_EDGES_SUFFIX, options);
    }
  }

  private void spillDiffCandidates() throws IOException {
    try {
      diffCandidatesCf = createColumnFamily(diffCandCfName, familyOptions);
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

  private ColumnFamilyHandle createColumnFamily(String name, ManagedColumnFamilyOptions options)
      throws RocksDBException {
    return db.get().createColumnFamily(
        new ColumnFamilyDescriptor(StringUtils.string2Bytes(name), options));
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
    newListCf = dropAndClose(newListCf);
    oldListCf = dropAndClose(oldListCf);
    diffCandidatesCf = dropAndClose(diffCandidatesCf);
    toEdgesCf = dropAndClose(toEdgesCf);
    fromEdgesCf = dropAndClose(fromEdgesCf);
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
}

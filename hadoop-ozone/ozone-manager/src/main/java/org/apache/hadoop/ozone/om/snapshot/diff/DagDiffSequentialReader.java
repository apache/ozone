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

import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyHigherString;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.DoubleConsumer;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.IteratorType;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * The DAG-based diff multi-stage sequential read that produces the intermediate structures
 * consumed by the later merge-join and path-resolution stages.
 *
 * <p>Call {@link #scanFileTables} then {@link #scanDirectoryTables} (FSO only) in that order.
 * Each method runs the to-side delta SST scan first, then the from-side load for the same
 * table pair. Directory stages full-scan {@code fromSnapshot.directoryTable} before
 * {@code toSnapshot.directoryTable}.
 *
 * <p>Scans iterate the raw snapshot tables ({@code Table<byte[], byte[]>} from
 * {@code DBStore#getTable(String)}) or raw delta SST files so {@link SnapshotDiffValueParser} reads the exact
 * persisted protobuf bytes and compare signatures match on-disk layout.
 *
 * <p>File/key from-side loads are driven by delta table keys batched at {@code writeBatchSize}.
 * Directory {@code DiffCandidateSet} membership is populated only from directory delta SSTs and
 * consulted during the from-side directory scan; directory delta tombstones are tracked in a
 * separate spilled key set with the same in-memory limit.
 *
 * <p>Sequence gating ({@code fromSnapshot.dbTxSequenceNumber}) is passed to the K-way merge
 * iterator; this reader consumes only entries the iterator emits (tombstones and rows with
 * {@code sequence > fromSnapshot.dbTxSequenceNumber}).
 *
 * <p>Delta SST scans honor the bucket {@code keyPrefix} via native SST iterator bounds. The
 * {@code keyPrefix} array must not be modified for the duration of each delta SST scan.
 */
public class DagDiffSequentialReader {

  private static final double DIRECTORY_DELTA_PROGRESS_END = 0.33;
  private static final double DIRECTORY_FROM_PROGRESS_END = 0.66;
  private static final double DIRECTORY_TO_PROGRESS_END = 1.0;

  private final SnapDiffJobStore store;
  private final long exclusiveMinSequenceNumber;

  /**
   * @param store                      per-job temp column families for this DAG diff job
   * @param exclusiveMinSequenceNumber {@code fromSnapshot.dbTxSequenceNumber}; passed to the
   *                                   K-way merge iterator
   */
  public DagDiffSequentialReader(SnapDiffJobStore store, long exclusiveMinSequenceNumber) {
    this.store = store;
    this.exclusiveMinSequenceNumber = exclusiveMinSequenceNumber;
  }

  /**
   * Scans file/key-table delta SSTs with interleaved batched {@code multiGetSkipCache} against
   * {@code fromSnapshot.file/keyTable}.
   *
   * @param keyPrefix     optional bucket prefix as stored in RocksDB; {@code null} scans the full
   *                      delta SST key range
   * @param deltaSstFiles delta SST paths for the to-side file/key table
   * @param fromFileTable raw {@code fromSnapshot}'s file or key table
   */
  public void scanFileTables(byte[] keyPrefix, Collection<Path> deltaSstFiles,
      Table<byte[], byte[]> fromFileTable, DoubleConsumer progress,
      long estimatedDeltaKeys) throws IOException {
    if (deltaSstFiles.isEmpty()) {
      if (progress != null) {
        progress.accept(1.0);
      }
      return;
    }
    try (LatestVersionedKWayMergeIterator iterator =
        LatestVersionedKWayMergeIterator.overRawSstFilesFromSequence(deltaSstFiles,
            exclusiveMinSequenceNumber, keyPrefix, prefixUpperBoundExclusive(keyPrefix))) {
      scanFileDeltaEntries(iterator, fromFileTable, progress, estimatedDeltaKeys);
    }
  }

  /**
   * Scans directory-table delta SSTs, then full scans of {@code fromSnapshot.directoryTable}
   * and {@code toSnapshot.directoryTable} (FSO only).
   *
   * @param keyPrefix          optional bucket prefix as stored in RocksDB; {@code null} scans the
   *                           full table
   * @param deltaSstFiles      delta SST paths for the to-side directory table
   * @param fromDirectoryTable raw {@code fromSnapshot.directoryTable}
   * @param toDirectoryTable   raw {@code toSnapshot.directoryTable}
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public void scanDirectoryTables(byte[] keyPrefix, Collection<Path> deltaSstFiles,
      Table<byte[], byte[]> fromDirectoryTable, Table<byte[], byte[]> toDirectoryTable,
      DoubleConsumer progress, long estimatedDeltaKeys, long estimatedFromDirectoryKeys,
      long estimatedToDirectoryKeys) throws IOException {
    if (!deltaSstFiles.isEmpty()) {
      try (LatestVersionedKWayMergeIterator iterator =
          LatestVersionedKWayMergeIterator.overRawSstFilesFromSequence(deltaSstFiles,
              exclusiveMinSequenceNumber, keyPrefix, prefixUpperBoundExclusive(keyPrefix))) {
        scanDirectoryDeltaEntries(iterator, progress, estimatedDeltaKeys);
      }
    } else if (progress != null) {
      progress.accept(DIRECTORY_DELTA_PROGRESS_END);
    }
    scanFromDirectoryTable(fromDirectoryTable, keyPrefix, progress, estimatedFromDirectoryKeys);
    scanToDirectoryTable(toDirectoryTable, keyPrefix, progress, estimatedToDirectoryKeys);
  }

  @VisibleForTesting
  void scanFileTablesFromDelta(ClosableIterator<MergedKeyValue> deltaEntries,
      Table<byte[], byte[]> fromFileTable) throws IOException {
    scanFileDeltaEntries(deltaEntries, fromFileTable, null, 1L);
  }

  @VisibleForTesting
  void scanDirectoryTablesFromDelta(ClosableIterator<MergedKeyValue> deltaEntries, byte[] keyPrefix,
      Table<byte[], byte[]> fromDirectoryTable, Table<byte[], byte[]> toDirectoryTable)
      throws IOException {
    scanDirectoryDeltaEntries(deltaEntries, null, 1L);
    scanFromDirectoryTable(fromDirectoryTable, keyPrefix, null, 1L);
    scanToDirectoryTable(toDirectoryTable, keyPrefix, null, 1L);
  }

  private void scanFileDeltaEntries(ClosableIterator<MergedKeyValue> entries,
      Table<byte[], byte[]> fromFileTable, DoubleConsumer progress,
      long estimatedDeltaKeys) throws IOException {
    int batchSize = store.getWriteBatchSize();
    List<byte[]> lookupBatch = new ArrayList<>(batchSize);
    long processedDeltaKeys = 0L;
    SnapDiffProgressHelper.TenPercentCheckpoint checkpoint =
        new SnapDiffProgressHelper.TenPercentCheckpoint();
    try {
      while (entries.hasNext()) {
        MergedKeyValue entry = entries.next();
        processedDeltaKeys++;
        SnapDiffProgressHelper.reportEveryTenPercentInSlice(
            progress, 0.0, 1.0, processedDeltaKeys, estimatedDeltaKeys, checkpoint);
        lookupBatch.add(entry.getUserKey());
        if (!entry.isTombstone()) {
          byte[] value = entry.getValue();
          SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, false);
          byte[] signature = computeSignature(value, false);
          store.putNewList(info.getObjectId(),
              new EntryValue(info.getParentId(), info.getName(), false, signature).toBytes());
        }
        if (lookupBatch.size() >= batchSize) {
          multiGetAndPutOldList(lookupBatch, fromFileTable);
          lookupBatch.clear();
        }
      }
    } finally {
      entries.close();
    }
    if (!lookupBatch.isEmpty()) {
      multiGetAndPutOldList(lookupBatch, fromFileTable);
    }
    store.flushWrites();
    if (progress != null) {
      progress.accept(1.0);
    }
  }

  private void scanDirectoryDeltaEntries(ClosableIterator<MergedKeyValue> entries,
      DoubleConsumer progress, long estimatedDeltaKeys) throws IOException {
    long processedDeltaKeys = 0L;
    SnapDiffProgressHelper.TenPercentCheckpoint checkpoint =
        new SnapDiffProgressHelper.TenPercentCheckpoint();
    try {
      while (entries.hasNext()) {
        MergedKeyValue entry = entries.next();
        processedDeltaKeys++;
        SnapDiffProgressHelper.reportEveryTenPercentInSlice(progress, 0.0, DIRECTORY_DELTA_PROGRESS_END,
            processedDeltaKeys, estimatedDeltaKeys, checkpoint);
        if (entry.isTombstone()) {
          store.addDirTombstoneKey(entry.getUserKey());
          continue;
        }
        byte[] value = entry.getValue();
        SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, true);
        store.addDiffCandidate(info.getObjectId());
        byte[] signature = computeSignature(value, true);
        store.putNewList(info.getObjectId(),
            new EntryValue(info.getParentId(), info.getName(), true, signature).toBytes());
      }
    } finally {
      entries.close();
      store.flushWrites();
    }
    if (progress != null) {
      progress.accept(DIRECTORY_DELTA_PROGRESS_END);
    }
  }

  private void scanFromDirectoryTable(Table<byte[], byte[]> table, byte[] keyPrefix,
      DoubleConsumer progress, long estimatedDirectoryKeys) throws IOException {
    store.flushWrites();
    long processedRows = 0L;
    SnapDiffProgressHelper.TenPercentCheckpoint checkpoint =
        new SnapDiffProgressHelper.TenPercentCheckpoint();
    try (Table.KeyValueIterator<byte[], byte[]> iterator =
        table.iterator(keyPrefix, IteratorType.KEY_AND_VALUE)) {
      while (iterator.hasNext()) {
        Table.KeyValue<byte[], byte[]> kv = iterator.next();
        byte[] tableKey = kv.getKey();
        byte[] value = kv.getValue();
        SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, true);
        long objectId = info.getObjectId();
        store.putFromEdge(info.getParentId(), objectId, nameBytes(info.getName()));

        if (store.isDiffCandidate(objectId) || store.isDirTombstoneKey(tableKey)) {
          byte[] signature = computeSignature(value, true);
          store.putOldList(objectId,
              new EntryValue(info.getParentId(), info.getName(), true, signature).toBytes());
        }
        processedRows++;
        SnapDiffProgressHelper.reportEveryTenPercentInSlice(progress, DIRECTORY_DELTA_PROGRESS_END,
            DIRECTORY_FROM_PROGRESS_END, processedRows, estimatedDirectoryKeys, checkpoint);
      }
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException(e);
    }
    store.clearDiffCandidates();
    store.clearDirTombstoneKeys();
    store.flushWrites();
    if (progress != null) {
      progress.accept(DIRECTORY_FROM_PROGRESS_END);
    }
  }

  private void scanToDirectoryTable(Table<byte[], byte[]> table, byte[] keyPrefix,
      DoubleConsumer progress, long estimatedDirectoryKeys) throws IOException {
    long processedRows = 0L;
    SnapDiffProgressHelper.TenPercentCheckpoint checkpoint =
        new SnapDiffProgressHelper.TenPercentCheckpoint();
    try (Table.KeyValueIterator<byte[], byte[]> iterator =
        table.iterator(keyPrefix, IteratorType.VALUE_ONLY)) {
      while (iterator.hasNext()) {
        byte[] value = iterator.next().getValue();
        SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, true);
        store.putToEdge(info.getParentId(), info.getObjectId(), nameBytes(info.getName()));
        processedRows++;
        SnapDiffProgressHelper.reportEveryTenPercentInSlice(progress, DIRECTORY_FROM_PROGRESS_END,
            DIRECTORY_TO_PROGRESS_END, processedRows, estimatedDirectoryKeys, checkpoint);
      }
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException(e);
    }
    store.flushWrites();
    if (progress != null) {
      progress.accept(DIRECTORY_TO_PROGRESS_END);
    }
  }

  private void multiGetAndPutOldList(List<byte[]> keys, Table<byte[], byte[]> fromFileTable)
      throws IOException {
    List<byte[]> values;
    try {
      values = fromFileTable.multiGetSkipCache(keys);
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException("Failed to read table keys in batch", e);
    }
    for (byte[] value : values) {
      if (value == null) {
        continue;
      }
      SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, false);
      byte[] signature = computeSignature(value, false);
      store.putOldList(info.getObjectId(),
          new EntryValue(info.getParentId(), info.getName(), false, signature).toBytes());
    }
  }

  private static SnapshotDiffValueParser.ParsedRequiredInfo parseRequired(byte[] value, boolean isDir)
      throws IOException {
    return isDir
        ? SnapshotDiffValueParser.parseDirectoryInfoRequiredFields(value, false)
        : SnapshotDiffValueParser.parseKeyInfoRequiredFields(value, false);
  }

  private static byte[] computeSignature(byte[] value, boolean isDir) throws IOException {
    return isDir
        ? SnapshotDiffValueParser.computeDirectoryInfoCompareSignature(value)
        : SnapshotDiffValueParser.computeKeyInfoCompareSignature(value);
  }

  private static byte[] nameBytes(String name) {
    return (name == null ? "" : name).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] prefixUpperBoundExclusive(byte[] keyPrefix) {
    if (keyPrefix == null || keyPrefix.length == 0) {
      return null;
    }
    return StringUtils.string2Bytes(
        getLexicographicallyHigherString(new String(keyPrefix, StandardCharsets.UTF_8)));
  }
}

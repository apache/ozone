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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * The DAG-based diff multi-stage sequential reader that produces the intermediate structures
 * consumed by the later merge-join and path-resolution stages.
 *
 * <p>{@link org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager} drives two entry points
 * that align with existing object-id-map sub-statuses:
 * <ul>
 *   <li>{@link #scanFileTables} — delta SST scan via
 *       {@link LatestVersionedKWayMergeIterator}, then batched point lookups against
 *       {@code fromSnapshot}'s file/key table.</li>
 *   <li>{@link #scanDirectoryTables} — directory-table delta SST scan (FSO only), then
 *       bucket-scoped full scans of {@code fromSnapshot.directoryTable} and
 *       {@code toSnapshot.directoryTable}.</li>
 * </ul>
 *
 * <p>Sequence gating ({@code fromSnapshot.dbTxSequenceNumber}) is supplied at construction time
 * and passed to the K-way merge iterator; this reader consumes only entries the iterator emits.
 */
public class DagDiffSequentialReader {

  private static final int MULTI_GET_BATCH_SIZE = SnapDiffJobStore.DEFAULT_WRITE_BATCH_SIZE;

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
   * Scans file/key-table delta SSTs and populates {@code newList}, the file candidate set, and
   * {@code oldList} from {@code fromFileTable}.
   *
   * <p>No table prefix is required: delta SST user keys are full table keys and the from-side
   * load is a batched point lookup, not a prefix-scoped table scan.
   *
   * @param deltaSstFiles   delta SST paths for {@code fromFileTable}
   * @param fromFileTable   {@code fromSnapshot}'s file or key table
   */
  public void scanFileTables(Collection<Path> deltaSstFiles,
      Table<String, ? extends WithParentObjectId> fromFileTable) throws IOException {
    requireFileTable(fromFileTable.getName());
    scanDeltaSstFiles(deltaSstFiles, false);
    loadFromFileTable(fromFileTable);
  }

  /**
   * Scans directory-table delta SSTs and populates directory candidates, {@code newList},
   * {@code oldList}, and FSO {@code to-edges}/{@code from-edges}.
   *
   * @param tablePrefix        bucket-scoped prefix for directory-table full scans
   * @param deltaSstFiles      delta SST paths for {@code fromDirectoryTable}
   * @param fromDirectoryTable {@code fromSnapshot.directoryTable}
   * @param toDirectoryTable   {@code toSnapshot.directoryTable}
   */
  public void scanDirectoryTables(String tablePrefix, Collection<Path> deltaSstFiles,
      Table<String, OmDirectoryInfo> fromDirectoryTable,
      Table<String, OmDirectoryInfo> toDirectoryTable) throws IOException {
    requireDirectoryTable(fromDirectoryTable.getName());
    requireDirectoryTable(toDirectoryTable.getName());
    if (!store.isFso()) {
      throw new IllegalStateException("Directory stage requires an FSO bucket");
    }
    scanDeltaSstFiles(deltaSstFiles, true);
    scanFromDirectoryTable(fromDirectoryTable, tablePrefix);
    scanToDirectoryTable(toDirectoryTable, tablePrefix);
  }

  @VisibleForTesting
  void consumeDeltaEntries(ClosableIterator<MergedKeyValue> entries, boolean isDir) throws IOException {
    try {
      while (entries.hasNext()) {
        processMergedEntry(entries.next(), isDir);
      }
    } finally {
      entries.close();
      store.flushWrites();
    }
  }

  private void scanDeltaSstFiles(Collection<Path> deltaSstFiles, boolean isDir) throws IOException {
    if (deltaSstFiles.isEmpty()) {
      return;
    }
    try (LatestVersionedKWayMergeIterator iterator =
        LatestVersionedKWayMergeIterator.overRawSstFilesFromSequence(deltaSstFiles, exclusiveMinSequenceNumber)) {
      consumeDeltaEntries(iterator, isDir);
    }
  }

  private void processMergedEntry(MergedKeyValue entry, boolean isDir) throws IOException {
    if (isDir) {
      store.putDirCandidate(entry.getUserKey());
    } else {
      store.putFileCandidate(entry.getUserKey());
    }
    if (!entry.isTombstone()) {
      byte[] value = entry.getValue();
      SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, isDir);
      store.putNewList(info.getObjectId(),
          new EntryValue(info.getParentId(), info.getName(), isDir, computeSignature(value, isDir)).toBytes());
    }
  }

  private void scanToDirectoryTable(Table<String, OmDirectoryInfo> table, String tablePrefix)
      throws IOException {
    try (Table.KeyValueIterator<String, OmDirectoryInfo> iterator = openDirectoryIterator(table, tablePrefix)) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> kv = iterator.next();
        byte[] value = toPersistedFormat(kv.getValue());
        SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, true);
        store.putToEdge(info.getParentId(), info.getObjectId(), nameBytes(info.getName()));
      }
    }
    store.flushWrites();
  }

  private void scanFromDirectoryTable(Table<String, OmDirectoryInfo> table, String tablePrefix)
      throws IOException {
    store.flushWrites();
    try (Table.KeyValueIterator<String, OmDirectoryInfo> iterator = openDirectoryIterator(table, tablePrefix)) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> kv = iterator.next();
        byte[] value = toPersistedFormat(kv.getValue());
        SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, true);
        store.putFromEdge(info.getParentId(), info.getObjectId(), nameBytes(info.getName()));
        if (store.hasDirCandidate(StringUtils.string2Bytes(kv.getKey()))) {
          store.putOldList(info.getObjectId(),
              new EntryValue(info.getParentId(), info.getName(), true, computeSignature(value, true)).toBytes());
        }
      }
    }
    store.flushWrites();
    store.discardDirCandidates();
  }

  private void loadFromFileTable(Table<String, ? extends WithParentObjectId> fromFileTable) throws IOException {
    store.flushWrites();
    List<byte[]> batch = new ArrayList<>(MULTI_GET_BATCH_SIZE);
    try (ClosableIterator<byte[]> iterator = store.iterateFileCandidateKeys()) {
      while (iterator.hasNext()) {
        batch.add(iterator.next());
        if (batch.size() >= MULTI_GET_BATCH_SIZE) {
          loadFileBatch(batch, fromFileTable);
          batch.clear();
        }
      }
    }
    if (!batch.isEmpty()) {
      loadFileBatch(batch, fromFileTable);
    }
    store.flushWrites();
    store.discardFileCandidates();
  }

  private void loadFileBatch(List<byte[]> keys, Table<String, ? extends WithParentObjectId> fromFileTable)
      throws IOException {
    List<String> tableKeys = new ArrayList<>(keys.size());
    for (byte[] rawKey : keys) {
      tableKeys.add(StringUtils.bytes2String(rawKey));
    }
    List<? extends WithParentObjectId> entries = tableMultiGetSkipCache(fromFileTable, tableKeys);
    for (WithParentObjectId entry : entries) {
      if (entry == null) {
        continue;
      }
      byte[] value = toPersistedFormat(entry);
      SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, false);
      store.putOldList(info.getObjectId(),
          new EntryValue(info.getParentId(), info.getName(), false, computeSignature(value, false)).toBytes());
    }
  }

  private static List<? extends WithParentObjectId> tableMultiGetSkipCache(
      Table<String, ? extends WithParentObjectId> table, List<String> keys) throws IOException {
    try {
      return table.multiGetSkipCache(keys);
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException("Failed to read table keys in batch", e);
    }
  }

  private static Table.KeyValueIterator<String, OmDirectoryInfo> openDirectoryIterator(
      Table<String, OmDirectoryInfo> table, String tablePrefix) throws IOException {
    try {
      return table.iterator(tablePrefix);
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException("Failed to open directory table iterator", e);
    }
  }

  private static byte[] toPersistedFormat(WithParentObjectId entry) throws IOException {
    try {
      if (entry instanceof OmDirectoryInfo) {
        return OmDirectoryInfo.getCodec().toPersistedFormat((OmDirectoryInfo) entry);
      }
      return OmKeyInfo.getKeyTableCodec().toPersistedFormat((OmKeyInfo) entry);
    } catch (CodecException e) {
      throw new IOException("Failed to encode table value", e);
    }
  }

  private static void requireFileTable(String tableName) {
    if (!KEY_TABLE.equals(tableName) && !FILE_TABLE.equals(tableName)) {
      throw new IllegalArgumentException("Expected file or key table, got: " + tableName);
    }
  }

  private static void requireDirectoryTable(String tableName) {
    if (!DIRECTORY_TABLE.equals(tableName)) {
      throw new IllegalArgumentException("Expected directory table, got: " + tableName);
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
}

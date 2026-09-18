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

import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_OM_UPDATE_ID;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.IteratorType;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * The multi-stage sequential read in FULL diff mode that produces the
 * intermediate structures consumed by the later merge-join and path-resolution stages.
 *
 * <p>Call {@link #scanFileTables} then {@link #scanDirectoryTables} (FSO only)
 * in that order. Each method runs the to-side scan first, then the from-side
 * scan for the same table pair.
 *
 * <p>Scans iterate the raw snapshot tables ({@code Table<byte[], byte[]>} from
 * {@code DBStore#getTable(String)}) so {@link SnapshotDiffValueParser} reads the exact
 * persisted protobuf bytes and compare signatures match on-disk layout.
 *
 * <p>Every to-side row is written to {@code newList} once: either a present-marker
 * (unchanged-marker; membership only) or a full {@link EntryValue} with signature when it
 * passes the update-id gate. Every from-side row is written to {@code oldList}:
 * {@code DiffCandidateSet} members store a full {@link EntryValue} with signature; all
 * other rows store {@code parentId}, {@code name}, and {@code isDir} with an empty
 * signature.
 *
 * <p>When an update-id gate is supplied (HA OM), to-side gating normally admits
 * rows with {@code updateID > fromSnapshotDbTxSequenceNumber}. HA OM write paths
 * are expected to bump {@code updateID} on every meaningful metadata change.
 * Rows with a missing {@code updateID}, {@code updateID == 0}, or
 * {@code updateID == DEFAULT_OM_UPDATE_ID} ({@code -1}) are always treated as
 * candidates as a conservative fallback for legacy or ambiguous rows.
 *
 * <p>When no gate is supplied (non-HA), every to-side row is a candidate and a
 * compare signature is computed for each.
 */
public class FullDiffSequentialReader {

  private final SnapDiffJobStore store;
  private final long updateIdGate;
  private final boolean gatingEnabled;

  /**
   * Non-HA full diff: gating is disabled and every to-side entry is a candidate.
   */
  public FullDiffSequentialReader(SnapDiffJobStore store) {
    this(store, null);
  }

  /**
   * @param store        per-job temp column families for this full diff job
   * @param updateIdGate when non-null, enables HA gating using this from-snapshot
   *                     transaction index; when null, gating is disabled (non-HA)
   */
  public FullDiffSequentialReader(SnapDiffJobStore store, Long updateIdGate) {
    this.store = store;
    this.gatingEnabled = updateIdGate != null;
    this.updateIdGate = updateIdGate != null ? updateIdGate : 0L;
  }

  /**
   * Scans {@code toSnapshot.keyTable}/{@code fileTable} then the from-side counterpart.
   *
   * @param fromTable raw from-snapshot key/file table
   * @param toTable   raw to-snapshot key/file table
   * @param keyPrefix optional bucket prefix as stored in RocksDB; {@code null} scans the full table
   */
  public void scanFileTables(Table<byte[], byte[]> fromTable,
      Table<byte[], byte[]> toTable, byte[] keyPrefix) throws IOException {
    scanToTable(toTable, keyPrefix, false);
    scanFromTable(fromTable, keyPrefix, false);
  }

  /**
   * Scans {@code toSnapshot.directoryTable} then {@code fromSnapshot.directoryTable}.
   *
   * @param fromTable raw from-snapshot directory table
   * @param toTable   raw to-snapshot directory table
   * @param keyPrefix optional bucket prefix as stored in RocksDB; {@code null} scans the full table
   */
  public void scanDirectoryTables(Table<byte[], byte[]> fromTable,
      Table<byte[], byte[]> toTable, byte[] keyPrefix) throws IOException {
    scanToTable(toTable, keyPrefix, true);
    scanFromTable(fromTable, keyPrefix, true);
  }

  private void scanToTable(Table<byte[], byte[]> table, byte[] keyPrefix, boolean isDir)
      throws IOException {
    try (Table.KeyValueIterator<byte[], byte[]> iter =
        table.iterator(keyPrefix, IteratorType.VALUE_ONLY)) {
      while (iter.hasNext()) {
        byte[] value = iter.next().getValue();
        processToSideEntry(value, isDir);
      }
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException(e);
    }
    store.flushWrites();
  }

  private void scanFromTable(Table<byte[], byte[]> table, byte[] keyPrefix, boolean isDir)
      throws IOException {
    store.flushWrites();
    try (Table.KeyValueIterator<byte[], byte[]> iter =
        table.iterator(keyPrefix, IteratorType.VALUE_ONLY)) {
      while (iter.hasNext()) {
        byte[] value = iter.next().getValue();
        processFromSideEntry(value, isDir);
      }
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException(e);
    }
    store.clearDiffCandidates();
    store.flushWrites();
  }

  private void processToSideEntry(byte[] value, boolean isDir) throws IOException {
    SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, isDir, gatingEnabled);
    long objectId = info.getObjectId();

    if (isDir) {
      store.putToEdge(info.getParentId(), objectId, nameBytes(info.getName()));
    }

    if (isCandidateOnToSide(info)) {
      store.addDiffCandidate(objectId);
      byte[] signature = computeSignature(value, isDir);
      store.putNewList(objectId,
          new EntryValue(info.getParentId(), info.getName(), isDir, signature).toBytes());
    } else {
      store.putNewListPresentMarker(objectId);
    }
  }

  private void processFromSideEntry(byte[] value, boolean isDir) throws IOException {
    SnapshotDiffValueParser.ParsedRequiredInfo info = parseRequired(value, isDir, false);
    long objectId = info.getObjectId();

    if (isDir) {
      store.putFromEdge(info.getParentId(), objectId, nameBytes(info.getName()));
    }

    byte[] oldListValue;
    if (store.isDiffCandidate(objectId)) {
      byte[] signature = computeSignature(value, isDir);
      oldListValue = new EntryValue(info.getParentId(), info.getName(), isDir, signature).toBytes();
    } else {
      oldListValue = new EntryValue(info.getParentId(), info.getName(), isDir, null).toBytes();
    }
    store.putOldList(objectId, oldListValue);
  }

  private boolean isCandidateOnToSide(SnapshotDiffValueParser.ParsedRequiredInfo info) {
    if (!gatingEnabled) {
      return true;
    }
    if (!info.hasUpdateId()) {
      return true;
    }
    long updateId = info.getUpdateId();
    if (updateId == 0L || updateId == DEFAULT_OM_UPDATE_ID) {
      return true;
    }
    return updateId > updateIdGate;
  }

  private static SnapshotDiffValueParser.ParsedRequiredInfo parseRequired(byte[] value,
      boolean isDir, boolean includeUpdateId)
      throws IOException {
    return isDir
        ? SnapshotDiffValueParser.parseDirectoryInfoRequiredFields(value, includeUpdateId)
        : SnapshotDiffValueParser.parseKeyInfoRequiredFields(value, includeUpdateId);
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

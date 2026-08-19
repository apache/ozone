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

import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Tests the DAG-diff Stage 1 multi-stage sequential read (HDDS-15393).
 */
class TestDagDiffSequentialReader {

  private static final String VOLUME = "vol";
  private static final String BUCKET = "buck";
  private static final long BUCKET_OBJECT_ID = 1L;
  private static final String TABLE_PREFIX = "";

  @TempDir
  private static File tempDir;
  private static ManagedRocksDB db;
  private static ManagedDBOptions dbOptions;
  private static ManagedColumnFamilyOptions columnFamilyOptions;
  private static CodecRegistry codecRegistry;
  private static final AtomicInteger JOB_ID = new AtomicInteger(0);

  @BeforeAll
  static void init() throws RocksDBException {
    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    columnFamilyOptions = new ManagedColumnFamilyOptions();
    codecRegistry = CodecRegistry.newBuilder().build();

    File dbDir = new File(tempDir, "dag-diff-stage1.db");
    List<ColumnFamilyDescriptor> descriptors = Collections.singletonList(
        new ColumnFamilyDescriptor(StringUtils.string2Bytes(DEFAULT_COLUMN_FAMILY_NAME), columnFamilyOptions));
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    db = ManagedRocksDB.open(dbOptions, dbDir.getAbsolutePath(), descriptors, handles);
  }

  @AfterAll
  static void teardown() {
    if (db != null) {
      db.close();
    }
    if (columnFamilyOptions != null) {
      columnFamilyOptions.close();
    }
    if (dbOptions != null) {
      dbOptions.close();
    }
  }

  @Test
  void testSequenceGateAndTombstones() throws Exception {
    long gate = 50L;
    List<MergedKeyValue> delta = Arrays.asList(
        merged(userKey("below-gate"), 40L, 0, null),
        merged(userKey("tombstone"), 60L, 0, null),
        merged(userKey("create"), 70L, 1, keyBytes("create", 1L, 0L, 100L)));

    try (SnapDiffJobStore store = newStore(false)) {
      runStage1(store, gate, delta, Collections.emptyList(), mockFileTable(), mockDirectoryTables());
      assertNull(store.getNewList(99L));
      assertNotNull(store.getNewList(1L));
    }
  }

  @Test
  void testFileCandidateSetSpillsWhenThresholdExceeded() throws Exception {
    try (SnapDiffJobStore store = newStore(false, 1L)) {
      store.putFileCandidate(userKey("first"));
      assertFalse(store.areFileCandidatesSpilled());
      store.putFileCandidate(userKey("second"));
      assertTrue(store.areFileCandidatesSpilled());
    }
  }

  @Test
  void testKeyDiffShapesWithSpilledCandidates() throws Exception {
    long gate = 50L;
    byte[] modifyKey = userKey("modify");
    byte[] oldNameKey = userKey("oldname");
    byte[] newNameKey = userKey("newname");
    byte[] deletedKey = userKey("deleted");

    byte[] modifyFrom = keyBytes("modify", 2L, 0L, 100L);
    byte[] modifyTo = keyBytes("modify", 2L, 0L, 200L);
    byte[] oldRename = keyBytes("oldname", 3L, 0L, 100L);
    byte[] newRename = keyBytes("newname", 3L, 0L, 100L);
    byte[] deletedFrom = keyBytes("deleted", 4L, 0L, 100L);

    List<MergedKeyValue> delta = Arrays.asList(
        merged(modifyKey, 70L, 1, modifyTo),
        merged(oldNameKey, 70L, 0, null),
        merged(newNameKey, 80L, 1, newRename),
        merged(deletedKey, 70L, 0, null),
        merged(userKey("create"), 80L, 1, keyBytes("create", 1L, 0L, 100L)));

    try (SnapDiffJobStore store = newStore(false, 0L)) {
      runStage1(store, gate, delta, Collections.emptyList(),
          mockFileTable(modifyFrom, oldRename, deletedFrom), mockDirectoryTables());
      assertNotNull(store.getOldList(4L));
    }
  }

  @Test
  void testKeyDiffShapes() throws Exception {
    long gate = 50L;
    byte[] modifyKey = userKey("modify");
    byte[] oldNameKey = userKey("oldname");
    byte[] newNameKey = userKey("newname");
    byte[] deletedKey = userKey("deleted");

    byte[] modifyFrom = keyBytes("modify", 2L, 0L, 100L);
    byte[] modifyTo = keyBytes("modify", 2L, 0L, 200L);
    byte[] oldRename = keyBytes("oldname", 3L, 0L, 100L);
    byte[] newRename = keyBytes("newname", 3L, 0L, 100L);
    byte[] deletedFrom = keyBytes("deleted", 4L, 0L, 100L);

    List<MergedKeyValue> delta = Arrays.asList(
        merged(modifyKey, 70L, 1, modifyTo),
        merged(oldNameKey, 70L, 0, null),
        merged(newNameKey, 80L, 1, newRename),
        merged(deletedKey, 70L, 0, null),
        merged(userKey("create"), 80L, 1, keyBytes("create", 1L, 0L, 100L)));

    try (SnapDiffJobStore store = newStore(false)) {
      runStage1(store, gate, delta, Collections.emptyList(),
          mockFileTable(modifyFrom, oldRename, deletedFrom), mockDirectoryTables());

      assertNotNull(store.getNewList(1L)); // CREATE
      assertNotNull(store.getNewList(2L)); // MODIFY
      assertNotNull(store.getNewList(3L)); // RENAME target
      assertNull(store.getNewList(4L));    // DELETE tombstone only

      assertNull(store.getOldList(1L));    // CREATE has no from entry
      assertNotNull(store.getOldList(2L));
      assertNotNull(store.getOldList(3L));
      assertNotNull(store.getOldList(4L));

      EntryValue newRenameVal = EntryValue.fromBytes(store.getNewList(3L));
      EntryValue oldRenameVal = EntryValue.fromBytes(store.getOldList(3L));
      assertEquals("newname", newRenameVal.getName());
      assertEquals("oldname", oldRenameVal.getName());
      assertArrayEquals(newRenameVal.getSignature(), oldRenameVal.getSignature());

      EntryValue newModify = EntryValue.fromBytes(store.getNewList(2L));
      EntryValue oldModify = EntryValue.fromBytes(store.getOldList(2L));
      assertFalse(Arrays.equals(newModify.getSignature(), oldModify.getSignature()));
    }
  }

  @Test
  void testFsoDirectoryEdgesAndDirCandidates() throws Exception {
    long gate = 50L;
    byte[] dirKey = userKey("dir-a");
    byte[] dirValue = dirBytes("a", 100L, BUCKET_OBJECT_ID);

    List<MergedKeyValue> dirDelta = Collections.singletonList(merged(dirKey, 60L, 1, dirValue));
    DirectoryTables dirTables = mockDirectoryTables(dirKey, dirValue);

    try (SnapDiffJobStore store = newStore(true)) {
      runStage1(store, gate, Collections.emptyList(), dirDelta, mockFileTable(), dirTables);
      assertEquals("a", name(store.getToEdgeName(BUCKET_OBJECT_ID, 100L)));
      assertEquals("a", name(store.getFromEdgeName(BUCKET_OBJECT_ID, 100L)));
      assertNotNull(store.getNewList(100L));
      assertNotNull(store.getOldList(100L));
    }
  }

  private static SnapDiffJobStore newStore(boolean fso) throws IOException {
    return newStore(fso, SnapDiffJobStore.DEFAULT_WRITE_BATCH_SIZE,
        OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_IN_MEMORY_ENTRIES_PER_JOB_DEFAULT);
  }

  private static SnapDiffJobStore newStore(boolean fso, long maxInMemoryEntries) throws IOException {
    return newStore(fso, SnapDiffJobStore.DEFAULT_WRITE_BATCH_SIZE, maxInMemoryEntries);
  }

  private static SnapDiffJobStore newStore(boolean fso, int writeBatchSize, long maxInMemoryEntries)
      throws IOException {
    return SnapDiffJobStore.open(db, codecRegistry, columnFamilyOptions,
        "job" + JOB_ID.incrementAndGet(), fso, SnapDiffJobStore.Mode.DAG, writeBatchSize,
        maxInMemoryEntries);
  }

  private static void runStage1(SnapDiffJobStore store, long gate,
      List<MergedKeyValue> fileDelta,
      List<MergedKeyValue> dirDelta,
      Table<String, OmKeyInfo> fromFileTable,
      DirectoryTables directoryTables) throws Exception {
    DagDiffSequentialReader reader = new DagDiffSequentialReader(store, gate);
    if (!fileDelta.isEmpty()) {
      reader.consumeDeltaEntries(mergeIterator(fileDelta, gate), false);
    }
    reader.buildFileIntermediates(Collections.emptyList(), fromFileTable);
    if (store.isFso()) {
      if (!dirDelta.isEmpty()) {
        reader.consumeDeltaEntries(mergeIterator(dirDelta, gate), true);
      }
      reader.buildDirectoryIntermediates(TABLE_PREFIX, Collections.emptyList(), directoryTables.fromTable,
          directoryTables.toTable);
    }
  }

  private static LatestVersionedKWayMergeIterator mergeIterator(List<MergedKeyValue> entries, long gate) {
    ClosableIterator<MergedKeyValue> source = new ClosableIterator<MergedKeyValue>() {
      private final Iterator<MergedKeyValue> delegate = entries.iterator();

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public MergedKeyValue next() {
        return delegate.next();
      }

      @Override
      public void close() {
        // no-op
      }
    };
    return LatestVersionedKWayMergeIterator.forTest(Collections.singletonList(source), gate);
  }

  @SuppressWarnings("unchecked")
  private static Table<String, OmKeyInfo> mockFileTable(byte[]... persistedValues) throws Exception {
    Table<String, OmKeyInfo> table = mock(Table.class);
    when(table.getName()).thenReturn(KEY_TABLE);
    java.util.Map<String, OmKeyInfo> entries = new java.util.HashMap<>();
    for (byte[] persisted : persistedValues) {
      OmKeyInfo keyInfo = OmKeyInfo.getKeyTableCodec().fromPersistedFormat(persisted);
      entries.put(keyInfo.getKeyName(), keyInfo);
    }
    when(table.get(anyString())).thenAnswer(invocation -> entries.get(invocation.getArgument(0)));
    when(table.multiGetSkipCache(anyList())).thenAnswer(invocation -> {
      List<String> keys = invocation.getArgument(0);
      List<OmKeyInfo> values = new ArrayList<>(keys.size());
      for (String key : keys) {
        values.add(entries.get(key));
      }
      return values;
    });
    return table;
  }

  private static DirectoryTables mockDirectoryTables() throws Exception {
    return mockDirectoryTables(Collections.emptyList(), Collections.emptyList());
  }

  private static DirectoryTables mockDirectoryTables(byte[] dirKey, byte[] dirValue) throws Exception {
    byte[][] row = new byte[][] {dirKey, dirValue};
    return mockDirectoryTables(Collections.singletonList(row), Collections.singletonList(row));
  }

  @SuppressWarnings("unchecked")
  private static DirectoryTables mockDirectoryTables(List<byte[][]> fromRows, List<byte[][]> toRows)
      throws Exception {
    Table<String, OmDirectoryInfo> fromTable = mock(Table.class);
    Table<String, OmDirectoryInfo> toTable = mock(Table.class);
    when(fromTable.getName()).thenReturn(DIRECTORY_TABLE);
    when(toTable.getName()).thenReturn(DIRECTORY_TABLE);
    when(fromTable.iterator(eq(TABLE_PREFIX))).thenReturn(directoryIterator(fromRows));
    when(toTable.iterator(eq(TABLE_PREFIX))).thenReturn(directoryIterator(toRows));
    return new DirectoryTables(fromTable, toTable);
  }

  private static Table.KeyValueIterator<String, OmDirectoryInfo> directoryIterator(List<byte[][]> rows)
      throws Exception {
    List<Table.KeyValue<String, OmDirectoryInfo>> entries = new ArrayList<>();
    for (byte[][] row : rows) {
      byte[] key = row[0];
      byte[] value = row[1];
      OmDirectoryInfo dirInfo = OmDirectoryInfo.getCodec().fromPersistedFormat(value);
      entries.add(Table.newKeyValue(StringUtils.bytes2String(key), dirInfo));
    }
    Iterator<Table.KeyValue<String, OmDirectoryInfo>> delegate = entries.iterator();
    return new Table.KeyValueIterator<String, OmDirectoryInfo>() {
      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public Table.KeyValue<String, OmDirectoryInfo> next() {
        return delegate.next();
      }

      @Override
      public void close() {
        // no-op
      }

      @Override
      public void seekToFirst() {
      }

      @Override
      public void seekToLast() {
      }

      @Override
      public Table.KeyValue<String, OmDirectoryInfo> seek(String s) {
        return null;
      }

      @Override
      public void removeFromDB() {
      }
    };
  }

  private static MergedKeyValue merged(byte[] key, long sequence, int type, byte[] value) {
    return MergedKeyValue.of(key, sequence, type, value);
  }

  private static byte[] userKey(String name) {
    return name.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] keyBytes(String keyName, long objectId, long parentId, long dataSize) throws Exception {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(keyName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setObjectID(objectId)
        .setParentObjectID(parentId)
        .setUpdateID(10L)
        .setDataSize(dataSize)
        .build();
    return OmKeyInfo.getKeyTableCodec().toPersistedFormat(keyInfo);
  }

  private static byte[] dirBytes(String name, long objectId, long parentId) throws Exception {
    OmDirectoryInfo dirInfo = OmDirectoryInfo.newBuilder()
        .setName(name)
        .setObjectID(objectId)
        .setParentObjectID(parentId)
        .setUpdateID(10L)
        .build();
    return OmDirectoryInfo.getCodec().toPersistedFormat(dirInfo);
  }

  private static String name(byte[] value) {
    return value == null ? null : new String(value, StandardCharsets.UTF_8);
  }

  private static final class DirectoryTables {
    private final Table<String, OmDirectoryInfo> fromTable;
    private final Table<String, OmDirectoryInfo> toTable;

    private DirectoryTables(Table<String, OmDirectoryInfo> fromTable,
        Table<String, OmDirectoryInfo> toTable) {
      this.fromTable = fromTable;
      this.toTable = toTable;
    }
  }
}

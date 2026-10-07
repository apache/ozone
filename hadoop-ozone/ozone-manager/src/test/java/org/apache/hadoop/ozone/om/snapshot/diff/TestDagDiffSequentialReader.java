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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
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

  @TempDir
  private static File tempDir;
  private static ManagedRocksDB db;
  private static ManagedDBOptions dbOptions;
  private static ManagedColumnFamilyOptions columnFamilyOptions;
  private static CodecRegistry codecRegistry;
  private static ColumnFamilyHandle snapDiffReportCfh;
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
    snapDiffReportCfh = db.get().createColumnFamily(
        new ColumnFamilyDescriptor(StringUtils.string2Bytes("snap-diff-report"), columnFamilyOptions));
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
        toMergedKV(modifyKey, 70L, 1, modifyTo),
        toMergedKV(oldNameKey, 70L, 0, null),
        toMergedKV(newNameKey, 80L, 1, newRename),
        toMergedKV(deletedKey, 70L, 0, null),
        toMergedKV(userKey("create"), 80L, 1, keyBytes("create", 1L, 0L, 100L)));

    DirectoryTables directoryTables = directoryTables(InMemoryTestTable.forRawBytes(DIRECTORY_TABLE),
        InMemoryTestTable.forRawBytes(DIRECTORY_TABLE));

    try (SnapDiffJobStore store = newStore(false)) {
      runReader(store, false, gate, delta, Collections.emptyList(),
          rawFileTable(modifyKey, modifyFrom, oldNameKey, oldRename, deletedKey, deletedFrom),
          directoryTables);

      assertNotNull(store.getNewList(1L)); // CREATE
      assertNotNull(store.getNewList(2L)); // MODIFY
      assertNotNull(store.getNewList(3L)); // RENAME target
      assertNull(store.getNewList(4L));    // DELETE tombstone only

      assertNull(store.getOldList(1L));    // CREATE has no from entry
      assertNotNull(store.getOldList(2L));
      assertNotNull(store.getOldList(3L));
      assertNotNull(store.getOldList(4L));
      assertEquals(0, store.getDiffCandidateCount());

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
  void testDirectoryDiffShapes() throws Exception {
    long gate = 50L;
    byte[] createKey = userKey("create");
    byte[] modifyKey = userKey("modify");
    byte[] oldNameKey = userKey("oldname");
    byte[] newNameKey = userKey("newname");
    byte[] deletedKey = userKey("deleted");

    byte[] createTo = dirBytes("create", 1L, BUCKET_OBJECT_ID);
    byte[] modifyFrom = dirBytes("modify", 2L, BUCKET_OBJECT_ID, metadata("ver", "from"));
    byte[] modifyTo = dirBytes("modify", 2L, BUCKET_OBJECT_ID, metadata("ver", "to"));
    byte[] oldRename = dirBytes("oldname", 3L, BUCKET_OBJECT_ID);
    byte[] newRename = dirBytes("newname", 3L, BUCKET_OBJECT_ID);
    byte[] deletedFrom = dirBytes("deleted", 4L, BUCKET_OBJECT_ID);

    List<MergedKeyValue> dirDelta = Arrays.asList(
        toMergedKV(modifyKey, 70L, 1, modifyTo),
        toMergedKV(oldNameKey, 70L, 0, null),
        toMergedKV(newNameKey, 80L, 1, newRename),
        toMergedKV(deletedKey, 70L, 0, null),
        toMergedKV(createKey, 80L, 1, createTo));

    DirectoryTables dirTables = directoryTables(
        rawDirectoryTable(modifyKey, modifyFrom, oldNameKey, oldRename, deletedKey, deletedFrom),
        rawDirectoryTable(createKey, createTo, modifyKey, modifyTo, newNameKey, newRename));

    try (SnapDiffJobStore store = newStore(true)) {
      runReader(store, true, gate, Collections.emptyList(), dirDelta, rawFileTable(), dirTables);

      assertNotNull(store.getNewList(1L)); // CREATE
      assertNotNull(store.getNewList(2L)); // MODIFY
      assertNotNull(store.getNewList(3L)); // RENAME target
      assertNull(store.getNewList(4L));    // DELETE tombstone only

      assertNull(store.getOldList(1L));    // CREATE has no from entry
      assertNotNull(store.getOldList(2L));
      assertNotNull(store.getOldList(3L));
      assertNotNull(store.getOldList(4L));
      assertEquals(0, store.getDiffCandidateCount());

      EntryValue createVal = EntryValue.fromBytes(store.getNewList(1L));
      assertTrue(createVal.isDir());
      assertEquals("create", createVal.getName());
      assertEquals(BUCKET_OBJECT_ID, createVal.getParentId());

      EntryValue newRenameVal = EntryValue.fromBytes(store.getNewList(3L));
      EntryValue oldRenameVal = EntryValue.fromBytes(store.getOldList(3L));
      assertTrue(newRenameVal.isDir());
      assertTrue(oldRenameVal.isDir());
      assertEquals("newname", newRenameVal.getName());
      assertEquals("oldname", oldRenameVal.getName());
      assertArrayEquals(newRenameVal.getSignature(), oldRenameVal.getSignature());

      EntryValue newModify = EntryValue.fromBytes(store.getNewList(2L));
      EntryValue oldModify = EntryValue.fromBytes(store.getOldList(2L));
      assertFalse(Arrays.equals(newModify.getSignature(), oldModify.getSignature()));

      assertFromEdge(store, 1L, null, null);
      assertFromEdge(store, 2L, BUCKET_OBJECT_ID, "modify");
      assertFromEdge(store, 3L, BUCKET_OBJECT_ID, "oldname");
      assertFromEdge(store, 4L, BUCKET_OBJECT_ID, "deleted");

      assertToEdge(store, 1L, BUCKET_OBJECT_ID, "create");
      assertToEdge(store, 2L, BUCKET_OBJECT_ID, "modify");
      assertToEdge(store, 3L, BUCKET_OBJECT_ID, "newname");
      assertToEdge(store, 4L, null, null);
    }
  }

  private static void assertFromEdge(SnapDiffJobStore store, long objectId, Long parentId, String name)
      throws IOException {
    assertEdge(store.multiGetFromEdgeValues(Collections.singletonList(objectId)).get(0), parentId, name);
  }

  private static void assertToEdge(SnapDiffJobStore store, long objectId, Long parentId, String name)
      throws IOException {
    assertEdge(store.multiGetToEdgeValues(Collections.singletonList(objectId)).get(0), parentId, name);
  }

  private static void assertEdge(byte[] value, Long parentId, String name) {
    if (parentId == null) {
      assertNull(value);
      return;
    }
    assertNotNull(value);
    assertEquals(parentId.longValue(), SnapDiffJobStore.decodeEdgeLinkParentId(value));
    if (name != null) {
      assertEquals(name, SnapDiffJobStore.decodeEdgeLinkName(value));
    }
  }

  private static SnapDiffJobStore newStore(boolean fso) throws IOException {
    return SnapDiffJobStore.open(db, codecRegistry, columnFamilyOptions,
        "job" + JOB_ID.incrementAndGet(), fso, snapDiffReportCfh, null, null);
  }

  private static void runReader(SnapDiffJobStore store, boolean fso, long gate,
      List<MergedKeyValue> fileDelta,
      List<MergedKeyValue> dirDelta,
      Table<byte[], byte[]> fromFileTable,
      DirectoryTables directoryTables) throws Exception {
    DagDiffSequentialReader reader = new DagDiffSequentialReader(store, gate);
    if (!fileDelta.isEmpty()) {
      reader.scanFileTablesFromDelta(mergeIterator(fileDelta, gate), fromFileTable);
    }
    if (fso && !dirDelta.isEmpty()) {
      reader.scanDirectoryTablesFromDelta(mergeIterator(dirDelta, gate), null,
          directoryTables.fromTable, directoryTables.toTable);
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

  private static Table<byte[], byte[]> rawFileTable(byte[]... keyValuePairs) {
    InMemoryTestTable<byte[], byte[]> table = InMemoryTestTable.forRawBytes(KEY_TABLE);
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      table.put(keyValuePairs[i], keyValuePairs[i + 1]);
    }
    return table;
  }

  private static DirectoryTables directoryTables(Table<byte[], byte[]> fromTable,
      Table<byte[], byte[]> toTable) {
    return new DirectoryTables(fromTable, toTable);
  }

  private static Table<byte[], byte[]> rawDirectoryTable(byte[]... keyValuePairs) {
    InMemoryTestTable<byte[], byte[]> table = InMemoryTestTable.forRawBytes(DIRECTORY_TABLE);
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      table.put(keyValuePairs[i], keyValuePairs[i + 1]);
    }
    return table;
  }

  private static MergedKeyValue toMergedKV(byte[] key, long sequence, int type, byte[] value) {
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
    return dirBytes(name, objectId, parentId, Collections.emptyMap());
  }

  private static byte[] dirBytes(String name, long objectId, long parentId, Map<String, String> metadata)
      throws Exception {
    OmDirectoryInfo.Builder builder = OmDirectoryInfo.newBuilder()
        .setName(name)
        .setObjectID(objectId)
        .setParentObjectID(parentId)
        .setUpdateID(10L);
    if (!metadata.isEmpty()) {
      builder.addAllMetadata(metadata);
    }
    return OmDirectoryInfo.getCodec().toPersistedFormat(builder.build());
  }

  private static Map<String, String> metadata(String key, String value) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(key, value);
    return metadata;
  }

  private static final class DirectoryTables {
    private final Table<byte[], byte[]> fromTable;
    private final Table<byte[], byte[]> toTable;

    private DirectoryTables(Table<byte[], byte[]> fromTable, Table<byte[], byte[]> toTable) {
      this.fromTable = fromTable;
      this.toTable = toTable;
    }
  }
}

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
import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_OM_UPDATE_ID;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Tests the full-diff Stage 1 multi-stage sequential read (HDDS-15394).
 */
class TestFullDiffSequentialReader {

  private static final String VOLUME = "vol";
  private static final String BUCKET = "buck";
  private static final long BUCKET_OBJECT_ID = 1L;

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

    File dbDir = new File(tempDir, "full-diff-stage1.db");
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
  void testKeyDiffShapesWithGating() throws Exception {
    long gate = 50L;
    Table<byte[], byte[]> toTable = InMemoryTestTable.forRawBytes();
    putKey(toTable, "k1", keyInfo("create", 1L, 0L, 60L, 100L));
    putKey(toTable, "k2", keyInfo("modify", 2L, 0L, 70L, 200L));
    putKey(toTable, "k3", keyInfo("newname", 3L, 0L, 70L, 100L));
    putKey(toTable, "k5", keyInfo("unchanged", 5L, 0L, 10L, 100L));

    Table<byte[], byte[]> fromTable = InMemoryTestTable.forRawBytes();
    putKey(fromTable, "k2", keyInfo("modify", 2L, 0L, 10L, 100L));
    putKey(fromTable, "k3", keyInfo("oldname", 3L, 0L, 10L, 100L));
    putKey(fromTable, "k4", keyInfo("deleted", 4L, 0L, 10L, 100L));
    putKey(fromTable, "k5", keyInfo("unchanged", 5L, 0L, 10L, 100L));

    try (SnapDiffJobStore store = newStore(false)) {
      new FullDiffSequentialReader(store, gate).scanFileTables(fromTable, toTable, null);

      assertTrue(store.isNewListCandidate(1L));
      assertTrue(store.isNewListCandidate(2L));
      assertTrue(store.isNewListCandidate(3L));
      assertFalse(store.hasNewListEntry(4L));
      assertTrue(store.hasNewListEntry(5L));
      assertFalse(store.isNewListCandidate(5L));

      assertNull(store.getOldList(1L));
      assertNotNull(store.getOldList(2L));
      assertNotNull(store.getOldList(3L));
      assertNotNull(store.getOldList(4L));
      assertNotNull(store.getOldList(5L));
      assertEquals(0, store.getDiffCandidateCount());

      EntryValue unchangedOld = EntryValue.fromBytes(store.getOldList(5L));
      assertEquals(0, unchangedOld.getSignature().length);

      EntryValue newRename = EntryValue.fromBytes(store.getNewList(3L));
      EntryValue oldRename = EntryValue.fromBytes(store.getOldList(3L));
      assertEquals("newname", newRename.getName());
      assertEquals("oldname", oldRename.getName());
      assertArrayEquals(newRename.getSignature(), oldRename.getSignature());

      EntryValue newModify = EntryValue.fromBytes(store.getNewList(2L));
      EntryValue oldModify = EntryValue.fromBytes(store.getOldList(2L));
      assertFalse(java.util.Arrays.equals(newModify.getSignature(), oldModify.getSignature()));
    }
  }

  @Test
  void testGatingDisabledAdmitsAllToEntries() throws Exception {
    Table<byte[], byte[]> toTable = InMemoryTestTable.forRawBytes();
    putKey(toTable, "k1", keyInfo("a", 1L, 0L, 5L, 100L));
    putKey(toTable, "k2", keyInfo("b", 2L, 0L, 5L, 100L));

    Table<byte[], byte[]> fromTable = InMemoryTestTable.forRawBytes();
    putKey(fromTable, "k1", keyInfo("a", 1L, 0L, 5L, 100L));

    try (SnapDiffJobStore store = newStore(false)) {
      new FullDiffSequentialReader(store).scanFileTables(fromTable, toTable, null);
      assertTrue(store.isNewListCandidate(1L));
      assertTrue(store.isNewListCandidate(2L));
      assertNotNull(store.getOldList(1L));
      assertEquals(0, store.getDiffCandidateCount());
    }
  }

  @Test
  void testDeleteCandidateHasMetadataWithoutSignature() throws Exception {
    long gate = 50L;
    Table<byte[], byte[]> toTable = InMemoryTestTable.forRawBytes();
    putKey(toTable, "k5", keyInfo("unchanged", 5L, 0L, 10L, 100L));

    Table<byte[], byte[]> fromTable = InMemoryTestTable.forRawBytes();
    putKey(fromTable, "k4", keyInfo("deleted", 4L, 0L, 10L, 100L));

    try (SnapDiffJobStore store = newStore(false)) {
      new FullDiffSequentialReader(store, gate).scanFileTables(fromTable, toTable, null);

      EntryValue deleted = EntryValue.fromBytes(store.getOldList(4L));
      assertEquals(0, deleted.getSignature().length);
      assertEquals("deleted", deleted.getName());
    }
  }

  @Test
  void testStaleUpdateIdValuesAreCandidates() throws Exception {
    long gate = 50L;
    Table<byte[], byte[]> toTable = InMemoryTestTable.forRawBytes();
    putKey(toTable, "k10", keyInfo("zero", 10L, 0L, 0L, 100L));
    putKey(toTable, "k11", keyInfo("default-id", 11L, 0L, DEFAULT_OM_UPDATE_ID, 100L));

    Table<byte[], byte[]> fromTable = InMemoryTestTable.forRawBytes();
    putKey(fromTable, "k10", keyInfo("zero", 10L, 0L, 0L, 100L));
    putKey(fromTable, "k11", keyInfo("default-id", 11L, 0L, DEFAULT_OM_UPDATE_ID, 100L));

    try (SnapDiffJobStore store = newStore(false)) {
      new FullDiffSequentialReader(store, gate).scanFileTables(fromTable, toTable, null);

      assertTrue(store.isNewListCandidate(10L));
      assertTrue(store.isNewListCandidate(11L));
      assertNotNull(store.getOldList(10L));
      assertNotNull(store.getOldList(11L));
      assertEquals(0, store.getDiffCandidateCount());
    }
  }

  @Test
  void testDiffCandidatesSpillToRocksDb() throws Exception {
    Table<byte[], byte[]> toTable = InMemoryTestTable.forRawBytes();
    putKey(toTable, "k1", keyInfo("a", 1L, 0L, 60L, 100L));
    putKey(toTable, "k2", keyInfo("b", 2L, 0L, 60L, 100L));
    putKey(toTable, "k3", keyInfo("c", 3L, 0L, 60L, 100L));

    Table<byte[], byte[]> fromTable = InMemoryTestTable.forRawBytes();
    putKey(fromTable, "k1", keyInfo("a", 1L, 0L, 10L, 100L));
    putKey(fromTable, "k2", keyInfo("b", 2L, 0L, 10L, 100L));
    putKey(fromTable, "k3", keyInfo("c", 3L, 0L, 10L, 100L));

    try (SnapDiffJobStore store = SnapDiffJobStore.open(db, codecRegistry, columnFamilyOptions,
        "job" + JOB_ID.incrementAndGet(), false, SnapDiffJobStore.Mode.FULL,
        SnapDiffJobStore.DEFAULT_WRITE_BATCH_SIZE, 2L)) {
      new FullDiffSequentialReader(store, 50L).scanFileTables(fromTable, toTable, null);

      assertTrue(store.isNewListCandidate(1L));
      assertTrue(store.isNewListCandidate(2L));
      assertTrue(store.isNewListCandidate(3L));
      assertNotNull(store.getOldList(1L));
      assertNotNull(store.getOldList(2L));
      assertNotNull(store.getOldList(3L));
      assertEquals(0, store.getDiffCandidateCount());
    }
  }

  @Test
  void testFsoDirectoryEdgesPopulated() throws Exception {
    Table<byte[], byte[]> toTable = InMemoryTestTable.forRawBytes();
    putDir(toTable, "d100", dirInfo("a", 100L, BUCKET_OBJECT_ID, 60L));
    putDir(toTable, "d101", dirInfo("b", 101L, 100L, 60L));

    Table<byte[], byte[]> fromTable = InMemoryTestTable.forRawBytes();
    putDir(fromTable, "d100", dirInfo("a", 100L, BUCKET_OBJECT_ID, 10L));
    putDir(fromTable, "d101", dirInfo("b", 101L, 100L, 10L));

    try (SnapDiffJobStore store = newStore(true)) {
      new FullDiffSequentialReader(store, 50L).scanDirectoryTables(fromTable, toTable, null);

      assertEquals("a", name(store.getToEdgeName(BUCKET_OBJECT_ID, 100L)));
      assertEquals("b", name(store.getToEdgeName(100L, 101L)));
      assertEquals("a", name(store.getFromEdgeName(BUCKET_OBJECT_ID, 100L)));
      assertEquals("b", name(store.getFromEdgeName(100L, 101L)));
      assertEquals(0, store.getDiffCandidateCount());
    }
  }

  private static SnapDiffJobStore newStore(boolean fso) throws IOException {
    return SnapDiffJobStore.open(db, codecRegistry, columnFamilyOptions,
        "job" + JOB_ID.incrementAndGet(), fso, SnapDiffJobStore.Mode.FULL);
  }

  private static void putKey(Table<byte[], byte[]> table, String key, OmKeyInfo keyInfo)
      throws CodecException, RocksDatabaseException {
    table.put(tableKey(key), OmKeyInfo.getKeyTableCodec().toPersistedFormat(keyInfo));
  }

  private static void putDir(Table<byte[], byte[]> table, String key, OmDirectoryInfo dirInfo)
      throws CodecException, RocksDatabaseException {
    table.put(tableKey(key), OmDirectoryInfo.getCodec().toPersistedFormat(dirInfo));
  }

  private static byte[] tableKey(String key) throws CodecException {
    return StringCodec.get().toPersistedFormat(key);
  }

  private static OmKeyInfo keyInfo(String keyName, long objectId, long parentId, long updateId, long dataSize) {
    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(keyName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setObjectID(objectId)
        .setParentObjectID(parentId)
        .setUpdateID(updateId)
        .setDataSize(dataSize)
        .build();
  }

  private static OmDirectoryInfo dirInfo(String name, long objectId, long parentId, long updateId) {
    return OmDirectoryInfo.newBuilder()
        .setName(name)
        .setObjectID(objectId)
        .setParentObjectID(parentId)
        .setUpdateID(updateId)
        .build();
  }

  private static String name(byte[] value) {
    return value == null ? null : new String(value, StandardCharsets.UTF_8);
  }
}

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
import static org.apache.hadoop.ozone.om.snapshot.diff.MergeJoinSnapDiffWriter.writeReport;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone.getDiffReportEntryCodec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Unit tests for merge-join classification, delete retention, and ordering (HDDS-15391).
 */
class TestMergeJoinSnapDiffWriter {

  private static final long BUCKET_OBJECT_ID = 1L;
  private static final AtomicInteger JOB_ID = new AtomicInteger(0);

  @TempDir
  private static java.io.File tempDir;
  private static ManagedRocksDB db;
  private static ManagedDBOptions dbOptions;
  private static ManagedColumnFamilyOptions columnFamilyOptions;
  private static CodecRegistry codecRegistry;
  private static ColumnFamilyHandle snapDiffReportCfh;

  @BeforeAll
  static void init() throws RocksDBException {
    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    columnFamilyOptions = new ManagedColumnFamilyOptions();
    codecRegistry = CodecRegistry.newBuilder()
        .addCodec(DiffReportEntry.class, getDiffReportEntryCodec())
        .build();
    java.io.File dbDir = new java.io.File(tempDir, "merge-join-test.db");
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    db = ManagedRocksDB.open(dbOptions, dbDir.getAbsolutePath(),
        Collections.singletonList(new ColumnFamilyDescriptor(
            StringUtils.string2Bytes("default"), columnFamilyOptions)),
        handles);
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
  void testClassificationMatrixObs() throws Exception {
    byte[] sig = signature("sig");
    byte[] sig2 = signature("sig2");
    try (SnapDiffJobStore store = newStore(false)) {
      store.putNewList(10L, entry(0L, "created", false, sig));
      store.putOldList(20L, entry(0L, "deleted", false, sig));
      store.putNewListPresentMarker(30L);
      store.putOldList(30L, entry(0L, "same", false, sig));
      store.putNewList(40L, entry(0L, "newname", false, sig));
      store.putOldList(40L, entry(0L, "oldname", false, sig));
      store.putNewList(50L, entry(0L, "modified", false, sig2));
      store.putOldList(50L, entry(0L, "modified", false, sig));
      store.flushWrites();

      // dependency ordering flag is not relevant for OBS entries
      List<DiffReportEntry> entries = runWriteReport(store, BUCKET_OBJECT_ID, false, true);
      assertEquals(4, entries.size());
      assertEquals(DELETE, entries.get(0).getType());
      assertEquals("deleted", new String(entries.get(0).getSourcePath(), StandardCharsets.UTF_8));
      assertEquals(MODIFY, entries.get(1).getType());
      assertEquals("modified", new String(entries.get(1).getSourcePath(), StandardCharsets.UTF_8));
      assertEquals(RENAME, entries.get(2).getType());
      assertEquals("oldname", new String(entries.get(2).getSourcePath(), StandardCharsets.UTF_8));
      assertEquals("newname", new String(entries.get(2).getTargetPath(), StandardCharsets.UTF_8));
      assertEquals(CREATE, entries.get(3).getType());
      assertEquals("created", new String(entries.get(3).getSourcePath(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void testModificationTimeOnlyDoesNotModify() throws Exception {
    byte[] sig = signature("same-content");
    try (SnapDiffJobStore store = newStore(false)) {
      store.putNewListPresentMarker(60L);
      store.putOldList(60L, entry(0L, "stable", false, sig));
      store.putNewList(61L, entry(0L, "changed-meta", false, sig));
      store.putOldList(61L, entry(0L, "changed-meta", false, sig));
      store.flushWrites();

      List<DiffReportEntry> entries = runWriteReport(store, BUCKET_OBJECT_ID, false, false);
      assertTrue(entries.isEmpty());
    }
  }

  @Test
  void testIsDirMismatchFailsJob() throws Exception {
    try (SnapDiffJobStore store = newStore(false)) {
      store.putNewList(70L, entry(0L, "x", true, signature("a")));
      store.putOldList(70L, entry(0L, "x", false, signature("a")));
      store.flushWrites();

      SnapshotDiffManager manager = mockManagerPassthroughDeletes();
      assertThrows(IOException.class,
          () -> writeReport(manager, store, BUCKET_OBJECT_ID, false, false));
    }
  }

  @Test
  void testTopLevelDeleteRetentionFso() throws Exception {
    byte[] sig = signature("d");
    try (SnapDiffJobStore store = newStore(true)) {
      store.putFromEdge(BUCKET_OBJECT_ID, 100L, nameBytes("dirA"));
      store.putFromEdge(100L, 101L, nameBytes("dirB"));
      store.putOldList(100L, entry(BUCKET_OBJECT_ID, "dirA", true, sig));
      store.putOldList(101L, entry(100L, "dirB", true, sig));
      store.putOldList(102L, entry(101L, "fileC", false, sig));
      store.flushWrites();

      SnapshotDiffManager manager = mockManagerWithRealDeleteRetention();
      List<DiffReportEntry> entries = runWriteReportWithManager(store, BUCKET_OBJECT_ID, true,
          manager, false);
      assertEquals(1, entries.size());
      assertEquals(DELETE, entries.get(0).getType());
      assertEquals("dirA", new String(entries.get(0).getSourcePath(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void testTopLevelDeleteRetentionUsesBatchedAncestorLookup() throws Exception {
    byte[] sig = signature("d");
    SnapDiffJobStore store = spy(newStore(true));
    try (SnapDiffJobStore ignored = store) {
      for (long i = 0; i < 1001; i++) {
        long dirObjectId = 10_000L + i;
        long fileObjectId = 20_000L + i;
        store.putFromEdge(BUCKET_OBJECT_ID, dirObjectId, nameBytes("dir-" + i));
        store.putOldList(fileObjectId, entry(dirObjectId, "file-" + i, false, sig));
      }
      store.flushWrites();

      AtomicInteger multiGetCalls = new AtomicInteger();
      doAnswer(invocation -> {
        multiGetCalls.incrementAndGet();
        return invocation.callRealMethod();
      }).when(store).multiGetFromEdgeValues(any());

      long reportIndex = writeReport(mockManagerWithRealDeleteRetention(), store,
          BUCKET_OBJECT_ID, true, false).getKey();

      assertEquals(2, multiGetCalls.get());
      assertEquals(1001, reportIndex);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFsoPathResolutionAndOrdering(boolean ordering) throws Exception {
    byte[] oldSig = signature("o");
    byte[] newSig = signature("n");
    try (SnapDiffJobStore store = newStore(true)) {
      store.putToEdge(BUCKET_OBJECT_ID, 203L, nameBytes("parent1"));
      store.putToEdge(BUCKET_OBJECT_ID, 201L, nameBytes("parent2"));
      store.putFromEdge(BUCKET_OBJECT_ID, 200L, nameBytes("parent1"));
      store.putFromEdge(BUCKET_OBJECT_ID, 201L, nameBytes("parent2"));

      store.putNewListPresentMarker(201L);
      store.putNewList(203L, entry(BUCKET_OBJECT_ID, "parent1", true, newSig));
      store.putNewList(204L, entry(203L, "child", false, newSig));
      store.putNewList(202L, entry(201L, "child", false, newSig));

      store.putOldList(200L, entry(BUCKET_OBJECT_ID, "parent1", true, oldSig));
      store.putOldList(201L, entry(BUCKET_OBJECT_ID, "parent2", true, null));
      store.putOldList(202L, entry(200L, "child", false, oldSig));

      store.flushWrites();

      List<DiffReportEntry> entries = runWriteReport(store, BUCKET_OBJECT_ID, true, ordering);
      assertEquals(5, entries.size());
      if (ordering) {
        assertEquals(MODIFY, entries.get(0).getType());
        assertEquals("parent1/child", new String(entries.get(0).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals(RENAME, entries.get(1).getType());
        assertEquals("parent1/child", new String(entries.get(1).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals("parent2/child", new String(entries.get(1).getTargetPath(), StandardCharsets.UTF_8));
        assertEquals(DELETE, entries.get(2).getType());
        assertEquals("parent1", new String(entries.get(2).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals(CREATE, entries.get(3).getType());
        assertEquals("parent1", new String(entries.get(3).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals(CREATE, entries.get(4).getType());
        assertEquals("parent1/child", new String(entries.get(4).getSourcePath(), StandardCharsets.UTF_8));
      } else {
        assertEquals(DELETE, entries.get(0).getType());
        assertEquals("parent1", new String(entries.get(0).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals(MODIFY, entries.get(1).getType());
        assertEquals("parent1/child", new String(entries.get(1).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals(RENAME, entries.get(2).getType());
        assertEquals("parent1/child", new String(entries.get(2).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals("parent2/child", new String(entries.get(2).getTargetPath(), StandardCharsets.UTF_8));
        assertEquals(CREATE, entries.get(3).getType());
        assertEquals("parent1", new String(entries.get(3).getSourcePath(), StandardCharsets.UTF_8));
        assertEquals(CREATE, entries.get(4).getType());
        assertEquals("parent1/child", new String(entries.get(4).getSourcePath(), StandardCharsets.UTF_8));
      }

    }
  }

  @Test
  void testDependencyOrderingUsesBatchedReportLookup() throws Exception {
    byte[] sig = signature("s");
    SnapDiffJobStore store = spy(newStore(true));
    try (SnapDiffJobStore ignored = store) {
      store.putToEdge(BUCKET_OBJECT_ID, 199L, nameBytes("parent"));
      for (long objectId = 200L; objectId < 1201L; objectId++) {
        String name = "child-" + objectId;
        store.putNewList(objectId, entry(199L, name, false, sig));
      }
      store.flushWrites();

      AtomicInteger batchLookups = new AtomicInteger();
      doAnswer(invocation -> {
        batchLookups.incrementAndGet();
        return invocation.callRealMethod();
      }).when(store).multiGetDependencyReportEntries(any());

      writeReport(mockManagerPassthroughDeletes(), store,
          BUCKET_OBJECT_ID, true, true);

      assertEquals(2, batchLookups.get());
    }
  }

  @Test
  void testDependencyOrderingFallsBackWhenOverLimit() throws Exception {
    byte[] sig = signature("s");
    SnapDiffJobStore store = spy(newStore(true, 1L));
    try (SnapDiffJobStore ignored = store) {
      store.putToEdge(BUCKET_OBJECT_ID, 200L, nameBytes("parent"));
      store.putNewList(201L, entry(200L, "child1", false, sig));
      store.putNewList(202L, entry(200L, "child2", false, sig));
      store.flushWrites();

      writeReport(mockManagerPassthroughDeletes(), store,
          BUCKET_OBJECT_ID, true, true);

      List<DiffReportEntry> entries = store.readReportEntriesForTest();
      assertEquals(2, entries.size());
      verify(store, times(1)).putDependencyNode(anyInt(), any());
      verify(store, never()).putOrderedReportEntries(anyList());
    }
  }

  private static List<DiffReportEntry> runWriteReport(SnapDiffJobStore store, long bucketObjectId,
      boolean fso, boolean ordering) throws Exception {
    return runWriteReportWithManager(store, bucketObjectId, fso, mockManagerPassthroughDeletes(), ordering);
  }

  private static List<DiffReportEntry> runWriteReportWithManager(SnapDiffJobStore store,
      long bucketObjectId, boolean fso, SnapshotDiffManager manager, boolean ordering) throws IOException {
    writeReport(manager, store, bucketObjectId, fso, ordering);
    return store.readReportEntriesForTest();
  }

  private static SnapshotDiffManager mockManagerPassthroughDeletes() throws Exception {
    SnapshotDiffManager manager = mock(SnapshotDiffManager.class);
    when(manager.hasDeletedAncestors(any(), any(), any(),
        any(SnapshotDiffManager.ParentIdBatchLookup.class), anyLong(), any()))
        .thenAnswer(invocation -> new boolean[((List<Long>) invocation.getArgument(0)).size()]);
    return manager;
  }

  private static SnapshotDiffManager mockManagerWithRealDeleteRetention() throws Exception {
    SnapshotDiffManager manager = mock(SnapshotDiffManager.class);
    when(manager.hasDeletedAncestors(any(), any(), any(),
        any(SnapshotDiffManager.ParentIdBatchLookup.class), anyLong(), any()))
        .thenCallRealMethod();
    return manager;
  }

  private static SnapDiffJobStore newStore(boolean fso) throws IOException {
    return newStore(fso, 1_000_000L);
  }

  private static SnapDiffJobStore newStore(boolean fso, long maxInMemoryEntries) throws IOException {
    return SnapDiffJobStore.open(db, codecRegistry, columnFamilyOptions,
        jobName(), fso, snapDiffReportCfh, null, maxInMemoryEntries);
  }

  private static String jobName() {
    return "job" + JOB_ID.incrementAndGet();
  }

  private static byte[] entry(long parentId, String name, boolean isDir, byte[] signature) {
    return new EntryValue(parentId, name, isDir, signature).toBytes();
  }

  private static byte[] nameBytes(String name) {
    return name.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] signature(String seed) {
    return Arrays.copyOf(seed.getBytes(StandardCharsets.UTF_8), 32);
  }
}

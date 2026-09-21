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

      List<DiffReportEntry> entries = runWriteReport(store, BUCKET_OBJECT_ID, false);
      assertEquals(4, entries.size());
      assertTrue(containsType(entries, CREATE));
      assertTrue(containsType(entries, DELETE));
      assertTrue(containsType(entries, RENAME));
      assertTrue(containsType(entries, MODIFY));
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

      List<DiffReportEntry> entries = runWriteReport(store, BUCKET_OBJECT_ID, false);
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
          () -> MergeJoinSnapDiffWriter.writeReport(manager, store, BUCKET_OBJECT_ID, false));
    }
  }

  @Test
  void testTopLevelDeleteRetentionFso() throws Exception {
    byte[] sig = signature("d");
    try (SnapDiffJobStore store = newStore(true)) {
      store.putFromEdge(BUCKET_OBJECT_ID, 100L, nameBytes("dirA"));
      store.putFromEdge(100L, 101L, nameBytes("dirB"));
      store.putFromEdge(101L, 102L, nameBytes("fileC"));
      store.putOldList(100L, entry(BUCKET_OBJECT_ID, "dirA", true, sig));
      store.putOldList(101L, entry(100L, "dirB", true, sig));
      store.putOldList(102L, entry(101L, "fileC", false, sig));
      store.flushWrites();

      SnapshotDiffManager manager = mockManagerWithRealDeleteRetention();
      List<DiffReportEntry> entries = runWriteReportWithManager(store, BUCKET_OBJECT_ID, true, manager);
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
        store.putFromEdge(dirObjectId, fileObjectId, nameBytes("file-" + i));
        store.putOldList(fileObjectId, entry(dirObjectId, "dir-" + i + "/file-" + i, false, sig));
      }
      store.flushWrites();

      AtomicInteger multiGetCalls = new AtomicInteger();
      doAnswer(invocation -> {
        multiGetCalls.incrementAndGet();
        return invocation.callRealMethod();
      }).when(store).multiGetFromEdgeValues(any());

      MergeJoinSnapDiffWriter.writeReport(mockManagerWithRealDeleteRetention(), store,
          BUCKET_OBJECT_ID, true);

      assertEquals(2, multiGetCalls.get());
    }
  }

  @Test
  void testFsoPathResolutionAndOrdering() throws Exception {
    byte[] sig = signature("s");
    try (SnapDiffJobStore store = newStore(true)) {
      store.putToEdge(BUCKET_OBJECT_ID, 200L, nameBytes("parent"));
      store.putFromEdge(BUCKET_OBJECT_ID, 200L, nameBytes("parent"));
      store.putToEdge(200L, 201L, nameBytes("child"));
      store.putFromEdge(200L, 202L, nameBytes("gone"));
      store.putNewList(201L, entry(200L, "child", false, sig));
      store.putOldList(202L, entry(200L, "gone", false, sig));
      store.flushWrites();

      List<DiffReportEntry> entries = runWriteReport(store, BUCKET_OBJECT_ID, true);
      assertEquals(2, entries.size());
      DiffReportEntry create = entries.stream()
          .filter(e -> e.getType() == CREATE).findFirst()
          .orElseThrow(() -> new AssertionError("missing CREATE"));
      DiffReportEntry delete = entries.stream()
          .filter(e -> e.getType() == DELETE).findFirst()
          .orElseThrow(() -> new AssertionError("missing DELETE"));
      assertEquals("parent/child", new String(create.getSourcePath(), StandardCharsets.UTF_8));
      assertEquals("parent/gone", new String(delete.getSourcePath(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void testDependencyOrderingUsesBatchedReportLookup() throws Exception {
    byte[] sig = signature("s");
    SnapDiffJobStore store = spy(newStore(true));
    try (SnapDiffJobStore ignored = store) {
      for (long objectId = 200L; objectId < 1201L; objectId++) {
        String name = "child-" + objectId;
        store.putToEdge(BUCKET_OBJECT_ID, objectId, nameBytes(name));
        store.putNewList(objectId, entry(BUCKET_OBJECT_ID, name, false, sig));
      }
      store.flushWrites();

      AtomicInteger batchLookups = new AtomicInteger();
      doAnswer(invocation -> {
        batchLookups.incrementAndGet();
        return invocation.callRealMethod();
      }).when(store).multiGetDependencyReportEntries(any());

      MergeJoinSnapDiffWriter.writeReport(mockManagerPassthroughDeletes(), store,
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
      store.putToEdge(200L, 201L, nameBytes("child1"));
      store.putToEdge(200L, 202L, nameBytes("child2"));
      store.putNewList(201L, entry(200L, "child1", false, sig));
      store.putNewList(202L, entry(200L, "child2", false, sig));
      store.flushWrites();

      SnapshotDiffManager manager = mockManagerPassthroughDeletes();
      List<DiffReportEntry> captured = new ArrayList<>();
      doAnswer(invocation -> {
        captured.addAll(invocation.getArgument(0));
        return invocation.callRealMethod();
      }).when(store).putReportEntries(anyList());

      MergeJoinSnapDiffWriter.writeReport(manager, store, BUCKET_OBJECT_ID, true, true);
      assertEquals(2, captured.size());
      verify(store, never()).putDependencyNodes(anyInt(), anyList());
    }
  }

  private static List<DiffReportEntry> runWriteReport(SnapDiffJobStore store, long bucketObjectId,
      boolean fso) throws Exception {
    return runWriteReportWithManager(store, bucketObjectId, fso, mockManagerPassthroughDeletes());
  }

  private static List<DiffReportEntry> runWriteReportWithManager(SnapDiffJobStore store,
      long bucketObjectId, boolean fso, SnapshotDiffManager manager) throws IOException {
    MergeJoinSnapDiffWriter.writeReport(manager, store, bucketObjectId, fso);
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

  private static boolean containsType(List<DiffReportEntry> entries,
      org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType type) {
    return entries.stream().anyMatch(e -> e.getType() == type);
  }

  private static SnapDiffJobStore newStore(boolean fso) throws IOException {
    return newStore(fso, 1_000_000L);
  }

  private static SnapDiffJobStore newStore(boolean fso, long maxInMemoryEntries) throws IOException {
    return SnapDiffJobStore.open(db, codecRegistry, columnFamilyOptions,
        jobName(), fso, SnapDiffJobStore.Mode.FULL, SnapDiffJobStore.DEFAULT_BATCH_SIZE,
        maxInMemoryEntries, null);
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

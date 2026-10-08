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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Unit tests for {@link SnapDiffPathResolver}.
 */
class TestSnapDiffPathResolver {

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
    codecRegistry = CodecRegistry.newBuilder().build();
    java.io.File dbDir = new java.io.File(tempDir, "path-resolver-test.db");
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
  void testDeepChainMaterializesRootToLeaf() throws Exception {
    try (SnapDiffJobStore store = newStore()) {
      putFromEdge(store, BUCKET_OBJECT_ID, 10L, "a");
      putFromEdge(store, 10L, 11L, "b");
      putFromEdge(store, 11L, 12L, "c");
      store.flushWrites();

      SnapDiffPathResolver resolver = store.newFromPathResolver(BUCKET_OBJECT_ID);

      List<Long> objectIds = Arrays.asList(BUCKET_OBJECT_ID, 10L, 11L, 12L);
      List<String> paths = resolver.resolvePaths(objectIds);

      assertEquals("", paths.get(0));
      assertEquals("a", paths.get(1));
      assertEquals("a/b", paths.get(2));
      assertEquals("a/b/c", paths.get(3));
    }
  }

  @Test
  void testSharedAncestorUsesPathCache() throws Exception {
    try (SnapDiffJobStore store = newStore()) {
      putFromEdge(store, BUCKET_OBJECT_ID, 20L, "dir");
      putFromEdge(store, 20L, 21L, "left");
      putFromEdge(store, 20L, 22L, "right");
      store.flushWrites();

      SnapDiffPathResolver resolver = store.newFromPathResolver(BUCKET_OBJECT_ID);
      AtomicInteger dbLookupKeyCount = new AtomicInteger();

      try (MockedStatic<SnapDiffJobStore> mockedStore = mockStatic(SnapDiffJobStore.class, CALLS_REAL_METHODS)) {
        mockedStore.when(() -> SnapDiffJobStore.multiGet(any(ManagedRocksDB.class),
            any(ColumnFamilyHandle.class), anyList())).thenAnswer(invocation -> {
              dbLookupKeyCount.addAndGet(((List<?>) invocation.getArgument(2)).size());
              return invocation.callRealMethod();
            });

        // Lookup dir and left from DB.
        assertEquals("dir/left", resolver.resolvePaths(Collections.singletonList(21L)).get(0));
        assertEquals(2, dbLookupKeyCount.get());

        // Resolve right; prefetch still loads its edge and the shared parent edge.
        assertEquals("dir/right", resolver.resolvePaths(Collections.singletonList(22L)).get(0));
        assertEquals(4, dbLookupKeyCount.get());
      }
      assertTrue(resolver.isPathCached(20L));
      assertTrue(resolver.isPathCached(21L));
      assertTrue(resolver.isPathCached(22L));
    }
  }

  @Test
  void testMissingLinkReturnsNull() throws Exception {
    try (SnapDiffJobStore store = newStore()) {
      store.flushWrites();
      SnapDiffPathResolver resolver = store.newFromPathResolver(BUCKET_OBJECT_ID);
      assertNull(resolver.resolvePaths(Collections.singletonList(99L)).get(0));
    }
  }

  @Test
  void testBrokenChainReturnsNull() throws Exception {
    try (SnapDiffJobStore store = newStore()) {
      putFromEdge(store, BUCKET_OBJECT_ID, 30L, "top");
      store.flushWrites();

      SnapDiffPathResolver resolver = store.newFromPathResolver(BUCKET_OBJECT_ID);
      assertNull(resolver.resolvePaths(Collections.singletonList(31L)).get(0));
    }
  }

  @Test
  void testBatchResolveSurvivesLruEvictionWithinBatch() throws Exception {
    try (SnapDiffJobStore store = newStoreWithPathCacheCapacity(2L)) {
      putFromEdge(store, BUCKET_OBJECT_ID, 10L, "dir");
      putFromEdge(store, 10L, 11L, "a");
      putFromEdge(store, 10L, 12L, "b");
      store.flushWrites();

      SnapDiffPathResolver resolver = store.newFromPathResolver(BUCKET_OBJECT_ID);
      assertEquals("dir/a", resolver.resolvePaths(Collections.singletonList(11L)).get(0));

      List<String> paths = resolver.resolvePaths(Arrays.asList(12L, 11L));
      assertEquals("dir/b", paths.get(0));
      assertEquals("dir/a", paths.get(1));
    }
  }

  @Test
  void testToAndFromSideUseSeparateEdgeIndexes() throws Exception {
    try (SnapDiffJobStore store = newStore()) {
      putToEdge(store, BUCKET_OBJECT_ID, 40L, "to-name");
      putFromEdge(store, BUCKET_OBJECT_ID, 40L, "from-name");
      store.flushWrites();

      SnapDiffPathResolver toResolver = store.newToPathResolver(BUCKET_OBJECT_ID);
      SnapDiffPathResolver fromResolver = store.newFromPathResolver(BUCKET_OBJECT_ID);
      assertEquals("to-name", toResolver.resolvePaths(Collections.singletonList(40L)).get(0));
      assertEquals("from-name", fromResolver.resolvePaths(Collections.singletonList(40L)).get(0));
    }
  }

  private static void putFromEdge(SnapDiffJobStore store, long parentId, long objectId, String name)
      throws IOException {
    store.putFromEdge(parentId, objectId, nameBytes(name));
  }

  private static void putToEdge(SnapDiffJobStore store, long parentId, long objectId, String name)
      throws IOException {
    store.putToEdge(parentId, objectId, nameBytes(name));
  }

  private static byte[] nameBytes(String name) {
    return name.getBytes(StandardCharsets.UTF_8);
  }

  private static SnapDiffJobStore newStore() throws IOException {
    return newStoreWithPathCacheCapacity(null);
  }

  private static SnapDiffJobStore newStoreWithPathCacheCapacity(Long maxInMemoryEntries)
      throws IOException {
    return SnapDiffJobStore.open(db, codecRegistry, columnFamilyOptions,
        "path-resolver-" + JOB_ID.incrementAndGet(), true, snapDiffReportCfh, null,
        maxInMemoryEntries);
  }
}

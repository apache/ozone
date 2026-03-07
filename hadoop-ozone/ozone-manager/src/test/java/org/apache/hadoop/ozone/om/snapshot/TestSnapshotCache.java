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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.cache.CacheLoader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.lock.OmReadOnlyLock;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;

/**
 * Tests SnapshotCache.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class TestSnapshotCache {

  private static final int CACHE_SIZE_LIMIT = 3;
  private static CacheLoader<UUID, OmSnapshot> cacheLoader;
  private static IOzoneManagerLock lock;
  private SnapshotCache snapshotCache;

  private OMMetrics omMetrics;

  @BeforeAll
  static void beforeAll() throws Exception {
    cacheLoader = mock(CacheLoader.class);
    // Set SnapshotCache log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(SnapshotCache.class, Level.DEBUG);
    lock = spy(new OmReadOnlyLock());
  }

  @BeforeEach
  void setUp() throws Exception {
    // Reset cache for each test case
    omMetrics = OMMetrics.create();
    // Create a difference mock OmSnapshot instance each time load() is called
    doAnswer((Answer<OmSnapshot>) invocation -> {
      final OmSnapshot omSnapshot = mock(OmSnapshot.class);
      // Mock the snapshotTable return value for the lookup inside release()
      final UUID snapshotID = (UUID) invocation.getArguments()[0];
      when(omSnapshot.getSnapshotTableKey()).thenReturn(snapshotID.toString());
      when(omSnapshot.getSnapshotID()).thenReturn(snapshotID);

      OMMetadataManager metadataManager = mock(OMMetadataManager.class);
      org.apache.hadoop.hdds.utils.db.DBStore store = mock(org.apache.hadoop.hdds.utils.db.DBStore.class);
      when(omSnapshot.getMetadataManager()).thenReturn(metadataManager);
      when(metadataManager.getStore()).thenReturn(store);

      Table<?, ?> table1 = mock(Table.class);
      Table<?, ?> table2 = mock(Table.class);
      Table<?, ?> keyTable = mock(Table.class);
      when(table1.getName()).thenReturn("table1");
      when(table2.getName()).thenReturn("table2");
      when(keyTable.getName()).thenReturn("keyTable"); // This is in COLUMN_FAMILIES_TO_TRACK
      final List<Table<?, ?>> tables = new ArrayList<>();
      tables.add(table1);
      tables.add(table2);
      tables.add(keyTable);
      when(store.listTables()).thenReturn(tables);

      return omSnapshot;
    }).when(cacheLoader).load(any(UUID.class));
    snapshotCache = new SnapshotCache(cacheLoader, CACHE_SIZE_LIMIT, omMetrics, 50, true, lock);
  }

  @AfterEach
  void tearDown() {
    // Not strictly needed. Added for symmetry
    snapshotCache = null;
  }

  @Test
  @DisplayName("get()")
  void testGet() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    assertEquals(0, omMetrics.getNumSnapshotCacheSize());
    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertNotNull(omSnapshot.get());
    assertInstanceOf(OmSnapshot.class, omSnapshot.get());
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());
  }

  @Test
  @DisplayName("Tests get() fails on read lock failure")
  public void testGetFailsOnReadLock() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    final UUID dbKey2 = UUID.randomUUID();
    when(lock.acquireReadLock(eq(SNAPSHOT_DB_LOCK), eq(dbKey1.toString())))
        .thenReturn(OMLockDetails.EMPTY_DETAILS_LOCK_NOT_ACQUIRED);
    assertThrows(OMException.class, () -> snapshotCache.get(dbKey1));
    snapshotCache.get(dbKey2);
    assertEquals(1, snapshotCache.size());
  }

  @Test
  @DisplayName("Tests get() releases readLock when load() fails")
  public void testGetReleasesReadLockOnLoadFailure() throws Exception {
    clearInvocations(lock);
    final UUID dbKey = UUID.randomUUID();
    when(cacheLoader.load(eq(dbKey))).thenThrow(new Exception("Dummy exception thrown"));
    assertThrows(IllegalStateException.class, () -> snapshotCache.get(dbKey));
    verify(lock, times(1)).acquireReadLock(eq(SNAPSHOT_DB_LOCK), eq(dbKey.toString()));
    verify(lock, times(1)).releaseReadLock(eq(SNAPSHOT_DB_LOCK), eq(dbKey.toString()));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 5, 10})
  @DisplayName("Tests get() holds a read lock")
  public void testGetHoldsReadLock(int numberOfLocks) throws IOException {
    clearInvocations(lock);
    final UUID dbKey1 = UUID.randomUUID();
    final UUID dbKey2 = UUID.randomUUID();
    for (int i = 0; i < numberOfLocks; i++) {
      snapshotCache.get(dbKey1);
      snapshotCache.get(dbKey2);
    }
    assertEquals(numberOfLocks > 0 ? 2 : 0, snapshotCache.size());
    verify(lock, times(numberOfLocks)).acquireReadLock(eq(SNAPSHOT_DB_LOCK), eq(dbKey1.toString()));
    verify(lock, times(numberOfLocks)).acquireReadLock(eq(SNAPSHOT_DB_LOCK), eq(dbKey2.toString()));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 5, 10})
  @DisplayName("Tests lock() holds a write lock")
  public void testLockHoldsWriteLock(int numberOfLocks) {
    clearInvocations(lock);
    for (int i = 0; i < numberOfLocks; i++) {
      snapshotCache.lock();
    }
    verify(lock, times(numberOfLocks)).acquireResourceWriteLock(eq(SNAPSHOT_DB_LOCK));
  }

  @Test
  public void testLockSupplierReturnsLockWithAnotherLockReleased() {
    IOzoneManagerLock ozoneManagerLock = new OzoneManagerLock(new OzoneConfiguration());
    snapshotCache = new SnapshotCache(cacheLoader, CACHE_SIZE_LIMIT, omMetrics, 50, true, ozoneManagerLock);
    try (UncheckedAutoCloseableSupplier<OMLockDetails> lockDetails = snapshotCache.lock()) {
      ozoneManagerLock.acquireWriteLock(VOLUME_LOCK, "vol1");
      ozoneManagerLock.releaseWriteLock(VOLUME_LOCK, "vol1");
      assertTrue(lockDetails.get().isLockAcquired());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 5, 10})
  @DisplayName("Tests lock(snapshotId) holds a write lock")
  public void testLockHoldsWriteLockSnapshotId(int numberOfLocks) {
    clearInvocations(lock);
    UUID snapshotId = UUID.randomUUID();
    for (int i = 0; i < numberOfLocks; i++) {
      snapshotCache.lock(snapshotId);
    }
    verify(lock, times(numberOfLocks)).acquireWriteLock(eq(SNAPSHOT_DB_LOCK), eq(snapshotId.toString()));
  }

  @Test
  @DisplayName("get() same entry twice yields one cache entry only")
  void testGetTwice() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot1again = snapshotCache.get(dbKey1);
    // Should be the same instance
    assertEquals(omSnapshot1.get(), omSnapshot1again.get());
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());
  }

  @Test
  @DisplayName("release(String)")
  void testReleaseByDbKey() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertNotNull(omSnapshot1.get());
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());
  }

  @Test
  @DisplayName("invalidate()")
  void testInvalidate() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    snapshotCache.invalidate(dbKey1);
    assertEquals(0, snapshotCache.size());
    assertEquals(0, omMetrics.getNumSnapshotCacheSize());
  }

  @Test
  @DisplayName("invalidateAll()")
  void testInvalidateAll() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey2 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot2 = snapshotCache.get(dbKey2);
    assertNotNull(omSnapshot2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());
    // Should be difference omSnapshot instances
    assertNotEquals(omSnapshot1, omSnapshot2);

    final UUID dbKey3 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot3 = snapshotCache.get(dbKey3);
    assertNotNull(omSnapshot3);
    assertEquals(3, snapshotCache.size());
    assertEquals(3, omMetrics.getNumSnapshotCacheSize());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(3, snapshotCache.size());
    assertEquals(3, omMetrics.getNumSnapshotCacheSize());

    snapshotCache.invalidate(dbKey1);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());

    snapshotCache.invalidateAll();
    assertEquals(0, snapshotCache.size());
    assertEquals(0, omMetrics.getNumSnapshotCacheSize());
  }

  private void assertEntryExistence(UUID key, boolean shouldExist) {
    if (shouldExist) {
      snapshotCache.getDbMap().computeIfAbsent(key, k -> {
        fail(k + " should not have been evicted");
        return null;
      });
    } else {
      snapshotCache.getDbMap().computeIfPresent(key, (k, v) -> {
        fail(k + " should have been evicted");
        return null;
      });
    }
  }

  @Test
  @DisplayName("Basic cache eviction")
  void testEviction1() throws IOException, InterruptedException, TimeoutException {

    final UUID dbKey1 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> snapshot1 = snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());
    snapshotCache.release(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey2 = UUID.randomUUID();
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());
    snapshotCache.release(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey3 = UUID.randomUUID();
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());
    assertEquals(3, omMetrics.getNumSnapshotCacheSize());
    snapshotCache.release(dbKey3);
    assertEquals(3, snapshotCache.size());
    assertEquals(3, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey4 = UUID.randomUUID();
    snapshotCache.get(dbKey4);
    // dbKey1, dbKey2 and dbKey3 would have been evicted by the end of the last scheduled cleanup() because
    // those were released.
    GenericTestUtils.waitFor(() -> snapshotCache.size() == 1, 50, 3000);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());
    assertEntryExistence(dbKey1, false);

    // Verify compaction was called on the tables
    org.apache.hadoop.hdds.utils.db.DBStore store1 = snapshot1.get().getMetadataManager().getStore();
    verify(store1, times(1)).compactTable("table1");
    verify(store1, times(1)).compactTable("table2");
    // Verify compaction was NOT called on the reserved table
    verify(store1, times(0)).compactTable("keyTable");
  }

  @Test
  @DisplayName("Cache eviction while exceeding soft limit")
  void testEviction2() throws IOException, InterruptedException, TimeoutException {

    final UUID dbKey1 = UUID.randomUUID();
    snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey2 = UUID.randomUUID();
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey3 = UUID.randomUUID();
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());
    assertEquals(3, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey4 = UUID.randomUUID();
    snapshotCache.get(dbKey4);
    // dbKey1 would not have been evicted because it is not release()d
    assertEquals(4, snapshotCache.size());
    assertEquals(4, omMetrics.getNumSnapshotCacheSize());
    assertEntryExistence(dbKey1, true);

    // Releasing dbKey2 at this point should immediately trigger its eviction
    // because the cache size exceeded the soft limit
    snapshotCache.release(dbKey2);
    GenericTestUtils.waitFor(() -> snapshotCache.size() == 3, 50, 3000);
    assertEquals(3, snapshotCache.size());
    assertEquals(3, omMetrics.getNumSnapshotCacheSize());
    assertEntryExistence(dbKey2, false);
    assertEntryExistence(dbKey1, true);
  }

  @Test
  @DisplayName("Cache eviction with try-with-resources")
  void testEviction3WithClose() throws IOException, InterruptedException, TimeoutException {

    final UUID dbKey1 = UUID.randomUUID();
    try (UncheckedAutoCloseableSupplier<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey1)) {
      assertEquals(1L, snapshotCache.totalRefCount(rcOmSnapshot.get().getSnapshotID()));
      assertEquals(1, snapshotCache.size());
      assertEquals(1, omMetrics.getNumSnapshotCacheSize());
    }
    // ref count should have been decreased because it would be close()d
    // upon exiting try-with-resources.
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey1).getTotalRefCount());
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey2 = UUID.randomUUID();
    try (UncheckedAutoCloseableSupplier<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey2)) {
      assertEquals(1L, snapshotCache.totalRefCount(rcOmSnapshot.get().getSnapshotID()));
      assertEquals(2, snapshotCache.size());
      assertEquals(2, omMetrics.getNumSnapshotCacheSize());
      // Get dbKey2 entry a second time
      try (UncheckedAutoCloseableSupplier<OmSnapshot> rcOmSnapshot2 = snapshotCache.get(dbKey2)) {
        assertEquals(2L, snapshotCache.totalRefCount(rcOmSnapshot.get().getSnapshotID()));
        assertEquals(2L, snapshotCache.totalRefCount(rcOmSnapshot2.get().getSnapshotID()));
        assertEquals(2, snapshotCache.size());
        assertEquals(2, omMetrics.getNumSnapshotCacheSize());
      }
      assertEquals(1L, snapshotCache.totalRefCount(rcOmSnapshot.get().getSnapshotID()));
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey2).getTotalRefCount());
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey3 = UUID.randomUUID();
    try (UncheckedAutoCloseableSupplier<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey3)) {
      assertEquals(1L, snapshotCache.totalRefCount(rcOmSnapshot.get().getSnapshotID()));
      assertEquals(3, snapshotCache.size());
      assertEquals(3, omMetrics.getNumSnapshotCacheSize());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey3).getTotalRefCount());
    assertEquals(3, snapshotCache.size());
    assertEquals(3, omMetrics.getNumSnapshotCacheSize());

    final UUID dbKey4 = UUID.randomUUID();
    try (UncheckedAutoCloseableSupplier<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey4)) {
      GenericTestUtils.waitFor(() -> snapshotCache.size() == 1, 50, 3000);
      assertEquals(1L, snapshotCache.totalRefCount(rcOmSnapshot.get().getSnapshotID()));
      assertEquals(1, snapshotCache.size());
      assertEquals(1, omMetrics.getNumSnapshotCacheSize());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey4).getTotalRefCount());
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());
  }

  @Test
  @DisplayName("Snapshot operations not blocked during compaction")
  void testSnapshotOperationsNotBlockedDuringCompaction() throws IOException, InterruptedException, TimeoutException {
    omMetrics = OMMetrics.create();
    snapshotCache = new SnapshotCache(cacheLoader, 1, omMetrics, 50, true,
        lock);
    final UUID dbKey1 = UUID.randomUUID();
    UncheckedAutoCloseableSupplier<OmSnapshot> snapshot1 = snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());
    snapshotCache.release(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, omMetrics.getNumSnapshotCacheSize());

    // Simulate compaction blocking
    final Semaphore compactionLock = new Semaphore(1);
    final AtomicBoolean table1Compacting = new AtomicBoolean(false);
    final AtomicBoolean table1CompactedFinish = new AtomicBoolean(false);
    final AtomicBoolean table2CompactedFinish = new AtomicBoolean(false);
    org.apache.hadoop.hdds.utils.db.DBStore store1 = snapshot1.get().getMetadataManager().getStore();
    doAnswer(invocation -> {
      table1Compacting.set(true);
      // Simulate compaction lock
      compactionLock.acquire();
      table1CompactedFinish.set(true);
      return null;
    }).when(store1).compactTable("table1");
    doAnswer(invocation -> {
      table2CompactedFinish.set(true);
      return null;
    }).when(store1).compactTable("table2");
    compactionLock.acquire();

    final UUID dbKey2 = UUID.randomUUID();
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());
    snapshotCache.release(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, omMetrics.getNumSnapshotCacheSize());

    // wait for compaction to start
    GenericTestUtils.waitFor(() -> table1Compacting.get(), 50, 3000);

    snapshotCache.get(dbKey1); // this should not be blocked

    // wait for compaction to finish
    assertFalse(table1CompactedFinish.get());
    compactionLock.release();
    GenericTestUtils.waitFor(() -> table1CompactedFinish.get(), 50, 3000);
    GenericTestUtils.waitFor(() -> table2CompactedFinish.get(), 50, 3000);

    verify(store1, times(1)).compactTable("table1");
    verify(store1, times(1)).compactTable("table2");
    verify(store1, times(0)).compactTable("keyTable");
  }
}

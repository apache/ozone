/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.cache.CacheLoader;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests SnapshotCache.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class TestSnapshotCache {

  private static final int CACHE_SIZE_LIMIT = 3;
  private static CacheLoader<UUID, OmSnapshot> cacheLoader;
  private SnapshotCache snapshotCache;

  @BeforeAll
  static void beforeAll() throws Exception {
    cacheLoader = mock(CacheLoader.class);
    // Create a difference mock OmSnapshot instance each time load() is called
    when(cacheLoader.load(any())).thenAnswer(
        (Answer<OmSnapshot>) invocation -> {
          final OmSnapshot omSnapshot = mock(OmSnapshot.class);
          // Mock the snapshotTable return value for the lookup inside release()
          final UUID snapshotID = (UUID) invocation.getArguments()[0];
          when(omSnapshot.getSnapshotTableKey()).thenReturn(snapshotID.toString());

          return omSnapshot;
        }
    );

    // Set SnapshotCache log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(SnapshotCache.LOG, Level.DEBUG);
  }

  @BeforeEach
  void setUp() {
    // Reset cache for each test case
    snapshotCache = new SnapshotCache(cacheLoader, CACHE_SIZE_LIMIT);
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
    ReferenceCounted<OmSnapshot> omSnapshot = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertNotNull(omSnapshot.get());
    assertInstanceOf(OmSnapshot.class, omSnapshot.get());
    assertEquals(1, snapshotCache.size());
  }

  @Test
  @DisplayName("get() same entry twice yields one cache entry only")
  void testGetTwice() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    ReferenceCounted<OmSnapshot> omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());

    ReferenceCounted<OmSnapshot> omSnapshot1again = snapshotCache.get(dbKey1);
    // Should be the same instance
    assertEquals(omSnapshot1, omSnapshot1again);
    assertEquals(omSnapshot1.get(), omSnapshot1again.get());
    assertEquals(1, snapshotCache.size());
  }

  @Test
  @DisplayName("release(String)")
  void testReleaseByDbKey() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    ReferenceCounted<OmSnapshot> omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertNotNull(omSnapshot1.get());
    assertEquals(1, snapshotCache.size());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
  }

  @Test
  @DisplayName("invalidate()")
  void testInvalidate() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    ReferenceCounted<OmSnapshot> omSnapshot = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertEquals(1, snapshotCache.size());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());

    snapshotCache.invalidate(dbKey1);
    assertEquals(0, snapshotCache.size());
  }

  @Test
  @DisplayName("invalidateAll()")
  void testInvalidateAll() throws IOException {
    final UUID dbKey1 = UUID.randomUUID();
    ReferenceCounted<OmSnapshot> omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());

    final UUID dbKey2 = UUID.randomUUID();
    ReferenceCounted<OmSnapshot> omSnapshot2 = snapshotCache.get(dbKey2);
    assertNotNull(omSnapshot2);
    assertEquals(2, snapshotCache.size());
    // Should be difference omSnapshot instances
    assertNotEquals(omSnapshot1, omSnapshot2);

    final UUID dbKey3 = UUID.randomUUID();
    ReferenceCounted<OmSnapshot> omSnapshot3 = snapshotCache.get(dbKey3);
    assertNotNull(omSnapshot3);
    assertEquals(3, snapshotCache.size());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(3, snapshotCache.size());

    snapshotCache.invalidate(dbKey1);
    assertEquals(2, snapshotCache.size());

    snapshotCache.invalidateAll();
    assertEquals(0, snapshotCache.size());
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
  void testEviction1() throws IOException {

    final UUID dbKey1 = UUID.randomUUID();
    snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());
    snapshotCache.release(dbKey1);
    assertEquals(1, snapshotCache.size());

    final UUID dbKey2 = UUID.randomUUID();
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());
    snapshotCache.release(dbKey2);
    assertEquals(2, snapshotCache.size());

    final UUID dbKey3 = UUID.randomUUID();
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());
    snapshotCache.release(dbKey3);
    assertEquals(3, snapshotCache.size());

    final UUID dbKey4 = UUID.randomUUID();
    snapshotCache.get(dbKey4);
    // dbKey1, dbKey2 and dbKey3 would have been evicted by the end of the last get() because
    // those were release()d.
    assertEquals(1, snapshotCache.size());
    assertEntryExistence(dbKey1, false);
  }

  @Test
  @DisplayName("Cache eviction while exceeding soft limit")
  void testEviction2() throws IOException {

    final UUID dbKey1 = UUID.randomUUID();
    snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());

    final UUID dbKey2 = UUID.randomUUID();
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());

    final UUID dbKey3 = UUID.randomUUID();
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());

    final UUID dbKey4 = UUID.randomUUID();
    snapshotCache.get(dbKey4);
    // dbKey1 would not have been evicted because it is not release()d
    assertEquals(4, snapshotCache.size());
    assertEntryExistence(dbKey1, true);

    // Releasing dbKey2 at this point should immediately trigger its eviction
    // because the cache size exceeded the soft limit
    snapshotCache.release(dbKey2);
    assertEquals(3, snapshotCache.size());
    assertEntryExistence(dbKey2, false);
    assertEntryExistence(dbKey1, true);
  }

  @Test
  @DisplayName("Cache eviction with try-with-resources")
  void testEviction3WithClose() throws IOException {

    final UUID dbKey1 = UUID.randomUUID();
    try (ReferenceCounted<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey1)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(1, snapshotCache.size());
    }
    // ref count should have been decreased because it would be close()d
    // upon exiting try-with-resources.
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey1).getTotalRefCount());
    assertEquals(1, snapshotCache.size());

    final UUID dbKey2 = UUID.randomUUID();
    try (ReferenceCounted<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey2)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(2, snapshotCache.size());
      // Get dbKey2 entry a second time
      try (ReferenceCounted<OmSnapshot> rcOmSnapshot2 = snapshotCache.get(dbKey2)) {
        assertEquals(2L, rcOmSnapshot.getTotalRefCount());
        assertEquals(2L, rcOmSnapshot2.getTotalRefCount());
        assertEquals(2, snapshotCache.size());
      }
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey2).getTotalRefCount());
    assertEquals(2, snapshotCache.size());

    final UUID dbKey3 = UUID.randomUUID();
    try (ReferenceCounted<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey3)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(3, snapshotCache.size());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey3).getTotalRefCount());
    assertEquals(3, snapshotCache.size());

    final UUID dbKey4 = UUID.randomUUID();
    try (ReferenceCounted<OmSnapshot> rcOmSnapshot = snapshotCache.get(dbKey4)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(1, snapshotCache.size());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey4).getTotalRefCount());
    assertEquals(1, snapshotCache.size());
  }
}

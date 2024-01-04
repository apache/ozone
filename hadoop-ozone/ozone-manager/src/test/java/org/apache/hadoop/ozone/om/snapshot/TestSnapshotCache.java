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
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
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

import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests SnapshotCache.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class TestSnapshotCache {

  private static final int CACHE_SIZE_LIMIT = 3;
  private static OmSnapshotManager omSnapshotManager;
  private static CacheLoader<String, OmSnapshot> cacheLoader;
  private SnapshotCache snapshotCache;

  @BeforeAll
  static void beforeAll() throws Exception {
    omSnapshotManager = mock(OmSnapshotManager.class);
    when(omSnapshotManager.isSnapshotStatus(any(), eq(SNAPSHOT_ACTIVE)))
        .thenReturn(true);
    cacheLoader = mock(CacheLoader.class);
    // Create a difference mock OmSnapshot instance each time load() is called
    when(cacheLoader.load(any())).thenAnswer(
        (Answer<OmSnapshot>) invocation -> {
          final OmSnapshot omSnapshot = mock(OmSnapshot.class);
          // Mock the snapshotTable return value for the lookup inside release()
          final String dbKey = (String) invocation.getArguments()[0];
          when(omSnapshot.getSnapshotTableKey()).thenReturn(dbKey);

          return omSnapshot;
        }
    );

    // Set SnapshotCache log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(SnapshotCache.LOG, Level.DEBUG);
  }

  @BeforeEach
  void setUp() {
    // Reset cache for each test case
    snapshotCache = new SnapshotCache(
        omSnapshotManager, cacheLoader, CACHE_SIZE_LIMIT);
  }

  @AfterEach
  void tearDown() {
    // Not strictly needed. Added for symmetry
    snapshotCache = null;
  }

  @Test
  @DisplayName("01. get()")
  void testGet() throws IOException {
    final String dbKey1 = "dbKey1";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot =
        snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertNotNull(omSnapshot.get());
    assertInstanceOf(OmSnapshot.class, omSnapshot.get());
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());
  }

  @Test
  @DisplayName("02. get() same entry twice yields one cache entry only")
  void testGetTwice() throws IOException {
    final String dbKey1 = "dbKey1";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot1 =
        snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());

    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot1again =
        snapshotCache.get(dbKey1);
    // Should be the same instance
    assertEquals(omSnapshot1, omSnapshot1again);
    assertEquals(omSnapshot1.get(), omSnapshot1again.get());
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());
  }

  @Test
  @DisplayName("03. release(String)")
  void testReleaseByDbKey() throws IOException {
    final String dbKey1 = "dbKey1";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot1 =
        snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertNotNull(omSnapshot1.get());
    assertEquals(1, snapshotCache.size());
    assertEquals(0, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
    // Entry is queued for eviction as its ref count reaches zero
    assertEquals(1, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());
  }

  @Test
  @DisplayName("04. release(OmSnapshot)")
  void testReleaseByOmSnapshotInstance() throws IOException {
    final String dbKey1 = "dbKey1";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot1 =
        snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());
    assertEquals(0, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    snapshotCache.release((OmSnapshot) omSnapshot1.get());
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
    // Entry is queued for eviction as its ref count reaches zero
    assertEquals(1, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());
  }

  @Test
  @DisplayName("05. invalidate()")
  void testInvalidate() throws IOException {
    final String dbKey1 = "dbKey1";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot =
        snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());

    snapshotCache.invalidate(dbKey1);
    assertEquals(0, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());
  }

  @Test
  @DisplayName("06. invalidateAll()")
  void testInvalidateAll() throws IOException {
    final String dbKey1 = "dbKey1";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot1 =
        snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey2 = "dbKey2";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot2 =
        snapshotCache.get(dbKey2);
    assertNotNull(omSnapshot2);
    assertEquals(2, snapshotCache.size());
    // Should be difference omSnapshot instances
    assertNotEquals(omSnapshot1, omSnapshot2);
    assertTrue(snapshotCache.isConsistent());

    final String dbKey3 = "dbKey3";
    ReferenceCounted<IOmMetadataReader, SnapshotCache> omSnapshot3 =
        snapshotCache.get(dbKey3);
    assertNotNull(omSnapshot3);
    assertEquals(3, snapshotCache.size());
    assertEquals(0, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(3, snapshotCache.size());
    assertEquals(1, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    snapshotCache.invalidate(dbKey1);
    assertEquals(2, snapshotCache.size());
    assertEquals(0, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    snapshotCache.invalidateAll();
    assertEquals(0, snapshotCache.size());
    assertEquals(0, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());
  }

  private void assertEntryExistence(String key, boolean shouldExist) {
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
  @DisplayName("07. Basic cache eviction")
  void testEviction1() throws IOException {

    final String dbKey1 = "dbKey1";
    snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());
    snapshotCache.release(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey2 = "dbKey2";
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());
    snapshotCache.release(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey3 = "dbKey3";
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());
    snapshotCache.release(dbKey3);
    assertEquals(3, snapshotCache.size());
    assertEquals(3, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey4 = "dbKey4";
    snapshotCache.get(dbKey4);
    // dbKey1 would have been evicted by the end of the last get() because
    // it was release()d.
    assertEquals(3, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());
    assertEntryExistence(dbKey1, false);
    assertEquals(2, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());
  }

  @Test
  @DisplayName("08. Cache eviction while exceeding soft limit")
  void testEviction2() throws IOException {

    final String dbKey1 = "dbKey1";
    snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey2 = "dbKey2";
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey3 = "dbKey3";
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey4 = "dbKey4";
    snapshotCache.get(dbKey4);
    // dbKey1 would not have been evicted because it is not release()d
    assertEquals(4, snapshotCache.size());
    assertEntryExistence(dbKey1, true);
    assertEquals(0, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    // Releasing dbKey2 at this point should immediately trigger its eviction
    // because the cache size exceeded the soft limit
    snapshotCache.release(dbKey2);
    assertEquals(3, snapshotCache.size());
    assertEntryExistence(dbKey2, false);
    assertEntryExistence(dbKey1, true);
    assertEquals(0, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());
  }

  @Test
  @DisplayName("09. Cache eviction with try-with-resources")
  void testEviction3WithClose() throws IOException {

    final String dbKey1 = "dbKey1";
    try (ReferenceCounted<IOmMetadataReader, SnapshotCache> rcOmSnapshot =
        snapshotCache.get(dbKey1)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(1, snapshotCache.size());
      assertEquals(0, snapshotCache.getPendingEvictionList().size());
      assertTrue(snapshotCache.isConsistent());
    }
    // ref count should have been decreased because it would be close()d
    // upon exiting try-with-resources.
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey1).getTotalRefCount());
    assertEquals(1, snapshotCache.size());
    assertEquals(1, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey2 = "dbKey2";
    try (ReferenceCounted<IOmMetadataReader, SnapshotCache> rcOmSnapshot =
        snapshotCache.get(dbKey2)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(2, snapshotCache.size());
      assertEquals(1, snapshotCache.getPendingEvictionList().size());
      assertTrue(snapshotCache.isConsistent());
      // Get dbKey2 entry a second time
      try (ReferenceCounted<IOmMetadataReader, SnapshotCache> rcOmSnapshot2 =
          snapshotCache.get(dbKey2)) {
        assertEquals(2L, rcOmSnapshot.getTotalRefCount());
        assertEquals(2L, rcOmSnapshot2.getTotalRefCount());
        assertEquals(2, snapshotCache.size());
        assertEquals(1, snapshotCache.getPendingEvictionList().size());
        assertTrue(snapshotCache.isConsistent());
      }
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertTrue(snapshotCache.isConsistent());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey2).getTotalRefCount());
    assertEquals(2, snapshotCache.size());
    assertEquals(2, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey3 = "dbKey3";
    try (ReferenceCounted<IOmMetadataReader, SnapshotCache> rcOmSnapshot =
        snapshotCache.get(dbKey3)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(3, snapshotCache.size());
      assertEquals(2, snapshotCache.getPendingEvictionList().size());
      assertTrue(snapshotCache.isConsistent());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey3).getTotalRefCount());
    assertEquals(3, snapshotCache.size());
    assertEquals(3, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());

    final String dbKey4 = "dbKey4";
    try (ReferenceCounted<IOmMetadataReader, SnapshotCache> rcOmSnapshot =
        snapshotCache.get(dbKey4)) {
      assertEquals(1L, rcOmSnapshot.getTotalRefCount());
      assertEquals(3, snapshotCache.size());
      // An entry has been evicted at this point
      assertEquals(2, snapshotCache.getPendingEvictionList().size());
      assertTrue(snapshotCache.isConsistent());
    }
    assertEquals(0L, snapshotCache.getDbMap().get(dbKey4).getTotalRefCount());
    // Reached cache size limit
    assertEquals(3, snapshotCache.size());
    assertEquals(3, snapshotCache.getPendingEvictionList().size());
    assertTrue(snapshotCache.isConsistent());
  }

}

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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    OmSnapshot omSnapshot = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertEquals(1, snapshotCache.size());
  }

  @Test
  @DisplayName("02. get() same entry twice yields one cache entry only")
  void testGetTwice() throws IOException {
    final String dbKey1 = "dbKey1";
    OmSnapshot omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());

    OmSnapshot omSnapshot1again = snapshotCache.get(dbKey1);
    // Should be the same instance
    assertEquals(omSnapshot1, omSnapshot1again);
    assertEquals(1, snapshotCache.size());
  }

  @Test
  @DisplayName("03. release(String)")
  void testReleaseByDbKey() throws IOException {
    final String dbKey1 = "dbKey1";
    OmSnapshot omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());
    assertEquals(0, snapshotCache.getInstancesEligibleForClosure().size());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
    // Entry is queued for eviction as its ref count reaches zero
    assertEquals(1, snapshotCache.getInstancesEligibleForClosure().size());
  }

  @Test
  @DisplayName("04. release(OmSnapshot)")
  void testReleaseByOmSnapshotInstance() throws IOException {
    final String dbKey1 = "dbKey1";
    OmSnapshot omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());
    assertEquals(0, snapshotCache.getInstancesEligibleForClosure().size());

    snapshotCache.release(omSnapshot1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());
    // Entry is queued for eviction as its ref count reaches zero
    assertEquals(1, snapshotCache.getInstancesEligibleForClosure().size());
  }

  @Test
  @DisplayName("05. invalidate()")
  void testInvalidate() throws IOException {
    final String dbKey1 = "dbKey1";
    OmSnapshot omSnapshot = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot);
    assertEquals(1, snapshotCache.size());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(1, snapshotCache.size());

    snapshotCache.invalidate(dbKey1);
    assertEquals(0, snapshotCache.size());
  }

  @Test
  @DisplayName("06. invalidateAll()")
  void testInvalidateAll() throws IOException {
    final String dbKey1 = "dbKey1";
    OmSnapshot omSnapshot1 = snapshotCache.get(dbKey1);
    assertNotNull(omSnapshot1);
    assertEquals(1, snapshotCache.size());

    final String dbKey2 = "dbKey2";
    OmSnapshot omSnapshot2 = snapshotCache.get(dbKey2);
    assertNotNull(omSnapshot2);
    assertEquals(2, snapshotCache.size());
    // Should be difference omSnapshot instances
    assertNotEquals(omSnapshot1, omSnapshot2);

    final String dbKey3 = "dbKey3";
    OmSnapshot omSnapshot3 = snapshotCache.get(dbKey3);
    assertNotNull(omSnapshot3);
    assertEquals(3, snapshotCache.size());
    assertEquals(0, snapshotCache.getInstancesEligibleForClosure().size());

    snapshotCache.release(dbKey1);
    // Entry will not be immediately evicted
    assertEquals(3, snapshotCache.size());
    assertEquals(1, snapshotCache.getInstancesEligibleForClosure().size());

    snapshotCache.invalidate(dbKey1);
    assertEquals(2, snapshotCache.size());
    assertEquals(0, snapshotCache.getInstancesEligibleForClosure().size());

    snapshotCache.invalidateAll();
    assertEquals(0, snapshotCache.size());
    assertEquals(0, snapshotCache.getInstancesEligibleForClosure().size());
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
    snapshotCache.release(dbKey1);
    assertEquals(1, snapshotCache.size());
    assertEquals(1, snapshotCache.getInstancesEligibleForClosure().size());

    final String dbKey2 = "dbKey2";
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());
    snapshotCache.release(dbKey2);
    assertEquals(2, snapshotCache.size());
    assertEquals(2, snapshotCache.getInstancesEligibleForClosure().size());

    final String dbKey3 = "dbKey3";
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());
    snapshotCache.release(dbKey3);
    assertEquals(3, snapshotCache.size());
    assertEquals(3, snapshotCache.getInstancesEligibleForClosure().size());

    final String dbKey4 = "dbKey4";
    snapshotCache.get(dbKey4);
    // dbKey1 would have been evicted by the end of the last get() because
    // it was release()d.
    assertEquals(3, snapshotCache.size());
    assertEntryExistence(dbKey1, false);
    assertEquals(2, snapshotCache.getInstancesEligibleForClosure().size());
  }

  @Test
  @DisplayName("08. Cache eviction while exceeding soft limit")
  void testEviction2() throws IOException {

    final String dbKey1 = "dbKey1";
    snapshotCache.get(dbKey1);
    assertEquals(1, snapshotCache.size());

    final String dbKey2 = "dbKey2";
    snapshotCache.get(dbKey2);
    assertEquals(2, snapshotCache.size());

    final String dbKey3 = "dbKey3";
    snapshotCache.get(dbKey3);
    assertEquals(3, snapshotCache.size());

    final String dbKey4 = "dbKey4";
    snapshotCache.get(dbKey4);
    // dbKey1 would not have been evicted because it is not release()d
    assertEquals(4, snapshotCache.size());
    assertEntryExistence(dbKey1, true);
    assertEquals(0, snapshotCache.getInstancesEligibleForClosure().size());

    // Releasing dbKey2 at this point should immediately trigger its eviction
    // because the cache size exceeded the soft limit
    snapshotCache.release(dbKey2);
    assertEquals(3, snapshotCache.size());
    assertEntryExistence(dbKey2, false);
    assertEntryExistence(dbKey1, true);
    assertEquals(0, snapshotCache.getInstancesEligibleForClosure().size());
  }

  // TODO: [SNAPSHOT]
  //  Ensure SnapshotCache behaves as expected when arbitrary combinations of
  //  get()s and release()s are called.

}

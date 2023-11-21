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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.event.Level;


/**
 * Class tests partial table cache.
 */
public class TestTableCache {

  private TableCache<String, String> tableCache;

  @BeforeAll
  public static void setLogLevel() {
    GenericTestUtils.setLogLevel(FullTableCache.LOG, Level.DEBUG);
  }

  private void createTableCache(TableCache.CacheType cacheType) {
    if (cacheType == TableCache.CacheType.FULL_CACHE) {
      tableCache = new FullTableCache<>("");
    } else {
      tableCache = new PartialTableCache<>("");
    }
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testPartialTableCache(TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    for (int i = 0; i < 10; i++) {
      tableCache.put(new CacheKey<>(Integer.toString(i)),
          CacheValue.get(i, Integer.toString(i)));
    }


    for (int i = 0; i < 10; i++) {
      Assertions.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }

    ArrayList<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);
    epochs.add(4L);
    // On a full table cache if someone calls cleanup it is a no-op.
    tableCache.evictCache(epochs);

    for (int i = 5; i < 10; i++) {
      Assertions.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }

    verifyStats(tableCache, 15, 0, 0);
  }

  private void verifyStats(TableCache<?, ?> cache,
                           long expectedHits,
                           long expectedMisses,
                           long expectedIterations) {
    CacheStats stats = cache.getStats();
    Assertions.assertEquals(expectedHits, stats.getCacheHits());
    Assertions.assertEquals(expectedMisses, stats.getCacheMisses());
    Assertions.assertEquals(expectedIterations, stats.getIterationTimes());
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testTableCacheWithRenameKey(TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    // putting cache with same epoch and different keyNames
    for (int i = 0; i < 3; i++) {
      tableCache.put(new CacheKey<>(Integer.toString(i).concat("A")),
              CacheValue.get(i));
      tableCache.put(new CacheKey<>(Integer.toString(i).concat("B")),
              CacheValue.get(i, Integer.toString(i)));
    }

    // Epoch entries should be like (long, (key1, key2, ...))
    // (0, (0A, 0B))  (1, (1A, 1B))  (2, (2A, 1B))
    Assertions.assertEquals(3, tableCache.getEpochEntries().size());
    Assertions.assertEquals(2, tableCache.getEpochEntries().get(0L).size());
    
    // Cache should be like (key, (cacheValue, long))
    // (0A, (null, 0))   (0B, (0, 0))
    // (1A, (null, 1))   (1B, (0, 1))
    // (2A, (null, 2))   (2B, (0, 2))
    for (int i = 0; i < 3; i++) {
      Assertions.assertNull(tableCache.get(new CacheKey<>(
          Integer.toString(i).concat("A"))).getCacheValue());
      Assertions.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i).concat("B")))
              .getCacheValue());
    }

    ArrayList<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);
    epochs.add(4L);

    tableCache.evictCache(epochs);

    Assertions.assertEquals(0, tableCache.getEpochEntries().size());

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      Assertions.assertEquals(0, tableCache.size());
    } else {
      Assertions.assertEquals(3, tableCache.size());
    }

    verifyStats(tableCache, 6, 0, 0);
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testPartialTableCacheWithNotContinuousEntries(
      TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    int totalCount = 0;
    int insertedCount = 3000;

    int cleanupCount = 0;

    ArrayList<Long> epochs = new ArrayList<>();
    for (long i = 0; i < insertedCount; i += 2) {
      if (cleanupCount++ < 1000) {
        epochs.add(i);
      }
      tableCache.put(new CacheKey<>(Long.toString(i)),
          CacheValue.get(i, Long.toString(i)));
      totalCount++;
    }

    Assertions.assertEquals(totalCount, tableCache.size());

    tableCache.evictCache(epochs);

    final int count = totalCount;

    // If cleanup policy is manual entries should have been removed.
    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      Assertions.assertEquals(count - epochs.size(), tableCache.size());

      // Check remaining entries exist or not and deleted entries does not
      // exist.
      for (long i = 0; i < insertedCount; i += 2) {
        if (!epochs.contains(i)) {
          Assertions.assertEquals(Long.toString(i),
              tableCache.get(new CacheKey<>(Long.toString(i))).getCacheValue());
        } else {
          Assertions.assertNull(
              tableCache.get(new CacheKey<>(Long.toString(i))));
        }
      }
    } else {
      for (long i = 0; i < insertedCount; i += 2) {
        Assertions.assertEquals(Long.toString(i),
            tableCache.get(new CacheKey<>(Long.toString(i))).getCacheValue());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testPartialTableCacheWithOverrideEntries(
      TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    tableCache.put(new CacheKey<>(Long.toString(0)),
          CacheValue.get(0, Long.toString(0)));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        CacheValue.get(1, Long.toString(1)));
    tableCache.put(new CacheKey<>(Long.toString(2)),
        CacheValue.get(2, Long.toString(2)));


    //Override first 2 entries
    // This is to simulate a case like create mpu key, commit part1, commit
    // part2. They override the same key.
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(3, Long.toString(0)));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        CacheValue.get(4, Long.toString(1)));




    Assertions.assertEquals(3, tableCache.size());
    // It will have 2 additional entries because we have 2 override entries.
    Assertions.assertEquals(3 + 2,
        tableCache.getEpochEntries().size());

    // Now remove

    List<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);
    epochs.add(4L);

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {

      tableCache.evictCache(epochs);

      Assertions.assertEquals(0, tableCache.size());

      Assertions.assertEquals(0, tableCache.getEpochEntries().size());
    }

    // Add a new entry.
    tableCache.put(new CacheKey<>(Long.toString(5)),
        CacheValue.get(5, Long.toString(5)));

    epochs = new ArrayList<>();
    epochs.add(5L);
    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      tableCache.evictCache(epochs);

      Assertions.assertEquals(0, tableCache.size());

      // Overridden entries would have been deleted.
      Assertions.assertEquals(0, tableCache.getEpochEntries().size());
    }

    verifyStats(tableCache, 0, 0, 0);
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testPartialTableCacheWithOverrideAndDelete(
      TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(0, Long.toString(0)));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        CacheValue.get(1, Long.toString(1)));
    tableCache.put(new CacheKey<>(Long.toString(2)),
        CacheValue.get(2, Long.toString(2)));


    // Override entries
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(3, Long.toString(0)));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        CacheValue.get(4, Long.toString(1)));

    // Finally, mark them for deleted
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(5));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        CacheValue.get(6));

    // So now our cache epoch entries looks like
    // 0-0, 1-1, 2-2, 0-3, 1-4, 0-5, 1-6
    // Cache looks like
    // 0-5, 1-6, 2-2


    Assertions.assertEquals(3, tableCache.size());
    // It will have 4 additional entries because we have 4 override entries.
    Assertions.assertEquals(3 + 4,
        tableCache.getEpochEntries().size());

    // Now remove

    List<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);
    epochs.add(4L);
    epochs.add(5L);
    epochs.add(6L);


    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      tableCache.evictCache(epochs);

      Assertions.assertEquals(0, tableCache.size());

      Assertions.assertEquals(0, tableCache.getEpochEntries().size());
    } else {
      tableCache.evictCache(epochs);

      Assertions.assertEquals(1, tableCache.size());

      // Epoch entries which are overridden also will be cleaned up.
      Assertions.assertEquals(0, tableCache.getEpochEntries().size());
    }

    // Add a new entry, now old override entries will be cleaned up.
    tableCache.put(new CacheKey<>(Long.toString(3)),
        CacheValue.get(7, Long.toString(3)));

    epochs = new ArrayList<>();
    epochs.add(7L);

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      tableCache.evictCache(epochs);

      Assertions.assertEquals(0, tableCache.size());

      // Epoch entries which are overridden now would have been deleted.
      Assertions.assertEquals(0, tableCache.getEpochEntries().size());
    } else {
      tableCache.evictCache(epochs);

      // 2 entries will be in cache, as 2 are not deleted.
      Assertions.assertEquals(2, tableCache.size());

      // Epoch entries which are not marked for delete will also be cleaned up.
      // As they are override entries in full cache.
      Assertions.assertEquals(0, tableCache.getEpochEntries().size());
    }

    verifyStats(tableCache, 0, 0, 0);
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testPartialTableCacheParallel(
      TableCache.CacheType cacheType) throws Exception {

    createTableCache(cacheType);

    int totalCount = 0;
    CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(() -> {
          try {
            return writeToCache(10, 1, 0);
          } catch (InterruptedException ex) {
            Assertions.fail("writeToCache got interrupt exception");
          }
          return 0;
        });
    int value = future.get();
    Assertions.assertEquals(10, value);

    totalCount += value;

    future =
        CompletableFuture.supplyAsync(() -> {
          try {
            return writeToCache(10, 11, 100);
          } catch (InterruptedException ex) {
            Assertions.fail("writeToCache got interrupt exception");
          }
          return 0;
        });

    // Check we have first 10 entries in cache.
    for (int i = 1; i <= 10; i++) {
      Assertions.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }


    value = future.get();
    Assertions.assertEquals(10, value);

    totalCount += value;

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      int deleted = 5;

      // cleanup first 5 entries

      ArrayList<Long> epochs = new ArrayList<>();
      epochs.add(1L);
      epochs.add(2L);
      epochs.add(3L);
      epochs.add(4L);
      epochs.add(5L);
      tableCache.evictCache(epochs);

      // We should totalCount - deleted entries in cache.
      Assertions.assertEquals(totalCount - deleted, tableCache.size());
      // Check if we have remaining entries.
      for (int i = 6; i <= totalCount; i++) {
        Assertions.assertEquals(Integer.toString(i), tableCache.get(
            new CacheKey<>(Integer.toString(i))).getCacheValue());
      }

      epochs = new ArrayList<>();
      for (long i = 6; i <= totalCount; i++) {
        epochs.add(i);
      }

      tableCache.evictCache(epochs);

      // Cleaned up all entries, so cache size should be zero.
      Assertions.assertEquals(0, tableCache.size());
    } else {
      ArrayList<Long> epochs = new ArrayList<>();
      for (long i = 0; i <= totalCount; i++) {
        epochs.add(i);
      }
      tableCache.evictCache(epochs);
      Assertions.assertEquals(totalCount, tableCache.size());
    }
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testTableCache(TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    // In non-HA epoch entries might be out of order.
    // Scenario is like create vol, set vol, set vol, delete vol
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(0, Long.toString(0)));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(1, Long.toString(1)));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(3, Long.toString(2)));

    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(2));

    List<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);

    tableCache.evictCache(epochs);

    Assertions.assertEquals(0, tableCache.size());
    Assertions.assertEquals(0, tableCache.getEpochEntries().size());

    verifyStats(tableCache, 0, 0, 0);
  }


  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testTableCacheWithNonConsecutiveEpochList(
      TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    // In non-HA epoch entries might be out of order.
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(0, Long.toString(0)));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(1, Long.toString(1)));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        CacheValue.get(3, Long.toString(3)));

    tableCache.put(new CacheKey<>(Long.toString(0)),
          CacheValue.get(2, Long.toString(2)));

    tableCache.put(new CacheKey<>(Long.toString(1)),
        CacheValue.get(4, Long.toString(1)));

    List<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(3L);

    tableCache.evictCache(epochs);

    Assertions.assertEquals(2, tableCache.size());
    Assertions.assertEquals(2, tableCache.getEpochEntries().size());

    Assertions.assertNotNull(tableCache.get(new CacheKey<>(Long.toString(0))));
    Assertions.assertEquals(2,
        tableCache.get(new CacheKey<>(Long.toString(0))).getEpoch());

    Assertions.assertNotNull(tableCache.get(new CacheKey<>(Long.toString(1))));
    Assertions.assertEquals(4,
        tableCache.get(new CacheKey<>(Long.toString(1))).getEpoch());

    // now evict 2,4
    epochs = new ArrayList<>();
    epochs.add(2L);
    epochs.add(4L);

    tableCache.evictCache(epochs);

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      Assertions.assertEquals(0, tableCache.size());
      Assertions.assertEquals(0, tableCache.getEpochEntries().size());
    } else {
      Assertions.assertEquals(2, tableCache.size());
      Assertions.assertEquals(0, tableCache.getEpochEntries().size());

      // Entries should exist, as the entries are not delete entries
      Assertions.assertNotNull(
          tableCache.get(new CacheKey<>(Long.toString(0))));
      Assertions.assertEquals(2,
          tableCache.get(new CacheKey<>(Long.toString(0))).getEpoch());

      Assertions.assertNotNull(
          tableCache.get(new CacheKey<>(Long.toString(1))));
      Assertions.assertEquals(4,
          tableCache.get(new CacheKey<>(Long.toString(1))).getEpoch());
    }
  }

  @ParameterizedTest
  @EnumSource(TableCache.CacheType.class)
  public void testTableCacheStats(TableCache.CacheType cacheType) {

    createTableCache(cacheType);

    tableCache.put(new CacheKey<>("0"),
        CacheValue.get(0, "0"));
    tableCache.put(new CacheKey<>("1"),
        CacheValue.get(1, "1"));

    Assertions.assertNotNull(tableCache.get(new CacheKey<>("0")));
    Assertions.assertNotNull(tableCache.get(new CacheKey<>("0")));
    Assertions.assertNotNull(tableCache.get(new CacheKey<>("1")));
    Assertions.assertNull(tableCache.get(new CacheKey<>("2")));
    Assertions.assertNull(tableCache.get(new CacheKey<>("3")));

    tableCache.iterator();
    tableCache.iterator();

    verifyStats(tableCache, 3, 2, 2);
  }

  private int writeToCache(int count, int startVal, long sleep)
      throws InterruptedException {
    int counter = 1;
    while (counter <= count) {
      tableCache.put(new CacheKey<>(Integer.toString(startVal)),
          CacheValue.get(startVal, Integer.toString(startVal)));
      startVal++;
      counter++;
      Thread.sleep(sleep);
    }
    return count;
  }
}

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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Optional;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

import static org.junit.Assert.fail;

/**
 * Class tests partial table cache.
 */
@RunWith(value = Parameterized.class)
public class TestTableCache {
  private TableCache<CacheKey<String>, CacheValue<String>> tableCache;

  private final TableCache.CacheType cacheType;


  @Parameterized.Parameters
  public static Collection<Object[]> policy() {
    Object[][] params = new Object[][] {
        {TableCache.CacheType.FULL_CACHE},
        {TableCache.CacheType.PARTIAL_CACHE}
    };
    return Arrays.asList(params);
  }

  public TestTableCache(
      TableCache.CacheType cacheType) {
    GenericTestUtils.setLogLevel(FullTableCache.LOG, Level.DEBUG);
    this.cacheType = cacheType;
  }


  @Before
  public void create() {
    if (cacheType == TableCache.CacheType.FULL_CACHE) {
      tableCache = new FullTableCache<>();
    } else {
      tableCache = new PartialTableCache<>();
    }
  }
  @Test
  public void testPartialTableCache() {


    for (int i = 0; i< 10; i++) {
      tableCache.put(new CacheKey<>(Integer.toString(i)),
          new CacheValue<>(Optional.of(Integer.toString(i)), i));
    }


    for (int i=0; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }

    ArrayList<Long> epochs = new ArrayList();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);
    epochs.add(4L);
    // On a full table cache if some one calls cleanup it is a no-op.
    tableCache.evictCache(epochs);

    for (int i=5; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }
  }


  @Test
  public void testPartialTableCacheWithNotContinousEntries() throws Exception {
    int totalCount = 0;
    int insertedCount = 3000;

    int cleanupCount = 0;

    ArrayList<Long> epochs = new ArrayList();
    for (long i=0; i<insertedCount; i+=2) {
      if (cleanupCount++ < 1000) {
        epochs.add(i);
      }
      tableCache.put(new CacheKey<>(Long.toString(i)),
          new CacheValue<>(Optional.of(Long.toString(i)), i));
      totalCount++;
    }

    Assert.assertEquals(totalCount, tableCache.size());

    tableCache.evictCache(epochs);

    final int count = totalCount;

    // If cleanup policy is manual entries should have been removed.
    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      Assert.assertEquals(count - epochs.size(), tableCache.size());

      // Check remaining entries exist or not and deleted entries does not
      // exist.
      for (long i = 0; i < insertedCount; i += 2) {
        if (!epochs.contains(i)) {
          Assert.assertEquals(Long.toString(i),
              tableCache.get(new CacheKey<>(Long.toString(i))).getCacheValue());
        } else {
          Assert.assertEquals(null,
              tableCache.get(new CacheKey<>(Long.toString(i))));
        }
      }
    } else {
      for (long i = 0; i < insertedCount; i += 2) {
        Assert.assertEquals(Long.toString(i),
            tableCache.get(new CacheKey<>(Long.toString(i))).getCacheValue());
      }
    }

  }

  @Test
  public void testPartialTableCacheWithOverrideEntries() throws Exception {

    tableCache.put(new CacheKey<>(Long.toString(0)),
          new CacheValue<>(Optional.of(Long.toString(0)), 0));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        new CacheValue<>(Optional.of(Long.toString(1)), 1));
    tableCache.put(new CacheKey<>(Long.toString(2)),
        new CacheValue<>(Optional.of(Long.toString(2)), 2));


    //Override first 2 entries
    // This is to simulate a case like create mpu key, commit part1, commit
    // part2. They override the same key.
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(0)), 3));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        new CacheValue<>(Optional.of(Long.toString(1)), 4));




    Assert.assertEquals(3, tableCache.size());
    // It will have 2 additional entries because we have 2 override entries.
    Assert.assertEquals(3 + 2,
        tableCache.getEpochEntrySet().size());

    // Now remove

    List<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);
    epochs.add(4L);

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {

      tableCache.evictCache(epochs);

      Assert.assertEquals(0, tableCache.size());

      Assert.assertEquals(0, tableCache.getEpochEntrySet().size());
    }

    // Add a new entry.
    tableCache.put(new CacheKey<>(Long.toString(5)),
        new CacheValue<>(Optional.of(Long.toString(5)), 5));

    epochs = new ArrayList<>();
    epochs.add(5L);
    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      tableCache.evictCache(epochs);

      Assert.assertEquals(0, tableCache.size());

      // Overrided entries would have been deleted.
      Assert.assertEquals(0, tableCache.getEpochEntrySet().size());
    }


  }

  @Test
  public void testPartialTableCacheWithOverrideAndDelete() throws Exception {

    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(0)), 0));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        new CacheValue<>(Optional.of(Long.toString(1)), 1));
    tableCache.put(new CacheKey<>(Long.toString(2)),
        new CacheValue<>(Optional.of(Long.toString(2)), 2));


    //Override entries
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(0)), 3));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        new CacheValue<>(Optional.of(Long.toString(1)), 4));

    // Finally mark them for deleted
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.absent(), 5));
    tableCache.put(new CacheKey<>(Long.toString(1)),
        new CacheValue<>(Optional.absent(), 6));

    // So now our cache epoch entries looks like
    // 0-0, 1-1, 2-2, 0-3, 1-4, 0-5, 1-6
    // Cache looks like
    // 0-5, 1-6, 2-2


    Assert.assertEquals(3, tableCache.size());
    // It will have 4 additional entries because we have 4 override entries.
    Assert.assertEquals(3 + 4,
        tableCache.getEpochEntrySet().size());

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

      Assert.assertEquals(0, tableCache.size());

      Assert.assertEquals(0, tableCache.getEpochEntrySet().size());
    } else {
      tableCache.evictCache(epochs);

      Assert.assertEquals(1, tableCache.size());

      // Epoch entries which are overrided also will be cleaned up.
      Assert.assertEquals(0, tableCache.getEpochEntrySet().size());
    }

    // Add a new entry, now old override entries will be cleaned up.
    tableCache.put(new CacheKey<>(Long.toString(3)),
        new CacheValue<>(Optional.of(Long.toString(3)), 7));

    epochs = new ArrayList<>();
    epochs.add(7L);

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      tableCache.evictCache(epochs);

      Assert.assertEquals(0, tableCache.size());

      // Epoch entries which are overrided now would have been deleted.
      Assert.assertEquals(0, tableCache.getEpochEntrySet().size());
    } else {
      tableCache.evictCache(epochs);

      // 2 entries will be in cache, as 2 are not deleted.
      Assert.assertEquals(2, tableCache.size());

      // Epoch entries which are not marked for delete will also be cleaned up.
      // As they are override entries in full cache.
      Assert.assertEquals(0, tableCache.getEpochEntrySet().size());
    }


  }

  @Test
  public void testPartialTableCacheParallel() throws Exception {

    int totalCount = 0;
    CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(() -> {
          try {
            return writeToCache(10, 1, 0);
          } catch (InterruptedException ex) {
            fail("writeToCache got interrupt exception");
          }
          return 0;
        });
    int value = future.get();
    Assert.assertEquals(10, value);

    totalCount += value;

    future =
        CompletableFuture.supplyAsync(() -> {
          try {
            return writeToCache(10, 11, 100);
          } catch (InterruptedException ex) {
            fail("writeToCache got interrupt exception");
          }
          return 0;
        });

    // Check we have first 10 entries in cache.
    for (int i=1; i <= 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }


    value = future.get();
    Assert.assertEquals(10, value);

    totalCount += value;

    if (cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      int deleted = 5;

      // cleanup first 5 entires

      ArrayList<Long> epochs = new ArrayList<>();
      epochs.add(1L);
      epochs.add(2L);
      epochs.add(3L);
      epochs.add(4L);
      epochs.add(5L);
      tableCache.evictCache(epochs);

      // We should totalCount - deleted entries in cache.
      final int tc = totalCount;
      Assert.assertEquals(tc - deleted, tableCache.size());
      // Check if we have remaining entries.
      for (int i=6; i <= totalCount; i++) {
        Assert.assertEquals(Integer.toString(i), tableCache.get(
            new CacheKey<>(Integer.toString(i))).getCacheValue());
      }

      epochs = new ArrayList<>();
      for (long i=6; i<= totalCount; i++) {
        epochs.add(i);
      }

      tableCache.evictCache(epochs);

      // Cleaned up all entries, so cache size should be zero.
      Assert.assertEquals(0, tableCache.size());
    } else {
      ArrayList<Long> epochs = new ArrayList<>();
      for (long i=0; i<= totalCount; i++) {
        epochs.add(i);
      }
      tableCache.evictCache(epochs);
      Assert.assertEquals(totalCount, tableCache.size());
    }


  }

  @Test
  public void testTableCache() {

    // In non-HA epoch entries might be out of order.
    // Scenario is like create vol, set vol, set vol, delete vol
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(0)), 0));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(1)), 1));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(2)), 3));

    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.absent(), 2));

    List<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(2L);
    epochs.add(3L);

    tableCache.evictCache(epochs);

    Assert.assertTrue(tableCache.size() == 0);
    Assert.assertTrue(tableCache.getEpochEntrySet().size() == 0);
  }


  @Test
  public void testTableCacheWithNonConsecutiveEpochList() {

    // In non-HA epoch entries might be out of order.
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(0)), 0));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(1)), 1));
    tableCache.put(new CacheKey<>(Long.toString(0)),
        new CacheValue<>(Optional.of(Long.toString(3)), 3));

    tableCache.put(new CacheKey<>(Long.toString(0)),
          new CacheValue<>(Optional.of(Long.toString(2)), 2));

    tableCache.put(new CacheKey<>(Long.toString(1)),
        new CacheValue<>(Optional.of(Long.toString(1)), 4));

    List<Long> epochs = new ArrayList<>();
    epochs.add(0L);
    epochs.add(1L);
    epochs.add(3L);

    tableCache.evictCache(epochs);

    Assert.assertTrue(tableCache.size() == 2);
    Assert.assertTrue(tableCache.getEpochEntrySet().size() == 2);

    Assert.assertNotNull(tableCache.get(new CacheKey<>(Long.toString(0))));
    Assert.assertEquals(2,
        tableCache.get(new CacheKey<>(Long.toString(0))).getEpoch());

    Assert.assertNotNull(tableCache.get(new CacheKey<>(Long.toString(1))));
    Assert.assertEquals(4,
        tableCache.get(new CacheKey<>(Long.toString(1))).getEpoch());

    // now evict 2,4
    epochs = new ArrayList<>();
    epochs.add(2L);
    epochs.add(4L);

    tableCache.evictCache(epochs);

    if(cacheType == TableCache.CacheType.PARTIAL_CACHE) {
      Assert.assertTrue(tableCache.size() == 0);
      Assert.assertTrue(tableCache.getEpochEntrySet().size() == 0);
    } else {
      Assert.assertTrue(tableCache.size() == 2);
      Assert.assertTrue(tableCache.getEpochEntrySet().size() == 0);

      // Entries should exist, as the entries are not delete entries
      Assert.assertNotNull(tableCache.get(new CacheKey<>(Long.toString(0))));
      Assert.assertEquals(2,
          tableCache.get(new CacheKey<>(Long.toString(0))).getEpoch());

      Assert.assertNotNull(tableCache.get(new CacheKey<>(Long.toString(1))));
      Assert.assertEquals(4,
          tableCache.get(new CacheKey<>(Long.toString(1))).getEpoch());
    }

  }

  private int writeToCache(int count, int startVal, long sleep)
      throws InterruptedException {
    int counter = 1;
    while (counter <= count){
      tableCache.put(new CacheKey<>(Integer.toString(startVal)),
          new CacheValue<>(Optional.of(Integer.toString(startVal)), startVal));
      startVal++;
      counter++;
      Thread.sleep(sleep);
    }
    return count;
  }
}

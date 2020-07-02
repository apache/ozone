/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test for ResourceLimitCache.
 */
public class TestResourceLimitCache {

  @Test
  public void testResourceLimitCache()
      throws InterruptedException, TimeoutException {
    Cache<Integer, String> resourceCache =
        new ResourceLimitCache<>(new ConcurrentHashMap<>(),
            (k, v) -> new int[] {k}, 10);
    resourceCache.put(6, "a");
    resourceCache.put(4, "a");

    // put should pass as key 4 will be overwritten
    resourceCache.put(4, "a");

    // Create a future which blocks to put 1. Currently map has acquired 10
    // permits out of 10
    CompletableFuture future = CompletableFuture.supplyAsync(() -> {
      try {
        return resourceCache.put(1, "a");
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    Assert.assertTrue(!future.isDone());
    Thread.sleep(100);
    Assert.assertTrue(!future.isDone());

    // remove 4 so that permits are released for key 1 to be put. Currently map
    // has acquired 6 permits out of 10
    resourceCache.remove(4);

    GenericTestUtils.waitFor(future::isDone, 100, 1000);
    // map has the key 1
    Assert.assertTrue(future.isDone() && !future.isCompletedExceptionally());
    Assert.assertNotNull(resourceCache.get(1));

    // Create a future which blocks to put 4. Currently map has acquired 7
    // permits out of 10
    ExecutorService pool = Executors.newCachedThreadPool();
    future = CompletableFuture.supplyAsync(() -> {
      try {
        return resourceCache.put(4, "a");
      } catch (InterruptedException e) {
        return null;
      }
    }, pool);
    Assert.assertTrue(!future.isDone());
    Thread.sleep(100);
    Assert.assertTrue(!future.isDone());

    // Shutdown the thread pool for putting key 4
    pool.shutdownNow();
    // Mark the future as cancelled
    future.cancel(true);
    // remove key 1 so currently map has acquired 6 permits out of 10
    resourceCache.remove(1);
    Assert.assertNull(resourceCache.get(4));
  }
}

package org.apache.hadoop.hdds.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test for ResourceLimitCache
 */
public class TestResourceLimitCache {

  @Test
  public void testResourceLimitCache() throws InterruptedException {
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
    Thread.sleep(100);
    Assert.assertTrue(!future.isDone());

    // remove 4 so that permits are released for key 1 to be put. Currently map
    // has acquired 6 permits out of 10
    resourceCache.remove(4);

    Thread.sleep(100);
    // map has the ket 1
    Assert.assertTrue(future.isDone() && !future.isCompletedExceptionally());
    Assert.assertNotNull(resourceCache.get(1));

    // Create a future which blocks to put 4. Currently map has acquired 7
    // permits out of 10
    future = CompletableFuture.supplyAsync(() -> {
      try {
        return resourceCache.put(4, "a");
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    Thread.sleep(100);
    Assert.assertTrue(!future.isDone());

    // Cancel the future for putting key 4
    future.cancel(true);
    // remove key 1 so currently map has acquired 6 permits out of 10
    resourceCache.remove(1);
    Assert.assertNull(resourceCache.get(4));
  }
}

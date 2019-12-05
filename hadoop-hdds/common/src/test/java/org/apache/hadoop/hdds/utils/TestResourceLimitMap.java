package org.apache.hadoop.hdds.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TestResourceLimitMap {

  @Test
  public void testResourceLimitMap() throws InterruptedException {
    Map<Integer, String> resourceMap =
        new ResourceLimitMap<>(new ConcurrentHashMap<>(),
            (k, v) -> new int[] { k }, 10);
    resourceMap.put(6, "a");
    resourceMap.put(4, "a");

    // put should pass as key 4 will be overwritten
    resourceMap.put(4, "a");

    // Create a future which blocks to put 1. Currently map has acquired 10
    // permits out of 10
    CompletableFuture future = CompletableFuture.supplyAsync(() -> {
      try {
        return resourceMap.put(1, "a");
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    Thread.sleep(100);
    Assert.assertTrue(!future.isDone());

    // remove 4 so that permits are released for key 1 to be put. Currently map
    // has acquired 6 permits out of 10
    resourceMap.remove(4);

    Thread.sleep(100);
    // map has the ket 1
    Assert.assertTrue(future.isDone() && !future.isCompletedExceptionally());
    Assert.assertNotNull(resourceMap.get(1));

    // Create a future which blocks to put 4. Currently map has acquired 7
    // permits out of 10
    future = CompletableFuture.supplyAsync(() -> {
      try {
        return resourceMap.put(4, "a");
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
    resourceMap.remove(1);
    Assert.assertNull(resourceMap.get(4));
  }
}

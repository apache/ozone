package org.apache.hadoop.ozone.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test class for {@link UsageBasedCache}.
 */
public class UsageBasedCacheTest {
  // A dummy resource to simulate cached objects
  static class DummyResource {
    private final int id;

    DummyResource(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    @Override
    public String toString() {
      return "DummyResource-" + id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DummyResource that = (DummyResource) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  @Test
  public void testCacheCreatesUpToNInstances() {
    AtomicInteger counter = new AtomicInteger(0);
    UsageBasedCache<DummyResource> cache = new UsageBasedCache<>(3, () -> new DummyResource(counter.getAndIncrement()));

    // First 3 calls should create new instances
    DummyResource r1 = cache.get();
    DummyResource r2 = cache.get();
    DummyResource r3 = cache.get();

    assertEquals(0, r1.getId());
    assertEquals(1, r2.getId());
    assertEquals(2, r3.getId());

    // Next call should reuse one of them
    DummyResource r4 = cache.get();
    assertNotNull(r4);

    assertTrue(new HashSet<>(asList(r1, r2, r3)).contains(r4));
  }

  @Test
  public void testForEachIteratesAllCachedInstances() {
    AtomicInteger counter = new AtomicInteger(0);
    UsageBasedCache<DummyResource> cache = new UsageBasedCache<>(3, () -> new DummyResource(counter.getAndIncrement()));

    cache.get(); // ID 0
    cache.get(); // ID 1
    cache.get(); // ID 2

    Set<Integer> idsSeen = new HashSet<>();
    cache.forEach(resource -> idsSeen.add(resource.getId()));

    assertEquals(new HashSet<>(asList(0, 1, 2)), idsSeen);
  }

  @Test
  public void testThreadSafetyWithMultipleThreads() throws InterruptedException {
    int threadCount = 3;
    int iterationsPerThread = 10;
    AtomicInteger instanceCounter = new AtomicInteger(0);

    UsageBasedCache<DummyResource> cache = new UsageBasedCache<>(
        2,
        () -> new DummyResource(instanceCounter.getAndIncrement())
    );

    List<Thread> threads = getThreads(threadCount, iterationsPerThread, cache);

    for (Thread t : threads) {
      t.join();
    }

    // Verify that we only created at most 'capacity' number of instances
    assertTrue(instanceCounter.get() <= 2, "Should not have created more than capacity");
  }

  private static List<Thread> getThreads(
      int threadCount,
      int iterationsPerThread,
      UsageBasedCache<DummyResource> cache
  ) {
    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      Thread t = new Thread(() -> {
        for (int j = 0; j < iterationsPerThread; j++) {
          DummyResource res = cache.get();
          assertNotNull(res);
          try {
            Thread.sleep(5); // Simulate work
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }, "Worker-" + i);
      threads.add(t);
      t.start();
    }
    return threads;
  }
}

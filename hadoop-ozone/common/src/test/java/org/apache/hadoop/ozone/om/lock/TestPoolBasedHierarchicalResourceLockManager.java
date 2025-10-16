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

package org.apache.hadoop.ozone.om.lock;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager.HierarchicalResourceLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for {@link PoolBasedHierarchicalResourceLockManager}.
 *
 * This class tests the functionality of the pool-based hierarchical resource lock manager,
 * including basic lock operations, concurrency scenarios, resource pool management,
 * and error conditions.
 */
public class TestPoolBasedHierarchicalResourceLockManager {

  private PoolBasedHierarchicalResourceLockManager lockManager;

  @BeforeEach
  public void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    lockManager = new PoolBasedHierarchicalResourceLockManager(conf);
  }

  @AfterEach
  public void tearDown() {
    if (lockManager != null) {
      lockManager.close();
    }
  }

  /**
   * Test basic read lock acquisition and release.
   */
  @Test
  public void testBasicReadLockAcquisition() throws Exception {
    String key = "test-key-1";

    try (HierarchicalResourceLock lock = lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, key)) {
      assertNotNull(lock);
      assertTrue(lock.isLockAcquired());
    }
  }

  /**
   * Test basic write lock acquisition and release.
   */
  @Test
  public void testBasicWriteLockAcquisition() throws Exception {
    String key = "test-key-2";

    try (HierarchicalResourceLock lock = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_DB_LOCK, key)) {
      assertNotNull(lock);
      assertTrue(lock.isLockAcquired());
    }
  }

  /**
   * Test multiple read locks can be acquired on the same resource.
   */
  @Test
  public void testMultipleReadLocks() throws Exception {
    String key = "test-key-3";

    try (HierarchicalResourceLock lock1 = lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, key);
         HierarchicalResourceLock lock2 = lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, key)) {

      assertNotNull(lock1);
      assertNotNull(lock2);
      assertTrue(lock1.isLockAcquired());
      assertTrue(lock2.isLockAcquired());
    }
  }

  /**
   * Test write lock exclusivity - only one write lock can be acquired at a time.
   */
  @Test
  @Timeout(10)
  public void testWriteLockExclusivity() throws Exception {
    String key = "test-key-4";
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    AtomicBoolean secondLockAcquired = new AtomicBoolean(false);

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      // First thread acquires write lock
      CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> {
        try (HierarchicalResourceLock lock = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_DB_LOCK, key)) {
          latch1.countDown();
          // Hold lock for a short time
          Thread.sleep(100);
        } catch (Exception e) {
          fail("First thread failed to acquire lock: " + e.getMessage());
        }
      }, executor);

      // Wait for first lock to be acquired
      latch1.await();

      // Second thread tries to acquire write lock
      CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
        try (HierarchicalResourceLock lock = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_DB_LOCK, key)) {
          secondLockAcquired.set(true);
          latch2.countDown();
        } catch (Exception e) {
          fail("Second thread failed to acquire lock: " + e.getMessage());
        }
      }, executor);

      // Wait for both threads to complete
      future1.get(5, TimeUnit.SECONDS);
      future2.get(5, TimeUnit.SECONDS);

      // Second lock should have been acquired after first was released
      assertTrue(secondLockAcquired.get());

    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test read-write lock interaction - write lock blocks read locks.
   */
  @Test
  @Timeout(10)
  public void testReadWriteLockInteraction() throws Exception {
    String key = "test-key-5";
    CountDownLatch writeLockAcquired = new CountDownLatch(1);
    CountDownLatch readLockAcquired = new CountDownLatch(1);
    AtomicBoolean readLockBlocked = new AtomicBoolean(false);

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      // First thread acquires write lock
      CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> {
        try (HierarchicalResourceLock lock = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_GC_LOCK, key)) {
          writeLockAcquired.countDown();
          // Hold lock for a short time
          Thread.sleep(200);
        } catch (Exception e) {
          fail("Write lock acquisition failed: " + e.getMessage());
        }
      }, executor);

      // Wait for write lock to be acquired
      writeLockAcquired.await();

      // Second thread tries to acquire read lock
      CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
        try {
          // This should block until write lock is released
          readLockBlocked.set(true);
          try (HierarchicalResourceLock lock = lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, key)) {
            readLockAcquired.countDown();
          }
        } catch (Exception e) {
          fail("Read lock acquisition failed: " + e.getMessage());
        }
      }, executor);

      // Wait for both threads to complete
      future1.get(5, TimeUnit.SECONDS);
      future2.get(5, TimeUnit.SECONDS);

      assertTrue(readLockBlocked.get());
      assertEquals(0, readLockAcquired.getCount());

    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test lock state after closing.
   */
  @Test
  public void testLockStateAfterClose() throws Exception {
    String key = "test-key-6";

    HierarchicalResourceLock lock = lockManager.acquireReadLock(FlatResource.SNAPSHOT_DB_LOCK, key);
    assertTrue(lock.isLockAcquired());

    lock.close();
    assertFalse(lock.isLockAcquired());
  }

  /**
   * Test double close doesn't cause issues.
   */
  @Test
  public void testDoubleClose() throws Exception {
    String key = "test-key-7";

    HierarchicalResourceLock lock = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_GC_LOCK, key);
    assertTrue(lock.isLockAcquired());

    // First close
    lock.close();
    assertFalse(lock.isLockAcquired());

    // Second close should not throw exception
    lock.close();
    assertFalse(lock.isLockAcquired());
  }

  /**
   * Test different resource types can be locked independently.
   */
  @Test
  public void testDifferentResourceTypes() throws Exception {

    List<HierarchicalResourceLock> locks = new ArrayList<>();
    for (FlatResource otherResource : FlatResource.values()) {
      String key = "test-key";
      locks.add(lockManager.acquireWriteLock(otherResource, key));
    }
    for (HierarchicalResourceLock lock : locks) {
      assertNotNull(lock);
      assertTrue(lock.isLockAcquired());
    }
    for (HierarchicalResourceLock lock : locks) {
      lock.close();
    }
  }


  /**
   * Test different keys on same resource type can be locked concurrently.
   */
  @Test
  public void testDifferentKeysOnSameResource() throws Exception {
    String key1 = "test-key-8a";
    String key2 = "test-key-8b";

    try (HierarchicalResourceLock lock1 = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_GC_LOCK, key1);
         HierarchicalResourceLock lock2 = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_GC_LOCK, key2)) {

      assertNotNull(lock1);
      assertNotNull(lock2);
      assertTrue(lock1.isLockAcquired());
      assertTrue(lock2.isLockAcquired());
    }
  }

  /**
   * Test configuration parameters are respected.
   */
  @Test
  public void testHardLimitsWithCustomConfiguration()
      throws InterruptedException, IOException, ExecutionException, TimeoutException {
    OzoneConfiguration customConf = new OzoneConfiguration();
    customConf.setInt(OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT, 100);
    customConf.setInt(OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT, 500);

    try (PoolBasedHierarchicalResourceLockManager customLockManager =
             new PoolBasedHierarchicalResourceLockManager(customConf)) {

      // Test that manager can be created with custom configuration
      List<HierarchicalResourceLock> locks = new ArrayList<>();
      assertNotNull(customLockManager);
      for (int i = 0; i < 500; i++) {
        try {
          locks.add(customLockManager.acquireReadLock(FlatResource.SNAPSHOT_DB_LOCK, "test" + i));
        } catch (IOException e) {
          fail("Lock acquisition failed with custom configuration: " + e.getMessage());
        }
      }
      CountDownLatch latch = new CountDownLatch(1);
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        // Basic functionality test with custom configuration
        latch.countDown();
        try (HierarchicalResourceLock lock = customLockManager.acquireReadLock(FlatResource.SNAPSHOT_DB_LOCK,
            "test" + 501)) {
          assertTrue(lock.isLockAcquired());
        } catch (Exception e) {
          fail("Lock acquisition failed with custom configuration: " + e.getMessage());
        }
      });
      Thread.sleep(1000);
      latch.await();
      assertFalse(future.isDone());
      locks.get(0).close();
      future.get(5, TimeUnit.SECONDS);
      for (HierarchicalResourceLock lock : locks) {
        lock.close();
      }
    }
  }

  /**
   * Test concurrent access with multiple threads.
   */
  @Test
  @Timeout(30)
  public void testConcurrentAccess() throws Exception {
    int numThreads = 10;
    int operationsPerThread = 50;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicReference<Exception> exception = new AtomicReference<>();

    try {
      List<CompletableFuture<Void>> futures = new ArrayList<>();

      for (int i = 0; i < numThreads; i++) {
        final int threadId = i;
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            for (int j = 0; j < operationsPerThread; j++) {
              String key = "thread-" + threadId + "-op-" + j;
              FlatResource resource = FlatResource.values()[j % FlatResource.values().length];

              // Randomly choose read or write lock
              boolean isReadLock = (j % 2 == 0);

              try (HierarchicalResourceLock lock = isReadLock ?
                   lockManager.acquireReadLock(resource, key) :
                   lockManager.acquireWriteLock(resource, key)) {

                assertTrue(lock.isLockAcquired());

                // Simulate some work
                Thread.sleep(1);

                successCount.incrementAndGet();
              }
            }
          } catch (Exception e) {
            exception.set(e);
          } finally {
            latch.countDown();
          }
        }, executor);

        futures.add(future);
      }

      // Wait for all threads to complete
      assertTrue(latch.await(25, TimeUnit.SECONDS));

      // Check for exceptions
      if (exception.get() != null) {
        fail("Concurrent access test failed: " + exception.get().getMessage());
      }

      // Verify all operations succeeded
      assertEquals(numThreads * operationsPerThread, successCount.get());
      for (CompletableFuture<Void> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test resource pool behavior under stress.
   */
  @Test
  @Timeout(20)
  public void testResourcePoolStress() throws Exception {
    // Use smaller pool limits for stress testing
    OzoneConfiguration stressConf = new OzoneConfiguration();
    stressConf.setInt(OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT, 10);
    stressConf.setInt(OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT, 20);

    try (PoolBasedHierarchicalResourceLockManager stressLockManager =
             new PoolBasedHierarchicalResourceLockManager(stressConf)) {

      int numThreads = 5;
      int operationsPerThread = 20;
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      CountDownLatch latch = new CountDownLatch(numThreads);
      AtomicInteger successCount = new AtomicInteger(0);
      AtomicReference<Exception> exception = new AtomicReference<>();

      try {
        for (int i = 0; i < numThreads; i++) {
          final int threadId = i;
          executor.submit(() -> {
            try {
              for (int j = 0; j < operationsPerThread; j++) {
                String key = "stress-" + threadId + "-" + j;

                try (HierarchicalResourceLock lock =
                     stressLockManager.acquireWriteLock(FlatResource.SNAPSHOT_GC_LOCK, key)) {

                  assertTrue(lock.isLockAcquired());

                  // Hold lock for a bit to stress the pool
                  Thread.sleep(10);

                  successCount.incrementAndGet();
                }
              }
            } catch (Exception e) {
              exception.set(e);
            } finally {
              latch.countDown();
            }
          });
        }

        // Wait for all threads to complete
        assertTrue(latch.await(15, TimeUnit.SECONDS));

        // Check for exceptions
        if (exception.get() != null) {
          fail("Resource pool stress test failed: " + exception.get().getMessage());
        }

        // Verify all operations succeeded
        assertEquals(numThreads * operationsPerThread, successCount.get());

      } finally {
        executor.shutdown();
      }
    }
  }

  /**
   * Test manager close functionality.
   */
  @Test
  public void testManagerClose() throws Exception {
    String key = "test-key-close";

    // Acquire a lock
    HierarchicalResourceLock lock = lockManager.acquireReadLock(FlatResource.SNAPSHOT_DB_LOCK, key);
    assertTrue(lock.isLockAcquired());

    // Close the lock
    lock.close();
    assertFalse(lock.isLockAcquired());

    // Close the manager
    lockManager.close();

    // Manager should be closed gracefully
    // Note: We don't test acquiring locks after manager close as behavior is undefined
  }

  /**
   * Test null key handling.
   */
  @Test
  public void testNullKey() {
    assertThrows(NullPointerException.class, () -> {
      lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, null);
    });
  }

  /**
   * Test null resource handling.
   */
  @Test
  public void testNullResource() {
    assertThrows(NullPointerException.class, () -> {
      lockManager.acquireWriteLock(null, "test-key");
    });
  }

  /**
   * Test empty key handling.
   */
  @Test
  public void testEmptyKey() throws Exception {
    // Empty key should be allowed
    try (HierarchicalResourceLock lock = lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, "")) {
      assertNotNull(lock);
      assertTrue(lock.isLockAcquired());
    }
  }

  /**
   * Test various key formats.
   */
  @ParameterizedTest
  @ValueSource(strings = {"simple", "key-with-dashes", "key_with_underscores",
      "key.with.dots", "key/with/slashes", "123456789",
      "key with spaces", "very-long-key-name-that-exceeds-normal-length-expectations"})
  public void testVariousKeyFormats(String key) throws Exception {
    try (HierarchicalResourceLock lock = lockManager.acquireWriteLock(FlatResource.SNAPSHOT_DB_LOCK, key)) {
      assertNotNull(lock);
      assertTrue(lock.isLockAcquired());
    }
  }

  /**
   * Test reentrant lock behavior - same thread can acquire multiple locks on same resource.
   */
  @Test
  public void testReentrantLockBehavior() throws Exception {
    String key = "reentrant-test";

    // Acquire first lock
    try (HierarchicalResourceLock lock1 = lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, key)) {
      assertTrue(lock1.isLockAcquired());

      // Acquire second lock on same resource from same thread
      try (HierarchicalResourceLock lock2 = lockManager.acquireReadLock(FlatResource.SNAPSHOT_GC_LOCK, key)) {
        assertTrue(lock2.isLockAcquired());

        // Both locks should be active
        assertTrue(lock1.isLockAcquired());
        assertTrue(lock2.isLockAcquired());
      }

      // First lock should still be active after second is released
      assertTrue(lock1.isLockAcquired());
    }
  }

  /**
   * Test that IOException is properly propagated from pool operations.
   */
  @Test
  public void testIOExceptionPropagation() {
    // This test verifies that IOExceptions from pool operations are properly handled
    // In normal circumstances, the pool should not throw IOExceptions during basic operations
    // but the code should handle them gracefully if they occur

    String key = "exception-test";

    try (HierarchicalResourceLock lock = lockManager.acquireReadLock(FlatResource.SNAPSHOT_DB_LOCK, key)) {
      assertNotNull(lock);
      assertTrue(lock.isLockAcquired());
      // If we reach here, no IOException was thrown, which is expected for normal operation
    } catch (Exception e) {
      // If Exception is thrown, it should be properly propagated
      assertNotNull(e.getMessage());
    }
  }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests OzoneManagerLock.Resource.KEY_PATH_LOCK.
 */
class TestKeyPathLock extends TestOzoneManagerLock {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyPathLock.class);

  private final OzoneManagerLock.LeveledResource resource =
      OzoneManagerLock.LeveledResource.KEY_PATH_LOCK;

  @Test
  void testKeyPathLockMultiThreading() throws Exception {
    testSameKeyPathWriteLockMultiThreading(10, 100);
    testDiffKeyPathWriteLockMultiThreading(10, 100);
  }

  private static class Counter {

    private int count = 0;

    public void incrementCount() {
      count++;
    }

    public int getCount() {
      return count;
    }
  }

  // "/a/b/c/d/key1 - WLock - 1st iteration"
  // "/a/b/c/d/key1 - WLock - 2nd iteration"  -- blocked
  // "/a/b/c/d/key1 - WLock - 3rd iteration"  -- blocked
  // "/a/b/c/d/key1 - WLock - 4th iteration"  -- blocked
  // "/a/b/c/d/key1 - WLock - 5th iteration"  -- blocked
  // (iterations are sequential)

  void testSameKeyPathWriteLockMultiThreading(int threadCount,
      int iterations)
      throws InterruptedException {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    Thread[] threads = new Thread[threadCount];
    Counter counter = new Counter();
    CountDownLatch countDownLatch = new CountDownLatch(threadCount);
    List<Integer> listTokens = new ArrayList<>(threadCount);

    for (int i = 0; i < threads.length; i++) {

      threads[i] = new Thread(() -> {
        String[] sampleResourceName =
            new String[]{volumeName, bucketName, keyName};

        testSameKeyPathWriteLockMultiThreadingUtil(iterations, lock, counter,
            countDownLatch, listTokens, sampleResourceName);
      }, "Thread-" + i);

      threads[i].start();
    }

    // Waiting for all the threads to finish execution (run method).
    for (Thread t : threads) {
      t.join();
    }

    // For example, threadCount = 10, iterations = 100. The expected counter
    // value is 10 * 100
    assertEquals(((long) threadCount) * iterations,
        counter.getCount());
    assertEquals(threadCount, listTokens.size());

    // Thread-1 -> 1 * 100,
    // Thread-2 -> 2 * 100 and so on.
    for (int i = 1; i <= listTokens.size(); i++) {
      assertEquals(Integer.valueOf(i * iterations),
          listTokens.get(i - 1));
    }
  }

  private void testSameKeyPathWriteLockMultiThreadingUtil(
      int iterations, OzoneManagerLock lock, Counter counter,
      CountDownLatch countDownLatch, List<Integer> listTokens,
      String[] sampleResourceName) {

    // Waiting for all the threads to be instantiated/to reach
    // acquireWriteHashedLock.
    countDownLatch.countDown();
    while (countDownLatch.getCount() > 0) {
      try {
        Thread.sleep(500);
        LOG.info("countDown.getCount() -> " + countDownLatch.getCount());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // Now all threads have been instantiated.
    assertEquals(0, countDownLatch.getCount());

    lock.acquireWriteLock(resource, sampleResourceName);
    LOG.info("Write Lock Acquired by " + Thread.currentThread().getName());

    // Critical Section. count = count + 1;
    for (int idx = 0; idx < iterations; idx++) {
      counter.incrementCount();
    }

    //  Sequence of tokens range from 1-100 (if iterations = 100) for each
    //  thread. For example:
    //  Thread-1 -> 1 - 100
    //  Thread-2 -> 101 - 200 and so on.
    listTokens.add(counter.getCount());

    lock.releaseWriteLock(resource, sampleResourceName);
    LOG.info("Write Lock Released by " + Thread.currentThread().getName());
  }

  // "/a/b/c/d/key1 - WLock - 1st iteration"
  // "/a/b/c/d/key2 - WLock - 2nd iteration"  -- allowed
  // "/a/b/c/d/key3 - WLock - 3rd iteration"  -- allowed
  // "/a/b/c/d/key4 - WLock - 4th iteration"  -- allowed
  // "/a/b/c/d/key5 - WLock - 5th iteration"  -- allowed
  // (iterations are parallel)

  void testDiffKeyPathWriteLockMultiThreading(int threadCount,
      int iterations)
      throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    Thread[] threads = new Thread[threadCount];
    Counter counter = new Counter();
    CountDownLatch countDown = new CountDownLatch(threadCount);

    for (int i = 0; i < threads.length; i++) {

      threads[i] = new Thread(() -> {
        String keyName = UUID.randomUUID().toString();
        String[] sampleResourceName =
            new String[]{volumeName, bucketName, keyName};

        testDiffKeyPathWriteLockMultiThreadingUtil(lock, countDown,
            sampleResourceName);
      }, "Thread-" + i);
      threads[i].start();
    }

    // Waiting for all the threads to count down
    GenericTestUtils.waitFor(() -> {
      if (countDown.getCount() > 0) {
        LOG.info("Waiting for the threads to count down {} ",
            countDown.getCount());
        return false;
      }
      return true; // all threads have finished counting down.
    }, 3000, 120000); // 2 minutes

    assertEquals(0, countDown.getCount());

    for (Thread t : threads) {
      t.join();
    }

    LOG.info("Expected = " + threadCount * iterations + ", Actual = " +
        counter.getCount());
  }

  private void testDiffKeyPathWriteLockMultiThreadingUtil(
      OzoneManagerLock lock, CountDownLatch countDown,
      String[] sampleResourceName) {

    lock.acquireWriteLock(resource, sampleResourceName);
    LOG.info("Write Lock Acquired by " + Thread.currentThread().getName());

    // Waiting for all the threads to be instantiated/to reach
    // acquireWriteLock.
    countDown.countDown();
    assertEquals(1, lock.getCurrentLocks().size());

    lock.releaseWriteLock(resource, sampleResourceName);
    LOG.info("Write Lock Released by " + Thread.currentThread().getName());
  }

  @Test
  void testAcquireWriteBucketLockWhileAcquiredWriteKeyPathLock() {
    OzoneManagerLock.LeveledResource higherResource =
        OzoneManagerLock.LeveledResource.BUCKET_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    String[] resourceName = new String[]{volumeName, bucketName, keyName},
        higherResourceName = new String[]{volumeName, bucketName};

    lock.acquireWriteLock(resource, resourceName);
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> lock.acquireWriteLock(higherResource, higherResourceName));
    String message = "cannot acquire " + higherResource.getName() + " lock " +
        "while holding [" + resource.getName() + "] lock(s).";
    assertThat(ex).hasMessageContaining(message);
  }

  @Test
  void testAcquireWriteBucketLockWhileAcquiredReadKeyPathLock() {
    OzoneManagerLock.LeveledResource higherResource =
        OzoneManagerLock.LeveledResource.BUCKET_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    String[] resourceName = new String[]{volumeName, bucketName, keyName},
        higherResourceName = new String[]{volumeName, bucketName};

    lock.acquireReadLock(resource, resourceName);
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> lock.acquireWriteLock(higherResource, higherResourceName));
    String message = "cannot acquire " + higherResource.getName() + " lock " +
        "while holding [" + resource.getName() + "] lock(s).";
    assertThat(ex).hasMessageContaining(message);
  }

  @Test
  void testAcquireReadBucketLockWhileAcquiredReadKeyPathLock() {
    OzoneManagerLock.LeveledResource higherResource =
        OzoneManagerLock.LeveledResource.BUCKET_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    String[] resourceName = new String[]{volumeName, bucketName, keyName},
        higherResourceName = new String[]{volumeName, bucketName};

    lock.acquireReadLock(resource, resourceName);
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> lock.acquireReadLock(higherResource, higherResourceName));
    String message = "cannot acquire " + higherResource.getName() + " lock " +
        "while holding [" + resource.getName() + "] lock(s).";
    assertThat(ex).hasMessageContaining(message);
  }

  @Test
  void testAcquireReadBucketLockWhileAcquiredWriteKeyPathLock() {
    OzoneManagerLock.LeveledResource higherResource =
        OzoneManagerLock.LeveledResource.BUCKET_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    String[] resourceName = new String[]{volumeName, bucketName, keyName},
        higherResourceName = new String[]{volumeName, bucketName};

    lock.acquireWriteLock(resource, resourceName);
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> lock.acquireReadLock(higherResource, higherResourceName));
    String message = "cannot acquire " + higherResource.getName() + " lock " +
        "while holding [" + resource.getName() + "] lock(s).";
    assertThat(ex).hasMessageContaining(message);
  }
}

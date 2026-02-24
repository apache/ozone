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

import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.BOOTSTRAP_LOCK;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_CONTENT_LOCK;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_LOCK;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_GC_LOCK;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_LOCAL_DATA_LOCK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock.Resource;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class tests OzoneManagerLock.
 */
class TestOzoneManagerLock {

  @ParameterizedTest
  @EnumSource
  void acquireResourceLock(LeveledResource resource) {
    String[] resourceName = generateResourceName(resource);
    testResourceLock(resourceName, resource);
  }

  private void testResourceLock(String[] resourceName, LeveledResource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(resource, resourceName);
    assertDoesNotThrow(() -> lock.releaseWriteLock(resource, resourceName));
  }

  @ParameterizedTest
  @EnumSource
  void reacquireResourceLock(LeveledResource resource) {
    String[] resourceName = generateResourceName(resource);
    testResourceReacquireLock(resourceName, resource);
  }

  private void testResourceReacquireLock(String[] resourceName,
                                         LeveledResource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    // Lock re-acquire not allowed by same thread.
    if (resource == LeveledResource.USER_LOCK ||
        resource == LeveledResource.S3_SECRET_LOCK ||
        resource == LeveledResource.PREFIX_LOCK) {
      lock.acquireWriteLock(resource, resourceName);
      RuntimeException ex =
          assertThrows(RuntimeException.class, () -> lock.acquireWriteLock(resource, resourceName));
      String message = "cannot acquire " + resource.getName() + " lock " +
          "while holding [" + resource.getName() + "] lock(s).";
      assertThat(ex).hasMessageContaining(message);
      assertDoesNotThrow(() -> lock.releaseWriteLock(resource, resourceName));
    } else {
      lock.acquireWriteLock(resource, resourceName);
      lock.acquireWriteLock(resource, resourceName);
      assertDoesNotThrow(() -> lock.releaseWriteLock(resource, resourceName));
      assertDoesNotThrow(() -> lock.releaseWriteLock(resource, resourceName));
    }
  }

  @Test
  void testLockingOrder() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire lock, and then in inner loop acquire all locks with higher
    // lock level, finally release the locks.
    for (LeveledResource resource : LeveledResource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      resourceName = generateResourceName(resource);
      lock.acquireWriteLock(resource, resourceName);
      stack.push(new ResourceInfo(resourceName, resource));
      for (LeveledResource higherResource : LeveledResource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
        }
      }
      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        assertDoesNotThrow(() ->
            lock.releaseWriteLock(resourceInfo.getResource(), resourceInfo.getLockName()));
      }
    }
  }

  @ParameterizedTest
  @EnumSource
  public void testDAGLockWithParallelResource(DAGLeveledResource dagLeveledResource) {
    Map<DAGLeveledResource, Set<Resource>> forbiddenLockOrdering =
        ImmutableMap.of(SNAPSHOT_DB_CONTENT_LOCK, ImmutableSet.of(SNAPSHOT_DB_LOCK, SNAPSHOT_LOCAL_DATA_LOCK),
            BOOTSTRAP_LOCK, ImmutableSet.of(SNAPSHOT_GC_LOCK, SNAPSHOT_DB_LOCK, SNAPSHOT_DB_CONTENT_LOCK,
                SNAPSHOT_LOCAL_DATA_LOCK));
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    List<Resource> resources = new ArrayList<>();
    resources.addAll(Arrays.stream(LeveledResource.values()).collect(Collectors.toList()));
    resources.addAll(Arrays.stream(dagLeveledResource.values()).collect(Collectors.toList()));
    for (Resource otherResource : resources) {
      String[] otherResourceName = generateResourceName(otherResource);
      String[] dagResourceName = generateResourceName(dagLeveledResource);
      lock.acquireWriteLock(otherResource, otherResourceName);
      boolean secondLockAcquired = false;
      try {
        if (forbiddenLockOrdering.getOrDefault(dagLeveledResource, Collections.emptySet()).contains(otherResource)) {
          assertThrows(RuntimeException.class, () -> lock.acquireWriteLock(dagLeveledResource, dagResourceName));
        } else {
          lock.acquireWriteLock(dagLeveledResource, dagResourceName);
          secondLockAcquired = true;
        }
      } finally {
        lock.releaseWriteLock(otherResource, otherResourceName);
        if (secondLockAcquired) {
          lock.releaseWriteLock(dagLeveledResource, dagResourceName);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource
  void testLockViolationsWithOneHigherLevelLock(LeveledResource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    for (LeveledResource higherResource : LeveledResource.values()) {
      if (higherResource.getMask() > resource.getMask()) {
        String[] resourceName = generateResourceName(higherResource);
        lock.acquireWriteLock(higherResource, resourceName);
        try {
          Exception e = assertThrows(RuntimeException.class,
              () -> lock.acquireWriteLock(resource, generateResourceName(resource)));
          String message = "cannot acquire " + resource.getName() + " lock " +
              "while holding [" + higherResource.getName() + "] lock(s).";
          assertThat(e).hasMessageContaining(message);
        } finally {
          lock.releaseWriteLock(higherResource, resourceName);
        }
      }
    }
  }

  @Test
  void testLockViolations() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire an higher level lock above the resource, and then take the the
    // lock. This should fail. Like that it tries all error combinations.
    for (LeveledResource resource : LeveledResource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      List<String> currentLocks = new ArrayList<>();
      for (LeveledResource higherResource : LeveledResource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
          currentLocks.add(higherResource.getName());
          // try to acquire lower level lock
          RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            String[] resourceName1 = generateResourceName(resource);
            lock.acquireWriteLock(resource, resourceName1);
          });
          String message = "cannot acquire " + resource.getName() + " lock " +
              "while holding " + currentLocks + " lock(s).";
          assertThat(ex).hasMessageContaining(message);
        }
      }

      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseWriteLock(resourceInfo.getResource(),
            resourceInfo.getLockName());
      }
    }
  }

  @Test
  void releaseLockWithOutAcquiringLock() {
    OzoneManagerLock lock =
        new OzoneManagerLock(new OzoneConfiguration());
    assertThrows(IllegalMonitorStateException.class,
        () -> lock.releaseWriteLock(LeveledResource.USER_LOCK, "user3"));
  }

  private String[] generateResourceName(Resource resource) {
    if (resource == LeveledResource.BUCKET_LOCK) {
      return new String[]{UUID.randomUUID().toString(),
          UUID.randomUUID().toString()};
    } else if ((resource == LeveledResource.KEY_PATH_LOCK) ||
        (resource == LeveledResource.SNAPSHOT_LOCK)) {
      return new String[]{UUID.randomUUID().toString(),
          UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    } else {
      return new String[]{UUID.randomUUID().toString()};
    }
  }

  /**
   * Class used to store locked resource info.
   */
  private static class ResourceInfo {
    private final String[] lockName;
    private final LeveledResource resource;

    ResourceInfo(String[] resourceName, LeveledResource resource) {
      this.lockName = resourceName;
      this.resource = resource;
    }

    public String[] getLockName() {
      return lockName.clone();
    }

    public LeveledResource getResource() {
      return resource;
    }
  }

  @Test
  void acquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    assertDoesNotThrow(() -> lock.releaseMultiUserLock("user1", "user2"));
  }

  @Test
  void reAcquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    Exception e = assertThrows(RuntimeException.class,
        () -> lock.acquireMultiUserLock("user1", "user2"));
    assertThat(e)
        .hasMessageContaining("cannot acquire USER_LOCK lock while holding [USER_LOCK] lock(s).");
    lock.releaseMultiUserLock("user1", "user2");
  }

  @Test
  void acquireMultiUserLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(LeveledResource.USER_LOCK, "user3");
    Exception e = assertThrows(RuntimeException.class,
        () -> lock.acquireMultiUserLock("user1", "user2"));
    assertThat(e)
        .hasMessageContaining("cannot acquire USER_LOCK lock while holding [USER_LOCK] lock(s).");
    lock.releaseWriteLock(LeveledResource.USER_LOCK, "user3");
  }

  @Test
  void acquireUserLockAfterMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    Exception e = assertThrows(RuntimeException.class,
        () -> lock.acquireWriteLock(LeveledResource.USER_LOCK, "user3"));
    assertThat(e)
        .hasMessageContaining("cannot acquire USER_LOCK lock while holding [USER_LOCK] lock(s).");
    lock.releaseMultiUserLock("user1", "user2");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testLockResourceParallel(boolean fullResourceLock) throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (Resource resource : Stream.of(LeveledResource.values(), DAGLeveledResource.values())
        .flatMap(Arrays::stream).collect(Collectors.toList())) {
      final String[] resourceName = generateResourceName(resource);
      if (fullResourceLock) {
        lock.acquireResourceWriteLock(resource);
      } else {
        lock.acquireWriteLock(resource, resourceName);
      }

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        if (fullResourceLock) {
          lock.acquireResourceWriteLock(resource);
        } else {
          lock.acquireWriteLock(resource, resourceName);
        }
        gotLock.set(true);
        if (fullResourceLock) {
          lock.releaseResourceWriteLock(resource);
        } else {
          lock.releaseWriteLock(resource, resourceName);
        }

      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same resource,
      // it will wait.
      assertFalse(gotLock.get());
      if (fullResourceLock) {
        lock.releaseResourceWriteLock(resource);
      } else {
        lock.releaseWriteLock(resource, resourceName);
      }
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      assertTrue(gotLock.get());
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true, true",
      "true, false",
      "false, true",
      "false, false"
  })
  void testResourceLockFullResourceLockParallel(boolean mainThreadAcquireResourceLock, boolean acquireWriteLock)
      throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (Resource resource : Stream.of(LeveledResource.values(), DAGLeveledResource.values())
        .flatMap(Arrays::stream).collect(Collectors.toList())) {
      final String[] resourceName = generateResourceName(resource);
      if (mainThreadAcquireResourceLock) {
        lock.acquireResourceWriteLock(resource);
      } else {
        if (acquireWriteLock) {
          lock.acquireWriteLock(resource, resourceName);
        } else {
          lock.acquireReadLock(resource, resourceName);
        }
      }

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        if (!mainThreadAcquireResourceLock) {
          lock.acquireResourceWriteLock(resource);
        } else {
          if (acquireWriteLock) {
            lock.acquireWriteLock(resource, resourceName);
          } else {
            lock.acquireReadLock(resource, resourceName);
          }
        }
        gotLock.set(true);
        if (!mainThreadAcquireResourceLock) {
          lock.releaseResourceWriteLock(resource);
        } else {
          if (acquireWriteLock) {
            lock.releaseWriteLock(resource, resourceName);
          } else {
            lock.releaseReadLock(resource, resourceName);
          }
        }
      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same resource,
      // it will wait.
      assertFalse(gotLock.get());
      if (mainThreadAcquireResourceLock) {
        lock.releaseResourceWriteLock(resource);
      } else {
        if (acquireWriteLock) {
          lock.releaseWriteLock(resource, resourceName);
        } else {
          lock.releaseReadLock(resource, resourceName);
        }
      }
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      assertTrue(gotLock.get());
    }
  }

  @Test
  void testMultiLocksResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (LeveledResource resource : LeveledResource.values()) {
      final List<String[]> resourceName = Arrays.asList(generateResourceName(resource),
          generateResourceName(resource), generateResourceName(resource));
      lock.acquireWriteLocks(resource, resourceName.subList(1, resourceName.size()));

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        lock.acquireWriteLocks(resource, resourceName.subList(0, 2));
        gotLock.set(true);
        lock.releaseWriteLocks(resource, resourceName.subList(0, 2));
      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same resource,
      // it will wait.
      assertFalse(gotLock.get());
      lock.releaseWriteLocks(resource, resourceName.subList(1, resourceName.size()));
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      assertTrue(gotLock.get());
    }

  }

  @Test
  void testMultiLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user2", "user1");

    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireMultiUserLock("user1", "user2");
      gotLock.set(true);
      lock.releaseMultiUserLock("user1", "user2");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same resource, it will
    // wait.
    assertFalse(gotLock.get());
    lock.releaseMultiUserLock("user2", "user1");
    // Since we have released the lock, the new thread should have the lock
    // now.
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    assertTrue(gotLock.get());
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE,
      // USER_LOCK, S3_SECRET_LOCK and PREFIX_LOCK disallow lock re-acquire by
      // the same thread.
      names = { "PREFIX_LOCK", "S3_SECRET_LOCK", "USER_LOCK" })
  void testLockHoldCount(LeveledResource resource) {
    String[] resourceName = generateResourceName(resource);
    testLockHoldCountUtil(resource, resourceName);
  }

  private void testLockHoldCountUtil(LeveledResource resource,
                                     String[] resourceName) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    assertEquals(0, lock.getReadHoldCount(resource, resourceName));
    lock.acquireReadLock(resource, resourceName);
    assertEquals(1, lock.getReadHoldCount(resource, resourceName));

    lock.acquireReadLock(resource, resourceName);
    assertEquals(2, lock.getReadHoldCount(resource, resourceName));

    lock.releaseReadLock(resource, resourceName);
    assertEquals(1, lock.getReadHoldCount(resource, resourceName));

    lock.releaseReadLock(resource, resourceName);
    assertEquals(0, lock.getReadHoldCount(resource, resourceName));

    assertFalse(lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(0, lock.getWriteHoldCount(resource, resourceName));
    lock.acquireWriteLock(resource, resourceName);
    assertTrue(lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(1, lock.getWriteHoldCount(resource, resourceName));

    lock.acquireWriteLock(resource, resourceName);
    assertTrue(lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(2, lock.getWriteHoldCount(resource, resourceName));

    lock.releaseWriteLock(resource, resourceName);
    assertTrue(lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(1, lock.getWriteHoldCount(resource, resourceName));

    lock.releaseWriteLock(resource, resourceName);
    assertFalse(lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(0, lock.getWriteHoldCount(resource, resourceName));
  }

  @ParameterizedTest
  @EnumSource
  void testLockConcurrentStats(LeveledResource resource) throws InterruptedException {
    String[] resourceName = generateResourceName(resource);
    testReadLockConcurrentStats(resource, resourceName, 10);
    testWriteLockConcurrentStats(resource, resourceName, 5);
    testSyntheticReadWriteLockConcurrentStats(resource, resourceName, 10, 3);
  }

  private void testReadLockConcurrentStats(LeveledResource resource,
                                          String[] resourceName,
                                          int threadCount)
      throws InterruptedException {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        lock.acquireReadLock(resource, resourceName);
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseReadLock(resource, resourceName);
      });
      threads[i].start();
    }

    for (Thread t : threads) {
      t.join();
    }

    String readHeldStat = lock.getOMLockMetrics().getReadLockHeldTimeMsStat();
    assertThat(readHeldStat).contains("Samples = " + threadCount);

    String readWaitingStat =
        lock.getOMLockMetrics().getReadLockWaitingTimeMsStat();
    assertThat(readWaitingStat).contains("Samples = " + threadCount);
  }

  private void testWriteLockConcurrentStats(LeveledResource resource,
                                           String[] resourceName,
                                           int threadCount)
      throws InterruptedException {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        lock.acquireWriteLock(resource, resourceName);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseWriteLock(resource, resourceName);
      });
      threads[i].start();
    }

    for (Thread t : threads) {
      t.join();
    }

    String writeHeldStat = lock.getOMLockMetrics().getWriteLockHeldTimeMsStat();
    assertThat(writeHeldStat).contains("Samples = " + threadCount);

    String writeWaitingStat =
        lock.getOMLockMetrics().getWriteLockWaitingTimeMsStat();
    assertThat(writeWaitingStat).contains("Samples = " + threadCount);
  }

  private void testSyntheticReadWriteLockConcurrentStats(
      LeveledResource resource, String[] resourceName,
      int readThreadCount, int writeThreadCount)
      throws InterruptedException {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    Thread[] readThreads = new Thread[readThreadCount];
    Thread[] writeThreads = new Thread[writeThreadCount];

    for (int i = 0; i < readThreads.length; i++) {
      readThreads[i] = new Thread(() -> {
        lock.acquireReadLock(resource, resourceName);
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseReadLock(resource, resourceName);
      });
      readThreads[i].setName("ReadLockThread-" + i);
      readThreads[i].start();
    }

    for (int i = 0; i < writeThreads.length; i++) {
      writeThreads[i] = new Thread(() -> {
        lock.acquireWriteLock(resource, resourceName);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseWriteLock(resource, resourceName);
      });
      writeThreads[i].setName("WriteLockThread-" + i);
      writeThreads[i].start();
    }

    for (Thread r : readThreads) {
      r.join();
    }

    for (Thread w : writeThreads) {
      w.join();
    }

    final String readSamples = "Samples = " + readThreadCount;
    assertThat(lock.getOMLockMetrics().getReadLockHeldTimeMsStat())
        .contains(readSamples);

    assertThat(lock.getOMLockMetrics().getReadLockWaitingTimeMsStat())
        .contains(readSamples);

    final String writeSamples = "Samples = " + writeThreadCount;
    assertThat(lock.getOMLockMetrics().getWriteLockHeldTimeMsStat())
        .contains(writeSamples);

    assertThat(lock.getOMLockMetrics().getWriteLockWaitingTimeMsStat())
        .contains(writeSamples);
  }

  @Test
  void testOMLockMetricsRecords() {
    OMLockMetrics omLockMetrics = OMLockMetrics.create();
    try {
      MetricsCollectorImpl metricsCollector = new MetricsCollectorImpl();
      omLockMetrics.getMetrics(metricsCollector, true);
      List<? extends MetricsRecord> metricsRecords = metricsCollector.getRecords();
      assertEquals(1, metricsRecords.size());
      assertThat(metricsRecords.toString())
          .contains("ReadLockWaitingTime", "ReadLockHeldTime", "WriteLockWaitingTime", "WriteLockHeldTime");
    } finally {
      omLockMetrics.unRegister();
    }
  }
}

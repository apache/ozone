/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Class tests OzoneManagerLock.
 */
public class TestOzoneManagerLock {
  @Test
  public void acquireResourceLock() {
    String[] resourceName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      resourceName = generateResourceName(resource);
      testResourceLock(resourceName, resource);
    }
  }

  private void testResourceLock(String[] resourceName,
      OzoneManagerLock.Resource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(resource, resourceName);
    lock.releaseWriteLock(resource, resourceName);
    Assert.assertTrue(true);
  }

  @Test
  public void reacquireResourceLock() {
    String[] resourceName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      resourceName = generateResourceName(resource);
      testResourceReacquireLock(resourceName, resource);
    }
  }

  private void testResourceReacquireLock(String[] resourceName,
      OzoneManagerLock.Resource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    // Lock re-acquire not allowed by same thread.
    if (resource == OzoneManagerLock.Resource.USER_LOCK ||
        resource == OzoneManagerLock.Resource.S3_SECRET_LOCK ||
        resource == OzoneManagerLock.Resource.PREFIX_LOCK) {
      lock.acquireWriteLock(resource, resourceName);
      try {
        lock.acquireWriteLock(resource, resourceName);
        fail("reacquireResourceLock failed");
      } catch (RuntimeException ex) {
        String message = "cannot acquire " + resource.getName() + " lock " +
            "while holding [" + resource.getName() + "] lock(s).";
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
      }
      lock.releaseWriteLock(resource, resourceName);
      Assert.assertTrue(true);
    } else {
      lock.acquireWriteLock(resource, resourceName);
      lock.acquireWriteLock(resource, resourceName);
      lock.releaseWriteLock(resource, resourceName);
      lock.releaseWriteLock(resource, resourceName);
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testLockingOrder() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire lock, and then in inner loop acquire all locks with higher
    // lock level, finally release the locks.
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      resourceName = generateResourceName(resource);
      lock.acquireWriteLock(resource, resourceName);
      stack.push(new ResourceInfo(resourceName, resource));
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
        }
      }
      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseWriteLock(resourceInfo.getResource(),
            resourceInfo.getLockName());
      }
    }
    Assert.assertTrue(true);
  }

  @Test
  public void testLockViolationsWithOneHigherLevelLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          String[] resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          try {
            lock.acquireWriteLock(resource, generateResourceName(resource));
            fail("testLockViolationsWithOneHigherLevelLock failed");
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding [" + higherResource.getName() + "] lock(s).";
            Assert.assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
          lock.releaseWriteLock(higherResource, resourceName);
        }
      }
    }
  }

  @Test
  public void testLockViolations() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire an higher level lock above the resource, and then take the the
    // lock. This should fail. Like that it tries all error combinations.
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      List<String> currentLocks = new ArrayList<>();
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
          currentLocks.add(higherResource.getName());
          // try to acquire lower level lock
          try {
            resourceName = generateResourceName(resource);
            lock.acquireWriteLock(resource, resourceName);
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding " + currentLocks.toString() + " lock(s).";
            Assert.assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
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
  public void releaseLockWithOutAcquiringLock() {
    OzoneManagerLock lock =
        new OzoneManagerLock(new OzoneConfiguration());
    try {
      lock.releaseWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
      fail("releaseLockWithOutAcquiringLock failed");
    } catch (IllegalMonitorStateException ex) {
      String message = "Releasing lock on resource $user3 without acquiring " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
  }


  private String[] generateResourceName(OzoneManagerLock.Resource resource) {
    if (resource == OzoneManagerLock.Resource.BUCKET_LOCK) {
      return new String[]{UUID.randomUUID().toString(),
          UUID.randomUUID().toString()};
    } else {
      return new String[]{UUID.randomUUID().toString()};
    }
  }

  private String generateResourceLockName(OzoneManagerLock.Resource resource,
                                          String... resources) {
    if (resources.length == 1 &&
        resource != OzoneManagerLock.Resource.BUCKET_LOCK) {
      return OzoneManagerLockUtil.generateResourceLockName(resource,
          resources[0]);
    } else if (resources.length == 2 &&
        resource == OzoneManagerLock.Resource.BUCKET_LOCK) {
      return OzoneManagerLockUtil.generateBucketLockName(resources[0],
          resources[1]);
    } else {
      throw new IllegalArgumentException("acquire lock is supported on single" +
          " resource for all locks except for resource bucket");
    }
  }

  /**
   * Class used to store locked resource info.
   */
  public static class ResourceInfo {
    private String[] lockName;
    private OzoneManagerLock.Resource resource;

    ResourceInfo(String[] resourceName, OzoneManagerLock.Resource resource) {
      this.lockName = resourceName;
      this.resource = resource;
    }

    public String[] getLockName() {
      return lockName.clone();
    }

    public OzoneManagerLock.Resource getResource() {
      return resource;
    }
  }

  @Test
  public void acquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    lock.releaseMultiUserLock("user1", "user2");
    Assert.assertTrue(true);
  }

  @Test
  public void reAcquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    try {
      lock.acquireMultiUserLock("user1", "user2");
      fail("reAcquireMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock("user1", "user2");
  }

  @Test
  public void acquireMultiUserLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
    try {
      lock.acquireMultiUserLock("user1", "user2");
      fail("acquireMultiUserLockAfterUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
  }

  @Test
  public void acquireUserLockAfterMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    try {
      lock.acquireWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
      fail("acquireUserLockAfterMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock("user1", "user2");
  }

  @Test
  public void testLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      final String[] resourceName = generateResourceName(resource);
      lock.acquireWriteLock(resource, resourceName);

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        lock.acquireWriteLock(resource, resourceName);
        gotLock.set(true);
        lock.releaseWriteLock(resource, resourceName);
      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same resource,
      // it will wait.
      Assert.assertFalse(gotLock.get());
      lock.releaseWriteLock(resource, resourceName);
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      Assert.assertTrue(gotLock.get());
    }

  }

  @Test
  public void testMultiLockResourceParallel() throws Exception {
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
    Assert.assertFalse(gotLock.get());
    lock.releaseMultiUserLock("user2", "user1");
    // Since we have released the lock, the new thread should have the lock
    // now.
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test
  public void testLockHoldCount() {
    String[] resourceName;
    String resourceLockName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      // USER_LOCK, S3_SECRET_LOCK and PREFIX_LOCK disallow lock re-acquire by
      // the same thread.
      if (resource != OzoneManagerLock.Resource.USER_LOCK &&
          resource != OzoneManagerLock.Resource.S3_SECRET_LOCK &&
          resource != OzoneManagerLock.Resource.PREFIX_LOCK) {
        resourceName = generateResourceName(resource);
        resourceLockName = generateResourceLockName(resource, resourceName);
        testLockHoldCountUtil(resource, resourceName, resourceLockName);
      }
    }
  }

  private void testLockHoldCountUtil(OzoneManagerLock.Resource resource,
                                         String[] resourceName,
                                         String resourceLockName) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    assertEquals(0, lock.getReadHoldCount(resourceLockName));
    lock.acquireReadLock(resource, resourceName);
    assertEquals(1, lock.getReadHoldCount(resourceLockName));

    lock.acquireReadLock(resource, resourceName);
    assertEquals(2, lock.getReadHoldCount(resourceLockName));

    lock.releaseReadLock(resource, resourceName);
    assertEquals(1, lock.getReadHoldCount(resourceLockName));

    lock.releaseReadLock(resource, resourceName);
    assertEquals(0, lock.getReadHoldCount(resourceLockName));

    Assert.assertFalse(lock.isWriteLockedByCurrentThread(resourceLockName));
    assertEquals(0, lock.getWriteHoldCount(resourceLockName));
    lock.acquireWriteLock(resource, resourceName);
    Assert.assertTrue(lock.isWriteLockedByCurrentThread(resourceLockName));
    assertEquals(1, lock.getWriteHoldCount(resourceLockName));

    lock.acquireWriteLock(resource, resourceName);
    Assert.assertTrue(lock.isWriteLockedByCurrentThread(resourceLockName));
    assertEquals(2, lock.getWriteHoldCount(resourceLockName));

    lock.releaseWriteLock(resource, resourceName);
    Assert.assertTrue(lock.isWriteLockedByCurrentThread(resourceLockName));
    assertEquals(1, lock.getWriteHoldCount(resourceLockName));

    lock.releaseWriteLock(resource, resourceName);
    Assert.assertFalse(lock.isWriteLockedByCurrentThread(resourceLockName));
    assertEquals(0, lock.getWriteHoldCount(resourceLockName));
  }

  @Test
  public void testLockConcurrentStats() throws InterruptedException {
    String[] resourceName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      resourceName = generateResourceName(resource);
      testReadLockConcurrentStats(resource, resourceName, 10);
      testWriteLockConcurrentStats(resource, resourceName, 5);
      testSyntheticReadWriteLockConcurrentStats(resource, resourceName, 10, 3);
    }
  }


  public void testReadLockConcurrentStats(OzoneManagerLock.Resource resource,
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

    String readHeldStat = lock.getReadLockHeldTimeMsStat();
    Assert.assertTrue(
        "Expected " + threadCount +
            " samples in readLockHeldTimeMsStat: " + readHeldStat,
        readHeldStat.contains("Samples = " + threadCount));

    String readWaitingStat = lock.getReadLockWaitingTimeMsStat();
    Assert.assertTrue(
        "Expected " + threadCount +
            " samples in readLockWaitingTimeMsStat: " + readWaitingStat,
        readWaitingStat.contains("Samples = " + threadCount));
  }

  public void testWriteLockConcurrentStats(OzoneManagerLock.Resource resource,
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

    String writeHeldStat = lock.getWriteLockHeldTimeMsStat();
    Assert.assertTrue(
        "Expected " + threadCount +
            " samples in writeLockHeldTimeMsStat: " + writeHeldStat,
        writeHeldStat.contains("Samples = " + threadCount));

    String writeWaitingStat = lock.getWriteLockWaitingTimeMsStat();
    Assert.assertTrue(
        "Expected " + threadCount +
            " samples in writeLockWaitingTimeMsStat" + writeWaitingStat,
        writeWaitingStat.contains("Samples = " + threadCount));
  }

  public void testSyntheticReadWriteLockConcurrentStats(
      OzoneManagerLock.Resource resource, String[] resourceName,
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

    String readHeldStat = lock.getReadLockHeldTimeMsStat();
    Assert.assertTrue(
        "Expected " + readThreadCount +
            " samples in readLockHeldTimeMsStat: " + readHeldStat,
        readHeldStat.contains("Samples = " + readThreadCount));

    String readWaitingStat = lock.getReadLockWaitingTimeMsStat();
    Assert.assertTrue(
        "Expected " + readThreadCount +
            " samples in readLockWaitingTimeMsStat: " + readWaitingStat,
        readWaitingStat.contains("Samples = " + readThreadCount));

    String writeHeldStat = lock.getWriteLockHeldTimeMsStat();
    Assert.assertTrue(
        "Expected " + writeThreadCount +
            " samples in writeLockHeldTimeMsStat: " + writeHeldStat,
        writeHeldStat.contains("Samples = " + writeThreadCount));

    String writeWaitingStat = lock.getWriteLockWaitingTimeMsStat();
    Assert.assertTrue(
        "Expected " + writeThreadCount +
            " samples in writeLockWaitingTimeMsStat" + writeWaitingStat,
        writeWaitingStat.contains("Samples = " + writeThreadCount));
  }
}

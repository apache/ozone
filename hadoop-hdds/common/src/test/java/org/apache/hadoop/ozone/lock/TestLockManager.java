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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.lock;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test-cases to test LockManager.
 */
class TestLockManager {

  @Test
  @Timeout(1)
  void testWriteLockWithDifferentResource() {
    final LockManager<String> manager =
        new LockManager<>(new OzoneConfiguration());
    manager.writeLock("/resourceOne");
    // This should work, as they are different resource.
    assertDoesNotThrow(() -> manager.writeLock("/resourceTwo"));
    manager.writeUnlock("/resourceOne");
    manager.writeUnlock("/resourceTwo");
  }

  @Test
  void testWriteLockWithSameResource() throws Exception {
    final LockManager<String> manager =
        new LockManager<>(new OzoneConfiguration());
    final AtomicBoolean gotLock = new AtomicBoolean(false);
    manager.writeLock("/resourceOne");
    new Thread(() -> {
      manager.writeLock("/resourceOne");
      gotLock.set(true);
      manager.writeUnlock("/resourceOne");
    }).start();
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    // Since the other thread is trying to get write lock on same object,
    // it will wait.
    assertFalse(gotLock.get());
    manager.writeUnlock("/resourceOne");
    // Since we have released the write lock, the other thread should have
    // the lock now
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    assertTrue(gotLock.get());
  }

  @Test
  @Timeout(1)
  void testReadLockWithDifferentResource() {
    final LockManager<String> manager =
        new LockManager<>(new OzoneConfiguration());
    manager.readLock("/resourceOne");
    assertDoesNotThrow(() -> manager.readLock("/resourceTwo"));
    manager.readUnlock("/resourceOne");
    manager.readUnlock("/resourceTwo");
  }

  @Test
  void testReadLockWithSameResource() throws Exception {
    final LockManager<String> manager =
        new LockManager<>(new OzoneConfiguration());
    final AtomicBoolean gotLock = new AtomicBoolean(false);
    manager.readLock("/resourceOne");
    new Thread(() -> {
      manager.readLock("/resourceOne");
      gotLock.set(true);
      manager.readUnlock("/resourceOne");
    }).start();
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get read lock, it should work.
    assertTrue(gotLock.get());
    manager.readUnlock("/resourceOne");
  }

  @Test
  void testWriteReadLockWithSameResource() throws Exception {
    final LockManager<String> manager =
        new LockManager<>(new OzoneConfiguration());
    final AtomicBoolean gotLock = new AtomicBoolean(false);
    manager.writeLock("/resourceOne");
    new Thread(() -> {
      manager.readLock("/resourceOne");
      gotLock.set(true);
      manager.readUnlock("/resourceOne");
    }).start();
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    // Since the other thread is trying to get read lock on same object,
    // it will wait.
    assertFalse(gotLock.get());
    manager.writeUnlock("/resourceOne");
    // Since we have released the write lock, the other thread should have
    // the lock now
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    assertTrue(gotLock.get());
  }

  @Test
  void testReadWriteLockWithSameResource() throws Exception {
    final LockManager<String> manager =
        new LockManager<>(new OzoneConfiguration());
    final AtomicBoolean gotLock = new AtomicBoolean(false);
    manager.readLock("/resourceOne");
    new Thread(() -> {
      manager.writeLock("/resourceOne");
      gotLock.set(true);
      manager.writeUnlock("/resourceOne");
    }).start();
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    // Since the other thread is trying to get write lock on same object,
    // it will wait.
    assertFalse(gotLock.get());
    manager.readUnlock("/resourceOne");
    // Since we have released the read lock, the other thread should have
    // the lock now
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    assertTrue(gotLock.get());
  }

  @Test
  void testMultiReadWriteLockWithSameResource() throws Exception {
    final LockManager<String> manager =
        new LockManager<>(new OzoneConfiguration());
    final AtomicBoolean gotLock = new AtomicBoolean(false);
    manager.readLock("/resourceOne");
    manager.readLock("/resourceOne");
    new Thread(() -> {
      manager.writeLock("/resourceOne");
      gotLock.set(true);
      manager.writeUnlock("/resourceOne");
    }).start();
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    // Since the other thread is trying to get write lock on same object,
    // it will wait.
    assertFalse(gotLock.get());
    manager.readUnlock("/resourceOne");
    //We have only released one read lock, we still hold another read lock.
    Thread.sleep(100);
    assertFalse(gotLock.get());
    manager.readUnlock("/resourceOne");
    // Since we have released the read lock, the other thread should have
    // the lock now
    // Let's give some time for the other thread to run
    Thread.sleep(100);
    assertTrue(gotLock.get());
  }

  @Test
  void testConcurrentWriteLockWithDifferentResource() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int count = 100;
    final LockManager<Integer> manager = new LockManager<>(conf);
    final int sleep = 10;
    final AtomicInteger done = new AtomicInteger();
    for (int i = 0; i < count; i++) {
      final Integer id = i;
      Daemon d1 = new Daemon(() -> {
        try {
          manager.writeLock(id);
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } finally {
          manager.writeUnlock(id);
        }
        done.getAndIncrement();
      });
      d1.setName("Locker-" + i);
      d1.start();
    }
    GenericTestUtils.waitFor(() -> done.get() == count, 100,
        10 * count * sleep);
    assertEquals(count, done.get());
  }
}

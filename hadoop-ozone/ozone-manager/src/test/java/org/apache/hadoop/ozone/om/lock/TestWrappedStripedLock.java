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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.Test;

/**
 * Test for WrappedStripedLock.
 */
public class TestWrappedStripedLock {
  @Test
  public void testWriteLock() throws InterruptedException {
    WrappedStripedLock wrappedStripedLock = new WrappedStripedLock(1, 100, false);

    // check if same lock is tried to be taken, it will throw exception with timeout
    Lock lock = wrappedStripedLock.writeLock("test");
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> {
      Lock[] out = new Lock[1];
      assertDoesNotThrow(() -> out[0] = wrappedStripedLock.writeLock("test"));
      assertNull(out[0]);
    });
    rst.join();

    lock.unlock();
  }

  @Test
  public void testWriteThenReadLock() throws InterruptedException {
    WrappedStripedLock wrappedStripedLock = new WrappedStripedLock(1, 100, false);

    // check if same lock is tried to be taken, it will throw exception with timeout
    Lock lock = wrappedStripedLock.writeLock("test");
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> {
      Lock[] out = new Lock[1];
      assertDoesNotThrow(() -> out[0] = wrappedStripedLock.writeLock("test"));
      assertNull(out[0]);
    });
    rst.join();

    lock.unlock();
  }

  @Test
  public void testReadThenWriteLock() throws InterruptedException {
    WrappedStripedLock wrappedStripedLock = new WrappedStripedLock(1, 100, false);

    // check if same lock is tried to be taken, it will throw exception with timeout
    Lock lock = wrappedStripedLock.readLock("test");
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> {
      Lock[] out = new Lock[1];
      assertDoesNotThrow(() -> out[0] = wrappedStripedLock.writeLock("test"));
      assertNull(out[0]);
    });
    rst.join();

    lock.unlock();
  }

  @Test
  public void testLockListOrderSame() throws InterruptedException {
    WrappedStripedLock wrappedStripedLock = new WrappedStripedLock(1, 100, false);
    List<Lock> locks = new ArrayList<>();
    wrappedStripedLock.writeLock(Arrays.asList("test", "test1"), locks);
    locks.forEach(Lock::unlock);
    List<Lock> lockReverseOrder = new ArrayList<>();
    wrappedStripedLock.writeLock(Arrays.asList("test1", "test2"), lockReverseOrder);
    lockReverseOrder.forEach(Lock::unlock);

    assertEquals(locks.get(0), lockReverseOrder.get(0));
    assertEquals(locks.get(1), lockReverseOrder.get(1));
  }

  @Test
  public void testReadLockListOrderSame() throws InterruptedException {
    WrappedStripedLock wrappedStripedLock = new WrappedStripedLock(1, 100, false);
    List<Lock> locks = new ArrayList<>();
    wrappedStripedLock.readLock(Arrays.asList("test", "test1"), locks);
    locks.forEach(Lock::unlock);
    List<Lock> lockReverseOrder = new ArrayList<>();
    wrappedStripedLock.readLock(Arrays.asList("test1", "test2"), lockReverseOrder);
    lockReverseOrder.forEach(Lock::unlock);

    assertEquals(locks.get(0), lockReverseOrder.get(0));
    assertEquals(locks.get(1), lockReverseOrder.get(1));
  }

  @Test
  public void testLockListFailureOnRelock() throws InterruptedException {
    WrappedStripedLock wrappedStripedLock = new WrappedStripedLock(1, 100, false);
    List<Lock> locks = new ArrayList<>();
    wrappedStripedLock.writeLock(Arrays.asList("test", "test1"), locks);

    // test write lock failure
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> {
      Lock[] out = new Lock[1];
      assertDoesNotThrow(() -> out[0] = wrappedStripedLock.writeLock("test"));
      assertNull(out[0]);
    });
    rst.join();

    // test read lock failure
    rst = CompletableFuture.runAsync(() -> {
      Lock[] out = new Lock[1];
      assertDoesNotThrow(() -> out[0] = wrappedStripedLock.readLock("test1"));
      assertNull(out[0]);
    });
    rst.join();

    locks.forEach(Lock::unlock);

    // verify if lock is success after unlock
    Lock lock = wrappedStripedLock.readLock("test");
    lock.unlock();
  }
}

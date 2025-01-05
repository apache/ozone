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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for TestKeyLock.
 */
public class TestKeyLock {
  private static final Logger LOG = LoggerFactory.getLogger(TestKeyLock.class);

  @Test
  public void testWriteLock() throws IOException {
    KeyLock keyLock = new KeyLock(1, 100);
    
    // check if same lock is tried to be taken, it will throw exception with timeout
    Lock lock = keyLock.lock("test");
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> assertThrows(OMException.class,
        () -> keyLock.lock("test")));
    rst.join();

    lock.unlock();
  }

  @Test
  public void testWriteThenReadLock() throws IOException {
    KeyLock keyLock = new KeyLock(1, 100);

    // check if same lock is tried to be taken, it will throw exception with timeout
    Lock lock = keyLock.lock("test");
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> {
      OMException exp = assertThrows(OMException.class, () -> keyLock.readLock("test"));
      assertTrue(exp.getMessage().contains("read lock"));
    });
    rst.join();

    lock.unlock();
  }

  @Test
  public void testReadThenWriteLock() throws IOException {
    KeyLock keyLock = new KeyLock(1, 100);

    // check if same lock is tried to be taken, it will throw exception with timeout
    Lock lock = keyLock.readLock("test");
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> {
      OMException exp = assertThrows(OMException.class, () -> keyLock.lock("test"));
      assertTrue(exp.getMessage().contains("write lock"));
    });
    rst.join();

    lock.unlock();
  }

  @Test
  public void testLockListOrderSame() throws IOException {
    KeyLock keyLock = new KeyLock(1, 100);
    List<Lock> locks = keyLock.lock(Arrays.asList("test", "test1"));
    locks.forEach(Lock::unlock);
    List<Lock> lockReverseOrder = keyLock.lock(Arrays.asList("test1", "test2"));
    lockReverseOrder.forEach(Lock::unlock);

    assertEquals(locks.get(0), lockReverseOrder.get(0));
    assertEquals(locks.get(1), lockReverseOrder.get(1));
  }

  @Test
  public void testReadLockListOrderSame() throws IOException {
    KeyLock keyLock = new KeyLock(1, 100);
    List<Lock> locks = keyLock.readLock(Arrays.asList("test", "test1"));
    locks.forEach(Lock::unlock);
    List<Lock> lockReverseOrder = keyLock.readLock(Arrays.asList("test1", "test2"));
    lockReverseOrder.forEach(Lock::unlock);

    assertEquals(locks.get(0), lockReverseOrder.get(0));
    assertEquals(locks.get(1), lockReverseOrder.get(1));
  }

  @Test
  public void testLockListFailureOnRelock() throws IOException {
    KeyLock keyLock = new KeyLock(1, 100);
    List<Lock> locks = keyLock.lock(Arrays.asList("test", "test1"));

    // test write lock failure
    CompletableFuture<Void> rst = CompletableFuture.runAsync(() -> {
      OMException exp = assertThrows(OMException.class, () -> keyLock.lock("test"));
      assertTrue(exp.getMessage().contains("write lock"));
    });
    rst.join();

    // test read lock failure
    rst = CompletableFuture.runAsync(() -> {
      OMException exp = assertThrows(OMException.class, () -> keyLock.readLock("test1"));
      assertTrue(exp.getMessage().contains("read lock"));
    });
    rst.join();

    locks.forEach(Lock::unlock);

    // verify if lock is success after unlock
    Lock lock = keyLock.readLock("test");
    lock.unlock();
  }
}

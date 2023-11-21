/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLockImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.io.IOException;

/**
 * Tests {@link AuthorizerLockImpl}.
 */
public class TestAuthorizerLockImpl {

  @BeforeAll
  public static void init() {
    // Enable debug logging for the test
    GenericTestUtils.setLogLevel(AuthorizerLockImpl.LOG, Level.DEBUG);
  }

  /**
   * Tests StampedLock behavior.
   */
  @Test
  public void testStampedLockBehavior() throws InterruptedException {

    final AuthorizerLock authorizerLock = new AuthorizerLockImpl();

    // Case 1: A correct stamp can unlock without an issue
    long readLockStamp = authorizerLock.tryReadLock(100);
    authorizerLock.unlockRead(readLockStamp);
    long writeLockStamp = authorizerLock.tryWriteLock(100);
    authorizerLock.unlockWrite(writeLockStamp);

    // Case 1: An incorrect stamp won't be able to unlock, throws IMSE
    long stamp2 = authorizerLock.tryReadLock(100);
    Assertions.assertThrows(IllegalMonitorStateException.class,
        () -> authorizerLock.unlockRead(stamp2 - 1));
    authorizerLock.unlockRead(stamp2);
    long stamp3 = authorizerLock.tryWriteLock(100);
    Assertions.assertThrows(IllegalMonitorStateException.class,
        () -> authorizerLock.unlockWrite(stamp3 - 1));
    authorizerLock.unlockWrite(stamp3);

    // Case 2: Read lock is reentrant; Write lock is exclusive
    long readLockStamp1 = authorizerLock.tryReadLock(100);
    Assertions.assertTrue(readLockStamp1 > 0L);
    long readLockStamp2 = authorizerLock.tryReadLock(100);
    Assertions.assertTrue(readLockStamp2 > 0L);

    // Can't acquire write lock now, as read lock has been held
    long writeLockStamp1 = authorizerLock.tryWriteLock(100);
    // stamp == 0L means lock failure
    Assertions.assertEquals(0L, writeLockStamp1);

    // Release one read lock. Try again. Should fail
    authorizerLock.unlockRead(readLockStamp2);
    writeLockStamp1 = authorizerLock.tryWriteLock(100);
    Assertions.assertEquals(0L, writeLockStamp1);

    // Release the other read lock. And again. Should work
    authorizerLock.unlockRead(readLockStamp1);
    writeLockStamp1 = authorizerLock.tryWriteLock(100);
    Assertions.assertTrue(writeLockStamp1 > 0L);

    // But a second write lock won't work
    long writeLockStamp2 = authorizerLock.tryWriteLock(100);
    Assertions.assertEquals(0L, writeLockStamp2);

    // Read lock also won't work now that write lock is held
    readLockStamp = authorizerLock.tryReadLock(100);
    Assertions.assertEquals(0L, readLockStamp);

    authorizerLock.unlockWrite(writeLockStamp1);
  }

  @Test
  public void testLockInOneThreadUnlockInAnother() {

    final AuthorizerLock authorizerLock = new AuthorizerLockImpl();

    try {
      authorizerLock.tryWriteLockInOMRequest();

      // Spawn another thread to release the lock.
      // Works as long as they share the same AuthorizerLockImpl instance.
      final Thread thread1 = new Thread(authorizerLock::unlockWriteInOMRequest);
      thread1.start();
    } catch (IOException e) {
      Assertions.fail("Should not have thrown: " + e.getMessage());
    }
  }

  @Test
  public void testUnlockWriteInOMRequestShouldNotThrowOnFollowerOMs() {

    final AuthorizerLock authorizerLock = new AuthorizerLockImpl();

    // When a follower OM attempts to unlock write in validateAndUpdateCache,
    // even though it hasn't acquired the lock in preExecute,
    // the unlockWriteInOMRequest() method should not throw
    // IllegalMonitorStateException as it should have been handled gracefully.
    authorizerLock.unlockWriteInOMRequest();
  }

  @Test
  public void testIsWriteLockHeldByCurrentThread() throws IOException,
      InterruptedException {

    final AuthorizerLock authorizerLock = new AuthorizerLockImpl();

    Assertions.assertFalse(authorizerLock.isWriteLockHeldByCurrentThread());

    // Read lock does not affect the check
    long readLockStamp = authorizerLock.tryReadLock(100);
    Assertions.assertFalse(authorizerLock.isWriteLockHeldByCurrentThread());
    authorizerLock.unlockRead(readLockStamp);

    // Only a write lock acquired through InOMRequest variant affects the check
    authorizerLock.tryWriteLockInOMRequest();
    Assertions.assertTrue(authorizerLock.isWriteLockHeldByCurrentThread());
    authorizerLock.unlockWriteInOMRequest();

    // Regular write lock does not affect the check as well
    long writeLockStamp = authorizerLock.tryWriteLockThrowOnTimeout();
    Assertions.assertFalse(authorizerLock.isWriteLockHeldByCurrentThread());
    authorizerLock.unlockWrite(writeLockStamp);
  }

  @Test
  public void testOptimisticRead() throws Exception {
    final AuthorizerLock authorizerLock = new AuthorizerLockImpl();

    // With no competing operations, an optimistic read should be valid.
    long stamp = authorizerLock.tryOptimisticReadThrowOnTimeout();
    Assertions.assertTrue(authorizerLock.validateOptimisticRead(stamp));

    // With only competing reads, an optimistic read should be valid.
    long optStamp = authorizerLock.tryOptimisticReadThrowOnTimeout();
    long readStamp = authorizerLock.tryReadLock(100);
    Assertions.assertTrue(readStamp > 0L);
    Assertions.assertTrue(authorizerLock.validateOptimisticRead(optStamp));
    authorizerLock.unlockRead(readStamp);
    Assertions.assertTrue(authorizerLock.validateOptimisticRead(optStamp));

    // When a write lock is held, optimistic read should time out trying to get
    // a stamp.
    long writeStamp = authorizerLock.tryWriteLockThrowOnTimeout();
    Assertions.assertThrows(IOException.class,
        authorizerLock::tryOptimisticReadThrowOnTimeout);
    authorizerLock.unlockWrite(writeStamp);

    // When a write lock is acquired after the optimistic read stamp, the read
    // stamp should be invalidated.
    optStamp = authorizerLock.tryOptimisticReadThrowOnTimeout();
    writeStamp = authorizerLock.tryWriteLockThrowOnTimeout();
    Assertions.assertTrue(writeStamp > 0L);
    Assertions.assertFalse(authorizerLock.validateOptimisticRead(optStamp));
    authorizerLock.unlockWrite(writeStamp);
  }
}

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

package org.apache.hadoop.ozone.om.multitenant;

import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.AuthorizerOp;

/**
 * Authorizer access lock interface. Used by OMMultiTenantManager.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AuthorizerLock {
  /**
   * Attempt to acquire the read lock to the authorizer with a timeout.
   * @return stamp returned from lock op. required when releasing the lock
   */
  long tryReadLock(long timeout) throws InterruptedException;

  /**
   * Release the read lock to the authorizer.
   * Throws IllegalMonitorStateException if the provided stamp is incorrect.
   */
  void unlockRead(long stamp);

  /**
   * @return stamp that can be passed to
   *    {@link #validateOptimisticRead(long)} to check if a write lock was
   *    acquired since the stamp was issued.
   * @throws IOException If an ongoing write prevents the lock from moving to
   *    the read state for longer than the timeout.
   */
  long tryOptimisticReadThrowOnTimeout() throws IOException;

  /**
   * @return True if the write lock was not acquired since this stamp was
   *    issued for an optimistic read. False otherwise.
   */
  boolean validateOptimisticRead(long stamp);

  /**
   * Attempt to acquire the write lock to authorizer with a timeout.
   * @return stamp
   */
  long tryWriteLock(long timeout) throws InterruptedException;

  /**
   * Release the write lock to the authorizer.
   * Throws IllegalMonitorStateException if the provided stamp is incorrect.
   */
  void unlockWrite(long stamp);

  /**
   * A wrapper around tryWriteLock() that throws when timed out.
   * @return stamp
   */
  long tryWriteLockThrowOnTimeout() throws IOException;

  /**
   * A wrapper around tryWriteLockThrowOnTimeout() that is used exclusively
   * in OMRequests.
   *
   * MUST use paired with unlockWriteInOMRequest() for unlocking to ensure
   * correctness.
   */
  void tryWriteLockInOMRequest() throws IOException;

  /**
   * A wrapper around unlockWrite() that is used exclusively in OMRequests.
   */
  void unlockWriteInOMRequest();

  /**
   * Returns true if the authorizer write lock is held by the current thread.
   * Used in {@link AuthorizerOp}.
   */
  boolean isWriteLockHeldByCurrentThread();
}

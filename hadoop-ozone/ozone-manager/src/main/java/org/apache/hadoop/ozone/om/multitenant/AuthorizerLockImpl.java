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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_TENANT_AUTHORIZER_LOCK_WAIT_MILLIS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link AuthorizerLock}.
 */
public class AuthorizerLockImpl implements AuthorizerLock {

  private static final Logger LOG =
      LoggerFactory.getLogger(AuthorizerLockImpl.class);

  private final StampedLock authorizerStampedLock = new StampedLock();

  // No need to use atomic here as both fields can only be updated after
  // authorizer write lock is acquired.
  private long omRequestWriteLockStamp = 0L;
  private long omRequestWriteLockHolderTid = 0L;

  @Override
  public long tryReadLock(long timeout) throws InterruptedException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to acquire authorizer read lock from thread {}",
          Thread.currentThread().getId());
    }
    return authorizerStampedLock.tryReadLock(timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Release read lock on the authorizer.
   * This is only used by BG sync at the moment.
   */
  @Override
  public void unlockRead(long stamp) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Releasing authorizer read lock from thread {} with stamp {}",
          Thread.currentThread().getId(), stamp);
    }
    authorizerStampedLock.unlockRead(stamp);
  }

  @Override
  public long tryOptimisticReadThrowOnTimeout() throws IOException {

    long stamp;
    try {
      stamp = tryReadLock(OZONE_TENANT_AUTHORIZER_LOCK_WAIT_MILLIS);
    } catch (InterruptedException e) {
      throw new OMException(e, INTERNAL_ERROR);
    }
    if (stamp == 0L) {
      throw new OMException("Timed out acquiring authorizer read lock."
          + " Another multi-tenancy request is in-progress. Try again later",
          ResultCodes.TIMEOUT);
    }

    long optimisticStamp = authorizerStampedLock.tryConvertToOptimisticRead(
        stamp);
    if (optimisticStamp == 0L) {
      // This should never happen. If we reached this point we are holding a
      // read lock and providing its stamp so the lock should be in read mode
      // already. This would return a valid stamp, not 0.
      unlockRead(stamp);
      throw new OMException("Failed to convert read lock to optimistic read.",
              INTERNAL_ERROR);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Acquired authorizer optimistic read from thread {} with" +
          " stamp {}", Thread.currentThread().getId(), optimisticStamp);
    }
    return optimisticStamp;
  }

  @Override
  public boolean validateOptimisticRead(long stamp) {
    return authorizerStampedLock.validate(stamp);
  }

  @Override
  public long tryWriteLock(long timeout) throws InterruptedException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to acquire authorizer write lock from thread {}",
          Thread.currentThread().getId());
    }
    return authorizerStampedLock.tryWriteLock(timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Release read lock on the authorizer.
   * This is used by both BG sync and tenant requests.
   */
  @Override
  public void unlockWrite(long stamp) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Releasing authorizer write lock from thread {} with stamp {}",
          Thread.currentThread().getId(), stamp);
    }
    authorizerStampedLock.unlockWrite(stamp);
  }

  @Override
  public long tryWriteLockThrowOnTimeout() throws IOException {

    long stamp;
    try {
      stamp = tryWriteLock(OZONE_TENANT_AUTHORIZER_LOCK_WAIT_MILLIS);
    } catch (InterruptedException e) {
      throw new OMException(e, INTERNAL_ERROR);
    }
    if (stamp == 0L) {
      throw new OMException("Timed out acquiring authorizer write lock. "
          + "Another multi-tenancy request is in-progress. Try again later",
          ResultCodes.TIMEOUT);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Acquired authorizer write lock from thread {} with stamp {}",
          Thread.currentThread().getId(), stamp);
    }
    return stamp;
  }

  @Override
  public void tryWriteLockInOMRequest() throws IOException {

    long stamp = tryWriteLockThrowOnTimeout();

    // Sanity check. Must not have held a write lock in a tenant OMRequest.
    Preconditions.checkArgument(omRequestWriteLockStamp == 0L);
    Preconditions.checkArgument(omRequestWriteLockHolderTid == 0L);

    omRequestWriteLockStamp = stamp;
    omRequestWriteLockHolderTid = Thread.currentThread().getId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Set omRequestWriteLockStamp to {}, "
          + "omRequestWriteLockHolderTid to {}",
          omRequestWriteLockStamp, omRequestWriteLockHolderTid);
    }
  }

  @Override
  public void unlockWriteInOMRequest() {

    if (omRequestWriteLockStamp == 0L) {
      LOG.debug("Authorizer write lock is not held in this lock instance. "
          + "This OM might be follower, or leader changed. Ignored");
      return;
    }

    final long stamp = omRequestWriteLockStamp;

    // Reset the internal lock stamp record back to zero.
    omRequestWriteLockStamp = 0L;
    omRequestWriteLockHolderTid = 0L;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Restored omRequestWriteLockStamp to {}, "
              + "omRequestWriteLockHolderTid to {}",
          omRequestWriteLockStamp, omRequestWriteLockHolderTid);
    }
    unlockWrite(stamp);
  }

  @Override
  public boolean isWriteLockHeldByCurrentThread() {

    if (omRequestWriteLockHolderTid == 0L) {
      LOG.debug("Write lock is not held by any OMRequest thread");
      return false;
    }

    if (omRequestWriteLockHolderTid != Thread.currentThread().getId()) {
      LOG.debug("Write lock is not held by current thread");
      return false;
    }

    return true;
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Test for OmRequestGatekeeper.
 */
public class TestRequestGatekeeper {

  @Test
  public void testObsLockOprWithParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmRequestGatekeeper omLockOpr = new OmRequestGatekeeper();
    OmLockInfo lockInfo = new OmLockInfo.Builder()
        .addBucketReadLock("vol", "bucket")
        .addKeyWriteLock("vol", "bucket", "testkey").build();
    OmRequestGatekeeper.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(2, lockObject.getLocks().size());

    CompletableFuture<OmRequestGatekeeper.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmRequestGatekeeper.OmLockObject lockInfoAgain = omLockOpr.lock(lockInfo);
        omLockOpr.unlock(lockInfoAgain);
        return lockInfoAgain;
      } catch (IOException e) {
        fail("should not throw exception");
      }
      return null;
    });

    // parallel lock wait should fail as previous lock not released
    try {
      rst.get(1000, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
      assertTrue(true);
    }

    // after unlock, the thread should be able to get lock
    omLockOpr.unlock(lockObject);
    rst.get();
  }

  @Test
  public void testObsLockOprListKeyRepeated() throws IOException {
    OmRequestGatekeeper omLockOpr = new OmRequestGatekeeper();
    OmLockInfo lockInfo = new OmLockInfo.Builder()
        .addBucketReadLock("vol", "bucket")
        .addKeyWriteLock("vol", "bucket", "testkey")
        .addKeyWriteLock("vol", "bucket", "testkey2").build();
    OmRequestGatekeeper.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(3, lockObject.getLocks().size());

    omLockOpr.unlock(lockObject);

    lockObject = omLockOpr.lock(lockInfo);
    assertEquals(3, lockObject.getLocks().size());
    omLockOpr.unlock(lockObject);
  }

  @Test
  public void testBucketReadLock() throws IOException {
    OmRequestGatekeeper omLockOpr = new OmRequestGatekeeper();
    OmLockInfo lockInfo = new OmLockInfo.Builder().addBucketReadLock("vol", "bucket").build();
    OmRequestGatekeeper.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    omLockOpr.unlock(lockObject);
  }

  @Test
  public void testBucketReadWithWriteParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmRequestGatekeeper omLockOpr = new OmRequestGatekeeper();
    OmLockInfo lockInfo = new OmLockInfo.Builder().addBucketReadLock("vol", "bucket").build();
    OmRequestGatekeeper.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    OmLockInfo writeLockInfo = new OmLockInfo.Builder().addBucketWriteLock("vol", "bucket").build();

    CompletableFuture<OmRequestGatekeeper.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmRequestGatekeeper.OmLockObject lockInfoAgain = omLockOpr.lock(writeLockInfo);
        omLockOpr.unlock(lockInfoAgain);
        return lockInfoAgain;
      } catch (IOException e) {
        fail("should not throw exception");
      }
      return null;
    });

    // parallel lock wait should fail as previous lock not released
    try {
      rst.get(1000, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
      assertTrue(true);
    }

    // after unlock, the thread should be able to get lock
    omLockOpr.unlock(lockObject);
    rst.get();
  }

  @Test
  public void testVolumeReadWithWriteParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmRequestGatekeeper omLockOpr = new OmRequestGatekeeper();
    OmLockInfo lockInfo = new OmLockInfo.Builder().addVolumeReadLock("vol").build();

    OmRequestGatekeeper.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    OmLockInfo writeLockInfo = new OmLockInfo.Builder().addVolumeWriteLock("vol").build();
    CompletableFuture<OmRequestGatekeeper.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmRequestGatekeeper.OmLockObject lockInfoAgain = omLockOpr.lock(writeLockInfo);
        omLockOpr.unlock(lockInfoAgain);
        return lockInfoAgain;
      } catch (IOException e) {
        fail("should not throw exception");
      }
      return null;
    });

    // parallel lock wait should fail as previous lock not released
    try {
      rst.get(1000, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
      assertTrue(true);
    }

    // after unlock, the thread should be able to get lock
    omLockOpr.unlock(lockObject);
    rst.get();
  }

  @Test
  public void testVolWriteWithVolBucketRWParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmRequestGatekeeper omLockOpr = new OmRequestGatekeeper();
    OmLockInfo lockInfo = new OmLockInfo.Builder().addVolumeWriteLock("vol").build();

    OmRequestGatekeeper.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    OmLockInfo writeLockInfo = new OmLockInfo.Builder().addVolumeReadLock("vol")
        .addBucketWriteLock("vol", "buck1").build();

    CompletableFuture<OmRequestGatekeeper.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmRequestGatekeeper.OmLockObject lockInfoAgain = omLockOpr.lock(writeLockInfo);
        omLockOpr.unlock(lockInfoAgain);
        return lockInfoAgain;
      } catch (IOException e) {
        fail("should not throw exception");
      }
      return null;
    });

    // parallel lock wait should fail as previous lock not released
    try {
      rst.get(1000, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
      assertTrue(true);
    }

    // after unlock, the thread should be able to get lock
    omLockOpr.unlock(lockObject);
    rst.get();
  }
}

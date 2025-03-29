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
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Test for TestOmLockOperation.
 */
public class TestOmLockOperation {

  @Test
  public void testObsLockOprWithParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmLockOperation omLockOpr = new OmLockOperation();
    OmLockInfo.KeyLockInfo lockInfo =
        new OmLockInfo.KeyLockInfo("bucket", OmLockInfo.LockAction.READ, "testkey", OmLockInfo.LockAction.WRITE);
    OmLockOperation.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(2, lockObject.getLocks().size());

    CompletableFuture<OmLockOperation.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOperation.OmLockObject lockInfoAgain = omLockOpr.lock(lockInfo);
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
    OmLockOperation omLockOpr = new OmLockOperation();
    OmLockInfo.MultiKeyLockInfo lockInfo =
        new OmLockInfo.MultiKeyLockInfo("bucket", OmLockInfo.LockAction.READ,
            Arrays.asList("testkey", "testkey2"), OmLockInfo.LockAction.WRITE);
    OmLockOperation.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(3, lockObject.getLocks().size());

    omLockOpr.unlock(lockObject);

    lockObject = omLockOpr.lock(lockInfo);
    assertEquals(3, lockObject.getLocks().size());
    omLockOpr.unlock(lockObject);
  }

  @Test
  public void testBucketReadLock() throws IOException {
    OmLockOperation omLockOpr = new OmLockOperation();
    OmLockInfo.BucketLockInfo lockInfo = new OmLockInfo.BucketLockInfo("bucket", OmLockInfo.LockAction.READ);
    OmLockOperation.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    omLockOpr.unlock(lockObject);
  }

  @Test
  public void testBucketReadWithWriteParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmLockOperation omLockOpr = new OmLockOperation();
    OmLockInfo.BucketLockInfo lockInfo = new OmLockInfo.BucketLockInfo("bucket", OmLockInfo.LockAction.READ);
    OmLockOperation.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    OmLockInfo.BucketLockInfo writeLockInfo = new OmLockInfo.BucketLockInfo("bucket", OmLockInfo.LockAction.WRITE);

    CompletableFuture<OmLockOperation.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOperation.OmLockObject lockInfoAgain = omLockOpr.lock(writeLockInfo);
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
    OmLockOperation omLockOpr = new OmLockOperation();
    OmLockInfo.VolumeLockInfo lockInfo = new OmLockInfo.VolumeLockInfo("vol1", OmLockInfo.LockAction.READ);
    OmLockOperation.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    OmLockInfo.VolumeLockInfo writeLockInfo = new OmLockInfo.VolumeLockInfo("vol1", OmLockInfo.LockAction.WRITE);

    CompletableFuture<OmLockOperation.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOperation.OmLockObject lockInfoAgain = omLockOpr.lock(writeLockInfo);
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
    OmLockOperation omLockOpr = new OmLockOperation();
    OmLockInfo.VolumeLockInfo lockInfo = new OmLockInfo.VolumeLockInfo("vol1", OmLockInfo.LockAction.WRITE);
    OmLockOperation.OmLockObject lockObject = omLockOpr.lock(lockInfo);
    assertEquals(1, lockObject.getLocks().size());

    OmLockInfo.BucketLockInfo writeLockInfo = new OmLockInfo.BucketLockInfo("vol1",
        OmLockInfo.LockAction.READ, "buck1", OmLockInfo.LockAction.WRITE);

    CompletableFuture<OmLockOperation.OmLockObject> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOperation.OmLockObject lockInfoAgain = omLockOpr.lock(writeLockInfo);
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

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
 * Test for TestOmLockOpr.
 */
public class TestOmLockOpr {

  @Test
  public void testObsLockOprWithParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmLockOpr omLockOpr = new OmLockOpr();
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.obsLock("bucket", "testkey");
    assertEquals(2, omLockInfo.getLocks().size());

    CompletableFuture<OmLockOpr.OmLockInfo> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOpr.OmLockInfo lockInfoAgain = omLockOpr.obsLock("bucket", "testkey");
        omLockOpr.writeUnlock(lockInfoAgain);
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
    omLockOpr.writeUnlock(omLockInfo);
    rst.get();
  }

  @Test
  public void testObsLockOprListKeyRepeated() throws IOException {
    OmLockOpr omLockOpr = new OmLockOpr();
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.obsLock("bucket", Arrays.asList("testkey", "testkey2"));
    assertEquals(3, omLockInfo.getLocks().size());

    omLockOpr.writeUnlock(omLockInfo);

    omLockInfo = omLockOpr.obsLock("bucket", Arrays.asList("testkey", "testkey2"));
    assertEquals(3, omLockInfo.getLocks().size());
    omLockOpr.writeUnlock(omLockInfo);
  }

  @Test
  public void testBucketReadLock() throws IOException {
    OmLockOpr omLockOpr = new OmLockOpr();
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.bucketReadLock("bucket");
    assertEquals(1, omLockInfo.getLocks().size());

    omLockOpr.readUnlock(omLockInfo);
  }

  @Test
  public void testBucketReadWithWriteParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmLockOpr omLockOpr = new OmLockOpr();
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.bucketReadLock("bucket");
    assertEquals(1, omLockInfo.getLocks().size());

    CompletableFuture<OmLockOpr.OmLockInfo> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOpr.OmLockInfo lockInfoAgain = omLockOpr.bucketWriteLock("bucket");
        omLockOpr.writeUnlock(lockInfoAgain);
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
    omLockOpr.writeUnlock(omLockInfo);
    rst.get();
  }

  @Test
  public void testVolumeReadWithWriteParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmLockOpr omLockOpr = new OmLockOpr();
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.volumeReadLock("vol1");
    assertEquals(1, omLockInfo.getLocks().size());

    CompletableFuture<OmLockOpr.OmLockInfo> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOpr.OmLockInfo lockInfoAgain = omLockOpr.volumeWriteLock("vol1");
        omLockOpr.writeUnlock(lockInfoAgain);
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
    omLockOpr.writeUnlock(omLockInfo);
    rst.get();
  }

  @Test
  public void testVolWriteWithVolBucketRWParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmLockOpr omLockOpr = new OmLockOpr();
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.volumeWriteLock("vol1");
    assertEquals(1, omLockInfo.getLocks().size());

    CompletableFuture<OmLockOpr.OmLockInfo> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOpr.OmLockInfo lockInfoAgain = omLockOpr.volBucketRWLock("vol1", "buck1");
        omLockOpr.writeUnlock(lockInfoAgain);
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
    omLockOpr.writeUnlock(omLockInfo);
    rst.get();
  }
}

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for TestOmLockOpr.
 */
public class TestOmLockOpr {

  @Test
  public void testObsLockOprWithParallelLock() throws IOException, ExecutionException, InterruptedException {
    OmLockOpr omLockOpr = new OmLockOpr("test-");
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.obsLock("bucket", "testkey");
    assertEquals(2, omLockInfo.getLocks().size());
    assertNotNull(omLockOpr.getLockMonitorMap().get(omLockInfo));

    CompletableFuture<OmLockOpr.OmLockInfo> rst = CompletableFuture.supplyAsync(() -> {
      try {
        OmLockOpr.OmLockInfo lockInfoAgain = omLockOpr.obsLock("bucket", "testkey");
        assertNotNull(omLockOpr.getLockMonitorMap().get(lockInfoAgain));
        omLockOpr.writeUnlock(lockInfoAgain);
        return lockInfoAgain;
      } catch (IOException e) {
        fail("should not throw exception");
      }
      return null;
    });
    try {
      rst.get(1000, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
      assertTrue(true);
    }

    omLockOpr.writeUnlock(omLockInfo);
    assertNull(omLockOpr.getLockMonitorMap().get(omLockInfo));
    rst.get();
    assertEquals(0, omLockOpr.getLockMonitorMap().size());
  }

  @Test
  public void testObsLockOprListKeyRepeated() throws IOException {
    OmLockOpr omLockOpr = new OmLockOpr("test-");
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.obsLock("bucket", Arrays.asList("testkey", "testkey2"));
    assertEquals(3, omLockInfo.getLocks().size());
    assertNotNull(omLockOpr.getLockMonitorMap().get(omLockInfo));
  
    omLockOpr.writeUnlock(omLockInfo);
    assertEquals(0, omLockOpr.getLockMonitorMap().size());

    omLockInfo = omLockOpr.obsLock("bucket", Arrays.asList("testkey", "testkey2"));
    assertEquals(3, omLockInfo.getLocks().size());
    omLockOpr.writeUnlock(omLockInfo);
    assertEquals(0, omLockOpr.getLockMonitorMap().size());
  }

  @Test
  public void testBucketReadLock() throws IOException {
    OmLockOpr omLockOpr = new OmLockOpr("test-");
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.bucketReadLock("bucket");
    assertEquals(1, omLockInfo.getLocks().size());
    assertNotNull(omLockOpr.getLockMonitorMap().get(omLockInfo));

    omLockOpr.readUnlock(omLockInfo);
    assertEquals(0, omLockOpr.getLockMonitorMap().size());
  }

  @Test
  public void testStartStopMonitorRepeated() throws IOException {
    OmLockOpr omLockOpr = new OmLockOpr("test-");
    omLockOpr.start();
    OmLockOpr.OmLockInfo omLockInfo = omLockOpr.obsLock("bucket", "testkey");
    omLockOpr.monitor();
    omLockOpr.writeUnlock(omLockInfo);
    omLockOpr.stop();

    omLockOpr.start();
    omLockInfo = omLockOpr.obsLock("bucket", "testkey");
    omLockOpr.monitor();
    omLockOpr.writeUnlock(omLockInfo);
    omLockOpr.stop();
  }
}

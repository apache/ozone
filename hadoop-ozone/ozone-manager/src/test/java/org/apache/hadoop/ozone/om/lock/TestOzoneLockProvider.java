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

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.when;

/**
 * Test for OzoneLockProvider.
 */
@RunWith(Parameterized.class)
public class TestOzoneLockProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneLockProvider.class);

  private OzoneManager ozoneManager;
  private OzoneLockStrategy ozoneLockStrategy;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  public TestOzoneLockProvider(boolean setKeyPathLock,
                               boolean setFileSystemPaths) {
    // Ignored. Actual init done in initParam().
    // This empty constructor is still required to avoid argument exception.
  }

  @Parameterized.BeforeParam
  public static void initParam(boolean setKeyPathLock,
                               boolean setFileSystemPaths) {
    keyPathLockEnabled = setKeyPathLock;
    enableFileSystemPaths = setFileSystemPaths;
  }

  private static boolean keyPathLockEnabled;
  private static boolean enableFileSystemPaths;

  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
  }

  @Test
  public void testOzoneLockProvider() {
    for (BucketLayout bucketLayout : BucketLayout.values()) {
      testOzoneLockProviderUtil(bucketLayout);
    }
  }

  public void testOzoneLockProviderUtil(BucketLayout bucketLayout) {

    LOG.info("keyPathLockEnabled: " + keyPathLockEnabled);
    LOG.info("enableFileSystemPaths: " + enableFileSystemPaths);
    LOG.info("bucketLayout: " + bucketLayout + "\n");

    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(keyPathLockEnabled, enableFileSystemPaths));
    ozoneLockStrategy =
        ozoneManager.getOzoneLockProvider().createLockStrategy(bucketLayout);

    if (keyPathLockEnabled) {
      if (bucketLayout == BucketLayout.OBJECT_STORE) {
        Assert.assertTrue(ozoneLockStrategy instanceof OBSKeyPathLockStrategy);
      } else if (!enableFileSystemPaths &&
          bucketLayout == BucketLayout.LEGACY) {
        Assert.assertTrue(ozoneLockStrategy instanceof OBSKeyPathLockStrategy);
      }
    } else {
      Assert.assertTrue(ozoneLockStrategy instanceof RegularBucketLockStrategy);
    }
  }
}

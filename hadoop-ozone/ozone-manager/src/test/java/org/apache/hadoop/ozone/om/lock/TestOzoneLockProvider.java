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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for OzoneLockProvider.
 */
public class TestOzoneLockProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneLockProvider.class);

  private OzoneManager ozoneManager;

  private boolean keyPathLockEnabled;
  private boolean enableFileSystemPaths;

  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testOzoneLockProvider(boolean setKeyPathLock,
                                    boolean setFileSystemPaths) {
    this.keyPathLockEnabled = setKeyPathLock;
    this.enableFileSystemPaths = setFileSystemPaths;
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
    OzoneLockStrategy ozoneLockStrategy = ozoneManager.getOzoneLockProvider().createLockStrategy(bucketLayout);

    if (keyPathLockEnabled) {
      if (bucketLayout == BucketLayout.OBJECT_STORE) {
        assertInstanceOf(OBSKeyPathLockStrategy.class, ozoneLockStrategy);
      } else if (!enableFileSystemPaths &&
          bucketLayout == BucketLayout.LEGACY) {
        assertInstanceOf(OBSKeyPathLockStrategy.class, ozoneLockStrategy);
      }
    } else {
      assertInstanceOf(RegularBucketLockStrategy.class, ozoneLockStrategy);
    }
  }
}

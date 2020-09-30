/*
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

package org.apache.hadoop.ozone.container.common.volume;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

import static org.apache.hadoop.test.GenericTestUtils.getTestDir;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link RoundRobinVolumeChoosingPolicy}.
 */
public class TestRoundRobinVolumeChoosingPolicy {

  private RoundRobinVolumeChoosingPolicy policy;
  private final List<HddsVolume> volumes = new ArrayList<>();

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final String BASE_DIR =
      getTestDir(TestRoundRobinVolumeChoosingPolicy.class.getSimpleName())
          .getAbsolutePath();
  private static final String VOLUME_1 = BASE_DIR + "disk1";
  private static final String VOLUME_2 = BASE_DIR + "disk2";

  @Before
  public void setup() throws Exception {
    policy = new RoundRobinVolumeChoosingPolicy();

    SpaceUsageSource source1 = MockSpaceUsageSource.fixed(500, 100);
    SpaceUsageCheckFactory factory1 = MockSpaceUsageCheckFactory.of(
        source1, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
    HddsVolume vol1 = new HddsVolume.Builder(VOLUME_1)
        .conf(CONF)
        .usageCheckFactory(factory1)
        .build();
    SpaceUsageSource source2 = MockSpaceUsageSource.fixed(500, 200);
    SpaceUsageCheckFactory factory2 = MockSpaceUsageCheckFactory.of(
        source2, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
    HddsVolume vol2 = new HddsVolume.Builder(VOLUME_2)
        .conf(CONF)
        .usageCheckFactory(factory2)
        .build();

    volumes.add(vol1);
    volumes.add(vol2);
  }

  @After
  public void cleanUp() {
    volumes.forEach(HddsVolume::shutdown);
  }

  @Test
  public void testRRVolumeChoosingPolicy() throws Exception {
    HddsVolume hddsVolume1 = volumes.get(0);
    HddsVolume hddsVolume2 = volumes.get(1);

    Assert.assertEquals(100L, hddsVolume1.getAvailable());
    Assert.assertEquals(200L, hddsVolume2.getAvailable());

    // Test two rounds of round-robin choosing
    Assert.assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    Assert.assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));
    Assert.assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    Assert.assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));

    // The first volume has only 100L space, so the policy should
    // choose the second one in case we ask for more.
    Assert.assertEquals(hddsVolume2,
        policy.chooseVolume(volumes, 150));

    // Fail if no volume has enough space available
    try {
      policy.chooseVolume(volumes, Long.MAX_VALUE);
      Assert.fail();
    } catch (IOException e) {
      // Passed.
    }
  }

  @Test
  public void testRRPolicyExceptionMessage() throws Exception {
    int blockSize = 300;
    try {
      policy.chooseVolume(volumes, blockSize);
      Assert.fail("expected to throw DiskOutOfSpaceException");
    } catch(DiskOutOfSpaceException e) {
      Assert.assertEquals("Not returning the expected message",
          "Out of space: The volume with the most available space (=" + 200
              + " B) is less than the container size (=" + blockSize + " B).",
          e.getMessage());
    }
  }

}

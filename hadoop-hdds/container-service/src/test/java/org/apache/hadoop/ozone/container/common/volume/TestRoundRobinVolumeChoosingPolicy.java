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

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

import static org.apache.ozone.test.GenericTestUtils.getTestDir;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT;

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

  @BeforeEach
  public void setup() throws Exception {
    policy = new RoundRobinVolumeChoosingPolicy();

    // Use the exact capacity and availability specified in this test. Do not reserve space to prevent volumes from
    // filling up.
    CONF.setFloat(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT, 0);

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
    vol2.incCommittedBytes(50);

    volumes.add(vol1);
    volumes.add(vol2);
  }

  @AfterEach
  public void cleanUp() {
    volumes.forEach(HddsVolume::shutdown);
    FileUtil.fullyDelete(new File(VOLUME_1));
    FileUtil.fullyDelete(new File(VOLUME_2));
  }

  @Test
  public void testRRVolumeChoosingPolicy() throws Exception {
    HddsVolume hddsVolume1 = volumes.get(0);
    HddsVolume hddsVolume2 = volumes.get(1);

    Assertions.assertEquals(100L, hddsVolume1.getAvailable());
    Assertions.assertEquals(200L, hddsVolume2.getAvailable());

    // Test two rounds of round-robin choosing
    Assertions.assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    Assertions.assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));
    Assertions.assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    Assertions.assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));

    // The first volume has only 100L space, so the policy should
    // choose the second one in case we ask for more.
    Assertions.assertEquals(hddsVolume2,
        policy.chooseVolume(volumes, 120));
  }

  @Test
  public void throwsDiskOutOfSpaceIfRequestMoreThanAvailable() {
    Exception e = Assertions.assertThrows(DiskOutOfSpaceException.class,
        () -> policy.chooseVolume(volumes, 300));

    String msg = e.getMessage();
    assertTrue(
        msg.contains("No volumes have enough space for a new container.  " +
            "Most available space: 150 bytes"),
        msg);
  }

}

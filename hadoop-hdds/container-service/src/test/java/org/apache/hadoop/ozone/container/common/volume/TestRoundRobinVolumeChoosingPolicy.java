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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests {@link RoundRobinVolumeChoosingPolicy}.
 */
public class TestRoundRobinVolumeChoosingPolicy {

  private RoundRobinVolumeChoosingPolicy policy;
  private final List<HddsVolume> volumes = new ArrayList<>();

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  @TempDir
  private Path baseDir;

  @BeforeEach
  public void setup() throws Exception {
    String volume1 = baseDir + "disk1";
    String volume2 = baseDir + "disk2";
    policy = new RoundRobinVolumeChoosingPolicy();

    // Use the exact capacity and availability specified in this test. Do not reserve space to prevent volumes from
    // filling up.
    CONF.setFloat(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT, 0);

    SpaceUsageSource source1 = MockSpaceUsageSource.fixed(500, 100);
    SpaceUsageCheckFactory factory1 = MockSpaceUsageCheckFactory.of(
        source1, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
    HddsVolume vol1 = new HddsVolume.Builder(volume1)
        .conf(CONF)
        .usageCheckFactory(factory1)
        .build();
    SpaceUsageSource source2 = MockSpaceUsageSource.fixed(500, 200);
    SpaceUsageCheckFactory factory2 = MockSpaceUsageCheckFactory.of(
        source2, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
    HddsVolume vol2 = new HddsVolume.Builder(volume2)
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
  }

  @Test
  public void testRRVolumeChoosingPolicy() throws Exception {
    HddsVolume hddsVolume1 = volumes.get(0);
    HddsVolume hddsVolume2 = volumes.get(1);

    assertEquals(100L, hddsVolume1.getCurrentUsage().getAvailable());
    assertEquals(200L, hddsVolume2.getCurrentUsage().getAvailable());

    // Test two rounds of round-robin choosing
    assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));
    assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));

    // The first volume has only 100L space, so the policy should
    // choose the second one in case we ask for more.
    assertEquals(hddsVolume2,
        policy.chooseVolume(volumes, 120));
  }

  @Test
  public void throwsDiskOutOfSpaceIfRequestMoreThanAvailable() {
    Exception e = assertThrows(DiskOutOfSpaceException.class,
        () -> policy.chooseVolume(volumes, 300));

    String msg = e.getMessage();
    assertThat(msg).contains("No volumes have enough space for a new container.  " +
        "Most available space: 140 bytes");
  }

  @Test
  public void testVolumeCommittedSpace() throws Exception {
    Map<HddsVolume, Long> initialCommittedSpace = new HashMap<>();
    volumes.forEach(vol ->
        initialCommittedSpace.put(vol, vol.getCommittedBytes()));

    HddsVolume selectedVolume = policy.chooseVolume(volumes, 50);

    assertEquals(initialCommittedSpace.get(selectedVolume) + 50,
        selectedVolume.getCommittedBytes());
  }
}

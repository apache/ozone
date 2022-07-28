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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ozone.test.GenericTestUtils.getTestDir;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link CapacityVolumeChoosingPolicy}.
 */
public class TestCapacityVolumeChoosingPolicy {

  private CapacityVolumeChoosingPolicy policy;
  private final List<HddsVolume> volumes = new ArrayList<>();

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final String BASE_DIR =
      getTestDir(TestCapacityVolumeChoosingPolicy.class.getSimpleName())
          .getAbsolutePath();
  private static final String VOLUME_1 = BASE_DIR + "disk1";
  private static final String VOLUME_2 = BASE_DIR + "disk2";
  private static final String VOLUME_3 = BASE_DIR + "disk3";

  @Before
  public void setup() throws Exception {
    policy = new CapacityVolumeChoosingPolicy();

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
    SpaceUsageSource source3 = MockSpaceUsageSource.fixed(500, 300);
    SpaceUsageCheckFactory factory3 = MockSpaceUsageCheckFactory.of(
        source3, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
    HddsVolume vol3 = new HddsVolume.Builder(VOLUME_3)
        .conf(CONF)
        .usageCheckFactory(factory3)
        .build();
    vol3.incCommittedBytes(50);

    volumes.add(vol1);
    volumes.add(vol2);
    volumes.add(vol3);
  }

  @After
  public void cleanUp() {
    volumes.forEach(HddsVolume::shutdown);
  }

  @Test
  public void testCapacityVolumeChoosingPolicy() throws Exception {
    HddsVolume hddsVolume1 = volumes.get(0);
    HddsVolume hddsVolume2 = volumes.get(1);
    HddsVolume hddsVolume3 = volumes.get(2);

    Assert.assertEquals(100L, hddsVolume1.getAvailable());
    Assert.assertEquals(200L, hddsVolume2.getAvailable());
    Assert.assertEquals(300L, hddsVolume3.getAvailable());

    Map<HddsVolume, Integer> chooseCount = new HashMap<>();
    chooseCount.put(hddsVolume1, 0);
    chooseCount.put(hddsVolume2, 0);
    chooseCount.put(hddsVolume3, 0);

    // Test 1000 rounds of volume choosing
    for (int i = 0; i < 1000; i++) {
      HddsVolume volume = policy.chooseVolume(volumes, 0);
      chooseCount.put(volume, chooseCount.get(volume) + 1);
    }

    Assert.assertTrue(chooseCount.get(hddsVolume3) >
        chooseCount.get(hddsVolume1));
    Assert.assertTrue(chooseCount.get(hddsVolume3) >
        chooseCount.get(hddsVolume2));
  }

  @Test
  public void throwsDiskOutOfSpaceIfRequestMoreThanAvailable() {
    Exception e = Assert.assertThrows(DiskOutOfSpaceException.class,
        () -> policy.chooseVolume(volumes, 500));

    String msg = e.getMessage();
    assertTrue(msg,
        msg.contains("No volumes have enough space for a new container.  " +
            "Most available space: 250 bytes"));
  }

}

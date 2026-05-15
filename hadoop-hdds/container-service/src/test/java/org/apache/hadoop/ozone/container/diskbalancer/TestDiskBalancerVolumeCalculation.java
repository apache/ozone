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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for disk balancer volume calculations.
 */
class TestDiskBalancerVolumeCalculation {

  @TempDir
  private Path tempDir;

  @Test
  void getIdealUsageReturnsZeroForEmptyVolumeList() {
    assertEquals(0.0, DiskBalancerVolumeCalculation.getIdealUsage(
        Collections.emptyList()));
  }

  @Test
  void getIdealUsageReturnsZeroForZeroTotalCapacity() throws IOException {
    HddsVolume zeroCapacityVolume = createVolume("zero-capacity", 0, 0);

    assertEquals(0.0, DiskBalancerVolumeCalculation.getIdealUsage(
        Collections.singletonList(
            DiskBalancerVolumeCalculation.newVolumeFixedUsage(
                zeroCapacityVolume, null))));
  }

  @Test
  void getIdealUsageRejectsNegativeCapacity() throws IOException {
    HddsVolume negativeCapacityVolume = createVolume(
        "negative-capacity", -1, 0);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> DiskBalancerVolumeCalculation.getIdealUsage(
            Collections.singletonList(
                DiskBalancerVolumeCalculation.newVolumeFixedUsage(
                    negativeCapacityVolume, null))));

    assertEquals("Negative capacity = -1: " + negativeCapacityVolume,
        exception.getMessage());
  }

  @Test
  void getIdealUsageRejectsNegativeEffectiveUsed() throws IOException {
    HddsVolume volume = createVolume("negative-effective-used", 100, 100);
    DiskBalancerVolumeCalculation.VolumeFixedUsage volumeUsage =
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            volume, Collections.singletonMap(volume, -1L));

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> DiskBalancerVolumeCalculation.getIdealUsage(
            Collections.singletonList(volumeUsage)));

    assertEquals("Negative effective used = " + volumeUsage.getEffectiveUsed()
        + ": " + volume, exception.getMessage());
  }

  @Test
  void getIdealUsageRejectsEffectiveUsedGreaterThanCapacity()
      throws IOException {
    HddsVolume volume = createVolume("effective-used-exceeds-capacity", 100, 0);
    DiskBalancerVolumeCalculation.VolumeFixedUsage volumeUsage =
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            volume, Collections.singletonMap(volume, 1L));

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> DiskBalancerVolumeCalculation.getIdealUsage(
            Collections.singletonList(volumeUsage)));

    assertEquals("Effective used = " + volumeUsage.getEffectiveUsed()
        + " > capacity = " + volumeUsage.getUsage().getCapacity() + ": "
        + volume, exception.getMessage());
  }

  private HddsVolume createVolume(String name, long capacity, long available)
      throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    SpaceUsageSource source = MockSpaceUsageSource.fixed(capacity, available);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        source, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);

    return new HddsVolume.Builder(tempDir.resolve(name).toString())
        .conf(conf)
        .usageCheckFactory(factory)
        .build();
  }
}

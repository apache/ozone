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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.junit.jupiter.api.Test;

class TestVolumeInfoMetrics {

  @Test
  void testVolumeInfoMetricsExposeOzoneAndFilesystemGauges() {
    HddsVolume volume = mock(HddsVolume.class);
    when(volume.getStorageType()).thenReturn(StorageType.DISK);
    when(volume.getStorageDir()).thenReturn(new File("/tmp/vol-1"));
    when(volume.getDatanodeUuid()).thenReturn("dn-1");
    when(volume.getLayoutVersion()).thenReturn(1);
    when(volume.getStorageState()).thenReturn(HddsVolume.VolumeState.NORMAL);
    when(volume.getType()).thenReturn(HddsVolume.VolumeType.DATA_VOLUME);
    when(volume.getCommittedBytes()).thenReturn(10L);
    when(volume.getContainers()).thenReturn(3L);
    when(volume.getReportedFreeSpaceToSpare(anyLong())).thenReturn(20L);
    when(volume.getFreeSpaceToSpare(anyLong())).thenReturn(15L);

    VolumeUsage volumeUsage = mock(VolumeUsage.class);
    when(volume.getVolumeUsage()).thenReturn(volumeUsage);

    when(volumeUsage.getReservedInBytes()).thenReturn(50L);

    // Raw filesystem stats (used = Ozone DU usage on disk)
    SpaceUsageSource.Fixed fsUsage = new SpaceUsageSource.Fixed(2000L, 1100L, 500L);
    when(volumeUsage.realUsage()).thenReturn(fsUsage);

    // getCurrentUsage(real) preserves real.getUsedSpace(); capacity/available are adjusted for reserved
    when(volumeUsage.getCurrentUsage(any())).thenReturn(new SpaceUsageSource.Fixed(
        1950L,  // fsCapacity - reserved
        1100L,
        500L    // same as fsUsage.getUsedSpace()
    ));

    VolumeInfoMetrics metrics = new VolumeInfoMetrics("test-vol-1", volume);
    try {
      MetricsCollectorImpl collector = new MetricsCollectorImpl();
      metrics.getMetrics(collector, true);
      assertThat(collector.getRecords()).hasSize(1);

      MetricsRecordImpl rec = collector.getRecords().get(0);
      Iterable<AbstractMetric> all = rec.metrics();

      assertThat(findMetric(all, "OzoneCapacity")).isEqualTo(1950L);
      assertThat(findMetric(all, "OzoneAvailable")).isEqualTo(1100L);
      assertThat(findMetric(all, "OzoneUsed")).isEqualTo(500L);

      assertThat(findMetric(all, "FilesystemCapacity")).isEqualTo(2000L);
      assertThat(findMetric(all, "FilesystemAvailable")).isEqualTo(1100L);
      assertThat(findMetric(all, "FilesystemUsed")).isEqualTo(900L); // FilesystemCapacity - FilesystemAvailable

      assertThat(findMetric(all, "MinFreeSpace")).isEqualTo(20L);
      assertThat(findMetric(all, "HardMinFreeSpace")).isEqualTo(15L);
      // NonOzoneUsed = FilesystemUsed - OzoneUsed = 900 - 500
      assertThat(findMetric(all, "NonOzoneUsed")).isEqualTo(400L);
    } finally {
      metrics.unregister();
    }
  }

  private static long findMetric(Iterable<AbstractMetric> metrics, String name) {
    for (AbstractMetric m : metrics) {
      if (name.equals(m.name())) {
        return m.value().longValue();
      }
    }
    throw new AssertionError("Missing metric: " + name);
  }
}


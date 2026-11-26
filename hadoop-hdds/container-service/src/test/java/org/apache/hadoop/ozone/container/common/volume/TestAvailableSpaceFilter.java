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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AvailableSpaceFilter}.
 */
public class TestAvailableSpaceFilter {

  @Test
  public void testIncrementsSoftBandWhenBetweenReportedAndHard() {
    HddsVolume volume = mock(HddsVolume.class);
    VolumeInfoMetrics metrics = mock(VolumeInfoMetrics.class);
    StorageLocationReport report = mock(StorageLocationReport.class);
    when(volume.getReport()).thenReturn(report);
    when(report.getCapacity()).thenReturn(1000L);
    when(report.getRemaining()).thenReturn(100L);
    when(report.getCommitted()).thenReturn(0L);
    when(report.getUsableSpace()).thenReturn(0L);
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getVolumeInfoStats()).thenReturn(metrics);

    AvailableSpaceFilter filter = new AvailableSpaceFilter(50L);
    assertTrue(filter.test(volume));

    verify(metrics).incNumContainerCreateRequestsInSoftBandMinFreeSpace();
    verify(metrics, never()).incNumContainerCreateRequestsRejectedHardMinFreeSpace();
  }

  @Test
  public void testIncrementsHardRejectWhenHardLimitViolated() {
    HddsVolume volume = mock(HddsVolume.class);
    VolumeInfoMetrics metrics = mock(VolumeInfoMetrics.class);
    StorageLocationReport report = mock(StorageLocationReport.class);
    when(volume.getReport()).thenReturn(report);
    when(report.getCapacity()).thenReturn(1000L);
    when(report.getRemaining()).thenReturn(100L);
    when(report.getCommitted()).thenReturn(0L);
    when(report.getUsableSpace()).thenReturn(0L);
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getVolumeInfoStats()).thenReturn(metrics);

    AvailableSpaceFilter filter = new AvailableSpaceFilter(80L);
    assertFalse(filter.test(volume));

    verify(metrics).incNumContainerCreateRequestsRejectedHardMinFreeSpace();
    verify(metrics, never()).incNumContainerCreateRequestsInSoftBandMinFreeSpace();
  }

  @Test
  public void testNoMetricIncrementWhenWellAboveSoftBand() {
    HddsVolume volume = mock(HddsVolume.class);
    VolumeInfoMetrics metrics = mock(VolumeInfoMetrics.class);
    StorageLocationReport report = mock(StorageLocationReport.class);
    when(volume.getReport()).thenReturn(report);
    when(report.getCapacity()).thenReturn(1000L);
    when(report.getRemaining()).thenReturn(1000L);
    when(report.getCommitted()).thenReturn(0L);
    when(report.getUsableSpace()).thenReturn(900L);
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getVolumeInfoStats()).thenReturn(metrics);

    AvailableSpaceFilter filter = new AvailableSpaceFilter(50L);
    assertTrue(filter.test(volume));

    verify(metrics, never()).incNumContainerCreateRequestsInSoftBandMinFreeSpace();
    verify(metrics, never()).incNumContainerCreateRequestsRejectedHardMinFreeSpace();
  }

  /**
   * Without committed bytes: remaining(200) - hardSpare(30) = 170 > requiredSpace(60),
   * and 200 - softSpare(100) = 100 > 60 → well above both limits, no metric.
   * With committed(80): 200 - 80 - hardSpare(30) = 90 > 60 → still passes hard,
   * but 200 - 80 - softSpare(100) = 20 ≤ 60 → now inside the soft band.
   * Committed bytes representing in-flight pipeline allocations push the volume into
   * the soft band even though raw remaining space looks healthy.
   */
  @Test
  public void testCommittedBytesCanPushVolumeIntoSoftBand() {
    HddsVolume volume = mock(HddsVolume.class);
    VolumeInfoMetrics metrics = mock(VolumeInfoMetrics.class);
    StorageLocationReport report = mock(StorageLocationReport.class);
    when(volume.getReport()).thenReturn(report);
    when(report.getCapacity()).thenReturn(1000L);
    when(report.getRemaining()).thenReturn(200L);
    when(report.getCommitted()).thenReturn(80L);
    when(report.getUsableSpace()).thenReturn(20L);
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getVolumeInfoStats()).thenReturn(metrics);

    // available = 200 - 80 - 30 = 90 > requiredSpace(60) → passes hard check
    // getUsableSpace = 200 - 80 - spareOnReport(100) = 20 <= 60 → inside soft band
    AvailableSpaceFilter filter = new AvailableSpaceFilter(60L);
    assertTrue(filter.test(volume));

    verify(metrics).incNumContainerCreateRequestsInSoftBandMinFreeSpace();
    verify(metrics, never()).incNumContainerCreateRequestsRejectedHardMinFreeSpace();
  }

  /**
   * Committed bytes representing in-flight pipeline allocations cause a hard reject
   * that would not occur if committed were zero.
   * remaining(130) - committed(80) - hardSpare(30) = 20 < requiredSpace(50) → rejected.
   * Without committed: 130 - 0 - 30 = 100 > 50 → would have passed.
   */
  @Test
  public void testCommittedBytesCanCauseHardReject() {
    HddsVolume volume = mock(HddsVolume.class);
    VolumeInfoMetrics metrics = mock(VolumeInfoMetrics.class);
    StorageLocationReport report = mock(StorageLocationReport.class);
    when(volume.getReport()).thenReturn(report);
    when(report.getCapacity()).thenReturn(1000L);
    when(report.getRemaining()).thenReturn(130L);
    when(report.getCommitted()).thenReturn(80L);
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getVolumeInfoStats()).thenReturn(metrics);

    // available = 130 - 80 - 30 = 20 < requiredSpace(50) → hard rejected
    AvailableSpaceFilter filter = new AvailableSpaceFilter(50L);
    assertFalse(filter.test(volume));

    verify(metrics).incNumContainerCreateRequestsRejectedHardMinFreeSpace();
    verify(metrics, never()).incNumContainerCreateRequestsInSoftBandMinFreeSpace();
  }

  /**
   * Even with non-zero committed bytes, if remaining space is large enough,
   * the volume remains well above both limits and no metric fires.
   * remaining(300) - committed(50) - hardSpare(30) = 220 > requiredSpace(50): passes hard.
   * 300 - 50 - softSpare(100) = 150 > 50: not in soft band.
   */
  @Test
  public void testCommittedBytesDoNotAffectMetricsWhenVolumeStillHealthy() {
    HddsVolume volume = mock(HddsVolume.class);
    VolumeInfoMetrics metrics = mock(VolumeInfoMetrics.class);
    StorageLocationReport report = mock(StorageLocationReport.class);
    when(volume.getReport()).thenReturn(report);
    when(report.getCapacity()).thenReturn(1000L);
    when(report.getRemaining()).thenReturn(300L);
    when(report.getCommitted()).thenReturn(50L);
    when(report.getUsableSpace()).thenReturn(150L);
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getVolumeInfoStats()).thenReturn(metrics);

    // available = 300 - 50 - 30 = 220 > 50; getUsableSpace = 300 - 50 - 100 = 150 > 50
    AvailableSpaceFilter filter = new AvailableSpaceFilter(50L);
    assertTrue(filter.test(volume));

    verify(metrics, never()).incNumContainerCreateRequestsInSoftBandMinFreeSpace();
    verify(metrics, never()).incNumContainerCreateRequestsRejectedHardMinFreeSpace();
  }
}

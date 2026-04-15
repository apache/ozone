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
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getReportedFreeSpaceToSpare(1000L)).thenReturn(100L);
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
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getReportedFreeSpaceToSpare(1000L)).thenReturn(100L);
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
    when(volume.getFreeSpaceToSpare(1000L)).thenReturn(30L);
    when(volume.getReportedFreeSpaceToSpare(1000L)).thenReturn(100L);
    when(volume.getVolumeInfoStats()).thenReturn(metrics);

    AvailableSpaceFilter filter = new AvailableSpaceFilter(50L);
    assertTrue(filter.test(volume));

    verify(metrics, never()).getNumContainerCreateRequestsInSoftBandMinFreeSpace();
    verify(metrics, never()).incNumWriteRequestsRejectedHardMinFreeSpace();
  }
}

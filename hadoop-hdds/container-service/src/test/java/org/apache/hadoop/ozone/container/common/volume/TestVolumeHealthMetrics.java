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

import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for VolumeHealthMetrics.
 */
class TestVolumeHealthMetrics {

  private MetricsSystem metrics;
  private VolumeSet mockVolumeSet;
  private List<StorageVolume> volumes;
  private VolumeHealthMetrics volumeHealthMetrics;

  @BeforeEach
  void setUp() {
    metrics = DefaultMetricsSystem.instance();
    metrics.init("test");

    mockVolumeSet = mock(VolumeSet.class);
    volumes = new ArrayList<>();

    volumeHealthMetrics = VolumeHealthMetrics.create(StorageVolume.VolumeType.DATA_VOLUME, mockVolumeSet);
  }

  @AfterEach
  void tearDown() {
    if (volumeHealthMetrics != null) {
      volumeHealthMetrics.unregister();
    }

    metrics.stop();
    metrics.shutdown();
  }

  private StorageVolume createMockVolume(StorageVolume.VolumeState state) {
    StorageVolume volume = mock(StorageVolume.class);
    when(volume.getStorageState()).thenReturn(state);
    return volume;
  }

  /**
   * Test metrics with empty volume list.
   */
  @Test
  void testEmptyVolumeList() {
    when(mockVolumeSet.getVolumesList()).thenReturn(new ArrayList<>());

    MetricsRecordBuilder metricsRecords = getMetrics(volumeHealthMetrics);

    assertGauge("TotalVolumes", 0, metricsRecords);
    assertGauge("NumHealthyVolumes", 0, metricsRecords);
    assertGauge("NumFailedVolumes", 0, metricsRecords);
  }
  
  /**
   * Test that metrics are correctly initialized and reported.
   */
  @Test
  void testBasicMetrics() {
    StorageVolume normalVolume1 = createMockVolume(StorageVolume.VolumeState.NORMAL);
    StorageVolume normalVolume2 = createMockVolume(StorageVolume.VolumeState.NORMAL);
    StorageVolume failedVolume = createMockVolume(StorageVolume.VolumeState.FAILED);

    volumes.add(normalVolume1);
    volumes.add(normalVolume2);
    volumes.add(failedVolume);

    when(mockVolumeSet.getVolumesList()).thenReturn(volumes);

    MetricsRecordBuilder metricsRecords = getMetrics(volumeHealthMetrics);

    assertGauge("TotalVolumes", 3, metricsRecords);
    assertGauge("NumHealthyVolumes", 2, metricsRecords);
    assertGauge("NumFailedVolumes", 1, metricsRecords);
  }

  /**
   * Test that metrics are updated when volume states change.
   */
  @Test
  void testMetricsUpdateOnStateChange() {
    StorageVolume volume1 = createMockVolume(StorageVolume.VolumeState.NORMAL);
    StorageVolume volume2 = createMockVolume(StorageVolume.VolumeState.NORMAL);

    volumes.add(volume1);
    volumes.add(volume2);

    when(mockVolumeSet.getVolumesList()).thenReturn(volumes);

    // Verify initial metrics
    MetricsRecordBuilder initialMetrics = getMetrics(volumeHealthMetrics);
    assertGauge("TotalVolumes", 2, initialMetrics);
    assertGauge("NumHealthyVolumes", 2, initialMetrics);
    assertGauge("NumFailedVolumes", 0, initialMetrics);

    when(volume1.getStorageState()).thenReturn(StorageVolume.VolumeState.FAILED);

    // Verify updated metrics
    MetricsRecordBuilder updatedMetrics = getMetrics(volumeHealthMetrics);
    assertGauge("TotalVolumes", 2, updatedMetrics);
    assertGauge("NumHealthyVolumes", 1, updatedMetrics);
    assertGauge("NumFailedVolumes", 1, updatedMetrics);

    when(volume1.getStorageState()).thenReturn(StorageVolume.VolumeState.NORMAL);

    // Verify metrics are back to initial state
    MetricsRecordBuilder finalMetrics = getMetrics(volumeHealthMetrics);
    assertGauge("TotalVolumes", 2, finalMetrics);
    assertGauge("NumHealthyVolumes", 2, finalMetrics);
    assertGauge("NumFailedVolumes", 0, finalMetrics);
  }

  /**
   * Test metrics with volumes in states other than NORMAL or FAILED.
   */
  @Test
  void testOtherVolumeStates() {
    StorageVolume normalVolume = createMockVolume(StorageVolume.VolumeState.NORMAL);
    StorageVolume failedVolume = createMockVolume(StorageVolume.VolumeState.FAILED);
    StorageVolume nonExistentVolume = createMockVolume(StorageVolume.VolumeState.NON_EXISTENT);
    StorageVolume inconsistentVolume = createMockVolume(StorageVolume.VolumeState.INCONSISTENT);

    volumes.add(normalVolume);
    volumes.add(failedVolume);
    volumes.add(nonExistentVolume);
    volumes.add(inconsistentVolume);

    when(mockVolumeSet.getVolumesList()).thenReturn(volumes);

    MetricsRecordBuilder metricsRecords = getMetrics(volumeHealthMetrics);

    // Verify metrics values - only NORMAL and FAILED state volumes should be counted
    assertGauge("TotalVolumes", 4, metricsRecords);
    assertGauge("NumHealthyVolumes", 1, metricsRecords);
    assertGauge("NumFailedVolumes", 1, metricsRecords);
  }
}

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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getHealthyDataScanResult;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyMetadataScanResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for the background container metadata scanner.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestBackgroundContainerMetadataScanner extends
    TestContainerScannersAbstract {

  private BackgroundContainerMetadataScanner scanner;

  @Override
  @BeforeEach
  public void setup() {
    super.setup();
    scanner = new BackgroundContainerMetadataScanner(conf, controller);
  }

  @Test
  @Override
  public void testRecentlyScannedContainerIsSkipped() throws Exception {
    // If the last scan time is before than the configured gap, the container
    // should be scanned.
    setScannedTimestampRecent(healthy);
    scanner.runIteration();
    verify(healthy, never()).scanMetaData();
  }

  @Test
  @Override
  public void testPreviouslyScannedContainerIsScanned() throws Exception {
    setScannedTimestampOld(healthy);
    scanner.runIteration();
    verify(healthy, atLeastOnce()).scanMetaData();
  }

  @Test
  @Override
  public void testUnscannedContainerIsScanned() throws Exception {
    // If there is no last scanned time, the container should be scanned.
    when(healthy.getContainerData().lastDataScanTime())
        .thenReturn(Optional.empty());
    scanner.runIteration();
    verify(healthy, atLeastOnce()).scanMetaData();
  }

  @Test
  @Override
  public void testScannerMetrics() {
    scanner.runIteration();

    ContainerMetadataScannerMetrics metrics = scanner.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(4, metrics.getNumContainersScanned());
    assertEquals(0, metrics.getNumUnHealthyContainers());
  }

  @Test
  @Override
  public void testScannerMetricsUnregisters() {
    String name = scanner.getMetrics().getName();

    assertNotNull(DefaultMetricsSystem.instance().getSource(name));

    scanner.shutdown();
    scanner.run();

    assertNull(DefaultMetricsSystem.instance().getSource(name));
  }

  @Test
  @Override
  public void testUnhealthyContainersDetected() throws Exception {
    scanner.runIteration();
    verifyContainerMarkedUnhealthy(healthy, never());
    // Metadata scanner cannot detect data corruption.
    verifyContainerMarkedUnhealthy(corruptData, never());
    verifyContainerMarkedUnhealthy(openCorruptMetadata, atLeastOnce());
    verifyContainerMarkedUnhealthy(openContainer, never());
  }

  @Test
  @Override
  public void testUnhealthyContainersTriggersVolumeScan() throws Exception {
    when(controller.markContainerUnhealthy(anyLong(), any(ScanResult.class))).thenReturn(true);
    try (MockedStatic<StorageVolumeUtil> mockedStatic = mockStatic(StorageVolumeUtil.class)) {
      scanner.runIteration();
      verifyContainerMarkedUnhealthy(openCorruptMetadata, atLeastOnce());
      mockedStatic.verify(() ->
          StorageVolumeUtil.onFailure(openCorruptMetadata.getContainerData().getVolume()), times(1));
    }
  }

  @Test
  @Override
  public void testUnhealthyContainerRescanned() throws Exception {
    Container<?> unhealthy = mockKeyValueContainer();
    when(unhealthy.scanMetaData()).thenReturn(getUnhealthyMetadataScanResult());
    when(unhealthy.scanData(
        any(DataTransferThrottler.class), any(Canceler.class)))
        .thenReturn(getHealthyDataScanResult());
    when(controller.markContainerUnhealthy(eq(unhealthy.getContainerData().getContainerID()),
        any())).thenReturn(true);
    setContainers(unhealthy, healthy);

    // First iteration should find the unhealthy container.
    scanner.runIteration();
    verifyContainerMarkedUnhealthy(unhealthy, atMostOnce());
    ContainerMetadataScannerMetrics metrics = scanner.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(2, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());

    // The unhealthy container should have been moved to the unhealthy state.
    verify(unhealthy.getContainerData(), atMostOnce())
        .setState(UNHEALTHY);
    // Update the mock to reflect this.
    when(unhealthy.getContainerState()).thenReturn(UNHEALTHY);
    when(controller.markContainerUnhealthy(eq(unhealthy.getContainerData().getContainerID()),
        any())).thenReturn(false);
    scanner.runIteration();
    // The invocation of unhealthy on this container will also happen in the
    // next iteration.
    verifyContainerMarkedUnhealthy(unhealthy, atMost(2));
    // This iteration should skip the already unhealthy container.
    assertEquals(2, metrics.getNumScanIterations());
    assertEquals(4, metrics.getNumContainersScanned());
    // numUnHealthyContainers metrics is not incremented in the 2nd iteration.
    assertEquals(1, metrics.getNumUnHealthyContainers());
  }

  /**
   * Metadata scanner should not update container checksum, so any errors that may be injected here should have no
   * effect.
   */
  @Test
  @Override
  public void testChecksumUpdateFailure() throws Exception {
    doThrow(new IOException("Checksum update error for testing")).when(controller)
        .updateContainerChecksum(anyLong(), any());
    scanner.runIteration();
    verifyContainerMarkedUnhealthy(openCorruptMetadata, atMostOnce());
    verify(openCorruptMetadata.getContainerData(), atMostOnce()).setState(UNHEALTHY);
  }

  /**
   * A datanode will have one metadata scanner thread for the whole process.
   * When a volume fails, any the containers queued for scanning in that volume
   * should be skipped but the thread will continue to run.
   */
  @Test
  @Override
  public void testWithVolumeFailure() throws Exception {
    when(vol.isFailed()).thenReturn(true);

    ContainerMetadataScannerMetrics metrics = scanner.getMetrics();
    // Start metadata scanner thread in the background.
    scanner.start();
    // Wait for at least one iteration to complete.
    GenericTestUtils.waitFor(() -> metrics.getNumScanIterations() >= 1, 1000,
        5000);
    // Volume health should have been checked.
    // TODO: remove the mock return value assertion after we upgrade to spotbugs 4.8 up
    assertFalse(verify(vol, atLeastOnce()).isFailed());
    // Scanner should not have shutdown when it encountered the failed volume.
    assertTrue(scanner.isAlive());

    // Now explicitly shutdown the scanner.
    scanner.shutdown();
    // Wait for shutdown to finish.
    GenericTestUtils.waitFor(() -> !scanner.isAlive(), 1000, 5000);

    // All containers were on the failed volume, so they should not have
    // been scanned.
    assertEquals(0, metrics.getNumContainersScanned());
    assertEquals(0, metrics.getNumUnHealthyContainers());
    verify(healthy, never()).scanMetaData();
    verify(openContainer, never()).scanMetaData();
    verify(corruptData, never()).scanMetaData();
    verify(openCorruptMetadata, never()).scanMetaData();
  }

  @Test
  @Override
  public void testShutdownDuringScan() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    // Make the metadata scan block until interrupt.
    when(healthy.scanMetaData()).then(i -> {
      latch.countDown();
      Thread.sleep(Duration.ofDays(1).toMillis());
      return null;
    });

    scanner.start();
    // Wait for the scanner to reach the healthy container.
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    // Terminate the scanner while it is blocked scanning the healthy container.
    scanner.shutdown();
    // The container should remain healthy.
    verifyContainerMarkedUnhealthy(healthy, never());
  }
}

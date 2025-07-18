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
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getHealthyMetadataScanResult;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyDataScanResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
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
 * Unit tests for the background container data scanner.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestBackgroundContainerDataScanner extends
    TestContainerScannersAbstract {

  private BackgroundContainerDataScanner scanner;

  @Override
  @BeforeEach
  public void setup() {
    super.setup();
    scanner = new BackgroundContainerDataScanner(conf, controller, vol);
  }

  @Test
  @Override
  public void testRecentlyScannedContainerIsSkipped() throws Exception {
    setScannedTimestampRecent(healthy);
    scanner.runIteration();
    verify(healthy, never()).scanData(any(), any());
  }

  @Test
  @Override
  public void testPreviouslyScannedContainerIsScanned() throws Exception {
    // If the last scan time is before than the configured gap, the container
    // should be scanned.
    setScannedTimestampOld(healthy);
    scanner.runIteration();
    verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @Test
  @Override
  public void testUnscannedContainerIsScanned() throws Exception {
    // If there is no last scanned time, the container should be scanned.
    when(healthy.getContainerData().lastDataScanTime())
        .thenReturn(Optional.empty());
    scanner.runIteration();
    verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @Test
  @Override
  public void testScannerMetrics() {
    scanner.runIteration();

    ContainerDataScannerMetrics metrics = scanner.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(2, metrics.getNumContainersScanned());
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
    verifyContainerMarkedUnhealthy(corruptData, atLeastOnce());
    verifyContainerMarkedUnhealthy(openCorruptMetadata, never());
    verifyContainerMarkedUnhealthy(openContainer, never());
    // Deleted containers should not be marked unhealthy
    verifyContainerMarkedUnhealthy(deletedContainer, never());
  }

  @Test
  @Override
  public void testUnhealthyContainersTriggersVolumeScan() throws Exception {
    when(controller.markContainerUnhealthy(anyLong(), any(ScanResult.class))).thenReturn(true);
    try (MockedStatic<StorageVolumeUtil> mockedStatic = mockStatic(StorageVolumeUtil.class)) {
      scanner.runIteration();
      verifyContainerMarkedUnhealthy(corruptData, atLeastOnce());
      mockedStatic.verify(() -> StorageVolumeUtil.onFailure(corruptData.getContainerData().getVolume()), times(1));
    }
  }

  @Test
  public void testScanTimestampUpdated() throws Exception {
    scanner.runIteration();
    // Open containers should not be scanned.
    verify(controller, never())
        .updateDataScanTimestamp(
            eq(openContainer.getContainerData().getContainerID()), any());
    verify(controller, never())
        .updateDataScanTimestamp(
            eq(openCorruptMetadata.getContainerData().getContainerID()), any());
    // All other containers should have been scanned.
    verify(controller, atLeastOnce())
        .updateDataScanTimestamp(
            eq(healthy.getContainerData().getContainerID()), any());
    verify(controller, atLeastOnce())
        .updateDataScanTimestamp(
            eq(corruptData.getContainerData().getContainerID()), any());
    // Metrics for Deleted container should not be updated.
    verify(controller, never())
        .updateDataScanTimestamp(
            eq(deletedContainer.getContainerData().getContainerID()), any());
  }

  @Test
  @Override
  public void testUnhealthyContainerRescanned() throws Exception {
    Container<?> unhealthy = mockKeyValueContainer();
    when(unhealthy.scanMetaData()).thenReturn(getHealthyMetadataScanResult());
    when(unhealthy.scanData(any(DataTransferThrottler.class), any(Canceler.class)))
        .thenReturn(getUnhealthyDataScanResult());
    // If a container is not already in an unhealthy state, the controller will return true from this method.
    when(controller.markContainerUnhealthy(eq(unhealthy.getContainerData().getContainerID()),
        any())).thenReturn(true);

    setContainers(unhealthy, healthy);

    // First iteration should find the unhealthy container.
    scanner.runIteration();
    verifyContainerMarkedUnhealthy(unhealthy, atMostOnce());
    ContainerDataScannerMetrics metrics = scanner.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(2, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());

    // The unhealthy container should have been moved to the unhealthy state.
    verify(unhealthy.getContainerData(), atMostOnce())
        .setState(UNHEALTHY);
    // Update the mock to reflect this.
    when(unhealthy.getContainerState()).thenReturn(UNHEALTHY);
    assertTrue(unhealthy.shouldScanData());
    // Since the container is already unhealthy, the real controller would return false from this method.
    when(controller.markContainerUnhealthy(eq(unhealthy.getContainerData().getContainerID()),
        any())).thenReturn(false);
    scanner.runIteration();
    // The invocation of unhealthy on this container will also happen in the
    // next iteration.
    verifyContainerMarkedUnhealthy(unhealthy, atMost(2));
    // This iteration should scan the unhealthy container.
    assertEquals(2, metrics.getNumScanIterations());
    assertEquals(4, metrics.getNumContainersScanned());
    // numUnHealthyContainers metrics is not incremented in the 2nd iteration.
    assertEquals(1, metrics.getNumUnHealthyContainers());
  }

  @Test
  @Override
  public void testChecksumUpdateFailure() throws Exception {
    doThrow(new IOException("Checksum update error for testing")).when(controller)
        .updateContainerChecksum(anyLong(), any());
    scanner.runIteration();
    verifyContainerMarkedUnhealthy(corruptData, atMostOnce());
    verify(corruptData.getContainerData(), atMostOnce()).setState(UNHEALTHY);
  }

  /**
   * A datanode will have one background data scanner per volume. When the
   * volume fails, the scanner thread should be terminated.
   */
  @Test
  @Override
  public void testWithVolumeFailure() throws Exception {
    when(vol.isFailed()).thenReturn(true);
    // Run the scanner thread in the background. It should be terminated on
    // the first iteration because the volume is unhealthy.
    ContainerDataScannerMetrics metrics = scanner.getMetrics();
    scanner.start();
    GenericTestUtils.waitFor(() -> !scanner.isAlive(), 1000, 5000);

    // Volume health should have been checked.
    // TODO: remove the mock return value asseration after we upgrade to spotbugs 4.8 up
    assertFalse(verify(vol, atLeastOnce()).isFailed());
    // No iterations should have been run.
    assertEquals(0, metrics.getNumScanIterations());
    assertEquals(0, metrics.getNumContainersScanned());
    assertEquals(0, metrics.getNumUnHealthyContainers());
    // All containers were on the unhealthy volume, so they should not have
    // been scanned.
    verify(healthy, never()).scanData(any(), any());
    verify(openContainer, never()).scanData(any(), any());
    verify(corruptData, never()).scanData(any(), any());
    verify(openCorruptMetadata, never()).scanData(any(), any());
  }

  @Test
  @Override
  public void testShutdownDuringScan() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    // Make the data scan block until interrupt.
    when(healthy.scanData(any(), any())).then(i -> {
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

  @Test
  public void testMerkleTreeWritten() throws Exception {
    scanner.runIteration();

    // Merkle trees should not be written for open or deleted containers
    for (Container<ContainerData> container : Arrays.asList(openContainer, openCorruptMetadata, deletedContainer)) {
      verify(controller, times(0))
          .updateContainerChecksum(eq(container.getContainerData().getContainerID()), any());
    }

    // Merkle trees should be written for all other containers.
    for (Container<ContainerData> container : Arrays.asList(healthy, corruptData)) {
      verify(controller, times(1))
          .updateContainerChecksum(eq(container.getContainerData().getContainerID()), any());
    }
  }
}

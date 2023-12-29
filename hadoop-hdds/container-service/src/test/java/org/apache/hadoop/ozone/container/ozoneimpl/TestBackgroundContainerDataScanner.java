/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyScanResult;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the background container data scanner.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestBackgroundContainerDataScanner extends
    TestContainerScannersAbstract {

  private BackgroundContainerDataScanner scanner;

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
    Mockito.verify(healthy, never()).scanData(any(), any());
  }

  @Test
  @Override
  public void testPreviouslyScannedContainerIsScanned() throws Exception {
    // If the last scan time is before than the configured gap, the container
    // should be scanned.
    setScannedTimestampOld(healthy);
    scanner.runIteration();
    Mockito.verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @Test
  @Override
  public void testUnscannedContainerIsScanned() throws Exception {
    // If there is no last scanned time, the container should be scanned.
    Mockito.when(healthy.getContainerData().lastDataScanTime())
        .thenReturn(Optional.empty());
    scanner.runIteration();
    Mockito.verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @Test
  @Override
  public void testScannerMetrics() {
    scanner.runIteration();

    ContainerDataScannerMetrics metrics = scanner.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(2, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());
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
  }

  @Test
  public void testScanTimestampUpdated() throws Exception {
    scanner.runIteration();
    // Open containers should not be scanned.
    Mockito.verify(controller, never())
        .updateDataScanTimestamp(
            eq(openContainer.getContainerData().getContainerID()), any());
    Mockito.verify(controller, never())
        .updateDataScanTimestamp(
            eq(openCorruptMetadata.getContainerData().getContainerID()), any());
    // All other containers should have been scanned.
    Mockito.verify(controller, atLeastOnce())
        .updateDataScanTimestamp(
            eq(healthy.getContainerData().getContainerID()), any());
    Mockito.verify(controller, atLeastOnce())
        .updateDataScanTimestamp(
            eq(corruptData.getContainerData().getContainerID()), any());
  }

  @Test
  @Override
  public void testUnhealthyContainerNotRescanned() throws Exception {
    Container<?> unhealthy = mockKeyValueContainer();
    when(unhealthy.scanMetaData()).thenReturn(ScanResult.healthy());
    when(unhealthy.scanData(any(DataTransferThrottler.class),
        any(Canceler.class))).thenReturn(getUnhealthyScanResult());

    setContainers(unhealthy, healthy);

    // First iteration should find the unhealthy container.
    scanner.runIteration();
    verifyContainerMarkedUnhealthy(unhealthy, atMostOnce());
    ContainerDataScannerMetrics metrics = scanner.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(2, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());

    // The unhealthy container should have been moved to the unhealthy state.
    Mockito.verify(unhealthy.getContainerData(), atMostOnce())
        .setState(UNHEALTHY);
    // Update the mock to reflect this.
    Mockito.when(unhealthy.getContainerState()).thenReturn(UNHEALTHY);
    assertFalse(unhealthy.shouldScanData());

    // Clear metrics to check the next run.
    metrics.resetNumContainersScanned();
    metrics.resetNumUnhealthyContainers();

    scanner.runIteration();
    // The only invocation of unhealthy on this container should have been from
    // the previous scan.
    verifyContainerMarkedUnhealthy(unhealthy, atMostOnce());
    // This iteration should skip the already unhealthy container.
    assertEquals(2, metrics.getNumScanIterations());
    assertEquals(1, metrics.getNumContainersScanned());
    assertEquals(0, metrics.getNumUnHealthyContainers());
  }

  /**
   * A datanode will have one background data scanner per volume. When the
   * volume fails, the scanner thread should be terminated.
   */
  @Test
  @Override
  public void testWithVolumeFailure() throws Exception {
    Mockito.when(vol.isFailed()).thenReturn(true);
    // Run the scanner thread in the background. It should be terminated on
    // the first iteration because the volume is unhealthy.
    ContainerDataScannerMetrics metrics = scanner.getMetrics();
    scanner.start();
    GenericTestUtils.waitFor(() -> !scanner.isAlive(), 1000, 5000);

    // Volume health should have been checked.
    Mockito.verify(vol, atLeastOnce()).isFailed();
    // No iterations should have been run.
    assertEquals(0, metrics.getNumScanIterations());
    assertEquals(0, metrics.getNumContainersScanned());
    assertEquals(0, metrics.getNumUnHealthyContainers());
    // All containers were on the unhealthy volume, so they should not have
    // been scanned.
    Mockito.verify(healthy, never()).scanData(any(), any());
    Mockito.verify(openContainer, never()).scanData(any(), any());
    Mockito.verify(corruptData, never()).scanData(any(), any());
    Mockito.verify(openCorruptMetadata, never()).scanData(any(), any());
  }


  @Test
  @Override
  public void testShutdownDuringScan() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    // Make the data scan block until interrupt.
    Mockito.when(healthy.scanData(any(), any())).then(i -> {
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

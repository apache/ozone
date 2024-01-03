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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyScanResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the background container metadata scanner.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestBackgroundContainerMetadataScanner extends
    TestContainerScannersAbstract {

  private BackgroundContainerMetadataScanner scanner;

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
    // Metadata scanner cannot detect data corruption.
    verifyContainerMarkedUnhealthy(corruptData, never());
    verifyContainerMarkedUnhealthy(openCorruptMetadata, atLeastOnce());
    verifyContainerMarkedUnhealthy(openContainer, never());
  }

  @Test
  @Override
  public void testUnhealthyContainerNotRescanned() throws Exception {
    Container<?> unhealthy = mockKeyValueContainer();
    when(unhealthy.scanMetaData()).thenReturn(getUnhealthyScanResult());
    when(unhealthy.scanData(
        any(DataTransferThrottler.class), any(Canceler.class)))
        .thenReturn(ScanResult.healthy());

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
    assertFalse(unhealthy.shouldScanMetadata());

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
    verify(vol, atLeastOnce()).isFailed();
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

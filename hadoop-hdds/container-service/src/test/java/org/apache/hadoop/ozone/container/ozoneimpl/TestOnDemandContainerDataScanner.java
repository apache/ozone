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

import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyScanResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the on-demand container scanner.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestOnDemandContainerDataScanner extends
    TestContainerScannersAbstract {

  @BeforeEach
  public void setup() {
    super.setup();
  }

  @Test
  @Override
  public void testRecentlyScannedContainerIsSkipped() throws Exception {
    setScannedTimestampRecent(healthy);
    scanContainer(healthy);
    Mockito.verify(healthy, never()).scanData(any(), any());
  }

  @Test
  @Override
  public void testPreviouslyScannedContainerIsScanned() throws Exception {
    // If the last scan time is before than the configured gap, the container
    // should be scanned.
    setScannedTimestampOld(healthy);
    scanContainer(healthy);
    Mockito.verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @Test
  @Override
  public void testUnscannedContainerIsScanned() throws Exception {
    // If there is no last scanned time, the container should be scanned.
    Mockito.when(healthy.getContainerData().lastDataScanTime())
        .thenReturn(Optional.empty());
    scanContainer(healthy);
    Mockito.verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @AfterEach
  public void tearDown() {
    OnDemandContainerDataScanner.shutdown();
  }

  @Test
  public void testScanTimestampUpdated() throws Exception {
    OnDemandContainerDataScanner.init(conf, controller);
    Optional<Future<?>> scanFuture =
        OnDemandContainerDataScanner.scanContainer(healthy);
    Assertions.assertTrue(scanFuture.isPresent());
    scanFuture.get().get();
    Mockito.verify(controller, atLeastOnce())
        .updateDataScanTimestamp(
            eq(healthy.getContainerData().getContainerID()), any());
  }

  @Test
  public void testContainerScannerMultipleInitsAndShutdowns() throws Exception {
    OnDemandContainerDataScanner.init(conf, controller);
    OnDemandContainerDataScanner.init(conf, controller);
    OnDemandContainerDataScanner.shutdown();
    OnDemandContainerDataScanner.shutdown();
    //There shouldn't be an interaction after shutdown:
    OnDemandContainerDataScanner.scanContainer(corruptData);
    verifyContainerMarkedUnhealthy(corruptData, never());
  }

  @Test
  public void testSameContainerQueuedMultipleTimes() throws Exception {
    OnDemandContainerDataScanner.init(conf, controller);
    //Given a container that has not finished scanning
    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(corruptData.scanData(
            OnDemandContainerDataScanner.getThrottler(),
            OnDemandContainerDataScanner.getCanceler()))
        .thenAnswer((Answer<ScanResult>) invocation -> {
          latch.await();
          return getUnhealthyScanResult();
        });
    Optional<Future<?>> onGoingScan = OnDemandContainerDataScanner
        .scanContainer(corruptData);
    Assertions.assertTrue(onGoingScan.isPresent());
    Assertions.assertFalse(onGoingScan.get().isDone());
    //When scheduling the same container again
    Optional<Future<?>> secondScan = OnDemandContainerDataScanner
        .scanContainer(corruptData);
    //Then the second scan is not scheduled and the first scan can still finish
    Assertions.assertFalse(secondScan.isPresent());
    latch.countDown();
    onGoingScan.get().get();
    Mockito.verify(controller, atLeastOnce()).markContainerUnhealthy(
        eq(corruptData.getContainerData().getContainerID()), any());
  }

  @Test
  @Override
  public void testScannerMetrics() throws Exception {
    OnDemandContainerDataScanner.init(conf, controller);
    ArrayList<Optional<Future<?>>> resultFutureList = Lists.newArrayList();
    resultFutureList.add(OnDemandContainerDataScanner.scanContainer(
        corruptData));
    resultFutureList.add(
        OnDemandContainerDataScanner.scanContainer(openContainer));
    resultFutureList.add(
        OnDemandContainerDataScanner.scanContainer(openCorruptMetadata));
    resultFutureList.add(OnDemandContainerDataScanner.scanContainer(healthy));
    waitOnScannerToFinish(resultFutureList);
    OnDemandScannerMetrics metrics = OnDemandContainerDataScanner.getMetrics();
    //Containers with shouldScanData = false shouldn't increase
    // the number of scanned containers
    assertEquals(1, metrics.getNumUnHealthyContainers());
    assertEquals(2, metrics.getNumContainersScanned());
  }

  @Test
  @Override
  public void testScannerMetricsUnregisters() {
    OnDemandContainerDataScanner.init(conf, controller);
    String metricsName = OnDemandContainerDataScanner.getMetrics().getName();
    assertNotNull(DefaultMetricsSystem.instance().getSource(metricsName));
    OnDemandContainerDataScanner.shutdown();
    OnDemandContainerDataScanner.scanContainer(healthy);
    assertNull(DefaultMetricsSystem.instance().getSource(metricsName));
  }

  @Test
  @Override
  public void testUnhealthyContainersDetected() throws Exception {
    // Without initialization,
    // there shouldn't be interaction with containerController
    OnDemandContainerDataScanner.scanContainer(corruptData);
    Mockito.verifyZeroInteractions(controller);

    scanContainer(healthy);
    verifyContainerMarkedUnhealthy(healthy, never());
    scanContainer(corruptData);
    verifyContainerMarkedUnhealthy(corruptData, atLeastOnce());
    scanContainer(openCorruptMetadata);
    verifyContainerMarkedUnhealthy(openCorruptMetadata, never());
    scanContainer(openContainer);
    verifyContainerMarkedUnhealthy(openContainer, never());
  }

  /**
   * A datanode will have one on-demand scanner thread for the whole process.
   * When a volume fails, any the containers queued for scanning in that volume
   * should be skipped but the thread will continue to run and accept new
   * containers to scan.
   */
  @Test
  @Override
  public void testWithVolumeFailure() throws Exception {
    Mockito.when(vol.isFailed()).thenReturn(true);

    OnDemandContainerDataScanner.init(conf, controller);
    OnDemandScannerMetrics metrics = OnDemandContainerDataScanner.getMetrics();

    scanContainer(healthy);
    verifyContainerMarkedUnhealthy(healthy, never());
    scanContainer(corruptData);
    verifyContainerMarkedUnhealthy(corruptData, never());
    scanContainer(openCorruptMetadata);
    verifyContainerMarkedUnhealthy(openCorruptMetadata, never());
    scanContainer(openContainer);
    verifyContainerMarkedUnhealthy(openContainer, never());

    assertEquals(0, metrics.getNumContainersScanned());
    assertEquals(0, metrics.getNumUnHealthyContainers());
  }

  @Test
  @Override
  public void testShutdownDuringScan() throws Exception {
    // Make the on demand scan block until interrupt.
    Mockito.when(healthy.scanData(any(), any())).then(i -> {
      Thread.sleep(Duration.ofDays(1).toMillis()); return null;
    });

    // Start the blocking scan.
    OnDemandContainerDataScanner.init(conf, controller);
    OnDemandContainerDataScanner.scanContainer(healthy);
    // Shut down the on demand scanner. This will interrupt the blocked scan
    // on the healthy container.
    OnDemandContainerDataScanner.shutdown();
    // Interrupting the healthy container's scan should not mark it unhealthy.
    verifyContainerMarkedUnhealthy(healthy, never());
  }

  @Test
  @Override
  public void testUnhealthyContainerNotRescanned() throws Exception {
    Container<?> unhealthy = mockKeyValueContainer();
    when(unhealthy.scanMetaData()).thenReturn(ScanResult.healthy());
    when(unhealthy.scanData(
        any(DataTransferThrottler.class), any(Canceler.class)))
        .thenReturn(getUnhealthyScanResult());

    // First iteration should find the unhealthy container.
    scanContainer(unhealthy);
    verifyContainerMarkedUnhealthy(unhealthy, atMostOnce());
    OnDemandScannerMetrics metrics = OnDemandContainerDataScanner.getMetrics();
    assertEquals(1, metrics.getNumContainersScanned());
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

    scanContainer(unhealthy);
    // The only invocation of unhealthy on this container should have been from
    // the previous scan.
    verifyContainerMarkedUnhealthy(unhealthy, atMostOnce());
    // This iteration should skip the already unhealthy container.
    assertEquals(0, metrics.getNumContainersScanned());
    assertEquals(0, metrics.getNumUnHealthyContainers());
  }

  private void scanContainer(Container<?> container) throws Exception {
    OnDemandContainerDataScanner.init(conf, controller);
    Optional<Future<?>> scanFuture =
        OnDemandContainerDataScanner.scanContainer(container);
    if (scanFuture.isPresent()) {
      scanFuture.get().get();
    }
  }

  private void waitOnScannerToFinish(
      ArrayList<Optional<Future<?>>> resultFutureList)
      throws ExecutionException, InterruptedException {
    for (Optional<Future<?>> future : resultFutureList) {
      if (future.isPresent()) {
        future.get().get();
      }
    }
  }
}

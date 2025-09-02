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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for the on-demand container scanner.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestOnDemandContainerScanner extends
    TestContainerScannersAbstract {

  private OnDemandContainerScanner onDemandScanner;
  private static final String TEST_SCAN = "Test Scan";

  @Override
  @BeforeEach
  public void setup() {
    super.setup();
    onDemandScanner = new OnDemandContainerScanner(conf, controller);
  }

  @Test
  @Override
  public void testRecentlyScannedContainerIsSkipped() throws Exception {
    setScannedTimestampRecent(healthy);
    scanContainer(healthy);
    verify(healthy, never()).scanData(any(), any());
  }

  @Test
  public void testBypassScanGap() throws Exception {
    setScannedTimestampRecent(healthy);

    Optional<Future<?>> scanFutureOptional = onDemandScanner.scanContainerWithoutGap(healthy, TEST_SCAN);
    assertTrue(scanFutureOptional.isPresent());
    Future<?> scanFuture = scanFutureOptional.get();
    scanFuture.get();
    verify(healthy, times(1)).scanData(any(), any());
  }

  @Test
  @Override
  public void testPreviouslyScannedContainerIsScanned() throws Exception {
    // If the last scan time is before than the configured gap, the container
    // should be scanned.
    setScannedTimestampOld(healthy);
    scanContainer(healthy);
    verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @Test
  @Override
  public void testUnscannedContainerIsScanned() throws Exception {
    // If there is no last scanned time, the container should be scanned.
    when(healthy.getContainerData().lastDataScanTime())
        .thenReturn(Optional.empty());
    scanContainer(healthy);
    verify(healthy, atLeastOnce()).scanData(any(), any());
  }

  @AfterEach
  public void tearDown() {
    onDemandScanner.shutdown();
  }

  @Test
  public void testScanTimestampUpdated() throws Exception {
    Optional<Future<?>> scanFuture = onDemandScanner.scanContainer(healthy, TEST_SCAN);
    assertTrue(scanFuture.isPresent());
    scanFuture.get().get();
    verify(controller, atLeastOnce())
        .updateDataScanTimestamp(
            eq(healthy.getContainerData().getContainerID()), any());

    // Metrics for deleted container should not be updated.
    scanFuture = onDemandScanner.scanContainer(healthy, TEST_SCAN);
    assertTrue(scanFuture.isPresent());
    scanFuture.get().get();
    verify(controller, never())
        .updateDataScanTimestamp(
            eq(deletedContainer.getContainerData().getContainerID()), any());
  }

  @Test
  public void testContainerScannerMultipleShutdowns() {
    // No runtime exceptions should be thrown.
    onDemandScanner.shutdown();
    onDemandScanner.shutdown();
  }

  @Test
  public void testSameContainerQueuedMultipleTimes() throws Exception {
    //Given a container that has not finished scanning
    CountDownLatch latch = new CountDownLatch(1);
    when(corruptData.scanData(
            any(),
            any()))
        .thenAnswer((Answer<ScanResult>) invocation -> {
          latch.await();
          return getUnhealthyDataScanResult();
        });
    Optional<Future<?>> onGoingScan = onDemandScanner.scanContainer(corruptData, TEST_SCAN);
    assertTrue(onGoingScan.isPresent());
    assertFalse(onGoingScan.get().isDone());
    //When scheduling the same container again
    Optional<Future<?>> secondScan = onDemandScanner.scanContainer(corruptData, TEST_SCAN);
    //Then the second scan is not scheduled and the first scan can still finish
    assertFalse(secondScan.isPresent());
    latch.countDown();
    onGoingScan.get().get();
    verify(controller, atLeastOnce()).markContainerUnhealthy(
        eq(corruptData.getContainerData().getContainerID()), any());
  }

  @Test
  public void testSameOpenContainerQueuedMultipleTimes() throws Exception {
    //Given a container that has not finished scanning
    CountDownLatch latch = new CountDownLatch(1);
    when(openCorruptMetadata.scanMetaData())
        .thenAnswer((Answer<ScanResult>) invocation -> {
          latch.await();
          return getUnhealthyDataScanResult();
        });
    Optional<Future<?>> onGoingScan = onDemandScanner.scanContainer(openCorruptMetadata, TEST_SCAN);
    assertTrue(onGoingScan.isPresent());
    assertFalse(onGoingScan.get().isDone());
    //When scheduling the same container again
    Optional<Future<?>> secondScan = onDemandScanner.scanContainer(openCorruptMetadata, TEST_SCAN);
    //Then the second scan is not scheduled and the first scan can still finish
    assertFalse(secondScan.isPresent());
    latch.countDown();
    onGoingScan.get().get();
    verify(controller, atLeastOnce()).markContainerUnhealthy(
        eq(openCorruptMetadata.getContainerData().getContainerID()), any());
  }

  @Test
  @Override
  public void testScannerMetrics() throws Exception {
    ArrayList<Optional<Future<?>>> resultFutureList = Lists.newArrayList();
    resultFutureList.add(onDemandScanner.scanContainer(corruptData, TEST_SCAN));
    resultFutureList.add(onDemandScanner.scanContainer(openContainer, TEST_SCAN));
    resultFutureList.add(onDemandScanner.scanContainer(openCorruptMetadata, TEST_SCAN));
    resultFutureList.add(onDemandScanner.scanContainer(healthy, TEST_SCAN));
    // Deleted containers will not count towards the scan count metric.
    resultFutureList.add(onDemandScanner.scanContainer(deletedContainer, TEST_SCAN));
    waitOnScannerToFinish(resultFutureList);
    OnDemandScannerMetrics metrics = onDemandScanner.getMetrics();
    //Containers with shouldScanData = false shouldn't increase
    // the number of scanned containers
    assertEquals(0, metrics.getNumUnHealthyContainers());
    assertEquals(4, metrics.getNumContainersScanned());
  }

  @Test
  @Override
  public void testScannerMetricsUnregisters() {
    String metricsName = onDemandScanner.getMetrics().getName();
    assertNotNull(DefaultMetricsSystem.instance().getSource(metricsName));
    onDemandScanner.shutdown();
    assertNull(DefaultMetricsSystem.instance().getSource(metricsName));
  }

  @Test
  @Override
  public void testUnhealthyContainersDetected() throws Exception {
    scanContainer(healthy);
    verifyContainerMarkedUnhealthy(healthy, never());
    scanContainer(corruptData);
    verifyContainerMarkedUnhealthy(corruptData, atLeastOnce());
    scanContainer(openCorruptMetadata);
    verifyContainerMarkedUnhealthy(openCorruptMetadata, atLeastOnce());
    scanContainer(openContainer);
    verifyContainerMarkedUnhealthy(openContainer, never());
    // Deleted containers should not be marked unhealthy
    scanContainer(deletedContainer);
    verifyContainerMarkedUnhealthy(deletedContainer, never());
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
    when(vol.isFailed()).thenReturn(true);

    OnDemandScannerMetrics metrics = onDemandScanner.getMetrics();

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
    when(healthy.scanData(any(), any())).then(i -> {
      Thread.sleep(Duration.ofDays(1).toMillis()); return null;
    });

    // Start the blocking scan.
    onDemandScanner.scanContainer(healthy, TEST_SCAN);
    // Shut down the on demand scanner. This will interrupt the blocked scan
    // on the healthy container.
    onDemandScanner.shutdown();
    // Interrupting the healthy container's scan should not mark it unhealthy.
    verifyContainerMarkedUnhealthy(healthy, never());
  }

  @Test
  @Override
  public void testUnhealthyContainerRescanned() throws Exception {
    Container<?> unhealthy = mockKeyValueContainer();
    when(unhealthy.scanMetaData()).thenReturn(getHealthyMetadataScanResult());
    when(unhealthy.scanData(
        any(DataTransferThrottler.class), any(Canceler.class)))
        .thenReturn(getUnhealthyDataScanResult());
    when(controller.markContainerUnhealthy(eq(unhealthy.getContainerData().getContainerID()),
        any())).thenReturn(true);

    // First iteration should find the unhealthy container.
    scanContainer(unhealthy);
    verifyContainerMarkedUnhealthy(unhealthy, atMostOnce());
    OnDemandScannerMetrics metrics = onDemandScanner.getMetrics();
    assertEquals(1, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());

    // The unhealthy container should have been moved to the unhealthy state.
    verify(unhealthy.getContainerData(), atMostOnce())
        .setState(UNHEALTHY);
    // Update the mock to reflect this.
    when(unhealthy.getContainerState()).thenReturn(UNHEALTHY);
    assertTrue(unhealthy.shouldScanData());
    when(controller.markContainerUnhealthy(eq(unhealthy.getContainerData().getContainerID()),
        any())).thenReturn(false);

    // When rescanned the metrics should increase as we scan
    // UNHEALTHY containers as well.
    scanContainer(unhealthy);
    // The invocation of unhealthy on this container will also happen in the
    // next iteration.
    verifyContainerMarkedUnhealthy(unhealthy, atMost(2));
    assertEquals(2, metrics.getNumContainersScanned());
    // numUnHealthyContainers metrics is not incremented in the 2nd iteration.
    assertEquals(1, metrics.getNumUnHealthyContainers());
  }

  @Test
  @Override
  public void testChecksumUpdateFailure() throws Exception {
    doThrow(new IOException("Checksum update error for testing")).when(controller)
        .updateContainerChecksum(anyLong(), any());
    scanContainer(corruptData);
    verifyContainerMarkedUnhealthy(corruptData, atMostOnce());
    verify(corruptData.getContainerData(), atMostOnce()).setState(UNHEALTHY);
  }

  @Test
  public void testMerkleTreeWritten() throws Exception {
    // Merkle trees should not be written for open or deleted containers
    for (Container<ContainerData> container : Arrays.asList(openContainer, openCorruptMetadata, deletedContainer)) {
      scanContainer(container);
      verify(controller, times(0))
          .updateContainerChecksum(eq(container.getContainerData().getContainerID()), any());
    }

    // Merkle trees should be written for all other containers.
    for (Container<ContainerData> container : Arrays.asList(healthy, corruptData)) {
      scanContainer(container);
      verify(controller, times(1))
          .updateContainerChecksum(eq(container.getContainerData().getContainerID()), any());
    }
  }

  @Test
  @Override
  public void testUnhealthyContainersTriggersVolumeScan() throws Exception {
    when(controller.markContainerUnhealthy(anyLong(), any(ScanResult.class))).thenReturn(true);
    LogCapturer logCapturer = LogCapturer.captureLogs(OnDemandContainerScanner.class);
    scanContainer(corruptData);
    verifyContainerMarkedUnhealthy(corruptData, times(1));
    assertTrue(logCapturer.getOutput().contains("Triggering scan of volume"));
  }

  private void scanContainer(Container<?> container) throws Exception {
    Optional<Future<?>> scanFuture = onDemandScanner.scanContainer(container, TEST_SCAN);
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

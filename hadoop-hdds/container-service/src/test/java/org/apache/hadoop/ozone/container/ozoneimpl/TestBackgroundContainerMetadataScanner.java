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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;

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
  public void testRecentlyScannedContainerIsSkipped() {
    // If the last scan time is before than the configured gap, the container
    // should be scanned.
    setScannedTimestampRecent(healthy);
    scanner.runIteration();
    Mockito.verify(healthy, never()).scanMetaData();
  }

  @Test
  @Override
  public void testPreviouslyScannedContainerIsScanned() {
    setScannedTimestampOld(healthy);
    scanner.runIteration();
    Mockito.verify(healthy, atLeastOnce()).scanMetaData();
  }

  @Test
  @Override
  public void testUnscannedContainerIsScanned() throws Exception {
    // If there is no last scanned time, the container should be scanned.
    Mockito.when(healthy.getContainerData().lastDataScanTime())
        .thenReturn(Optional.empty());
    scanner.runIteration();
    Mockito.verify(healthy, atLeastOnce()).scanMetaData();
  }

  @Test
  @Override
  public void testScannerMetrics() {
    scanner.runIteration();

    ContainerMetadataScannerMetrics metrics = scanner.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(3, metrics.getNumContainersScanned());
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

  /**
   * A datanode will have one metadata scanner thread for the whole process.
   * When a volume fails, any the containers queued for scanning in that volume
   * should be skipped but the thread will continue to run.
   */
  @Test
  @Override
  // Override findbugs warning about Mockito.verify
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void testWithVolumeFailure() throws Exception {
    Mockito.when(vol.isFailed()).thenReturn(true);

    ContainerMetadataScannerMetrics metrics = scanner.getMetrics();
    // Start metadata scanner thread in the background.
    scanner.start();
    // Wait for at least one iteration to complete.
    GenericTestUtils.waitFor(() -> metrics.getNumScanIterations() >= 1, 1000,
        5000);
    // Volume health should have been checked.
    Mockito.verify(vol, atLeastOnce()).isFailed();
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
    Mockito.verify(healthy, never()).scanMetaData();
    Mockito.verify(openContainer, never()).scanMetaData();
    Mockito.verify(corruptData, never()).scanMetaData();
    Mockito.verify(openCorruptMetadata, never()).scanMetaData();
  }
}

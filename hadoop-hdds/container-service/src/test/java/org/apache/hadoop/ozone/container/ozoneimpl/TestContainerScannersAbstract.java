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

import static org.apache.hadoop.hdds.conf.OzoneConfiguration.newInstanceOf;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getHealthyDataScanResult;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getHealthyMetadataScanResult;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyDataScanResult;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyMetadataScanResult;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.CONTAINER_SCAN_MIN_GAP_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.verification.VerificationMode;

/**
 * General testing guidelines for the various container scanners whose tests
 * subclass this one.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class TestContainerScannersAbstract {

  private static final AtomicLong CONTAINER_SEQ_ID = new AtomicLong(100);

  @Mock
  protected Container<ContainerData> healthy;

  @Mock
  protected Container<ContainerData> openContainer;

  @Mock
  protected Container<ContainerData> openCorruptMetadata;

  @Mock
  protected Container<ContainerData> corruptData;

  @Mock
  protected Container<ContainerData> deletedContainer;

  @Mock
  protected HddsVolume vol;

  protected ContainerScannerConfiguration conf;
  protected ContainerController controller;

  private Collection<Container<?>> containers;

  public void setup() {
    containers = new ArrayList<>();
    conf = newInstanceOf(ContainerScannerConfiguration.class);
    conf.setMetadataScanInterval(0);
    conf.setDataScanInterval(0);
    conf.setEnabled(true);
    controller = mockContainerController();
  }

  // ALL SCANNERS SHOULD TEST THESE THINGS

  @Test
  public abstract void testRecentlyScannedContainerIsSkipped() throws Exception;

  @Test
  public abstract void testPreviouslyScannedContainerIsScanned()
      throws Exception;

  @Test
  public abstract void testUnscannedContainerIsScanned() throws Exception;

  @Test
  public abstract void testUnhealthyContainersDetected() throws Exception;

  @Test
  public abstract void testScannerMetrics() throws Exception;

  @Test
  public abstract void testScannerMetricsUnregisters() throws Exception;

  @Test
  public abstract void testWithVolumeFailure() throws Exception;

  @Test
  public abstract void testShutdownDuringScan() throws Exception;

  @Test
  public abstract void testUnhealthyContainerRescanned() throws Exception;

  /**
   * When the container checksum cannot be updated, the scan should still complete and move the container state without
   * throwing an exception.
   */
  @Test
  public abstract void testChecksumUpdateFailure() throws Exception;

  @Test
  public abstract void testUnhealthyContainersTriggersVolumeScan() throws Exception;

  // HELPER METHODS

  protected void setScannedTimestampOld(Container<ContainerData> container) {
    // If the last scan time is before than the configured gap, the container
    // should be scanned.
    Instant oldLastScanTime = Instant.now()
        .minus(CONTAINER_SCAN_MIN_GAP_DEFAULT, ChronoUnit.MILLIS)
        .minus(10, ChronoUnit.MINUTES);
    when(container.getContainerData().lastDataScanTime())
        .thenReturn(Optional.of(oldLastScanTime));
  }

  protected void setScannedTimestampRecent(Container<ContainerData> container) {
    // If the last scan time is within the configured gap, the container
    // should be skipped.
    Instant recentLastScanTime = Instant.now()
        .minus(CONTAINER_SCAN_MIN_GAP_DEFAULT, ChronoUnit.MILLIS)
        .plus(1, ChronoUnit.MINUTES);
    when(container.getContainerData().lastDataScanTime())
        .thenReturn(Optional.of(recentLastScanTime));
  }

  protected void verifyContainerMarkedUnhealthy(
      Container<?> container, VerificationMode invocationTimes)
      throws Exception {
    verify(controller, invocationTimes).markContainerUnhealthy(
        eq(container.getContainerData().getContainerID()), any());
  }

  /**
   * Mock a KeyValueContainer implementation instead of a container
   * interface like ContainerTestUtils#setupMockContainer.
   * This allows testing that the shouldScanData method skips unhealthy
   * containers.
   */
  protected Container<?> mockKeyValueContainer() {
    KeyValueContainer unhealthy = mock(KeyValueContainer.class);

    KeyValueContainerData data = mock(KeyValueContainerData.class);
    when(data.getContainerID()).thenReturn(CONTAINER_SEQ_ID.incrementAndGet());
    when(unhealthy.getContainerData()).thenReturn(data);
    when(unhealthy.getContainerState()).thenReturn(CLOSED);
    // The above mocks should be enough for the scanners to call this method
    // and test it.
    when(unhealthy.shouldScanData()).thenCallRealMethod();
    assertTrue(unhealthy.shouldScanData());

    when(unhealthy.getContainerData().getVolume()).thenReturn(vol);

    return unhealthy;
  }

  /**
   * Add a container to be returned by the mock ContainerController.
   */
  protected void setContainers(Container<?>... containers) {
    this.containers = Arrays.stream(containers).collect(Collectors.toList());
    when(controller.getContainers(vol))
        .thenAnswer(i -> this.containers.iterator());
    when(controller.getContainers()).thenReturn(this.containers);
  }

  private ContainerController mockContainerController() {
    DataScanResult healthyData = getHealthyDataScanResult();
    DataScanResult unhealthyData = getUnhealthyDataScanResult();
    MetadataScanResult healthyMetadata = getHealthyMetadataScanResult();
    MetadataScanResult unhealthyMetadata = getUnhealthyMetadataScanResult();

    File volLocation = mock(File.class);
    when(volLocation.getPath()).thenReturn("/temp/volume-testcontainerscanner");
    when(vol.getStorageDir()).thenReturn(volLocation);

    // healthy container
    ContainerTestUtils.setupMockContainer(healthy,
        true, healthyMetadata, healthyData,
        CONTAINER_SEQ_ID, vol);

    // Open container (only metadata can be scanned)
    ContainerTestUtils.setupMockContainer(openContainer,
        false, healthyMetadata, healthyData,
        CONTAINER_SEQ_ID, vol);

    // unhealthy container (corrupt data)
    ContainerTestUtils.setupMockContainer(corruptData,
        true, healthyMetadata, unhealthyData,
        CONTAINER_SEQ_ID, vol);

    // unhealthy container (corrupt metadata). To simulate container still
    // being open while metadata is corrupted, shouldScanData will return false.
    ContainerTestUtils.setupMockContainer(openCorruptMetadata,
        false, unhealthyMetadata, unhealthyData,
        CONTAINER_SEQ_ID, vol);

    // Mock container that has been deleted during scan.
    ContainerTestUtils.setupMockContainer(deletedContainer,
        true, healthyMetadata, DataScanResult.deleted(),
        CONTAINER_SEQ_ID, vol);

    containers.addAll(Arrays.asList(healthy, corruptData, openCorruptMetadata,
        deletedContainer));
    ContainerController mock = mock(ContainerController.class);
    when(mock.getContainers(vol)).thenReturn(containers.iterator());
    when(mock.getContainers()).thenReturn(containers);

    return mock;
  }
}

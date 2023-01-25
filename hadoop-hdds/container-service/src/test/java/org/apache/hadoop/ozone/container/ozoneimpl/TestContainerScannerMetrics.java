/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.conf.OzoneConfiguration.newInstanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test verifies the container scanner metrics functionality.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestContainerScannerMetrics {

  private final AtomicLong containerIdSeq = new AtomicLong(100);

  @Mock
  private Container<ContainerData> healthy;

  @Mock
  private Container<ContainerData> corruptMetadata;

  @Mock
  private Container<ContainerData> corruptData;

  @Mock
  private HddsVolume vol;

  private ContainerScannerConfiguration conf;
  private ContainerController controller;

  @BeforeEach
  public void setup() {
    conf = newInstanceOf(ContainerScannerConfiguration.class);
    conf.setMetadataScanInterval(0);
    conf.setDataScanInterval(0);
    conf.setEnabled(true);
    controller = mockContainerController();
  }

  @AfterEach
  public void tearDown() {
    OnDemandContainerScanner.shutdown();
  }

  @Test
  public void testContainerMetaDataScannerMetrics() {
    ContainerMetadataScanner subject =
        new ContainerMetadataScanner(conf, controller);
    subject.runIteration();

    ContainerMetadataScannerMetrics metrics = subject.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(3, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());
  }

  @Test
  public void testContainerMetaDataScannerMetricsUnregisters() {
    ContainerMetadataScanner subject =
        new ContainerMetadataScanner(conf, controller);
    String name = subject.getMetrics().getName();

    assertNotNull(DefaultMetricsSystem.instance().getSource(name));

    subject.shutdown();
    subject.run();

    assertNull(DefaultMetricsSystem.instance().getSource(name));
  }

  @Test
  public void testContainerDataScannerMetrics() {
    ContainerDataScanner subject =
        new ContainerDataScanner(conf, controller, vol);
    subject.runIteration();

    ContainerDataScannerMetrics metrics = subject.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(2, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());
  }

  @Test
  public void testContainerDataScannerMetricsUnregisters() throws IOException {
    HddsVolume volume = new HddsVolume.Builder("/").failedVolume(true).build();
    ContainerDataScanner subject =
        new ContainerDataScanner(conf, controller, volume);
    String name = subject.getMetrics().getName();

    assertNotNull(DefaultMetricsSystem.instance().getSource(name));

    subject.shutdown();
    subject.run();

    assertNull(DefaultMetricsSystem.instance().getSource(name));
  }

  @Test
  public void testOnDemandScannerMetrics() throws Exception {
    OnDemandContainerScanner.init(conf, controller);
    ArrayList<Optional<Future<?>>> resultFutureList = Lists.newArrayList();
    resultFutureList.add(OnDemandContainerScanner.scanContainer(corruptData));
    resultFutureList.add(
        OnDemandContainerScanner.scanContainer(corruptMetadata));
    resultFutureList.add(OnDemandContainerScanner.scanContainer(healthy));
    waitOnScannerToFinish(resultFutureList);
    OnDemandScannerMetrics metrics = OnDemandContainerScanner.getMetrics();
    //Containers with shouldScanData = false shouldn't increase
    // the number of scanned containers
    assertEquals(1, metrics.getNumUnHealthyContainers());
    assertEquals(2, metrics.getNumContainersScanned());
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

  @Test
  public void testOnDemandScannerMetricsUnregisters() {
    OnDemandContainerScanner.init(conf, controller);
    String metricsName = OnDemandContainerScanner.getMetrics().getName();
    assertNotNull(DefaultMetricsSystem.instance().getSource(metricsName));
    OnDemandContainerScanner.shutdown();
    OnDemandContainerScanner.scanContainer(healthy);
    assertNull(DefaultMetricsSystem.instance().getSource(metricsName));
  }

  private ContainerController mockContainerController() {
    // healthy container
    ContainerTestUtils.setupMockContainer(healthy,
        true, true, true, containerIdSeq);

    // unhealthy container (corrupt data)
    ContainerTestUtils.setupMockContainer(corruptData,
        true, true, false, containerIdSeq);

    // unhealthy container (corrupt metadata)
    ContainerTestUtils.setupMockContainer(corruptMetadata,
        false, false, false, containerIdSeq);

    Collection<Container<?>> containers = Arrays.asList(
        healthy, corruptData, corruptMetadata);
    ContainerController mock = mock(ContainerController.class);
    when(mock.getContainers(vol)).thenReturn(containers.iterator());
    when(mock.getContainers()).thenReturn(containers.iterator());

    return mock;
  }
}

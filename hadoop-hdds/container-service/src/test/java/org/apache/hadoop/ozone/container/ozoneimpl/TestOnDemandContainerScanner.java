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
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.conf.OzoneConfiguration.newInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the on-demand container scanner.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandContainerScanner {

  private final AtomicLong containerIdSeq = new AtomicLong(100);

  @Mock
  private Container<ContainerData> healthy;

  @Mock
  private Container<ContainerData> openContainer;

  @Mock
  private Container<ContainerData> corruptData;

  private ContainerScannerConfiguration conf;
  private ContainerController controller;

  @Before
  public void setup() {
    conf = newInstanceOf(ContainerScannerConfiguration.class);
    conf.setMetadataScanInterval(0);
    conf.setDataScanInterval(0);
    controller = mockContainerController();
  }

  @After
  public void tearDown() {
    OnDemandContainerScanner.INSTANCE.shutdown();
  }

  @Test
  public void testOnDemandContainerScanner() throws Exception {
    //Without initialization,
    // there shouldn't be interaction with containerController
    OnDemandContainerScanner.INSTANCE.scanContainer(corruptData);
    Mockito.verifyZeroInteractions(controller);
    OnDemandContainerScanner.INSTANCE.init(conf, controller);
    testContainerMarkedUnhealthy(healthy, never());
    testContainerMarkedUnhealthy(corruptData, atLeastOnce());
    testContainerMarkedUnhealthy(openContainer, never());
  }

  @Test
  public void testContainerScannerMultipleInitsAndShutdowns() throws Exception {
    OnDemandContainerScanner.INSTANCE.init(conf, controller);
    OnDemandContainerScanner.INSTANCE.init(conf, controller);
    OnDemandContainerScanner.INSTANCE.shutdown();
    OnDemandContainerScanner.INSTANCE.shutdown();
    //There shouldn't be an interaction after shutdown:
    testContainerMarkedUnhealthy(corruptData, never());
  }

  private void testContainerMarkedUnhealthy(
      Container<?> container, VerificationMode invocationTimes)
      throws IOException {
    OnDemandContainerScanner.INSTANCE.scanContainer(container);
    waitForScanToFinish();
    Mockito.verify(controller, invocationTimes).markContainerUnhealthy(
        container.getContainerData().getContainerID());
  }

  private void waitForScanToFinish() {
    try {
      Future<?> futureScan = OnDemandContainerScanner.INSTANCE
          .getLastScanFuture();
      if (futureScan == null) {
        return;
      }
      futureScan.get();
    } catch (Exception e) {
      throw new RuntimeException("Error while waiting" +
          " for on-demand scan to finish");
    }
  }

  private ContainerController mockContainerController() {
    // healthy container
    setupMockContainer(healthy, true, true);

    // unhealthy container (corrupt data)
    setupMockContainer(corruptData, true, false);

    // unhealthy container (corrupt metadata)
    setupMockContainer(openContainer, false,
        false);

    return mock(ContainerController.class);
  }

  private void setupMockContainer(
      Container<ContainerData> c, boolean shouldScanData,
      boolean scanDataSuccess) {
    ContainerData data = mock(ContainerData.class);
    when(data.getContainerID()).thenReturn(containerIdSeq.getAndIncrement());
    when(c.getContainerData()).thenReturn(data);
    when(c.shouldScanData()).thenReturn(shouldScanData);
    when(c.scanData(any(DataTransferThrottler.class), any(Canceler.class)))
        .thenReturn(scanDataSuccess);
  }
}

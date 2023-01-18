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

import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.conf.OzoneConfiguration.newInstanceOf;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

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
    OnDemandContainerScanner.shutdown();
  }

  @Test
  public void testOnDemandContainerScanner() throws Exception {
    //Without initialization,
    // there shouldn't be interaction with containerController
    OnDemandContainerScanner.scanContainer(corruptData);
    Mockito.verifyZeroInteractions(controller);
    OnDemandContainerScanner.init(conf, controller);
    testContainerMarkedUnhealthy(healthy, never());
    testContainerMarkedUnhealthy(corruptData, atLeastOnce());
    testContainerMarkedUnhealthy(openContainer, never());
  }

  @Test
  public void testContainerScannerMultipleInitsAndShutdowns() throws Exception {
    OnDemandContainerScanner.init(conf, controller);
    OnDemandContainerScanner.init(conf, controller);
    OnDemandContainerScanner.shutdown();
    OnDemandContainerScanner.shutdown();
    //There shouldn't be an interaction after shutdown:
    testContainerMarkedUnhealthy(corruptData, never());
  }

  @Test
  public void testSameContainerQueuedMultipleTimes() throws Exception {
    OnDemandContainerScanner.init(conf, controller);
    //Given a container that has not finished scanning
    CountDownLatch latch = new CountDownLatch(1);
    Mockito.lenient().when(corruptData.scanData(
            OnDemandContainerScanner.getThrottler(),
            OnDemandContainerScanner.getCanceler()))
        .thenAnswer((Answer<Boolean>) invocation -> {
          latch.await();
          return false;
        });
    Optional<Future<?>> onGoingScan = OnDemandContainerScanner
        .scanContainer(corruptData);
    Assert.assertTrue(onGoingScan.isPresent());
    Assert.assertFalse(onGoingScan.get().isDone());
    //When scheduling the same container again
    Optional<Future<?>> secondScan = OnDemandContainerScanner
        .scanContainer(corruptData);
    //Then the second scan is not scheduled and the first scan can still finish
    Assert.assertFalse(secondScan.isPresent());
    latch.countDown();
    onGoingScan.get().get();
    Mockito.verify(controller, atLeastOnce()).
        markContainerUnhealthy(corruptData.getContainerData().getContainerID());
  }

  private void testContainerMarkedUnhealthy(
      Container<?> container, VerificationMode invocationTimes)
      throws InterruptedException, ExecutionException, IOException {
    Optional<Future<?>> result =
        OnDemandContainerScanner.scanContainer(container);
    if (result.isPresent()) {
      result.get().get();
    }
    Mockito.verify(controller, invocationTimes).markContainerUnhealthy(
        container.getContainerData().getContainerID());
  }

  private ContainerController mockContainerController() {
    // healthy container
    ContainerTestUtils.setupMockContainer(healthy,
        true, true, containerIdSeq);

    // unhealthy container (corrupt data)
    ContainerTestUtils.setupMockContainer(corruptData,
        true, false, containerIdSeq);

    // unhealthy container (corrupt metadata)
    ContainerTestUtils.setupMockContainer(openContainer,
        false, false, containerIdSeq);

    return mock(ContainerController.class);
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.states.datanode;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_LIFELINE_REPORT_ENABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for RunningDatanodeState.
 */
public class TestRunningDatanodeState {

  private static final InetSocketAddress TEST_SCM_ENDPOINT1 =
      new InetSocketAddress("test-scm-1", 9861);
  private static final InetSocketAddress TEST_SCM_ENDPOINT2 =
      new InetSocketAddress("test-scm-2", 9861);
  private static final InetSocketAddress TEST_SCM_ENDPOINT3 =
      new InetSocketAddress("test-scm-3", 9861);

  @Test
  public void testAwait() throws InterruptedException {
    SCMConnectionManager connectionManager = mock(SCMConnectionManager.class);
    List<EndpointStateMachine> stateMachines = new ArrayList<>();
    when(connectionManager.getValues()).thenReturn(stateMachines);

    RunningDatanodeState state =
        new RunningDatanodeState(new OzoneConfiguration(), connectionManager, null);

    int threadPoolSize = 2;
    ExecutorService executorService = Executors.newFixedThreadPool(
        threadPoolSize);

    ExecutorCompletionService ecs =
        new ExecutorCompletionService<>(executorService);
    state.setExecutorCompletionService(ecs);

    for (int i = 0; i < threadPoolSize; i++) {
      stateMachines.add(new EndpointStateMachine(null, null, null, ""));
    }

    CompletableFuture<EndpointStateMachine.EndPointStates> futureOne =
        new CompletableFuture<>();
    for (int i = 0; i < threadPoolSize; i++) {
      ecs.submit(() -> futureOne.get());
    }

    long startTime = Time.monotonicNow();
    state.await(500, TimeUnit.MILLISECONDS);
    long endTime = Time.monotonicNow();
    assertThat(endTime - startTime).isGreaterThanOrEqualTo(500);

    futureOne.complete(SHUTDOWN);

    CompletableFuture<EndpointStateMachine.EndPointStates> futureTwo =
        new CompletableFuture<>();
    for (int i = 0; i < threadPoolSize; i++) {
      ecs.submit(() -> futureTwo.get());
    }
    futureTwo.complete(SHUTDOWN);

    startTime = Time.monotonicNow();
    state.await(500, TimeUnit.MILLISECONDS);
    endTime = Time.monotonicNow();
    assertThat(endTime - startTime).isLessThan(500);

    executorService.shutdown();
  }

  @Test
  public void testLifeline() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(HDDS_LIFELINE_REPORT_ENABLE, true);
    SCMConnectionManager connectionManager = mock(SCMConnectionManager.class);
    List<EndpointStateMachine> stateMachines = new ArrayList<>();
    StorageContainerDatanodeProtocolClientSideTranslatorPB proxy =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    EndpointStateMachine endpointStateMachine1 = mock(EndpointStateMachine.class);
    when(endpointStateMachine1.getEndPoint()).thenReturn(proxy);
    when(endpointStateMachine1.getAddress()).thenReturn(TEST_SCM_ENDPOINT1);
    stateMachines.add(endpointStateMachine1);
    EndpointStateMachine endpointStateMachine2 = mock(EndpointStateMachine.class);
    when(endpointStateMachine2.getEndPoint()).thenReturn(proxy);
    when(endpointStateMachine2.getAddress()).thenReturn(TEST_SCM_ENDPOINT2);
    stateMachines.add(endpointStateMachine2);
    EndpointStateMachine endpointStateMachine3 = mock(EndpointStateMachine.class);
    when(endpointStateMachine3.getEndPoint()).thenReturn(proxy);
    when(endpointStateMachine3.getAddress()).thenReturn(TEST_SCM_ENDPOINT3);
    stateMachines.add(endpointStateMachine3);

    when(connectionManager.getValues()).thenReturn(stateMachines);
    StateContext context = new StateContext(conf,
        DatanodeStateMachine.DatanodeStates.RUNNING,
        mock(DatanodeStateMachine.class), "");
    context.setTermOfLeaderSCM(1);
    RunningDatanodeState runningDatanodeState =
        new RunningDatanodeState(conf, connectionManager, context);
    List<Thread> threads = runningDatanodeState.getLifelineThreads();
    assertEquals(3, threads.size());
    for (Thread thread : threads) {
      assertTrue(thread.isAlive());
    }
  }
}

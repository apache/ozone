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

package org.apache.hadoop.ozone.container.common.states.datanode;

import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.SHUTDOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for RunningDatanodeState.
 */
public class TestRunningDatanodeState {
  @Test
  public void testAwait() throws InterruptedException {
    SCMConnectionManager connectionManager = mock(SCMConnectionManager.class);
    List<EndpointStateMachine> stateMachines = new ArrayList<>();
    when(connectionManager.getValues()).thenReturn(stateMachines);

    RunningDatanodeState state =
        new RunningDatanodeState(null, connectionManager, null);

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
    state.setExecutingEndpointCount(threadPoolSize);

    long startTime = Time.monotonicNow();
    state.await(500, TimeUnit.MILLISECONDS);
    long endTime = Time.monotonicNow();
    assertThat(endTime - startTime).isGreaterThanOrEqualTo(500);

    // The next heartbeat must still collect old completions, even if endpoints were removed.
    state.clear();
    stateMachines.clear();
    state.execute(executorService);
    futureOne.complete(SHUTDOWN);

    startTime = Time.monotonicNow();
    assertThat(state.await(500, TimeUnit.MILLISECONDS)).isEqualTo(DatanodeStates.SHUTDOWN);
    endTime = Time.monotonicNow();
    assertThat(endTime - startTime).isLessThan(500);

    executorService.shutdown();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void testStartupCompletesAfterHeartbeatTimeout(int failureType) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneContainer container = mock(OzoneContainer.class);
    DatanodeStateMachine datanode = mock(DatanodeStateMachine.class);
    SCMConnectionManager connections = mock(SCMConnectionManager.class);
    when(datanode.getConnectionManager()).thenReturn(connections);
    when(datanode.getContainer()).thenReturn(container);
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(scm.getVersion(null)).thenReturn(versionResponse().getProtobufMessage());
    CountDownLatch initializing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Exception failure = failureType == 1 ? new IOException("Corrupt Raft log")
        : new IllegalStateException("Failed to initRaftLog", new IOException("Corrupt Raft log"));
    doAnswer(invocation -> {
      initializing.countDown();
      assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
      if (failureType != 0) {
        throw failure;
      }
      return null;
    }).when(container).start("cluster");
    ExecutorService executor = Executors.newCachedThreadPool();
    try (LogCapturer logs = LogCapturer.captureLogs(VersionEndpointTask.class);
         EndpointStateMachine endpoint = new EndpointStateMachine(new HostAndPort("scm", 9861), scm, conf, "")) {
      when(connections.getValues()).thenReturn(Collections.singletonList(endpoint));
      // Keep startup blocked until both the inner future wait and this heartbeat have finished.
      context.execute(executor, 5, TimeUnit.SECONDS);
      assertThat(initializing.getCount()).isZero();
      assertThat(context.getState()).isEqualTo(DatanodeStates.RUNNING);
      assertThat(endpoint.getState()).isEqualTo(EndPointStates.GETVERSION);
      release.countDown();
      // The endpoint executor is serial; this barrier waits for the startup task to finish.
      endpoint.getExecutorService().submit(() -> { }).get(5, TimeUnit.SECONDS);
      if (failureType == 0) {
        assertThat(endpoint.getState()).isEqualTo(EndPointStates.REGISTER);
        assertThat(context.getShutdownOnError()).isFalse();
      } else {
        assertThat(endpoint.getState()).isEqualTo(SHUTDOWN);
        assertThat(logs.getOutput()).contains("Failed to start required container services", "scm:9861",
            failure.getClass().getName(), "Corrupt Raft log");
        context.execute(executor, 5, TimeUnit.SECONDS);
        assertThat(context.getState()).isEqualTo(DatanodeStates.SHUTDOWN);
        assertThat(context.getShutdownOnError()).isTrue();
      }
      verify(container, times(1)).start("cluster");
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void testUnavailableScmDoesNotPreventStartup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneContainer container = mock(OzoneContainer.class);
    DatanodeStateMachine datanode = mock(DatanodeStateMachine.class);
    SCMConnectionManager connections = mock(SCMConnectionManager.class);
    when(datanode.getConnectionManager()).thenReturn(connections);
    when(datanode.getContainer()).thenReturn(container);
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
    StorageContainerDatanodeProtocolClientSideTranslatorPB unavailable =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(unavailable.getVersion(null)).thenThrow(new IOException("SCM unavailable"));
    StorageContainerDatanodeProtocolClientSideTranslatorPB available =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(available.getVersion(null)).thenReturn(versionResponse().getProtobufMessage());
    ExecutorService executor = Executors.newCachedThreadPool();
    try (EndpointStateMachine first = new EndpointStateMachine(new HostAndPort("scm1", 9861), unavailable, conf, "");
         EndpointStateMachine second = new EndpointStateMachine(new HostAndPort("scm2", 9861), available, conf, "")) {
      when(connections.getValues()).thenReturn(Arrays.asList(first, second));
      context.execute(executor, 5, TimeUnit.SECONDS);
      assertThat(first.getState()).isEqualTo(EndPointStates.GETVERSION);
      assertThat(second.getState()).isEqualTo(EndPointStates.REGISTER);
      assertThat(context.getState()).isEqualTo(DatanodeStates.RUNNING);
      assertThat(context.getShutdownOnError()).isFalse();
      verify(container, times(1)).start("cluster");
    } finally {
      executor.shutdownNow();
    }
  }

  private static VersionResponse versionResponse() {
    return VersionResponse.newBuilder().setVersion(1)
        .addValue(OzoneConsts.SCM_ID, "scm")
        .addValue(OzoneConsts.CLUSTER_ID, "cluster")
        .build();
  }
}

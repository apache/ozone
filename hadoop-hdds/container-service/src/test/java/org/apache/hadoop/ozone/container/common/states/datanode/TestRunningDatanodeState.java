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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.SHUTDOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FullContainerReportLeaseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.hdds.server.StartupProgress;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.DatanodeStartupPhase;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
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
import org.apache.ozone.test.GenericTestUtils;
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

  @Test
  void testLeasedFcrSentWhileOtherScmsAreBlocked() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 30, TimeUnit.SECONDS);
    DatanodeStateMachine datanode = mock(DatanodeStateMachine.class);
    SCMConnectionManager connections = mock(SCMConnectionManager.class);
    OzoneContainer container = mock(OzoneContainer.class);
    ContainerSet containers = mock(ContainerSet.class);
    when(datanode.getContainer()).thenReturn(container);
    when(datanode.getDatanodeDetails()).thenReturn(MockDatanodeDetails.randomDatanodeDetails());
    DatanodeVersionManager versionManager = mock(DatanodeVersionManager.class);
    when(versionManager.getApparentVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(versionManager.getSoftwareVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(versionManager.getVersionForClient()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(versionManager.isAllowed(HDDSVersion.FULL_CONTAINER_REPORT_LEASE)).thenReturn(true);
    when(datanode.getVersionManager()).thenReturn(versionManager);
    when(datanode.getQueuedCommandCount()).thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    when(container.getContainerSet()).thenReturn(containers);
    when(container.getPipelineReport()).thenReturn(PipelineReportsProto.getDefaultInstance());
    when(containers.getContainerReport()).thenReturn(ContainerReportsProto.getDefaultInstance());
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanode, "");
    context.configureHeartbeatFrequency();
    CountDownLatch peersBlocked = new CountDownLatch(2);
    CountDownLatch releasePeers = new CountDownLatch(1);
    CountDownLatch leaseRequested = new CountDownLatch(1);
    CountDownLatch releaseLease = new CountDownLatch(1);
    CountDownLatch reportSent = new CountDownLatch(1);
    AtomicReference<SCMHeartbeatRequestProto> report = new AtomicReference<>();
    StorageContainerDatanodeProtocolClientSideTranslatorPB blockedScm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(blockedScm.sendHeartbeat(any())).thenAnswer(invocation -> {
      SCMHeartbeatRequestProto request = invocation.getArgument(0);
      peersBlocked.countDown();
      assertThat(releasePeers.await(30, TimeUnit.SECONDS)).isTrue();
      return SCMHeartbeatResponseProto.newBuilder().setDatanodeUUID(request.getDatanodeDetails().getUuid()).build();
    });
    StorageContainerDatanodeProtocolClientSideTranslatorPB healthyScm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(healthyScm.sendHeartbeat(any())).thenAnswer(invocation -> {
      SCMHeartbeatRequestProto request = invocation.getArgument(0);
      SCMHeartbeatResponseProto.Builder response =
          SCMHeartbeatResponseProto.newBuilder().setDatanodeUUID(request.getDatanodeDetails().getUuid());
      if (request.getRequestFullContainerReportLease()) {
        leaseRequested.countDown();
        assertThat(releaseLease.await(30, TimeUnit.SECONDS)).isTrue();
        response.setFullContainerReportLease(FullContainerReportLeaseProto.newBuilder().setId(99).setTerm(7));
      } else if (request.hasContainerReport()) {
        report.set(request);
        reportSent.countDown();
      }
      return response.build();
    });
    List<EndpointStateMachine> endpoints = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      for (int i = 0; i < 3; i++) {
        EndpointStateMachine endpoint =
            new EndpointStateMachine(new HostAndPort("scm" + i, 9861), i < 2 ? blockedScm : healthyScm, conf, "");
        endpoints.add(endpoint);
        endpoint.setState(EndPointStates.HEARTBEAT);
        endpoint.setVersion(VersionResponse.newBuilder().setVersion(1)
            .addValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED, Boolean.TRUE.toString()).build());
        context.addEndpoint(endpoint.getAddress());
      }
      when(connections.getValues()).thenReturn(endpoints);
      RunningDatanodeState running = new RunningDatanodeState(conf, connections, context);
      running.execute(executor);
      assertThat(peersBlocked.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(leaseRequested.await(5, TimeUnit.SECONDS)).isTrue();
      // Queue duplicate peer waits while the healthy endpoint's lease request is still in flight.
      running.execute(executor);
      releaseLease.countDown();

      assertThat(reportSent.await(5, TimeUnit.SECONDS)).as("FCR sent before releasing blocked SCM peers").isTrue();
      assertThat(releasePeers.getCount()).isEqualTo(1);
      EndpointStateMachine healthy = endpoints.get(2);
      healthy.getExecutorService().submit(() -> { }).get(5, TimeUnit.SECONDS);
      assertThat(report.get().getRequestFullContainerReportLease()).isFalse();
      assertThat(report.get().getContainerReport().getFullContainerReportLease().getId()).isEqualTo(99);
      assertThat(report.get().getContainerReport().getFullContainerReportLease().getTerm()).isEqualTo(7);
      assertThat(context.isFullContainerReportReady(healthy.getAddress())).isFalse();
      assertThat(healthy.hasFullContainerReportLease()).isFalse();
      verify(healthyScm, times(2)).sendHeartbeat(any());
    } finally {
      releaseLease.countDown();
      releasePeers.countDown();
      executor.shutdownNow();
      for (EndpointStateMachine endpoint : endpoints) {
        endpoint.getExecutorService().shutdownNow();
        endpoint.close();
      }
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      for (EndpointStateMachine endpoint : endpoints) {
        assertThat(endpoint.getExecutorService().awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  void testStartupCompletesAfterHeartbeatTimeout(int failureType) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneContainer container = mock(OzoneContainer.class);
    StartupProgress<DatanodeStartupPhase> startupProgress = new StartupProgress<>(DatanodeStartupPhase.class);
    when(container.getStartupProgress()).thenReturn(startupProgress);
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
    Throwable failure = failureType == 1 ? new IOException("Corrupt Raft log")
        : failureType == 2 ? new IllegalStateException("Failed to initRaftLog", new IOException("Corrupt Raft log"))
        : new AssertionError("Failed to initRaftLog", new IOException("Corrupt Raft log"));
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
        assertThat(startupProgress.isFailed()).isFalse();
      } else {
        assertThat(endpoint.getState()).isEqualTo(SHUTDOWN);
        assertThat(startupProgress.isFailed()).isTrue();
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
    when(container.getStartupProgress()).thenReturn(new StartupProgress<>(DatanodeStartupPhase.class));
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

  @Test
  void testReconHeartbeatInterval() throws Exception {
    StateContext context = mock(StateContext.class);
    when(context.getHeartbeatFrequency()).thenReturn(100L);
    when(context.getReconHeartbeatFrequency()).thenReturn(1000L);
    RunningDatanodeState state = new RunningDatanodeState(null, mock(SCMConnectionManager.class), context);
    EndpointStateMachine recon = mock(EndpointStateMachine.class);
    when(recon.isPassive()).thenReturn(true);
    when(recon.getState()).thenReturn(EndPointStates.HEARTBEAT);
    EndpointStateMachine scm = mock(EndpointStateMachine.class);
    when(scm.getState()).thenReturn(EndPointStates.HEARTBEAT);

    final long start = Time.monotonicNow();
    assertThat(state.isReconHeartbeatNotDue(recon)).isFalse();
    assertThat(state.isReconHeartbeatNotDue(recon)).isTrue();
    // SCM heartbeat and Recon registration are not delayed
    assertThat(state.isReconHeartbeatNotDue(scm)).isFalse();
    when(recon.getState()).thenReturn(EndPointStates.REGISTER);
    assertThat(state.isReconHeartbeatNotDue(recon)).isFalse();

    when(recon.getState()).thenReturn(EndPointStates.HEARTBEAT);
    GenericTestUtils.waitFor(() -> !state.isReconHeartbeatNotDue(recon), 10, 5000);
    // Recon interval minus half of the SCM heartbeat interval
    assertThat(Time.monotonicNow() - start).isGreaterThanOrEqualTo(950);
  }

  private static VersionResponse versionResponse() {
    return VersionResponse.newBuilder().setVersion(1)
        .addValue(OzoneConsts.SCM_ID, "scm")
        .addValue(OzoneConsts.CLUSTER_ID, "cluster")
        .build();
  }
}

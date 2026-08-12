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

package org.apache.hadoop.hdds.scm.ha;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.util.concurrent.ExecutorHelper;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests SCMStateMachine events and deferred datanode-server startup.
 */
public class TestSCMStateMachine {
  private static final long RETRY_INTERVAL_MS = 100L;

  private final AtomicBoolean commitIndexAvailable = new AtomicBoolean();
  private final AtomicBoolean scmStopped = new AtomicBoolean();
  private final AtomicLong lastAppliedIndex = new AtomicLong(5L);
  private final RaftPeerId followerId = RaftPeerId.valueOf("follower");
  private final RaftPeerId leaderId = RaftPeerId.valueOf("leader");

  private StorageContainerManager scm;
  private SCMMetrics metrics;
  private SCMHADBTransactionBuffer buffer;
  private SCMDatanodeProtocolServer datanodeProtocolServer;
  private SCMSafeModeManager safeModeManager;
  private SCMStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    scm = mock(StorageContainerManager.class);
    metrics = SCMMetrics.create();
    buffer = mock(SCMHADBTransactionBuffer.class);
    datanodeProtocolServer = mock(SCMDatanodeProtocolServer.class);
    safeModeManager = mock(SCMSafeModeManager.class);

    SCMContext scmContext = mock(SCMContext.class);
    SCMHAManager haManager = mock(SCMHAManager.class);
    SCMRatisServer ratisServer = mock(SCMRatisServer.class);
    RaftServer.Division division = mock(RaftServer.Division.class);
    DivisionInfo divisionInfo = mock(DivisionInfo.class);

    when(scm.getMetrics()).thenReturn(metrics);
    when(scm.isStopped()).thenAnswer(invocation -> scmStopped.get());
    when(scm.getScmContext()).thenReturn(scmContext);
    when(scm.getScmHAManager()).thenReturn(haManager);
    when(scm.getDatanodeProtocolServer()).thenReturn(datanodeProtocolServer);
    when(scm.getScmSafeModeManager()).thenReturn(safeModeManager);
    when(haManager.getRatisServer()).thenReturn(ratisServer);
    when(ratisServer.getDivision()).thenReturn(division);
    when(division.getInfo()).thenReturn(divisionInfo);
    when(divisionInfo.getLeaderId()).thenReturn(leaderId);
    when(divisionInfo.getLastAppliedIndex()).thenAnswer(invocation -> lastAppliedIndex.get());
    when(division.getCommitInfos()).thenAnswer(invocation -> commitIndexAvailable.get()
        ? Collections.singletonList(RaftProtos.CommitInfoProto.newBuilder()
            .setServer(RaftProtos.RaftPeerProto.newBuilder().setId(leaderId.toByteString()))
            .setCommitIndex(5L)
            .build())
        : Collections.emptyList());
    when(buffer.getLatestTrxInfo()).thenReturn(
        TransactionInfo.valueOf(TermIndex.valueOf(0, 0)));

    stateMachine = new SCMStateMachine(scm, buffer, RETRY_INTERVAL_MS);
  }

  @AfterEach
  void tearDown() {
    stateMachine.stopDNServerStartRetry();
    metrics.unRegister();
  }

  @Test
  void testRatisEventsRecording() {
    stateMachine.notifyConfigurationChanged(1, 1, RaftProtos.RaftConfigurationProto.getDefaultInstance());
    assertTrue(metrics.getRatisEvents().contains("Configuration changed at term index"));
  }

  @Test
  void testRetryStartsDNServerWhenLeaderCommitIndexBecomesAvailable() throws Exception {
    LogCapturer executorLogs = LogCapturer.captureLogs(ExecutorHelper.class);
    try {
      stateMachine.notifyLeaderChanged(memberId(), leaderId);
      verify(datanodeProtocolServer, never()).start();

      commitIndexAvailable.set(true);

      verify(datanodeProtocolServer, timeout(2000)).start();
      verify(safeModeManager, timeout(2000)).refreshAndValidate();
      assertThat(stateMachine.getIsStateMachineReady()).isTrue();
      assertThat(stateMachine.isDNServerStartRetryStopped()).isTrue();
      GenericTestUtils.waitFor(stateMachine::isDNServerStartRetryTerminated, 10, 2000);
      assertThat(executorLogs.getOutput()).doesNotContain("CancellationException");
    } finally {
      executorLogs.stopCapturing();
    }
  }

  @Test
  void testRetryWaitsUntilFollowerCatchesUp() {
    commitIndexAvailable.set(true);
    lastAppliedIndex.set(4L);

    stateMachine.notifyLeaderChanged(memberId(), leaderId);

    verify(datanodeProtocolServer, after(RETRY_INTERVAL_MS * 3).never()).start();
    lastAppliedIndex.set(5L);
    verify(datanodeProtocolServer, timeout(2000)).start();
    verify(safeModeManager, timeout(2000)).refreshAndValidate();
  }

  @Test
  void testAlreadyCaughtUpStartsDNServerExactlyOnce() throws Exception {
    commitIndexAvailable.set(true);

    stateMachine.notifyLeaderChanged(memberId(), leaderId);
    stateMachine.notifyLeaderChanged(memberId(), leaderId);

    verify(datanodeProtocolServer, times(1)).start();
    verify(safeModeManager, times(1)).refreshAndValidate();
    assertThat(stateMachine.getIsStateMachineReady()).isTrue();
    GenericTestUtils.waitFor(stateMachine::isDNServerStartRetryTerminated, 10, 2000);
  }

  @Test
  void testStopPreventsPendingRetryFromStartingDNServer() {
    stateMachine.notifyLeaderChanged(memberId(), leaderId);
    scmStopped.set(true);

    stateMachine.stopDNServerStartRetry();

    assertThat(stateMachine.isDNServerStartRetryStopped()).isTrue();
    commitIndexAvailable.set(true);
    verify(datanodeProtocolServer, after(RETRY_INTERVAL_MS * 2).never()).start();
  }

  @Test
  void testStopWaitsForRunningRetry() throws Exception {
    CountDownLatch startEntered = new CountDownLatch(1);
    CountDownLatch allowStart = new CountDownLatch(1);
    doAnswer(invocation -> {
      startEntered.countDown();
      assertTrue(allowStart.await(2, TimeUnit.SECONDS));
      return null;
    }).when(datanodeProtocolServer).start();
    stateMachine.notifyLeaderChanged(memberId(), leaderId);
    commitIndexAvailable.set(true);
    assertTrue(startEntered.await(2, TimeUnit.SECONDS));

    scmStopped.set(true);
    CompletableFuture<Void> stopFuture = CompletableFuture.runAsync(stateMachine::stopDNServerStartRetry);
    assertThrows(TimeoutException.class, () -> stopFuture.get(100, TimeUnit.MILLISECONDS));

    allowStart.countDown();
    stopFuture.get(2, TimeUnit.SECONDS);
    assertThat(stateMachine.isDNServerStartRetryTerminated()).isTrue();
    verify(datanodeProtocolServer, times(1)).start();
  }

  private RaftGroupMemberId memberId() {
    return RaftGroupMemberId.valueOf(followerId, RaftGroupId.randomId());
  }
}

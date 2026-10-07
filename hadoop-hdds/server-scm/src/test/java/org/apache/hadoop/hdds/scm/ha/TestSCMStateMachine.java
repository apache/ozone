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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.invoker.ScmInvoker;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.util.concurrent.ExecutorHelper;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests SCMStateMachine events and deferred datanode-server startup.
 */
public class TestSCMStateMachine {
  private static final Duration RETRY_INTERVAL = Duration.ofMillis(100);

  private final AtomicBoolean commitIndexAvailable = new AtomicBoolean();
  private final AtomicBoolean scmStopped = new AtomicBoolean();
  private final AtomicLong lastAppliedIndex = new AtomicLong(5L);
  private final AtomicLong leaderCommitIndex = new AtomicLong(5L);
  private final AtomicLong retryClock = new AtomicLong(1000L);
  private final RaftPeerId followerId = RaftPeerId.valueOf("follower");
  private final RaftPeerId leaderId = RaftPeerId.valueOf("leader");

  private SCMMetrics metrics;
  private SCMDatanodeProtocolServer datanodeProtocolServer;
  private SCMSafeModeManager safeModeManager;
  private SCMStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    metrics = SCMMetrics.create();
    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
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
            .setCommitIndex(leaderCommitIndex.get())
            .build())
        : Collections.emptyList());
    when(buffer.getLatestTrxInfo()).thenReturn(
        TransactionInfo.valueOf(TermIndex.valueOf(0, 0)));

    stateMachine = new SCMStateMachine(scm, buffer, RETRY_INTERVAL, retryClock::get);
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

    verify(datanodeProtocolServer, after(RETRY_INTERVAL.toMillis() * 3).never()).start();
    lastAppliedIndex.set(5L);
    verify(datanodeProtocolServer, timeout(2000)).start();
    verify(safeModeManager, timeout(2000)).refreshAndValidate();
  }

  @Test
  void testRetryUsesCapturedCommitIndexWhenLeaderAdvances() {
    commitIndexAvailable.set(true);
    lastAppliedIndex.set(4L);
    stateMachine.notifyLeaderChanged(memberId(), leaderId);

    leaderCommitIndex.set(100L);
    lastAppliedIndex.set(5L);

    verify(datanodeProtocolServer, timeout(2000).times(1)).start();
    verify(safeModeManager, timeout(2000).times(1)).refreshAndValidate();
    assertThat(stateMachine.getIsStateMachineReady()).isTrue();
  }

  @Test
  void testPendingRetryWarningsAreThrottledAndStopWhenReady() throws Exception {
    commitIndexAvailable.set(true);
    lastAppliedIndex.set(4L);
    LogCapturer logs = LogCapturer.captureLogs(SCMStateMachine.class);
    try {
      stateMachine.notifyLeaderChanged(memberId(), leaderId);
      retryClock.set(30999L);
      verify(datanodeProtocolServer, after(RETRY_INTERVAL.toMillis() * 2).never()).start();
      assertThat(logs.getOutput()).doesNotContain("still waiting for follower catch-up");

      retryClock.set(31000L);
      GenericTestUtils.waitFor(() -> logs.getOutput().contains("elapsed=30000ms"), 10, 2000);
      assertThat(logs.getOutput()).containsPattern(
          "attempts=[1-9][0-9]*, elapsed=30000ms, lastAppliedIndex=4, leaderCommitIndexOnStart=5");

      retryClock.set(330999L);
      verify(datanodeProtocolServer, after(RETRY_INTERVAL.toMillis() * 2).never()).start();
      assertThat(logs.getOutput()).containsOnlyOnce("still waiting for follower catch-up");

      retryClock.set(331000L);
      GenericTestUtils.waitFor(() -> logs.getOutput().contains("elapsed=330000ms"), 10, 2000);
      assertThat(logs.getOutput()).containsOnlyOnce("elapsed=330000ms");

      lastAppliedIndex.set(5L);
      verify(datanodeProtocolServer, timeout(2000)).start();
      GenericTestUtils.waitFor(stateMachine::isDNServerStartRetryTerminated, 10, 2000);
      logs.clearOutput();
      retryClock.set(1000000L);
      stateMachine.notifyLeaderChanged(memberId(), leaderId);
      assertThat(logs.getOutput()).doesNotContain("still waiting for follower catch-up");
    } finally {
      logs.stopCapturing();
    }
  }

  @Test
  void testPendingRetryWarningIncludesUnavailableLeaderCommitIndex() throws Exception {
    LogCapturer logs = LogCapturer.captureLogs(SCMStateMachine.class);
    try {
      stateMachine.notifyLeaderChanged(memberId(), leaderId);
      retryClock.set(31000L);

      GenericTestUtils.waitFor(() -> logs.getOutput().contains("elapsed=30000ms"), 10, 2000);
      assertThat(logs.getOutput()).contains("lastAppliedIndex=5, leaderCommitIndexOnStart=-1");
      verify(datanodeProtocolServer, never()).start();

      scmStopped.set(true);
      stateMachine.stopDNServerStartRetry();
      logs.clearOutput();
      retryClock.set(1000000L);
      assertThat(stateMachine.isDNServerStartRetryTerminated()).isTrue();
      assertThat(logs.getOutput()).doesNotContain("still waiting for follower catch-up");
    } finally {
      logs.stopCapturing();
    }
  }

  @Test
  void testBootstrapStateMachineSkipsSCMCallbacksAndRetryStop() throws Exception {
    try (SCMStateMachine bootstrap = new SCMStateMachine()) {
      bootstrap.notifyLeaderChanged(memberId(), leaderId);
      bootstrap.notifyLeaderChanged(memberId(), followerId);
      bootstrap.notifyTermIndexUpdated(1, 1);
      bootstrap.notifyLeaderReady();
      bootstrap.notifyNotLeader(Collections.emptyList());
      bootstrap.stopDNServerStartRetry();
      bootstrap.stopDNServerStartRetry();

      assertThat(bootstrap.getIsStateMachineReady()).isFalse();
      assertThat(bootstrap.getLatestSnapshot()).isNull();
    }
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
    verify(datanodeProtocolServer, after(RETRY_INTERVAL.toMillis() * 2).never()).start();
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

  @Test
  public void testApplyTransactionFlushesAfterRecordingTransactionInfo() throws Exception {
    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    SCMStateMachine applyingStateMachine = newStateMachine(buffer, succeedingInvoker());

    CompletableFuture<Message> result = applyingStateMachine.applyTransaction(newTransaction(7));

    assertTrue(result.isDone() && !result.isCompletedExceptionally());
    InOrder order = inOrder(buffer);
    order.verify(buffer).beginApplyingTransaction();
    order.verify(buffer).updateLatestTrxInfo(TransactionInfo.valueOf(TermIndex.valueOf(1, 7)));
    order.verify(buffer).flushIfPendingLimitReached();
    order.verify(buffer).endApplyingTransaction();
  }

  @Test
  public void testFlushCheckedForEveryAppliedTransaction() throws Exception {
    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    SCMStateMachine applyingStateMachine = newStateMachine(buffer, succeedingInvoker());

    for (int i = 1; i <= 5; i++) {
      applyingStateMachine.applyTransaction(newTransaction(i));
    }

    verify(buffer, times(5)).flushIfPendingLimitReached();
    verify(buffer, times(5)).endApplyingTransaction();
  }

  @Test
  public void testFlushStillCheckedWhenTransactionIsLogicallyRejected() throws Exception {
    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    ScmInvoker<?> invoker = mock(ScmInvoker.class);
    when(invoker.invokeLocal(anyString(), any()))
        .thenThrow(new SCMException("rejected", ResultCodes.FAILED_TO_FIND_CONTAINER));
    SCMStateMachine applyingStateMachine = newStateMachine(buffer, invoker);

    CompletableFuture<Message> result = applyingStateMachine.applyTransaction(newTransaction(3));

    assertTrue(result.isCompletedExceptionally());
    InOrder order = inOrder(buffer);
    order.verify(buffer).updateLatestTrxInfo(TransactionInfo.valueOf(TermIndex.valueOf(1, 3)));
    order.verify(buffer).flushIfPendingLimitReached();
    order.verify(buffer).endApplyingTransaction();
  }

  @Test
  public void testFlushFailureTerminatesAndClosesApplyingWindow() throws Exception {
    ExitUtils.disableSystemExit();
    try {
      SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
      doThrow(new IllegalStateException("injected flush failure")).when(buffer).flushIfPendingLimitReached();
      SCMStateMachine applyingStateMachine = newStateMachine(buffer, succeedingInvoker());

      assertThrows(ExitUtils.ExitException.class, () -> applyingStateMachine.applyTransaction(newTransaction(9)));

      verify(buffer).endApplyingTransaction();
    } finally {
      ExitUtils.clear();
    }
  }

  private static TransactionContext newTransaction(long index) throws Exception {
    SCMRatisRequest request = SCMRatisRequest.of(RequestType.PIPELINE, "op", new Class<?>[0]);
    RaftProtos.StateMachineLogEntryProto smLogEntry = RaftProtos.StateMachineLogEntryProto.newBuilder()
        .setLogData(request.encode().getContent())
        .build();
    RaftProtos.LogEntryProto logEntry = RaftProtos.LogEntryProto.newBuilder()
        .setTerm(1)
        .setIndex(index)
        .setStateMachineLogEntry(smLogEntry)
        .build();
    TransactionContext trx = mock(TransactionContext.class);
    when(trx.getStateMachineLogEntry()).thenReturn(smLogEntry);
    when(trx.getLogEntry()).thenReturn(logEntry);
    return trx;
  }

  private static SCMStateMachine newStateMachine(SCMHADBTransactionBuffer buffer, ScmInvoker<?> invoker) {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getMetrics()).thenReturn(mock(SCMMetrics.class));
    SCMContext context = mock(SCMContext.class);
    when(context.isLeader()).thenReturn(true);
    when(scm.getScmContext()).thenReturn(context);
    when(scm.getDatanodeProtocolServer()).thenReturn(mock(SCMDatanodeProtocolServer.class));
    when(scm.getScmSafeModeManager()).thenReturn(mock(SCMSafeModeManager.class));
    when(buffer.getLatestTrxInfo()).thenReturn(TransactionInfo.valueOf(TermIndex.valueOf(0, 0)));
    SCMStateMachine stateMachine = new SCMStateMachine(scm, buffer);
    stateMachine.registerInvoker(RequestType.PIPELINE, invoker);
    return stateMachine;
  }

  private static ScmInvoker<?> succeedingInvoker() throws Exception {
    ScmInvoker<?> invoker = mock(ScmInvoker.class);
    when(invoker.invokeLocal(anyString(), any())).thenReturn(Message.EMPTY);
    return invoker;
  }
}

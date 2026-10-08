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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.invoker.ScmInvoker;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Test SCMStateMachine events recording.
 */
public class TestSCMStateMachine {

  @Test
  public void testRatisEventsRecording() throws Exception {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    SCMMetrics metrics = SCMMetrics.create();
    when(scm.getMetrics()).thenReturn(metrics);

    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    when(buffer.getLatestTrxInfo()).thenReturn(TransactionInfo.valueOf(TermIndex.valueOf(0, 0)));

    SCMStateMachine stateMachine = new SCMStateMachine(scm, buffer);

    stateMachine.notifyConfigurationChanged(1, 1, RaftProtos.RaftConfigurationProto.getDefaultInstance());
    assertTrue(metrics.getRatisEvents().contains("Configuration changed at term index"));

    metrics.unRegister();
  }
  
  @Test
  public void testApplyTransactionFlushesAfterRecordingTransactionInfo() throws Exception {
    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    SCMStateMachine stateMachine = newStateMachine(buffer, succeedingInvoker());

    CompletableFuture<Message> result = stateMachine.applyTransaction(newTransaction(7));

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
    SCMStateMachine stateMachine = newStateMachine(buffer, succeedingInvoker());

    for (int i = 1; i <= 5; i++) {
      stateMachine.applyTransaction(newTransaction(i));
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
    SCMStateMachine stateMachine = newStateMachine(buffer, invoker);

    CompletableFuture<Message> result = stateMachine.applyTransaction(newTransaction(3));

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
      SCMStateMachine stateMachine = newStateMachine(buffer, succeedingInvoker());

      assertThrows(ExitUtils.ExitException.class, () -> stateMachine.applyTransaction(newTransaction(9)));

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

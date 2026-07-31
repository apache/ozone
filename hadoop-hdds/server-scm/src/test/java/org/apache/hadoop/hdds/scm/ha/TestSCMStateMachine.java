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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.ha.invoker.ScmInvoker;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.Test;

/**
 * Test SCMStateMachine events recording.
 */
public class TestSCMStateMachine {

  @Test
  public void testRatisEventsRecording() throws Exception {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    SCMMetrics metrics = SCMMetrics.create();
    when(scm.getMetrics()).thenReturn(metrics);
    mockCloseForStateMachine(scm);

    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    when(buffer.getLatestTrxInfo()).thenReturn(TransactionInfo.valueOf(TermIndex.valueOf(0, 0)));

    try (SCMStateMachine stateMachine = new SCMStateMachine(scm, buffer)) {
      stateMachine.notifyConfigurationChanged(1, 1, RaftProtos.RaftConfigurationProto.getDefaultInstance());
    }

    assertTrue(metrics.getRatisEvents().contains("Configuration changed at term index"));

    metrics.unRegister();
  }

  /**
   * A finalization step that throws an UpgradeException (an IOException, not an SCMException) must
   * crash SCM rather than be returned to the Ratis client. UpgradeException skips the inner
   * catch (SCMException) and hits the outer catch (Exception) -> ExitUtils.terminate.
   */
  @Test
  public void testUpgradeExceptionDuringApplyTerminates() throws Exception {
    ExitUtils.disableSystemExit();

    StorageContainerManager scm = mock(StorageContainerManager.class);
    SCMMetrics metrics = SCMMetrics.create();
    when(scm.getMetrics()).thenReturn(metrics);
    mockCloseForStateMachine(scm);

    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    when(buffer.getLatestTrxInfo()).thenReturn(TransactionInfo.valueOf(TermIndex.valueOf(0, 0)));

    try (SCMStateMachine stateMachine = new SCMStateMachine(scm, buffer)) {
      ScmInvoker<?> invoker = mock(ScmInvoker.class);
      when(invoker.invokeLocal(any(), any())).thenThrow(
          new UpgradeException(UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED));
      stateMachine.registerInvoker(RequestType.FINALIZE, invoker);

      SCMRatisRequest request = SCMRatisRequest.of(
          RequestType.FINALIZE, "finalize", new Class<?>[]{});
      StateMachineLogEntryProto smLogEntry = StateMachineLogEntryProto.newBuilder()
          .setLogData(request.encode().getContent())
          .build();
      LogEntryProto logEntry = LogEntryProto.newBuilder()
          .setTerm(1)
          .setIndex(1)
          .setStateMachineLogEntry(smLogEntry)
          .build();
      TransactionContext trx = mock(TransactionContext.class);
      when(trx.getStateMachineLogEntry()).thenReturn(smLogEntry);
      when(trx.getLogEntry()).thenReturn(logEntry);

      // terminate throws ExitException when system exit is disabled
      assertThrows(ExitUtils.ExitException.class,
          () -> stateMachine.applyTransaction(trx));
    }

    metrics.unRegister();
  }

  /**
   * Stub the SCM HA manager so SCMStateMachine.close() sees a stopped Ratis server and performs a
   * clean shutdown instead of dereferencing null when the state machine is auto-closed.
   */
  private static void mockCloseForStateMachine(StorageContainerManager scm) {
    SCMHAManager haManager = mock(SCMHAManager.class);
    SCMRatisServer ratisServer = mock(SCMRatisServer.class);
    when(scm.getScmHAManager()).thenReturn(haManager);
    when(haManager.getRatisServer()).thenReturn(ratisServer);
    when(ratisServer.isStopped()).thenReturn(true);
  }
}

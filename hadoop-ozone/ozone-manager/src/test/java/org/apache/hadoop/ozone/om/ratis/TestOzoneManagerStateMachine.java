/*
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.ratis;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequestArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Class to test OzoneManagerStateMachine.
 */
public class TestOzoneManagerStateMachine {

  @TempDir
  private Path tempDir;

  private OzoneManagerStateMachine ozoneManagerStateMachine;
  private OzoneManagerPrepareState prepareState;

  @BeforeEach
  public void setup() throws Exception {
    OzoneManagerRatisServer ozoneManagerRatisServer =
        Mockito.mock(OzoneManagerRatisServer.class);
    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
    // Allow testing of prepare pre-append gate.
    when(ozoneManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(true);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        tempDir.toAbsolutePath().toString());

    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf,
        ozoneManager);

    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    prepareState = new OzoneManagerPrepareState(conf);
    when(ozoneManager.getPrepareState()).thenReturn(prepareState);

    when(ozoneManagerRatisServer.getOzoneManager()).thenReturn(ozoneManager);
    when(ozoneManager.getTransactionInfo()).thenReturn(Mockito.mock(TransactionInfo.class));
    when(ozoneManager.getConfiguration()).thenReturn(conf);
    ozoneManagerStateMachine =
        new OzoneManagerStateMachine(ozoneManagerRatisServer, false);
  }

  static void assertTermIndex(long expectedTerm, long expectedIndex, TermIndex computed) {
    Assertions.assertEquals(expectedTerm, computed.getTerm());
    Assertions.assertEquals(expectedIndex, computed.getIndex());
  }

  @Test
  public void testLastAppliedIndex() {
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 0);
    assertTermIndex(0, RaftLog.INVALID_LOG_INDEX, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 0, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 1);
    assertTermIndex(0, RaftLog.INVALID_LOG_INDEX, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 1, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // call update last applied index
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(0, 2));
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(0, 3));

    assertTermIndex(0, 3, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 1, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(1L, 4L);

    assertTermIndex(0, 3, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(1, 4, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // Add some apply transactions.
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(1L, 5L));
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(1L, 6L));

    assertTermIndex(1, 6, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(1, 4, ozoneManagerStateMachine.getLastNotifiedTermIndex());
  }

  @Test
  public void testPreAppendTransaction() throws Exception {
    // Submit write request.
    KeyArgs args = KeyArgs
        .newBuilder()
        .setVolumeName("volume")
        .setBucketName("bucket")
        .setKeyName("key")
        .build();
    OMRequest createKeyRequest = OMRequest.newBuilder()
        .setCreateKeyRequest(CreateKeyRequest.newBuilder().setKeyArgs(args))
        .setCmdType(Type.CreateKey)
        .setClientId("123")
        .setUserInfo(UserInfo
            .newBuilder()
            .setUserName("user")
            .setHostName("localhost")
            .setRemoteAddress("127.0.0.1"))
        .build();
    // Without prepare enabled, the txn should be returned unaltered.
    TransactionContext submittedTrx = mockTransactionContext(createKeyRequest);
    TransactionContext returnedTrx =
        ozoneManagerStateMachine.preAppendTransaction(submittedTrx);
    Assertions.assertSame(submittedTrx, returnedTrx);

    Assertions.assertEquals(PrepareStatus.NOT_PREPARED,
        prepareState.getState().getStatus());

    // Submit prepare request.
    OMRequest prepareRequest = OMRequest.newBuilder()
        .setPrepareRequest(
            PrepareRequest.newBuilder()
                .setArgs(PrepareRequestArgs.getDefaultInstance()))
        .setCmdType(Type.Prepare)
        .setClientId("123")
        .setUserInfo(UserInfo
            .newBuilder()
            .setUserName("user")
            .setHostName("localhost")
            .setRemoteAddress("127.0.0.1"))
        .build();

    submittedTrx = mockTransactionContext(prepareRequest);
    returnedTrx = ozoneManagerStateMachine.preAppendTransaction(submittedTrx);
    Assertions.assertSame(submittedTrx, returnedTrx);

    // Prepare should be started.
    Assertions.assertEquals(PrepareStatus.PREPARE_GATE_ENABLED,
        prepareState.getState().getStatus());

    // Submitting a write request should now fail.
    try {
      ozoneManagerStateMachine.preAppendTransaction(
          mockTransactionContext(createKeyRequest));
      Assertions.fail("Expected StateMachineException to be thrown when " +
          "submitting write request while prepared.");
    } catch (StateMachineException smEx) {
      Assertions.assertFalse(smEx.leaderShouldStepDown());

      Throwable cause = smEx.getCause();
      Assertions.assertTrue(cause instanceof OMException);
      Assertions.assertEquals(((OMException) cause).getResult(),
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED);
    }

    // Should be able to prepare again without issue.
    submittedTrx = mockTransactionContext(prepareRequest);
    returnedTrx = ozoneManagerStateMachine.preAppendTransaction(submittedTrx);
    Assertions.assertSame(submittedTrx, returnedTrx);

    Assertions.assertEquals(PrepareStatus.PREPARE_GATE_ENABLED,
        prepareState.getState().getStatus());

    // Cancel prepare is handled in the cancel request apply txn step, not
    // the pre-append state machine step, so it is tested in other classes.
  }

  private TransactionContext mockTransactionContext(OMRequest request) {
    RaftProtos.StateMachineLogEntryProto logEntry =
        RaftProtos.StateMachineLogEntryProto.newBuilder()
            .setLogData(OMRatisHelper.convertRequestToByteString(request))
            .build();

    TransactionContext mockTrx = Mockito.mock(TransactionContext.class);
    when(mockTrx.getStateMachineLogEntry()).thenReturn(logEntry);
    when(mockTrx.getStateMachineContext()).thenReturn(request);

    return mockTrx;
  }
}

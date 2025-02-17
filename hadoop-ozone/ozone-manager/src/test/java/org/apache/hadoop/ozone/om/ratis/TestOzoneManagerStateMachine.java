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

package org.apache.hadoop.ozone.om.ratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequestArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Class to test OzoneManagerStateMachine.
 */
public class TestOzoneManagerStateMachine {

  @TempDir
  private Path tempDir;

  private OzoneManagerStateMachine ozoneManagerStateMachine;
  private OzoneManagerPrepareState prepareState;
  private AuditLogger auditLogger;

  @BeforeEach
  public void setup() throws Exception {
    OzoneManagerRatisServer ozoneManagerRatisServer =
        mock(OzoneManagerRatisServer.class);
    OzoneManager ozoneManager = mock(OzoneManager.class);
    // Allow testing of prepare pre-append gate.
    when(ozoneManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(true);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        tempDir.toAbsolutePath().toString());

    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf,
        ozoneManager);

    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    auditLogger = mock(AuditLogger.class);

    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    prepareState = new OzoneManagerPrepareState(conf);
    when(ozoneManager.getPrepareState()).thenReturn(prepareState);

    when(ozoneManagerRatisServer.getOzoneManager()).thenReturn(ozoneManager);
    when(ozoneManager.getTransactionInfo()).thenReturn(mock(TransactionInfo.class));
    when(ozoneManager.getConfiguration()).thenReturn(conf);
    final OmConfig omConfig = conf.getObject(OmConfig.class);
    when(ozoneManager.getConfig()).thenReturn(omConfig);
    ozoneManagerStateMachine =
        new OzoneManagerStateMachine(ozoneManagerRatisServer, false);
  }

  static void assertTermIndex(long expectedTerm, long expectedIndex, TermIndex computed) {
    assertEquals(expectedTerm, computed.getTerm());
    assertEquals(expectedIndex, computed.getIndex());
  }

  @Test
  public void testLastAppliedIndex() {
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 0);
    assertTermIndex(0, 0, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 0, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 1);
    assertTermIndex(0, 1, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 1, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // call update last applied index
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(0, 2));
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(0, 3));

    assertTermIndex(0, 3, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 1, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(1L, 4L);

    assertTermIndex(1, 4, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(1, 4, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // Add some apply transactions.
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(1L, 5L));
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(1L, 6L));

    assertTermIndex(1, 6, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(1, 4, ozoneManagerStateMachine.getLastNotifiedTermIndex());
  }

  @Test
  public void testNotifyTermIndexPendingBufferUpdateIndex() {
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 0);
    assertTermIndex(0, 0, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 0, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // notifyTermIndex with skipping one of transaction which is from applyTransaction
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 2);
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 3);
    assertTermIndex(0, 0, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 3, ozoneManagerStateMachine.getLastNotifiedTermIndex());

    // applyTransaction update with missing transaction as above
    ozoneManagerStateMachine.updateLastAppliedTermIndex(TermIndex.valueOf(0, 1));
    assertTermIndex(0, 3, ozoneManagerStateMachine.getLastAppliedTermIndex());

    assertTermIndex(0, 3, ozoneManagerStateMachine.getLastAppliedTermIndex());
    assertTermIndex(0, 3, ozoneManagerStateMachine.getLastNotifiedTermIndex());
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
    assertSame(submittedTrx, returnedTrx);

    assertEquals(PrepareStatus.NOT_PREPARED, prepareState.getState().getStatus());

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
    assertSame(submittedTrx, returnedTrx);

    // Prepare should be started.
    assertEquals(PrepareStatus.PREPARE_GATE_ENABLED,
        prepareState.getState().getStatus());

    // Submitting a write request should now fail.
    StateMachineException smEx =
        assertThrows(StateMachineException.class,
            () -> ozoneManagerStateMachine.preAppendTransaction(mockTransactionContext(createKeyRequest)),
            "Expected StateMachineException to be thrown when submitting write request while prepared.");
    assertFalse(smEx.leaderShouldStepDown());

    Throwable cause = smEx.getCause();
    OMException omException = assertInstanceOf(OMException.class, cause);
    assertEquals(omException.getResult(), OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED);

    // Should be able to prepare again without issue.
    submittedTrx = mockTransactionContext(prepareRequest);
    returnedTrx = ozoneManagerStateMachine.preAppendTransaction(submittedTrx);
    assertSame(submittedTrx, returnedTrx);

    assertEquals(PrepareStatus.PREPARE_GATE_ENABLED, prepareState.getState().getStatus());

    // Cancel prepare is handled in the cancel request apply txn step, not
    // the pre-append state machine step, so it is tested in other classes.
  }

  @Test
  public void testApplyTransactionExceptionAuditLog() throws Exception {
    ExitUtils.disableSystemExit();
    // submit a create volume request having null pointer exception
    OzoneManagerProtocolProtos.VolumeInfo volInfo = OzoneManagerProtocolProtos.VolumeInfo.newBuilder()
        .setAdminName("a").setOwnerName("a").setVolume("a").build();
    OMRequest createVolRequest = OMRequest.newBuilder()
        .setCreateVolumeRequest(OzoneManagerProtocolProtos.CreateVolumeRequest.newBuilder().setVolumeInfo(volInfo))
        .setCmdType(Type.CreateVolume).setClientId("123")
        .setUserInfo(UserInfo.newBuilder().setUserName("user").setHostName("localhost").setRemoteAddress("127.0.0.1"))
        .build();
    TransactionContext submittedTrx = mockTransactionContext(createVolRequest);
    Mockito.doAnswer((i) -> {
      if (!((AuditMessage) i.getArgument(0)).getFormattedMessage().contains("Transaction=10") ||
          !((AuditMessage) i.getArgument(0)).getFormattedMessage().contains("Command=CreateVolume")) {
        Assertions.fail("transaction and command not found");
      }
      // throw another exception to change to new exception to avoid terminate call
      throw new OMException("test", OMException.ResultCodes.VOLUME_IS_REFERENCED);
    }).when(auditLogger).logWrite(any());
    CompletableFuture<Message> messageCompletableFuture = ozoneManagerStateMachine.applyTransaction(submittedTrx);
    try {
      messageCompletableFuture.get();
    } catch (Exception ex) {
      // do nothing
    }
  }

  private TransactionContext mockTransactionContext(OMRequest request) {
    RaftProtos.StateMachineLogEntryProto logEntry =
        RaftProtos.StateMachineLogEntryProto.newBuilder()
            .setLogData(OMRatisHelper.convertRequestToByteString(request))
            .build();

    TransactionContext mockTrx = mock(TransactionContext.class);
    when(mockTrx.getStateMachineLogEntry()).thenReturn(logEntry);
    when(mockTrx.getStateMachineContext()).thenReturn(request);
    RaftProtos.LogEntryProto logEntryProto = LogProtoUtils.toLogEntryProto(10, 10, 10);
    when(mockTrx.getLogEntry()).thenReturn(logEntryProto);

    return mockTrx;
  }
}

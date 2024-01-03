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
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

    prepareState = new OzoneManagerPrepareState(conf);
    when(ozoneManager.getPrepareState()).thenReturn(prepareState);

    when(ozoneManagerRatisServer.getOzoneManager()).thenReturn(ozoneManager);
    when(ozoneManager.getTransactionInfo()).thenReturn(mock(TransactionInfo.class));
    when(ozoneManager.getConfiguration()).thenReturn(conf);
    ozoneManagerStateMachine =
        new OzoneManagerStateMachine(ozoneManagerRatisServer, false);
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 0);
  }

  @Test
  public void testLastAppliedIndex() {

    // Happy scenario.

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 1);
    assertEquals(0, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(1, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    List<Long> flushedEpochs = new ArrayList<>();

    // Add some apply transactions.
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0, 2);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0, 3);

    flushedEpochs.add(2L);
    flushedEpochs.add(3L);

    // call update last applied index
    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    assertEquals(0, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(3, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0L, 4L);

    assertEquals(0L, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(4L, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    // Add some apply transactions.
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 5L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 6L);

    flushedEpochs.clear();
    flushedEpochs.add(5L);
    flushedEpochs.add(6L);
    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    assertEquals(0L, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(6L, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());


  }


  @Test
  public void testApplyTransactionsUpdateLastAppliedIndexCalledLate() {
    // Now try a scenario where 1,2,3 transactions are in applyTransactionMap
    // and updateLastAppliedIndex is not called for them, and before that
    // notifyTermIndexUpdated is called with transaction 4. And see now at the
    // end when updateLastAppliedIndex is called with epochs we have
    // lastAppliedIndex as 4 or not.

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 1);
    assertEquals(0, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(1, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());



    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 2L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 3L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 4L);



    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0L, 5L);

  // Still it should be zero, as for 2,3,4 updateLastAppliedIndex is not yet
    // called so the lastAppliedIndex will be at older value.
    assertEquals(0L, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(1L, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    List<Long> flushedEpochs = new ArrayList<>();


    flushedEpochs.add(2L);
    flushedEpochs.add(3L);
    flushedEpochs.add(4L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    assertEquals(0L, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(5L, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

  }


  @Test
  public void testLastAppliedIndexWithMultipleExecutors() {

    // first flush batch
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 1L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 2L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 4L);

    List<Long> flushedEpochs = new ArrayList<>();


    flushedEpochs.add(1L);
    flushedEpochs.add(2L);
    flushedEpochs.add(4L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    assertEquals(0L, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(2L, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());




    // 2nd flush batch
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 3L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 5L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 6L);

    flushedEpochs.clear();
    flushedEpochs.add(3L);
    flushedEpochs.add(5L);
    flushedEpochs.add(6L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    assertEquals(0L, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(6L, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    // 3rd flush batch
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 7L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 8L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 9L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 10L);

    flushedEpochs.clear();
    flushedEpochs.add(7L);
    flushedEpochs.add(8L);
    flushedEpochs.add(9L);
    flushedEpochs.add(10L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    assertEquals(0L, ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    assertEquals(10L, ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());
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
    try {
      ozoneManagerStateMachine.preAppendTransaction(
          mockTransactionContext(createKeyRequest));
      fail("Expected StateMachineException to be thrown when " +
          "submitting write request while prepared.");
    } catch (StateMachineException smEx) {
      assertFalse(smEx.leaderShouldStepDown());

      Throwable cause = smEx.getCause();
      assertInstanceOf(OMException.class, cause);
      assertEquals(((OMException) cause).getResult(),
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED);
    }

    // Should be able to prepare again without issue.
    submittedTrx = mockTransactionContext(prepareRequest);
    returnedTrx = ozoneManagerStateMachine.preAppendTransaction(submittedTrx);
    assertSame(submittedTrx, returnedTrx);

    assertEquals(PrepareStatus.PREPARE_GATE_ENABLED, prepareState.getState().getStatus());

    // Cancel prepare is handled in the cancel request apply txn step, not
    // the pre-append state machine step, so it is tested in other classes.
  }

  private TransactionContext mockTransactionContext(OMRequest request) {
    RaftProtos.StateMachineLogEntryProto logEntry =
        RaftProtos.StateMachineLogEntryProto.newBuilder()
            .setLogData(OMRatisHelper.convertRequestToByteString(request))
            .build();

    TransactionContext mockTrx = mock(TransactionContext.class);
    when(mockTrx.getStateMachineLogEntry()).thenReturn(logEntry);
    when(mockTrx.getStateMachineContext()).thenReturn(request);

    return mockTrx;
  }
}

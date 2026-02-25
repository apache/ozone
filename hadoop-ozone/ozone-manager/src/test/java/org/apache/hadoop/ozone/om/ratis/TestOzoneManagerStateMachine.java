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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.ratis_snapshot.OmRatisSnapshotProvider;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequestArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.protocolPB.RequestHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * Class to test OzoneManagerStateMachine.
 */
public class TestOzoneManagerStateMachine {

  private OzoneManager om;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private RequestHandler handler;
  private ExecutorService executor;
  private OzoneManagerStateMachine sm;

  @BeforeEach
  public void setup() {
    ExitUtils.disableSystemExit();
    om = mock(OzoneManager.class);
    doubleBuffer = mock(OzoneManagerDoubleBuffer.class);
    handler = mock(RequestHandler.class);
    executor = Executors.newSingleThreadExecutor();
    sm = new OzoneManagerStateMachine(om, doubleBuffer, handler, executor, null);
  }

  @AfterEach
  public void tearDown() {
    sm.stop();
  }

  // --- startTransaction tests ---

  @Test
  public void testStartTransactionHappyPath() throws Exception {
    RaftGroupId groupId = initializeStateMachine();
    OMRequest omRequest = sampleWriteRequest();
    RaftClientRequest clientRequest = buildClientRequest(groupId, omRequest);

    TransactionContext trx = sm.startTransaction(clientRequest);

    assertNotNull(trx);
    assertNotNull(trx.getStateMachineContext());
    assertInstanceOf(OMRequest.class, trx.getStateMachineContext());
    assertNotNull(trx.getStateMachineLogEntry().getLogData());
    assertNull(trx.getException());
    verify(handler).validateRequest(any(OMRequest.class));
  }

  @Test
  public void testStartTransactionValidationFailure() throws Exception {
    RaftGroupId groupId = initializeStateMachine();
    OMRequest omRequest = sampleWriteRequest();
    RaftClientRequest clientRequest = buildClientRequest(groupId, omRequest);

    OMException validationError = new OMException("request validation failed",
        OMException.ResultCodes.INVALID_REQUEST);
    doThrow(validationError).when(handler).validateRequest(any(OMRequest.class));

    TransactionContext trx = sm.startTransaction(clientRequest);

    assertNotNull(trx);
    assertNotNull(trx.getException());
    assertEquals(validationError, trx.getException());
  }

  @Test
  public void testStartTransactionGroupIdMismatchThrows() throws Exception {
    initializeStateMachine();
    RaftGroupId wrongGroupId = RaftGroupId.randomId();
    OMRequest omRequest = sampleWriteRequest();
    RaftClientRequest clientRequest = buildClientRequest(wrongGroupId, omRequest);

    assertThrows(IllegalArgumentException.class,
        () -> sm.startTransaction(clientRequest));
  }

  // --- preAppendTransaction tests ---

  @Test
  public void testPreAppendTransactionPrepareGate() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneManagerPrepareState ps = new OzoneManagerPrepareState(conf);
    when(om.getPrepareState()).thenReturn(ps);
    when(om.isAdmin(any(UserGroupInformation.class))).thenReturn(true);

    OMRequest writeRequest = sampleWriteRequest();
    OMRequest prepareRequest = OMRequest.newBuilder()
        .setPrepareRequest(PrepareRequest.newBuilder()
            .setArgs(PrepareRequestArgs.getDefaultInstance()))
        .setCmdType(Type.Prepare)
        .setClientId("123")
        .setUserInfo(UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("localhost")
            .setRemoteAddress("127.0.0.1"))
        .build();

    // Write request passes before prepare
    TransactionContext writeTrx = mockTrx(writeRequest, 1, 1);
    assertSame(writeTrx, sm.preAppendTransaction(writeTrx));
    assertEquals(PrepareStatus.NOT_PREPARED, ps.getState().getStatus());

    // Prepare enables gate
    TransactionContext prepareTrx = mockTrx(prepareRequest, 1, 2);
    assertSame(prepareTrx, sm.preAppendTransaction(prepareTrx));
    assertEquals(PrepareStatus.PREPARE_GATE_ENABLED,
        ps.getState().getStatus());

    // Write request now blocked
    TransactionContext writeTrx2 = mockTrx(writeRequest, 1, 3);
    StateMachineException ex = assertThrows(StateMachineException.class,
        () -> sm.preAppendTransaction(writeTrx2));
    assertFalse(ex.leaderShouldStepDown());
    assertInstanceOf(OMException.class, ex.getCause());
    assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED,
        ((OMException) ex.getCause()).getResult());

    // Can prepare again
    TransactionContext prepareTrx2 = mockTrx(prepareRequest, 1, 4);
    assertSame(prepareTrx2, sm.preAppendTransaction(prepareTrx2));
  }

  @Test
  public void testPreAppendTransactionAclDenied() {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneManagerPrepareState ps = new OzoneManagerPrepareState(conf);
    when(om.getPrepareState()).thenReturn(ps);
    when(om.getAclsEnabled()).thenReturn(true);
    when(om.isAdmin(any(UserGroupInformation.class))).thenReturn(false);

    OMRequest prepareRequest = OMRequest.newBuilder()
        .setPrepareRequest(PrepareRequest.newBuilder()
            .setArgs(PrepareRequestArgs.getDefaultInstance()))
        .setCmdType(Type.Prepare)
        .setClientId("123")
        .setUserInfo(UserInfo.newBuilder()
            .setUserName("nonAdminUser")
            .setHostName("localhost")
            .setRemoteAddress("127.0.0.1"))
        .build();

    TransactionContext trx = mockTrx(prepareRequest, 1, 1);

    StateMachineException ex = assertThrows(StateMachineException.class,
        () -> sm.preAppendTransaction(trx));
    assertFalse(ex.leaderShouldStepDown());
    assertInstanceOf(OMException.class, ex.getCause());
    assertEquals(OMException.ResultCodes.ACCESS_DENIED,
        ((OMException) ex.getCause()).getResult());
  }

  // --- applyTransaction tests ---

  @Test
  public void testApplyTransactionHappyPath() throws Exception {
    OMRequest request = sampleWriteRequest();
    TransactionContext trx = mockTrx(request, 1, 5);

    OMResponse expectedResponse = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    OMClientResponse clientResponse = mock(OMClientResponse.class);
    when(clientResponse.getOMResponse()).thenReturn(expectedResponse);
    when(clientResponse.getOmLockDetails()).thenReturn(null);

    when(handler.handleWriteRequest(eq(request), any(), eq(doubleBuffer)))
        .thenReturn(clientResponse);

    CompletableFuture<Message> future = sm.applyTransaction(trx);
    Message result = future.get();

    assertNotNull(result);
    verify(doubleBuffer).acquireUnFlushedTransactions(1);
  }

  @Test
  public void testApplyTransactionFollowerPath() throws Exception {
    OMRequest request = sampleWriteRequest();
    // Null context simulates follower: request deserialized from log data
    TransactionContext trx = mockTrx(request, 1, 5, null);

    OMResponse expectedResponse = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    OMClientResponse clientResponse = mock(OMClientResponse.class);
    when(clientResponse.getOMResponse()).thenReturn(expectedResponse);
    when(clientResponse.getOmLockDetails()).thenReturn(null);

    when(handler.handleWriteRequest(any(OMRequest.class), any(), eq(doubleBuffer)))
        .thenReturn(clientResponse);

    CompletableFuture<Message> future = sm.applyTransaction(trx);
    Message result = future.get();

    assertNotNull(result);
  }

  @Test
  public void testApplyTransactionBackpressureInterrupt() throws Exception {
    OMRequest request = sampleWriteRequest();
    TransactionContext trx = mockTrx(request, 1, 5);

    doThrow(new InterruptedException("backpressure"))
        .when(doubleBuffer).acquireUnFlushedTransactions(1);

    CompletableFuture<Message> future = sm.applyTransaction(trx);

    ExecutionException ex = assertThrows(ExecutionException.class, future::get);
    assertInstanceOf(InterruptedException.class, ex.getCause());
  }

  // --- runCommand tests ---

  @Test
  public void testRunCommandHappyPath() throws Exception {
    OMRequest request = sampleWriteRequest();
    TermIndex ti = TermIndex.valueOf(1, 5);

    OMResponse expectedResponse = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    OMLockDetails lockDetails = mock(OMLockDetails.class);
    when(lockDetails.toProtobufBuilder()).thenReturn(
        OzoneManagerProtocolProtos.OMLockDetailsProto.newBuilder()
            .setIsLockAcquired(true)
            .setWaitLockNanos(100)
            .setReadLockNanos(50)
            .setWriteLockNanos(200));

    OMClientResponse clientResponse = mock(OMClientResponse.class);
    when(clientResponse.getOMResponse()).thenReturn(expectedResponse);
    when(clientResponse.getOmLockDetails()).thenReturn(lockDetails);

    when(handler.handleWriteRequest(eq(request), any(), eq(doubleBuffer)))
        .thenReturn(clientResponse);

    OMResponse result = sm.runCommand(request, ti);

    assertNotNull(result);
    assertTrue(result.getSuccess());
    assertEquals(Status.OK, result.getStatus());
    assertTrue(result.hasOmLockDetails());
  }

  @Test
  public void testRunCommandNullLockDetails() throws Exception {
    OMRequest request = sampleWriteRequest();
    TermIndex ti = TermIndex.valueOf(1, 5);

    OMResponse expectedResponse = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    OMClientResponse clientResponse = mock(OMClientResponse.class);
    when(clientResponse.getOMResponse()).thenReturn(expectedResponse);
    when(clientResponse.getOmLockDetails()).thenReturn(null);

    when(handler.handleWriteRequest(eq(request), any(), eq(doubleBuffer)))
        .thenReturn(clientResponse);

    OMResponse result = sm.runCommand(request, ti);

    assertNotNull(result);
    assertEquals(Status.OK, result.getStatus());
    assertFalse(result.hasOmLockDetails());
  }

  @Test
  public void testRunCommandIOException() throws Exception {
    OMRequest request = sampleWriteRequest();
    TermIndex ti = TermIndex.valueOf(1, 5);

    when(handler.handleWriteRequest(eq(request), any(), eq(doubleBuffer)))
        .thenThrow(new IOException("disk full"));

    OMResponse result = sm.runCommand(request, ti);

    assertNotNull(result);
    assertFalse(result.getSuccess());
    verify(doubleBuffer).add(any(), eq(ti));
  }

  @Test
  public void testRunCommandRuntimeExceptionTerminates() throws Exception {
    OMRequest request = sampleWriteRequest();
    TermIndex ti = TermIndex.valueOf(1, 5);

    when(handler.handleWriteRequest(eq(request), any(), eq(doubleBuffer)))
        .thenThrow(new NullPointerException("boom"));

    // With ExitUtils.disableSystemExit(), terminate throws ExitException
    // instead of calling System.exit(). The catch block in runCommand
    // calls ExitUtils.terminate which throws ExitException.
    assertThrows(ExitUtils.ExitException.class,
        () -> sm.runCommand(request, ti));
  }

  // --- processResponse tests ---

  @Test
  public void testProcessResponseSuccess() {
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    Message result = sm.processResponse(response);
    assertNotNull(result);
  }

  @Test
  public void testProcessResponseNonCriticalError() {
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.KEY_NOT_FOUND)
        .setSuccess(false)
        .setMessage("key not found")
        .build();

    Message result = sm.processResponse(response);
    assertNotNull(result);
  }

  @Test
  public void testProcessResponseInternalErrorTerminates() {
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.INTERNAL_ERROR)
        .setSuccess(false)
        .setMessage("internal error")
        .build();

    // terminate throws ExitException when system exit is disabled
    assertThrows(ExitUtils.ExitException.class,
        () -> sm.processResponse(response));
  }

  @Test
  public void testProcessResponseMetadataErrorTerminates() {
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(Type.CreateKey)
        .setStatus(Status.METADATA_ERROR)
        .setSuccess(false)
        .setMessage("metadata error")
        .build();

    assertThrows(ExitUtils.ExitException.class,
        () -> sm.processResponse(response));
  }

  // --- createErrorResponse tests ---

  @Test
  public void testCreateErrorResponse() {
    OMRequest request = sampleWriteRequest();
    IOException ex = new IOException("test error");
    TermIndex ti = TermIndex.valueOf(1, 5);

    OMResponse response = sm.createErrorResponse(request, ex, ti);

    assertNotNull(response);
    assertFalse(response.getSuccess());
    assertEquals(Type.CreateKey, response.getCmdType());
    assertEquals("test error", response.getMessage());
    verify(doubleBuffer).add(any(), eq(ti));
  }

  @Test
  public void testCreateErrorResponseNullMessage() {
    OMRequest request = sampleWriteRequest();
    IOException ex = new IOException((String) null);
    TermIndex ti = TermIndex.valueOf(1, 5);

    OMResponse response = sm.createErrorResponse(request, ex, ti);

    assertNotNull(response);
    assertFalse(response.getSuccess());
    assertFalse(response.hasMessage());
  }

  // --- query tests ---

  @Test
  public void testQueryHappyPath() throws Exception {
    OMRequest request = sampleReadRequest();
    OMResponse expectedResponse = OMResponse.newBuilder()
        .setCmdType(Type.ServiceList)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    when(handler.handleReadRequest(any(OMRequest.class)))
        .thenReturn(expectedResponse);

    Message queryMessage = Message.valueOf(
        OMRatisHelper.convertRequestToByteString(request));
    CompletableFuture<Message> future = sm.query(queryMessage);
    Message result = future.get();

    assertNotNull(result);
    verify(handler).handleReadRequest(any(OMRequest.class));
  }

  @Test
  public void testQueryIOException() {
    // Garbage bytes that can't be parsed as OMRequest
    Message badMessage = Message.valueOf("not-a-valid-protobuf");

    CompletableFuture<Message> future = sm.query(badMessage);

    ExecutionException ex = assertThrows(ExecutionException.class, future::get);
    assertInstanceOf(IOException.class, ex.getCause());
  }

  // --- notifyTermIndexUpdated tests ---

  @Test
  public void testNotifyTermIndexSameIndexOk() {
    sm.notifyTermIndexUpdated(1, 5);
    // Same index should not throw
    sm.notifyTermIndexUpdated(1, 5);

    assertTermIndex(1, 5, sm.getLastNotifiedTermIndex());
  }

  @Test
  public void testNotifyTermIndexDecreasingThrows() {
    sm.notifyTermIndexUpdated(1, 5);

    assertThrows(IllegalArgumentException.class,
        () -> sm.notifyTermIndexUpdated(1, 4));
  }

  // --- updateLastAppliedTermIndex tests ---

  @Test
  public void testUpdateLastAppliedInSkipRange() {
    sm.notifyTermIndexUpdated(0, 0);
    // Skip index 1 (from applyTransaction), notify 2 and 3
    sm.notifyTermIndexUpdated(0, 2);
    sm.notifyTermIndexUpdated(0, 3);

    assertTermIndex(0, 0, sm.getLastAppliedTermIndex());

    // Update with index 1 which is in skip range [1, 3]
    sm.updateLastAppliedTermIndex(TermIndex.valueOf(0, 1));

    // Should jump to lastNotified (3)
    assertTermIndex(0, 3, sm.getLastAppliedTermIndex());
  }

  @Test
  public void testUpdateLastAppliedBelowSkipRange() {
    sm.notifyTermIndexUpdated(0, 0);
    // Notify consecutive: no skip
    sm.notifyTermIndexUpdated(0, 1);

    assertTermIndex(0, 1, sm.getLastAppliedTermIndex());

    // Direct update
    sm.updateLastAppliedTermIndex(TermIndex.valueOf(0, 2));
    assertTermIndex(0, 2, sm.getLastAppliedTermIndex());
  }

  @Test
  public void testLastAppliedIndexSequence() {
    sm.notifyTermIndexUpdated(0, 0);
    assertTermIndex(0, 0, sm.getLastAppliedTermIndex());
    assertTermIndex(0, 0, sm.getLastNotifiedTermIndex());

    // Consecutive notification advances both
    sm.notifyTermIndexUpdated(0, 1);
    assertTermIndex(0, 1, sm.getLastAppliedTermIndex());
    assertTermIndex(0, 1, sm.getLastNotifiedTermIndex());

    // Direct updates advance lastApplied but NOT lastNotified
    sm.updateLastAppliedTermIndex(TermIndex.valueOf(0, 2));
    sm.updateLastAppliedTermIndex(TermIndex.valueOf(0, 3));
    assertTermIndex(0, 3, sm.getLastAppliedTermIndex());
    assertTermIndex(0, 1, sm.getLastNotifiedTermIndex());

    // Term change via notification advances both
    sm.notifyTermIndexUpdated(1L, 4L);
    assertTermIndex(1, 4, sm.getLastAppliedTermIndex());
    assertTermIndex(1, 4, sm.getLastNotifiedTermIndex());

    // More direct updates
    sm.updateLastAppliedTermIndex(TermIndex.valueOf(1L, 5L));
    sm.updateLastAppliedTermIndex(TermIndex.valueOf(1L, 6L));
    assertTermIndex(1, 6, sm.getLastAppliedTermIndex());
    assertTermIndex(1, 4, sm.getLastNotifiedTermIndex());
  }

  // --- takeSnapshot tests ---

  @Test
  public void testTakeSnapshotAlreadyCaughtUp() throws Exception {
    // Notify incrementally so lastApplied advances via the
    // "notified - applied == 1" fast path in notifyTermIndexUpdated.
    // A single jump from -1 to 1 would set lastSkipped=0 without
    // updating lastApplied, causing takeSnapshot to loop forever.
    sm.notifyTermIndexUpdated(1, 0);
    sm.notifyTermIndexUpdated(1, 1);
    // lastApplied=1, lastSkipped=-1 → already caught up
    assertTermIndex(1, 1, sm.getLastAppliedTermIndex());

    OMMetadataManager metaMgr = mock(OMMetadataManager.class);
    @SuppressWarnings("unchecked")
    Table<String, TransactionInfo> txnTable = mock(Table.class);
    DBStore store = mock(DBStore.class);
    when(om.getMetadataManager()).thenReturn(metaMgr);
    when(metaMgr.getTransactionInfoTable()).thenReturn(txnTable);
    when(metaMgr.getStore()).thenReturn(store);

    long snapshotIndex = sm.takeSnapshot();

    assertEquals(1, snapshotIndex);
    verify(store).flushDB();
    verify(om).setTransactionInfo(any(TransactionInfo.class));
  }

  @Test
  public void testTakeSnapshotWaitsForFlush() throws Exception {
    // Create a skip gap: notify 0, then skip to 3
    sm.notifyTermIndexUpdated(1, 0);
    sm.notifyTermIndexUpdated(1, 2);
    sm.notifyTermIndexUpdated(1, 3);
    // lastApplied=0, lastSkipped=1, so takeSnapshot will loop

    when(om.isStopped()).thenReturn(false);
    // On first awaitFlush, simulate the double buffer catching up
    doAnswer(invocation -> {
      // Simulate applyTransaction completing index 1
      sm.updateLastAppliedTermIndex(TermIndex.valueOf(1, 1));
      // This should jump to lastNotified=3 due to skip range
      return null;
    }).when(doubleBuffer).awaitFlush();

    OMMetadataManager metaMgr = mock(OMMetadataManager.class);
    @SuppressWarnings("unchecked")
    Table<String, TransactionInfo> txnTable = mock(Table.class);
    DBStore store = mock(DBStore.class);
    when(om.getMetadataManager()).thenReturn(metaMgr);
    when(metaMgr.getTransactionInfoTable()).thenReturn(txnTable);
    when(metaMgr.getStore()).thenReturn(store);

    long snapshotIndex = sm.takeSnapshot();

    assertEquals(3, snapshotIndex);
    verify(doubleBuffer).awaitFlush();
    verify(store).flushDB();
  }

  @Test
  public void testTakeSnapshotOmStopped() {
    when(om.isStopped()).thenReturn(true);

    // Set lastSkippedIndex > lastApplied to enter the wait loop
    sm.notifyTermIndexUpdated(0, 0);
    sm.notifyTermIndexUpdated(0, 2);  // skips index 1

    assertThrows(IOException.class, () -> sm.takeSnapshot());
  }

  // --- loadSnapshotInfoFromDB tests ---

  @Test
  public void testLoadSnapshotInfoFromDBExists(@TempDir Path tmpDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, tmpDir.toAbsolutePath().toString());
    OzoneManager realishOm = mock(OzoneManager.class);
    when(realishOm.getConfiguration()).thenReturn(conf);
    when(realishOm.getConfig()).thenReturn(conf.getObject(OmConfig.class));
    OMMetadataManager realMetaMgr = new OmMetadataManagerImpl(conf, realishOm);
    when(realishOm.getMetadataManager()).thenReturn(realMetaMgr);
    when(realishOm.getTransactionInfo()).thenReturn(mock(TransactionInfo.class));

    // Write a TransactionInfo to DB
    TransactionInfo txnInfo = TransactionInfo.valueOf(TermIndex.valueOf(3, 42));
    realMetaMgr.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY, txnInfo);

    OzoneManagerStateMachine testSm =
        new OzoneManagerStateMachine(realishOm, doubleBuffer, handler, executor, null);
    testSm.loadSnapshotInfoFromDB();

    assertTermIndex(3, 42, testSm.getLastAppliedTermIndex());
    verify(realishOm).setTransactionInfo(any(TransactionInfo.class));
  }

  @Test
  public void testLoadSnapshotInfoFromDBMissing(@TempDir Path tmpDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, tmpDir.toAbsolutePath().toString());
    OzoneManager realishOm = mock(OzoneManager.class);
    when(realishOm.getConfiguration()).thenReturn(conf);
    when(realishOm.getConfig()).thenReturn(conf.getObject(OmConfig.class));
    OMMetadataManager realMetaMgr = new OmMetadataManagerImpl(conf, realishOm);
    when(realishOm.getMetadataManager()).thenReturn(realMetaMgr);

    OzoneManagerStateMachine testSm =
        new OzoneManagerStateMachine(realishOm, doubleBuffer, handler, executor, null);
    // Should not throw when TransactionInfo is not found
    testSm.loadSnapshotInfoFromDB();

    // lastApplied should remain at default (0, -1)
    assertTermIndex(0, -1, testSm.getLastAppliedTermIndex());
  }

  // --- notifyLeaderChanged tests ---

  @Test
  public void testNotifyLeaderChangedSameNode() {
    RaftPeerId peerId = RaftPeerId.valueOf("om1");
    RaftGroupId groupId = RaftGroupId.randomId();
    RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(peerId, groupId);

    when(om.getConfiguration()).thenReturn(new OzoneConfiguration());
    when(om.buildAuditMessageForSuccess(any(), any()))
        .thenReturn(dummyAuditMessage());

    sm.notifyLeaderChanged(memberId, peerId);

    // Same node becomes leader → should warm up EDEK cache
    verify(om).initializeEdekCache(any());
    verify(om).omHAMetricsInit("om1");
  }

  @Test
  public void testNotifyLeaderChangedDifferentNode() {
    RaftPeerId localPeerId = RaftPeerId.valueOf("om1");
    RaftPeerId leaderPeerId = RaftPeerId.valueOf("om2");
    RaftGroupId groupId = RaftGroupId.randomId();
    RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(localPeerId, groupId);

    when(om.buildAuditMessageForSuccess(any(), any()))
        .thenReturn(dummyAuditMessage());

    sm.notifyLeaderChanged(memberId, leaderPeerId);

    // Different node is leader → should NOT warm up cache
    verify(om, never()).initializeEdekCache(any());
    verify(om).omHAMetricsInit("om2");
  }

  @Test
  public void testNotifyLeaderChangedAuditParams() {
    when(om.buildAuditMessageForSuccess(any(), any()))
        .thenReturn(dummyAuditMessage());

    RaftGroupId groupId = RaftGroupId.randomId();

    // First leader change: previousLeader should be "NONE"
    RaftPeerId leader1 = RaftPeerId.valueOf("om1");
    sm.notifyLeaderChanged(
        RaftGroupMemberId.valueOf(RaftPeerId.valueOf("om2"), groupId),
        leader1);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> captor =
        ArgumentCaptor.forClass(Map.class);
    verify(om).buildAuditMessageForSuccess(
        eq(OMSystemAction.LEADER_CHANGE), captor.capture());
    Map<String, String> params = captor.getValue();
    assertEquals("NONE", params.get("previousLeader"));
    assertEquals("om1", params.get("newLeader"));

    // Second leader change: previousLeader should be "om1"
    RaftPeerId leader2 = RaftPeerId.valueOf("om3");
    sm.notifyLeaderChanged(
        RaftGroupMemberId.valueOf(RaftPeerId.valueOf("om2"), groupId),
        leader2);

    verify(om, times(2)).buildAuditMessageForSuccess(
        eq(OMSystemAction.LEADER_CHANGE), captor.capture());
    Map<String, String> params2 = captor.getValue();
    assertEquals("om1", params2.get("previousLeader"));
    assertEquals("om3", params2.get("newLeader"));
  }

  // --- notifyConfigurationChanged tests ---

  @Test
  public void testNotifyConfigurationChanged() {
    RaftProtos.RaftPeerProto peer1 = RaftProtos.RaftPeerProto.newBuilder()
        .setId(ByteString.copyFromUtf8("om1"))
        .setAddress("host1:9862")
        .build();
    RaftProtos.RaftPeerProto peer2 = RaftProtos.RaftPeerProto.newBuilder()
        .setId(ByteString.copyFromUtf8("om2"))
        .setAddress("host2:9862")
        .build();
    RaftProtos.RaftPeerProto listener = RaftProtos.RaftPeerProto.newBuilder()
        .setId(ByteString.copyFromUtf8("om3"))
        .setAddress("host3:9862")
        .build();
    RaftProtos.RaftConfigurationProto config =
        RaftProtos.RaftConfigurationProto.newBuilder()
            .addPeers(peer1)
            .addPeers(peer2)
            .addListeners(listener)
            .build();

    sm.notifyConfigurationChanged(1, 5, config);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
    verify(om).updatePeerList(captor.capture());
    List<String> peerIds = captor.getValue();
    assertEquals(3, peerIds.size());
    assertTrue(peerIds.contains("om1"));
    assertTrue(peerIds.contains("om2"));
    assertTrue(peerIds.contains("om3"));
  }

  // --- notifySnapshotInstalled tests ---

  @Test
  public void testNotifySnapshotInstalledSuccess() {
    RaftPeer localPeer = RaftPeer.newBuilder()
        .setId("om1").setAddress("localhost:9862").build();

    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    RaftServer.Division division = mock(RaftServer.Division.class);
    when(division.getPeer()).thenReturn(localPeer);
    when(ratisServer.getServerDivision()).thenReturn(division);
    when(om.getOmRatisServer()).thenReturn(ratisServer);

    OmRatisSnapshotProvider snapshotProvider = mock(OmRatisSnapshotProvider.class);
    when(om.getOmSnapshotProvider()).thenReturn(snapshotProvider);

    sm.notifySnapshotInstalled(
        RaftProtos.InstallSnapshotResult.SUCCESS, 100, localPeer);

    verify(snapshotProvider).init();
  }

  @Test
  public void testNotifySnapshotInstalledDifferentPeer() {
    RaftPeer localPeer = RaftPeer.newBuilder()
        .setId("om1").setAddress("localhost:9862").build();
    RaftPeer remotePeer = RaftPeer.newBuilder()
        .setId("om2").setAddress("remote:9862").build();

    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    RaftServer.Division division = mock(RaftServer.Division.class);
    when(division.getPeer()).thenReturn(localPeer);
    when(ratisServer.getServerDivision()).thenReturn(division);
    when(om.getOmRatisServer()).thenReturn(ratisServer);

    OmRatisSnapshotProvider snapshotProvider = mock(OmRatisSnapshotProvider.class);
    when(om.getOmSnapshotProvider()).thenReturn(snapshotProvider);

    sm.notifySnapshotInstalled(
        RaftProtos.InstallSnapshotResult.SUCCESS, 100, remotePeer);

    // Different peer, init() should NOT be called
    verify(snapshotProvider, never()).init();
  }

  // --- notifyLeaderReady tests ---

  @Test
  public void testNotifyLeaderReady() {
    OmSnapshotManager snapshotManager = mock(OmSnapshotManager.class);
    when(om.getOmSnapshotManager()).thenReturn(snapshotManager);

    sm.notifyLeaderReady();

    verify(snapshotManager).resetInFlightSnapshotCount();
  }

  // --- getLatestSnapshot tests ---

  @Test
  public void testGetLatestSnapshot() {
    TransactionInfo txnInfo = TransactionInfo.valueOf(TermIndex.valueOf(2, 10));
    when(om.getTransactionInfo()).thenReturn(txnInfo);

    SnapshotInfo snapshot = sm.getLatestSnapshot();

    assertNotNull(snapshot);
    assertEquals(2, snapshot.getTermIndex().getTerm());
    assertEquals(10, snapshot.getTermIndex().getIndex());
  }

  // --- pause / stop / close lifecycle tests ---

  @Test
  public void testPauseStopsDoubleBuffer() {
    sm.pause();

    verify(doubleBuffer).stop();
  }

  @Test
  public void testPauseAlreadyPaused() {
    // First pause
    sm.pause();
    // Second pause should not throw
    sm.pause();

    verify(doubleBuffer, times(2)).stop();
  }

  @Test
  public void testStopShutdownsResources() {
    sm.stop();

    verify(doubleBuffer).stop();
    assertTrue(executor.isShutdown());
  }

  @Test
  public void testCloseWhenOmRunning() {
    when(om.isStopped()).thenReturn(false);

    sm.close();

    verify(om).shutDown(anyString());
  }

  @Test
  public void testCloseWhenOmAlreadyStopped() {
    when(om.isStopped()).thenReturn(true);

    sm.close();

    // stop() calls doubleBuffer.stop()
    verify(doubleBuffer).stop();
  }

  // --- other utility method tests ---

  @Test
  public void testToStateMachineLogEntryString() {
    OMRequest request = sampleWriteRequest();
    RaftProtos.StateMachineLogEntryProto smEntry =
        RaftProtos.StateMachineLogEntryProto.newBuilder()
            .setLogData(OMRatisHelper.convertRequestToByteString(request))
            .build();

    String result = sm.toStateMachineLogEntryString(smEntry);

    assertNotNull(result);
    assertTrue(result.contains("CreateKey") || result.contains("create_key"),
        "Expected result to contain command type, got: " + result);
  }

  @Test
  public void testAwaitDoubleBufferFlush() throws Exception {
    sm.awaitDoubleBufferFlush();

    verify(doubleBuffer).awaitFlush();
  }

  // --- private helpers ---

  private static void assertTermIndex(long expectedTerm, long expectedIndex, TermIndex computed) {
    assertEquals(expectedTerm, computed.getTerm());
    assertEquals(expectedIndex, computed.getIndex());
  }

  private OMRequest sampleWriteRequest() {
    return OMRequest.newBuilder()
        .setCmdType(Type.CreateKey)
        .setClientId("test-client")
        .setCreateKeyRequest(CreateKeyRequest.newBuilder()
            .setKeyArgs(KeyArgs.newBuilder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setKeyName("key")))
        .setUserInfo(UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("localhost")
            .setRemoteAddress("127.0.0.1"))
        .build();
  }

  private OMRequest sampleReadRequest() {
    return OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setClientId("test-client")
        .build();
  }

  private TransactionContext mockTrx(OMRequest request, long term, long index) {
    return mockTrx(request, term, index, request);
  }

  private TransactionContext mockTrx(OMRequest request, long term, long index, Object context) {
    RaftProtos.StateMachineLogEntryProto logEntry =
        RaftProtos.StateMachineLogEntryProto.newBuilder()
            .setLogData(OMRatisHelper.convertRequestToByteString(request))
            .build();
    TransactionContext trx = mock(TransactionContext.class);
    when(trx.getStateMachineLogEntry()).thenReturn(logEntry);
    when(trx.getStateMachineContext()).thenReturn(context);
    RaftProtos.LogEntryProto logEntryProto = LogProtoUtils.toLogEntryProto(term, index, index);
    when(trx.getLogEntry()).thenReturn(logEntryProto);
    return trx;
  }

  private AuditMessage dummyAuditMessage() {
    return new AuditMessage.Builder()
        .forOperation(() -> "LEADER_CHANGE")
        .withResult(AuditEventStatus.SUCCESS)
        .build();
  }

  private RaftGroupId initializeStateMachine() throws IOException {
    RaftGroupId groupId = RaftGroupId.randomId();
    RaftServer server = mock(RaftServer.class);
    RaftStorage storage = mock(RaftStorage.class);
    sm.initialize(server, groupId, storage);
    return groupId;
  }

  private RaftClientRequest buildClientRequest(
      RaftGroupId groupId, OMRequest omRequest) {
    return RaftClientRequest.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftPeerId.valueOf("om1"))
        .setGroupId(groupId)
        .setCallId(1L)
        .setMessage(Message.valueOf(
            OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }
}

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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINE_WRITE_WAIT_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class to ContainerStateMachine class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class TestContainerStateMachine {
  private ContainerDispatcher dispatcher;
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private ContainerStateMachine stateMachine;
  private final List<ThreadPoolExecutor> executor = IntStream.range(0, 2).mapToObj(i -> new ThreadPoolExecutor(1, 1,
      0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("ChunkWriter-" + i + "-%d")
      .build())).collect(Collectors.toList());
  private final boolean isLeader;
  private static final String CONTAINER_DATA = "Test Data";

  TestContainerStateMachine(boolean isLeader) {
    this.isLeader = isLeader;
  }

  @BeforeEach
  public void setup() throws IOException {
    conf.setTimeDuration(HDDS_CONTAINER_RATIS_STATEMACHINE_WRITE_WAIT_INTERVAL,
        1000_000_000, TimeUnit.NANOSECONDS);
    dispatcher = mock(ContainerDispatcher.class);
    ContainerController controller = mock(ContainerController.class);
    XceiverServerRatis ratisServer = mock(XceiverServerRatis.class);
    RaftServer raftServer = mock(RaftServer.class);
    RaftServer.Division division = mock(RaftServer.Division.class);
    RaftGroup raftGroup = mock(RaftGroup.class);
    DivisionInfo info = mock(DivisionInfo.class);
    RaftPeer raftPeer = mock(RaftPeer.class);
    when(ratisServer.getServer()).thenReturn(raftServer);
    when(raftServer.getDivision(any())).thenReturn(division);
    when(division.getGroup()).thenReturn(raftGroup);
    when(raftGroup.getPeer(any())).thenReturn(raftPeer);
    when(division.getInfo()).thenReturn(info);
    when(info.isLeader()).thenReturn(isLeader);
    when(ratisServer.getServerDivision(any())).thenReturn(division);
    stateMachine = new ContainerStateMachine(null,
        RaftGroupId.randomId(), dispatcher, controller, executor, ratisServer, conf, "containerOp");
  }

  @AfterEach
  public void teardown() {
    stateMachine.close();
  }

  @AfterAll
  public void shutdown() {
    executor.forEach(ThreadPoolExecutor::shutdown);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteFailure(boolean failWithException)
      throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
    RaftProtos.LogEntryProto entry = mock(RaftProtos.LogEntryProto.class);
    when(entry.getTerm()).thenReturn(1L);
    when(entry.getIndex()).thenReturn(1L);
    RaftProtos.LogEntryProto entryNext = mock(RaftProtos.LogEntryProto.class);
    when(entryNext.getTerm()).thenReturn(1L);
    when(entryNext.getIndex()).thenReturn(2L);
    TransactionContext trx = mock(TransactionContext.class);
    ContainerStateMachine.Context context = mock(ContainerStateMachine.Context.class);
    when(trx.getStateMachineContext()).thenReturn(context);

    setUpMockDispatcherReturn(failWithException);
    setUpMockRequestProtoReturn(context, 1, 1);

    Message message = stateMachine.write(entry, trx).get();
    verify(dispatcher, times(1)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    reset(dispatcher);
    ContainerProtos.ContainerCommandResponseProto containerCommandResponseProto =
        ContainerProtos.ContainerCommandResponseProto.parseFrom(message.getContent());
    if (failWithException) {
      assertEquals("Failed to write chunk data", containerCommandResponseProto.getMessage());
      assertEquals(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR, containerCommandResponseProto.getResult());
    } else {
      // If dispatcher returned failure response, the state machine should propagate the same failure.
      assertEquals(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR, containerCommandResponseProto.getResult());
    }

    // Writing data to another container(containerId 2) should also fail.
    setUpMockRequestProtoReturn(context, 2, 1);
    message = stateMachine.write(entryNext, trx).get();
    verify(dispatcher, times(0)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    containerCommandResponseProto
        = ContainerProtos.ContainerCommandResponseProto.parseFrom(message.getContent());
    assertTrue(containerCommandResponseProto.getMessage().contains("failed, stopping all writes to container"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testApplyTransactionFailure(boolean failWithException) throws ExecutionException,
      InterruptedException, IOException {
    RaftProtos.LogEntryProto entry = mock(RaftProtos.LogEntryProto.class);
    when(entry.getTerm()).thenReturn(1L);
    when(entry.getIndex()).thenReturn(1L);
    TransactionContext trx = mock(TransactionContext.class);
    ContainerStateMachine.Context context = mock(ContainerStateMachine.Context.class);
    when(trx.getLogEntry()).thenReturn(entry);
    when(trx.getStateMachineContext()).thenReturn(context);

    setUpMockDispatcherReturn(failWithException);
    // Failing apply transaction on congtainer 1.
    setUpLogProtoReturn(context, 1, 1);
    ThrowableCatcher catcher = new ThrowableCatcher();
    //apply transaction will fail because of runtime exception thrown by dispatcher, which marks the first
    // failure on container 1.
    stateMachine.applyTransaction(trx).exceptionally(catcher.asSetter()).get();
    verify(dispatcher, times(1)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    reset(dispatcher);
    assertNotNull(catcher.getCaught());
    assertResults(failWithException, catcher.getCaught());
    // Another apply transaction on same container 1 should fail because the previous apply transaction failed.
    stateMachine.applyTransaction(trx).exceptionally(catcher.asSetter()).get();
    verify(dispatcher, times(0)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    assertInstanceOf(StorageContainerException.class, catcher.getReceived());
    StorageContainerException sce = (StorageContainerException) catcher.getReceived();
    assertEquals(ContainerProtos.Result.CONTAINER_UNHEALTHY, sce.getResult());

    // Another apply transaction on a different container 2 shouldn't fail because the previous apply transaction
    // failure was only on container 1.
    setUpLogProtoReturn(context, 2, 1);
    reset(dispatcher);
    catcher.getCaught().set(null);
    when(dispatcher.dispatch(any(), any())).thenReturn(ContainerProtos.ContainerCommandResponseProto
        .newBuilder().setCmdType(ContainerProtos.Type.WriteChunk).setResult(ContainerProtos.Result.SUCCESS)
        .build());
    Message succcesfulTransaction = stateMachine.applyTransaction(trx).exceptionally(catcher.asSetter()).get();
    verify(dispatcher, times(1)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    assertNull(catcher.getReceived());
    ContainerProtos.ContainerCommandResponseProto resp =
        ContainerProtos.ContainerCommandResponseProto.parseFrom(succcesfulTransaction.getContent());
    assertEquals(ContainerProtos.Result.SUCCESS, resp.getResult());
  }

  @Test
  public void testWriteTimout() throws Exception {
    RaftProtos.LogEntryProto entry = mock(RaftProtos.LogEntryProto.class);
    when(entry.getTerm()).thenReturn(1L);
    when(entry.getIndex()).thenReturn(1L);
    RaftProtos.LogEntryProto entryNext = mock(RaftProtos.LogEntryProto.class);
    when(entryNext.getTerm()).thenReturn(1L);
    when(entryNext.getIndex()).thenReturn(2L);
    TransactionContext trx = mock(TransactionContext.class);
    ContainerStateMachine.Context context = mock(ContainerStateMachine.Context.class);
    when(trx.getStateMachineContext()).thenReturn(context);
    doAnswer(e -> {
      try {
        Thread.sleep(200000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw ie;
      }
      return null;
    }).when(dispatcher).dispatch(any(), any());

    setUpMockRequestProtoReturn(context, 1, 1);

    CompletableFuture<Message> firstWrite = stateMachine.write(entry, trx);
    Thread.sleep(2000);
    CompletableFuture<Message> secondWrite = stateMachine.write(entryNext, trx);
    ContainerProtos.ContainerCommandResponseProto containerCommandResponseProto
        = ContainerProtos.ContainerCommandResponseProto.parseFrom(firstWrite.get().getContent());
    assertEquals("Failed to write chunk data", containerCommandResponseProto.getMessage());

    containerCommandResponseProto
        = ContainerProtos.ContainerCommandResponseProto.parseFrom(secondWrite.get().getContent());
    assertEquals(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR, containerCommandResponseProto.getResult());
  }

  private void setUpMockDispatcherReturn(boolean failWithException) {
    if (failWithException) {
      when(dispatcher.dispatch(any(), any())).thenThrow(new RuntimeException());
    } else {
      when(dispatcher.dispatch(any(), any())).thenReturn(ContainerProtos.ContainerCommandResponseProto
          .newBuilder().setCmdType(ContainerProtos.Type.WriteChunk)
          .setResult(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR)
          .build());
    }
  }

  private void setUpMockRequestProtoReturn(ContainerStateMachine.Context context,
                                           int containerId, int localId) {
    when(context.getRequestProto()).thenReturn(ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.WriteChunk).setWriteChunk(
            ContainerProtos.WriteChunkRequestProto.newBuilder().setData(ByteString.copyFromUtf8(CONTAINER_DATA))
                .setBlockID(
                    ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(containerId)
                        .setLocalID(localId).build()).build())
        .setContainerID(containerId)
        .setDatanodeUuid(UUID.randomUUID().toString()).build());
  }

  private void assertResults(boolean failWithException, AtomicReference<Throwable> throwable) {
    if (failWithException) {
      assertInstanceOf(RuntimeException.class, throwable.get());
    } else {
      assertInstanceOf(StorageContainerException.class, throwable.get());
      StorageContainerException sce = (StorageContainerException) throwable.get();
      assertEquals(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR, sce.getResult());
    }
  }

  private void setUpLogProtoReturn(ContainerStateMachine.Context context, int containerId, int localId) {
    when(context.getLogProto()).thenReturn(ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.WriteChunk).setWriteChunk(
            ContainerProtos.WriteChunkRequestProto.newBuilder().setBlockID(
                ContainerProtos.DatanodeBlockID.newBuilder().
                    setContainerID(containerId).setLocalID(localId).build()).build())
        .setContainerID(containerId)
        .setDatanodeUuid(UUID.randomUUID().toString()).build());
  }

  private static class ThrowableCatcher {

    private final AtomicReference<Throwable> caught = new AtomicReference<>(null);

    public Function<Throwable, ? extends Message> asSetter() {
      return t -> {
        caught.set(t);
        return null;
      };
    }

    public AtomicReference<Throwable> getCaught() {
      return caught;
    }

    public Throwable getReceived() {
      return caught.get();
    }

    public void reset() {
      caught.set(null);
    }
  }
}

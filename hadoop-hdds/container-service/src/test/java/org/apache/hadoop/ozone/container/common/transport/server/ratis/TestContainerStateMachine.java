package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

/**
 * Test class to ContainerStateMachine class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class TestContainerStateMachine {
  private ContainerDispatcher dispatcher;
  private static XceiverServerRatis ratisServer;
  private static ContainerController controller;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private ContainerStateMachine stateMachine;
  private List<ThreadPoolExecutor> executor = IntStream.range(0, 2).mapToObj(i -> new ThreadPoolExecutor(1, 1,
      0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("ChunkWriter-" + i + "-%d")
      .build())).collect(Collectors.toList());
  private boolean isLeader;

  TestContainerStateMachine(boolean isLeader) {
    this.isLeader = isLeader;
  }

  @BeforeEach
  public void setup() throws IOException {
    dispatcher = mock(ContainerDispatcher.class);
    controller = mock(ContainerController.class);
    ratisServer = mock(XceiverServerRatis.class);
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
  public void testWriteFailure(boolean failWithException) throws ExecutionException, InterruptedException {
    RaftProtos.LogEntryProto entry = mock(RaftProtos.LogEntryProto.class);
    when(entry.getTerm()).thenReturn(1L);
    when(entry.getIndex()).thenReturn(1L);
    TransactionContext trx = mock(TransactionContext.class);
    ContainerStateMachine.Context context = mock(ContainerStateMachine.Context.class);
    when(trx.getStateMachineContext()).thenReturn(context);
    if (failWithException) {
      when(dispatcher.dispatch(any(), any())).thenThrow(new RuntimeException());
    } else {
      when(dispatcher.dispatch(any(), any())).thenReturn(ContainerProtos.ContainerCommandResponseProto
          .newBuilder().setCmdType(ContainerProtos.Type.WriteChunk)
          .setResult(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR)
          .build());
    }

    when(context.getRequestProto()).thenReturn(ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.WriteChunk).setWriteChunk(
            ContainerProtos.WriteChunkRequestProto.newBuilder().setData(ByteString.copyFromUtf8("Test Data"))
                .setBlockID(
                    ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(1).setLocalID(1).build()).build())
        .setContainerID(1)
        .setDatanodeUuid(UUID.randomUUID().toString()).build());
    AtomicReference<Throwable> throwable = new AtomicReference<>(null);
    Function<Throwable, ? extends Message> throwableSetter = t -> {
      throwable.set(t);
      return null;
    };
    stateMachine.write(entry, trx).exceptionally(throwableSetter).get();
    verify(dispatcher, times(1)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    reset(dispatcher);
    assertNotNull(throwable.get());
    if (failWithException) {
      assertInstanceOf(RuntimeException.class, throwable.get());
    } else {
      assertInstanceOf(StorageContainerException.class, throwable.get());
      StorageContainerException sce = (StorageContainerException) throwable.get();
      assertEquals(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR, sce.getResult());
    }
    // Writing data to another container(containerId 2) should also fail.
    when(context.getRequestProto()).thenReturn(ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.WriteChunk).setWriteChunk(
            ContainerProtos.WriteChunkRequestProto.newBuilder().setData(ByteString.copyFromUtf8("Test Data"))
                .setBlockID(
                    ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(2).setLocalID(1).build()).build())
        .setContainerID(2)
        .setDatanodeUuid(UUID.randomUUID().toString()).build());
    stateMachine.write(entry, trx).exceptionally(throwableSetter).get();
    verify(dispatcher, times(0)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    assertInstanceOf(StorageContainerException.class, throwable.get());
    StorageContainerException sce = (StorageContainerException) throwable.get();
    assertEquals(ContainerProtos.Result.CONTAINER_UNHEALTHY, sce.getResult());
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
    if (failWithException) {
      when(dispatcher.dispatch(any(), any())).thenThrow(new RuntimeException());
    } else {
      when(dispatcher.dispatch(any(), any())).thenReturn(ContainerProtos.ContainerCommandResponseProto
          .newBuilder().setCmdType(ContainerProtos.Type.WriteChunk)
          .setResult(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR)
          .build());
    }
    // Failing apply transaction on congtainer 1.
    when(context.getLogProto()).thenReturn(ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.WriteChunk).setWriteChunk(
            ContainerProtos.WriteChunkRequestProto.newBuilder().setBlockID(
                ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(1).setLocalID(1).build()).build())
        .setContainerID(1)
        .setDatanodeUuid(UUID.randomUUID().toString()).build());
    AtomicReference<Throwable> throwable = new AtomicReference<>(null);
    Function<Throwable, ? extends Message> throwableSetter = t -> {
      throwable.set(t);
      return null;
    };
    //apply transaction will fail because of runtime exception thrown by dispatcher, which marks the first
    // failure on container 1.
    stateMachine.applyTransaction(trx).exceptionally(throwableSetter).get();
    verify(dispatcher, times(1)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    reset(dispatcher);
    assertNotNull(throwable.get());
    if (failWithException) {
      assertInstanceOf(RuntimeException.class, throwable.get());
    } else {
      assertInstanceOf(StorageContainerException.class, throwable.get());
      StorageContainerException sce = (StorageContainerException) throwable.get();
      assertEquals(ContainerProtos.Result.CONTAINER_INTERNAL_ERROR, sce.getResult());
    }
    // Another apply transaction on same container 1 should fail because the previous apply transaction failed.
    stateMachine.applyTransaction(trx).exceptionally(throwableSetter).get();
    verify(dispatcher, times(0)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    assertInstanceOf(StorageContainerException.class, throwable.get());
    StorageContainerException sce = (StorageContainerException) throwable.get();
    assertEquals(ContainerProtos.Result.CONTAINER_UNHEALTHY, sce.getResult());

    // Another apply transaction on a different container 2 shouldn't fail because the previous apply transaction
    // failure was only on container 1.
    when(context.getLogProto()).thenReturn(ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.WriteChunk).setWriteChunk(
            ContainerProtos.WriteChunkRequestProto.newBuilder().setBlockID(
                ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(2).setLocalID(1).build()).build())
        .setContainerID(2)
        .setDatanodeUuid(UUID.randomUUID().toString()).build());

    reset(dispatcher);
    throwable.set(null);
    when(dispatcher.dispatch(any(), any())).thenReturn(ContainerProtos.ContainerCommandResponseProto
        .newBuilder().setCmdType(ContainerProtos.Type.WriteChunk).setResult(ContainerProtos.Result.SUCCESS)
        .build());
    Message succcesfulTransaction = stateMachine.applyTransaction(trx).exceptionally(throwableSetter).get();
    verify(dispatcher, times(1)).dispatch(any(ContainerProtos.ContainerCommandRequestProto.class),
        any(DispatcherContext.class));
    assertNull(throwable.get());
    ContainerProtos.ContainerCommandResponseProto resp =
        ContainerProtos.ContainerCommandResponseProto.parseFrom(succcesfulTransaction.getContent());
    assertEquals(ContainerProtos.Result.SUCCESS, resp.getResult());
  }
}

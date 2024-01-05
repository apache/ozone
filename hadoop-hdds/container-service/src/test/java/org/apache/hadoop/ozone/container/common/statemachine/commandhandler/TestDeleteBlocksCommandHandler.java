/**
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler.SchemaHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto
    .DeleteBlockTransactionResult;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.BLOCK_DELETE_COMMAND_WORKER_INTERVAL;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler.DeleteBlockTransactionExecutionResult;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for TestDeleteBlocksCommandHandler.
 */
@Timeout(300)
public class TestDeleteBlocksCommandHandler {
  @TempDir
  private Path folder;
  private OzoneConfiguration conf;
  private ContainerLayoutVersion layout;
  private OzoneContainer ozoneContainer;
  private ContainerSet containerSet;
  private DeleteBlocksCommandHandler handler;
  private String schemaVersion;
  private HddsVolume volume1;
  private BlockDeletingServiceMetrics blockDeleteMetrics;

  private void prepareTest(ContainerTestVersionInfo versionInfo)
      throws Exception {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
    setup();
  }

  private void setup() throws Exception {
    conf = new OzoneConfiguration();
    layout = ContainerLayoutVersion.FILE_PER_BLOCK;
    ozoneContainer = mock(OzoneContainer.class);
    containerSet = new ContainerSet(1000);
    volume1 = mock(HddsVolume.class);
    when(volume1.getStorageID()).thenReturn("uuid-1");
    for (int i = 0; i <= 10; i++) {
      KeyValueContainerData data =
          new KeyValueContainerData(i,
              layout,
              ContainerTestHelper.CONTAINER_MAX_SIZE,
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString());
      data.setSchemaVersion(schemaVersion);
      data.setVolume(volume1);
      KeyValueContainer container = new KeyValueContainer(data, conf);
      data.closeContainer();
      containerSet.addContainer(container);
    }
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);

    handler = spy(new DeleteBlocksCommandHandler(
        ozoneContainer, conf, dnConf, ""));
    blockDeleteMetrics = handler.getBlockDeleteMetrics();
    TestSchemaHandler testSchemaHandler1 = spy(new TestSchemaHandler());
    TestSchemaHandler testSchemaHandler2 = spy(new TestSchemaHandler());
    TestSchemaHandler testSchemaHandler3 = spy(new TestSchemaHandler());

    handler.getSchemaHandlers().put(SCHEMA_V1, testSchemaHandler1);
    handler.getSchemaHandlers().put(SCHEMA_V2, testSchemaHandler2);
    handler.getSchemaHandlers().put(SCHEMA_V3, testSchemaHandler3);
  }

  @AfterEach
  public void tearDown() {
    handler.stop();
    BlockDeletingServiceMetrics.unRegister();
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteBlocksCommandHandler(
      ContainerTestVersionInfo versionInfo) throws Exception {
    prepareTest(versionInfo);
    assertThat(containerSet.containerCount()).isGreaterThan(0);
    Container<?> container = containerSet.getContainerIterator(volume1).next();
    DeletedBlocksTransaction transaction = createDeletedBlocksTransaction(1,
        container.getContainerData().getContainerID());

    List<DeleteBlockTransactionResult> results =
        handler.executeCmdWithRetry(Arrays.asList(transaction));

    String schemaVersionOrDefault = ((KeyValueContainerData)
        container.getContainerData()).getSupportedSchemaVersionOrDefault();
    verify(handler.getSchemaHandlers().get(schemaVersionOrDefault),
        times(1)).handle(any(), any());
    // submitTasks will be executed only once, as if there were not retries
    verify(handler,
        times(1)).submitTasks(any());

    assertEquals(1, results.size());
    assertTrue(results.get(0).getSuccess());
    assertEquals(0,
        blockDeleteMetrics.getTotalLockTimeoutTransactionCount());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteBlocksCommandHandlerWithTimeoutFailed(
      ContainerTestVersionInfo versionInfo) throws Exception {
    prepareTest(versionInfo);
    assertThat(containerSet.containerCount()).isGreaterThanOrEqualTo(2);
    Iterator<Container<?>> iterator =
        containerSet.getContainerIterator(volume1);
    Container<?> lockedContainer = iterator.next();
    Container<?> nonLockedcontainer = iterator.next();
    DeletedBlocksTransaction transaction1 =
        createDeletedBlocksTransaction(1,
            lockedContainer.getContainerData().getContainerID());
    DeletedBlocksTransaction transaction2 =
        createDeletedBlocksTransaction(2,
            nonLockedcontainer.getContainerData().getContainerID());

    // By letting lockedContainer hold the lock and not releasing it,
    // lockedContainer's delete command processing will time out
    // and retry, but it will still fail eventually
    // nonLockedContainer will succeed because it does not hold the lock
    lockedContainer.writeLock();
    List<DeletedBlocksTransaction> transactions =
        Arrays.asList(transaction1, transaction2);
    List<DeleteBlockTransactionResult> results =
        handler.executeCmdWithRetry(transactions);
    String schemaVersionOrDefault = ((KeyValueContainerData) nonLockedcontainer.
        getContainerData()).getSupportedSchemaVersionOrDefault();
    verify(handler.getSchemaHandlers().get(schemaVersionOrDefault),
            times(1)).handle(eq((KeyValueContainerData)
            nonLockedcontainer.getContainerData()), eq(transaction2));

    // submitTasks will be executed twice, as if there were retries
    verify(handler,
        times(1)).submitTasks(eq(transactions));
    verify(handler,
        times(1)).submitTasks(eq(Arrays.asList(transaction1)));
    assertEquals(2, results.size());

    // Only one transaction will succeed
    Map<Long, DeleteBlockTransactionResult> resultsMap = new HashMap<>();
    results.forEach(result -> resultsMap.put(result.getTxID(), result));
    assertFalse(resultsMap.get(transaction1.getTxID()).getSuccess());
    assertTrue(resultsMap.get(transaction2.getTxID()).getSuccess());

    assertEquals(1,
        blockDeleteMetrics.getTotalLockTimeoutTransactionCount());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteBlocksCommandHandlerSuccessfulAfterFirstTimeout(
      ContainerTestVersionInfo versionInfo) throws Exception {
    prepareTest(versionInfo);
    assertThat(containerSet.containerCount()).isGreaterThan(0);
    Container<?> lockedContainer =
        containerSet.getContainerIterator(volume1).next();
    DeletedBlocksTransaction transaction = createDeletedBlocksTransaction(1,
        lockedContainer.getContainerData().getContainerID());
    // After the Container waits for the lock to time out for the first time,
    // release the lock on the Container, so the retry will succeed.
    lockedContainer.writeLock();
    doAnswer(new Answer<List<Future<DeleteBlockTransactionExecutionResult>>>() {
      @Override
      public List<Future<DeleteBlockTransactionExecutionResult>> answer(
          InvocationOnMock invocationOnMock) throws Throwable {
        List<Future<DeleteBlockTransactionExecutionResult>> result =
            (List<Future<DeleteBlockTransactionExecutionResult>>)
                invocationOnMock.callRealMethod();
        // Wait for the task to finish executing and then release the lock
        DeleteBlockTransactionExecutionResult res = result.get(0).get();
        if (lockedContainer.hasWriteLock()) {
          lockedContainer.writeUnlock();
        }
        CompletableFuture<DeleteBlockTransactionExecutionResult> future =
            new CompletableFuture<>();
        future.complete(res);
        result.clear();
        result.add(future);
        return result;
      }
    }).when(handler).submitTasks(any());

    List<DeleteBlockTransactionResult> results =
        handler.executeCmdWithRetry(Arrays.asList(transaction));

    // submitTasks will be executed twice, as if there were retries
    String schemaVersionOrDefault = ((KeyValueContainerData) lockedContainer
        .getContainerData()).getSupportedSchemaVersionOrDefault();
    verify(handler,
        times(2)).submitTasks(any());
    verify(handler.getSchemaHandlers().get(schemaVersionOrDefault),
        times(1)).handle(any(), any());
    assertEquals(1, results.size());
    assertTrue(results.get(0).getSuccess());

    assertEquals(0,
        blockDeleteMetrics.getTotalLockTimeoutTransactionCount());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDeleteCmdWorkerInterval(
      ContainerTestVersionInfo versionInfo) throws Exception {
    prepareTest(versionInfo);
    OzoneConfiguration tmpConf = new OzoneConfiguration();
    tmpConf.setTimeDuration(BLOCK_DELETE_COMMAND_WORKER_INTERVAL, 3,
        TimeUnit.SECONDS);
    OzoneContainer container = mock(OzoneContainer.class);
    DatanodeConfiguration dnConf =
        tmpConf.getObject(DatanodeConfiguration.class);
    DeleteBlocksCommandHandler commandHandler =
        spy(new DeleteBlocksCommandHandler(
            container, tmpConf, dnConf, "test"));

    assertEquals(tmpConf.getTimeDuration(
        BLOCK_DELETE_COMMAND_WORKER_INTERVAL,
        BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT.getSeconds(),
        TimeUnit.SECONDS), 3);
    DeleteBlocksCommandHandler.DeleteCmdWorker deleteCmdWorker =
        commandHandler.new DeleteCmdWorker(4000);
    assertEquals(deleteCmdWorker.getInterval(), 4000);
  }

  @Test
  public void testDeleteBlockCommandHandleWhenDeleteCommandQueuesFull()
      throws IOException {
    int blockDeleteQueueLimit = 5;
    // Setting up the test environment
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.toString());
    DatanodeDetails datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeConfiguration dnConf =
        configuration.getObject(DatanodeConfiguration.class);
    OzoneContainer container = ContainerTestUtils.getOzoneContainer(datanodeDetails, configuration);
    DatanodeStateMachine stateMachine = mock(DatanodeStateMachine.class);
    when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    StateContext context = new StateContext(configuration,
        mock(DatanodeStateMachine.DatanodeStates.class),
        stateMachine, "");

    // Set Queue limit
    dnConf.setBlockDeleteQueueLimit(blockDeleteQueueLimit);
    handler = new DeleteBlocksCommandHandler(
        container, configuration, dnConf, "");

    // Check if the command status is as expected: PENDING when queue is not full, FAILED when queue is full
    for (int i = 0; i < blockDeleteQueueLimit + 2; i++) {
      DeleteBlocksCommand deleteBlocksCommand = new DeleteBlocksCommand(emptyList());
      context.addCommand(deleteBlocksCommand);
      handler.handle(deleteBlocksCommand, container, context, mock(SCMConnectionManager.class));
      CommandStatus cmdStatus = context.getCmdStatus(deleteBlocksCommand.getId());
      if (i < blockDeleteQueueLimit) {
        assertEquals(cmdStatus.getStatus(), Status.PENDING);
      } else {
        assertEquals(cmdStatus.getStatus(), Status.FAILED);
        assertEquals(cmdStatus.getProtoBufMessage().getBlockDeletionAck().getResultsCount(), 0);
      }
    }
  }

  private DeletedBlocksTransaction createDeletedBlocksTransaction(long txID,
      long containerID) {
    return DeletedBlocksTransaction.newBuilder()
        .setContainerID(containerID)
        .setCount(0)
        .addLocalID(1L)
        .setTxID(txID)
        .build();
  }

  private static class TestSchemaHandler implements SchemaHandler {
    @Override
    public void handle(KeyValueContainerData containerData,
        DeletedBlocksTransaction tx) throws IOException {
      // doNoting just for Test
    }
  }
}

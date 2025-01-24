/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.block;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto
    .DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for DeletedBlockLog.
 */
public class TestDeletedBlockLog {

  private  DeletedBlockLogImpl deletedBlockLog;
  private static final int BLOCKS_PER_TXN = 5;
  private OzoneConfiguration conf;
  @TempDir
  private File testDir;
  private ContainerManager containerManager;
  private Table<ContainerID, ContainerInfo> containerTable;
  private StorageContainerManager scm;
  private List<DatanodeDetails> dnList;
  private SCMHADBTransactionBuffer scmHADBTransactionBuffer;
  private Map<Long, ContainerInfo> containers = new HashMap<>();
  private Map<Long, Set<ContainerReplica>> replicas = new HashMap<>();
  private ScmBlockDeletingServiceMetrics metrics;
  private static final int THREE = ReplicationFactor.THREE_VALUE;
  private static final int ONE = ReplicationFactor.ONE_VALUE;

  private ReplicationManager replicationManager;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.setInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    replicationManager = mock(ReplicationManager.class);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setReplicationManager(replicationManager);
    scm = HddsTestUtils.getScm(conf, configurator);
    containerManager = mock(ContainerManager.class);
    containerTable = scm.getScmMetadataStore().getContainerTable();
    scmHADBTransactionBuffer =
        new SCMHADBTransactionBufferStub(scm.getScmMetadataStore().getStore());
    metrics = mock(ScmBlockDeletingServiceMetrics.class);
    deletedBlockLog = new DeletedBlockLogImpl(conf,
        scm,
        containerManager,
        scmHADBTransactionBuffer,
        metrics);
    dnList = new ArrayList<>(3);
    setupContainerManager();
  }

  private void setupContainerManager() throws IOException {
    dnList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());
    dnList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());
    dnList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());

    when(containerManager.getContainerReplicas(any()))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return replicas.get(cid.getId());
        });
    when(containerManager.getContainer(any()))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return containerTable.get(cid);
        });
    when(containerManager.getContainers())
        .thenReturn(new ArrayList<>(containers.values()));
    doAnswer(invocationOnMock -> {
      Map<ContainerID, Long> map =
          (Map<ContainerID, Long>) invocationOnMock.getArguments()[0];
      for (Map.Entry<ContainerID, Long> e : map.entrySet()) {
        ContainerInfo info = containers.get(e.getKey().getId());
        try {
          assertThat(e.getValue()).isGreaterThan(info.getDeleteTransactionId());
        } catch (AssertionError err) {
          throw new Exception("New TxnId " + e.getValue() + " < " + info
              .getDeleteTransactionId());
        }
        info.updateDeleteTransactionId(e.getValue());
        scmHADBTransactionBuffer.addToBuffer(containerTable, e.getKey(), info);
      }
      return null;
    }).when(containerManager).updateDeleteTransactionId(any());
  }

  private void updateContainerMetadata(long cid,
      HddsProtos.LifeCycleState state) throws IOException {
    final ContainerInfo container =
        new ContainerInfo.Builder()
            .setContainerID(cid)
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                ReplicationFactor.THREE))
            .setState(state)
            .setOwner("TestDeletedBlockLog")
            .setPipelineID(PipelineID.randomId())
            .build();
    final Set<ContainerReplica> replicaSet = dnList.stream()
        .map(datanodeDetails -> ContainerReplica.newBuilder()
            .setContainerID(container.containerID())
            .setContainerState(ContainerReplicaProto.State.OPEN)
            .setDatanodeDetails(datanodeDetails)
            .build())
        .collect(Collectors.toSet());
    containers.put(cid, container);
    containerTable.put(ContainerID.valueOf(cid), container);
    replicas.put(cid, replicaSet);
  }

  @AfterEach
  public void tearDown() throws Exception {
    deletedBlockLog.close();
    scm.stop();
    scm.join();
  }

  private Map<Long, List<Long>> generateData(int dataSize) throws IOException {
    return generateData(dataSize, HddsProtos.LifeCycleState.CLOSED);
  }

  private Map<Long, List<Long>> generateData(int dataSize,
      HddsProtos.LifeCycleState state) throws IOException {
    Map<Long, List<Long>> blockMap = new HashMap<>();
    int continerIDBase = RandomUtils.nextInt(0, 100);
    int localIDBase = RandomUtils.nextInt(0, 1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      updateContainerMetadata(containerID, state);
      List<Long> blocks = new ArrayList<>();
      for (int j = 0; j < BLOCKS_PER_TXN; j++)  {
        long localID = localIDBase + j;
        blocks.add(localID);
      }
      blockMap.put(containerID, blocks);
    }
    return blockMap;
  }

  private void addTransactions(Map<Long, List<Long>> containerBlocksMap,
      boolean shouldFlush)
      throws IOException, TimeoutException {
    deletedBlockLog.addTransactions(containerBlocksMap);
    if (shouldFlush) {
      scmHADBTransactionBuffer.flush();
    }
  }

  private void incrementCount(List<Long> txIDs)
      throws IOException, TimeoutException {
    deletedBlockLog.incrementCount(txIDs);
    scmHADBTransactionBuffer.flush();
    // mock scmHADBTransactionBuffer does not flush deletedBlockLog
    deletedBlockLog.onFlush();
  }

  private void resetCount(List<Long> txIDs)
      throws IOException, TimeoutException {
    deletedBlockLog.resetCount(txIDs);
    scmHADBTransactionBuffer.flush();
    deletedBlockLog.onFlush();
  }

  private void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults,
      DatanodeDetails... dns) throws IOException {
    for (DatanodeDetails dnDetails : dns) {
      deletedBlockLog.getSCMDeletedBlockTransactionStatusManager()
          .commitTransactions(transactionResults, dnDetails.getUuid());
    }
    scmHADBTransactionBuffer.flush();
  }

  private void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults)
      throws IOException {
    commitTransactions(transactionResults,
        dnList.toArray(new DatanodeDetails[0]));
  }

  private void commitTransactions(
      Collection<DeletedBlocksTransaction> deletedBlocksTransactions,
      DatanodeDetails... dns) throws IOException {
    commitTransactions(deletedBlocksTransactions.stream()
        .map(this::createDeleteBlockTransactionResult)
        .collect(Collectors.toList()), dns);
  }

  private void commitTransactions(
      Collection<DeletedBlocksTransaction> deletedBlocksTransactions)
      throws IOException {
    commitTransactions(deletedBlocksTransactions.stream()
        .map(this::createDeleteBlockTransactionResult)
        .collect(Collectors.toList()));
  }

  private DeleteBlockTransactionResult createDeleteBlockTransactionResult(
      DeletedBlocksTransaction transaction) {
    return DeleteBlockTransactionResult.newBuilder()
        .setContainerID(transaction.getContainerID()).setSuccess(true)
        .setTxID(transaction.getTxID()).build();
  }

  private List<DeletedBlocksTransaction> getAllTransactions() throws Exception {
    return getTransactions(Integer.MAX_VALUE);
  }

  private List<DeletedBlocksTransaction> getTransactions(
      int maximumAllowedBlocksNum) throws IOException, TimeoutException {
    DatanodeDeletedBlockTransactions transactions =
        deletedBlockLog.getTransactions(maximumAllowedBlocksNum,
            dnList.stream().collect(Collectors.toSet()));
    List<DeletedBlocksTransaction> txns = new LinkedList<>();
    for (DatanodeDetails dn : dnList) {
      txns.addAll(Optional.ofNullable(
          transactions.getDatanodeTransactionMap().get(dn.getUuid()))
          .orElseGet(LinkedList::new));
    }
    // Simulated transactions are sent
    for (Map.Entry<UUID, List<DeletedBlocksTransaction>> entry :
        transactions.getDatanodeTransactionMap().entrySet()) {
      DeleteBlocksCommand command = new DeleteBlocksCommand(entry.getValue());
      recordScmCommandToStatusManager(entry.getKey(), command);
      sendSCMDeleteBlocksCommand(entry.getKey(), command);
    }
    return txns;
  }

  @Test
  public void testContainerManagerTransactionId() throws Exception {
    // Initially all containers should have deleteTransactionId as 0
    for (ContainerInfo containerInfo : containerManager.getContainers()) {
      assertEquals(0, containerInfo.getDeleteTransactionId());
    }

    // Create 30 TXs
    addTransactions(generateData(30), false);
    // Since transactions are not yet flushed deleteTransactionId should be
    // 0 for all containers
    assertEquals(0, getAllTransactions().size());
    for (ContainerInfo containerInfo : containerManager.getContainers()) {
      assertEquals(0, containerInfo.getDeleteTransactionId());
    }

    scmHADBTransactionBuffer.flush();
    // After flush there should be 30 transactions in deleteTable
    // All containers should have positive deleteTransactionId
    mockContainerHealthResult(true);
    assertEquals(30 * THREE, getAllTransactions().size());
    for (ContainerInfo containerInfo : containerManager.getContainers()) {
      assertThat(containerInfo.getDeleteTransactionId()).isGreaterThan(0);
    }
  }

  @Test
  public void testIncrementCount() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    // Create 30 TXs in the log.
    addTransactions(generateData(30), true);
    mockContainerHealthResult(true);

    // This will return all TXs, total num 30.
    List<DeletedBlocksTransaction> blocks = getAllTransactions();
    List<Long> txIDs = blocks.stream().map(DeletedBlocksTransaction::getTxID)
        .distinct().collect(Collectors.toList());
    assertEquals(30, txIDs.size());

    for (DeletedBlocksTransaction block : blocks) {
      assertEquals(0, block.getCount());
    }

    for (int i = 0; i < maxRetry; i++) {
      incrementCount(txIDs);
    }
    blocks = getAllTransactions();
    for (DeletedBlocksTransaction block : blocks) {
      assertEquals(maxRetry, block.getCount());
    }

    // Increment another time so it exceed the maxRetry.
    // On this call, count will be set to -1 which means TX eventually fails.
    incrementCount(txIDs);
    blocks = getAllTransactions();
    for (DeletedBlocksTransaction block : blocks) {
      assertEquals(-1, block.getCount());
    }

    // If all TXs are failed, getTransactions call will always return nothing.
    blocks = getAllTransactions();
    assertEquals(0, blocks.size());
  }

  private void mockContainerHealthResult(Boolean healthy) {
    ContainerInfo containerInfo = mock(ContainerInfo.class);
    ContainerHealthResult healthResult =
        new ContainerHealthResult.HealthyResult(containerInfo);
    if (!healthy) {
      healthResult = new ContainerHealthResult.UnHealthyResult(containerInfo);
    }
    doReturn(healthResult).when(replicationManager)
        .getContainerReplicationHealth(any(), any());
  }

  @Test
  public void testResetCount() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    // Create 30 TXs in the log.
    addTransactions(generateData(30), true);
    mockContainerHealthResult(true);

    // This will return all TXs, total num 30.
    List<DeletedBlocksTransaction> blocks = getAllTransactions();
    List<Long> txIDs = blocks.stream().map(DeletedBlocksTransaction::getTxID)
        .distinct().collect(Collectors.toList());

    for (int i = 0; i < maxRetry; i++) {
      incrementCount(txIDs);
    }

    // Increment another time so it exceed the maxRetry.
    // On this call, count will be set to -1 which means TX eventually fails.
    incrementCount(txIDs);
    blocks = getAllTransactions();
    for (DeletedBlocksTransaction block : blocks) {
      assertEquals(-1, block.getCount());
    }

    // If all TXs are failed, getTransactions call will always return nothing.
    blocks = getAllTransactions();
    assertEquals(0, blocks.size());

    // Reset the retry count, these transactions should be accessible.
    resetCount(txIDs);
    blocks = getAllTransactions();
    for (DeletedBlocksTransaction block : blocks) {
      assertEquals(0, block.getCount());
    }

    // Increment for the reset transactions.
    // Lets the SCM delete the transaction and wait for the DN reply
    // to timeout, thus allowing the transaction to resend the
    deletedBlockLog.setScmCommandTimeoutMs(-1L);
    incrementCount(txIDs);
    blocks = getAllTransactions();
    for (DeletedBlocksTransaction block : blocks) {
      assertEquals(1, block.getCount());
    }

    assertEquals(30 * THREE, blocks.size());
  }


  @Test
  public void testSCMDelIteratorProgress() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    // CASE1: When all transactions are valid and available
    // Create 8 TXs in the log.
    int noOfTransactions = 8;
    addTransactions(generateData(noOfTransactions), true);
    mockContainerHealthResult(true);
    List<DeletedBlocksTransaction> blocks;

    List<Long> txIDs = new ArrayList<>();
    int i = 1;
    while (i < noOfTransactions) {
      // In each iteration read two transaction, API returns all the transactions in order.
      // 1st iteration: {1, 2}
      // 2nd iteration: {3, 4}
      // 3rd iteration: {5, 6}
      // 4th iteration: {7, 8}
      blocks = getTransactions(2 * BLOCKS_PER_TXN * THREE);
      assertEquals(blocks.get(0).getTxID(), i++);
      assertEquals(blocks.get(1).getTxID(), i++);
    }

    // CASE2: When some transactions are not available for delete in the current iteration,
    // either due to max retry reach or some other issue.
    // New transactions Id is { 9, 10, 11, 12, 13, 14, 15, 16}
    addTransactions(generateData(noOfTransactions), true);
    mockContainerHealthResult(true);

    // Mark transaction Id 11 as reached max retry count so that it will be ignored
    // by scm deleting service while fetching transaction for delete
    int ignoreTransactionId = 11;
    txIDs.add((long) ignoreTransactionId);
    for (i = 0; i < maxRetry; i++) {
      incrementCount(txIDs);
    }
    incrementCount(txIDs);

    i = 9;
    while (true) {
      // In each iteration read two transaction.
      // If any transaction which is not available for delete in the current iteration,
      // it will be ignored and will be re-checked again only after complete table is read.
      // 1st iteration: {9, 10}
      // 2nd iteration: {12, 13} Transaction 11 is ignored here
      // 3rd iteration: {14, 15} Transaction 11 is available here,
      // but it will be read only when all db records are read till the end.
      // 4th iteration: {16, 11} Since iterator reached at the end of table after reading transaction 16,
      // Iterator starts from beginning again, and it returns transaction 11 as well
      blocks = getTransactions(2 * BLOCKS_PER_TXN * THREE);
      if (i == ignoreTransactionId) {
        i++;
      }
      assertEquals(blocks.get(0).getTxID(), i++);
      if (i == 17) {
        assertEquals(blocks.get(1).getTxID(), ignoreTransactionId);
        break;
      }
      assertEquals(blocks.get(1).getTxID(), i++);

      if (i == 14) {
        // Reset transaction 11 so that it will be available in scm key deleting service in the subsequent iterations.
        resetCount(txIDs);
      }
    }
  }

  @Test
  public void testCommitTransactions() throws Exception {
    deletedBlockLog.setScmCommandTimeoutMs(Long.MAX_VALUE);
    addTransactions(generateData(50), true);
    mockContainerHealthResult(true);
    List<DeletedBlocksTransaction> blocks =
        getTransactions(20 * BLOCKS_PER_TXN * THREE);
    // Add an invalid txn.
    blocks.add(
        DeletedBlocksTransaction.newBuilder().setContainerID(1).setTxID(70)
            .setCount(0).addLocalID(0).build());
    commitTransactions(blocks);

    blocks = getTransactions(50 * BLOCKS_PER_TXN * THREE);
    assertEquals(30 * THREE, blocks.size());
    commitTransactions(blocks, dnList.get(1), dnList.get(2),
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());

    blocks = getTransactions(50 * BLOCKS_PER_TXN * THREE);
    // SCM will not repeat a transaction until it has timed out.
    assertEquals(0, blocks.size());
    // Lets the SCM delete the transaction and wait for the DN reply
    // to timeout, thus allowing the transaction to resend the
    deletedBlockLog.setScmCommandTimeoutMs(-1L);
    blocks = getTransactions(50 * BLOCKS_PER_TXN * THREE);
    // only uncommitted dn have transactions
    assertEquals(30, blocks.size());
    commitTransactions(blocks, dnList.get(0));

    blocks = getTransactions(50 * BLOCKS_PER_TXN * THREE);
    assertEquals(0, blocks.size());
  }

  private void recordScmCommandToStatusManager(
      UUID dnId, DeleteBlocksCommand command) {
    Set<Long> dnTxSet = command.blocksTobeDeleted()
        .stream().map(DeletedBlocksTransaction::getTxID)
        .collect(Collectors.toSet());
    deletedBlockLog.recordTransactionCreated(dnId, command.getId(), dnTxSet);
  }

  private void sendSCMDeleteBlocksCommand(UUID dnId, SCMCommand<?> scmCommand) {
    deletedBlockLog.onSent(
        DatanodeDetails.newBuilder().setUuid(dnId).build(), scmCommand);
  }

  private void assertNoDuplicateTransactions(
      DatanodeDeletedBlockTransactions transactions1,
      DatanodeDeletedBlockTransactions transactions2) {
    Map<UUID, List<DeletedBlocksTransaction>> map1 =
        transactions1.getDatanodeTransactionMap();
    Map<UUID, List<DeletedBlocksTransaction>> map2 =
        transactions2.getDatanodeTransactionMap();

    for (Map.Entry<UUID, List<DeletedBlocksTransaction>> entry :
        map1.entrySet()) {
      UUID dnId = entry.getKey();
      Set<DeletedBlocksTransaction> txSet1 = new HashSet<>(entry.getValue());
      Set<DeletedBlocksTransaction> txSet2 = new HashSet<>(map2.get(dnId));

      txSet1.retainAll(txSet2);
      assertEquals(0, txSet1.size(),
          String.format("Duplicate Transactions found first transactions %s " +
              "second transactions %s for Dn %s", txSet1, txSet2, dnId));
    }
  }


  private void assertContainsAllTransactions(
      DatanodeDeletedBlockTransactions transactions1,
      DatanodeDeletedBlockTransactions transactions2) {
    Map<UUID, List<DeletedBlocksTransaction>> map1 =
        transactions1.getDatanodeTransactionMap();
    Map<UUID, List<DeletedBlocksTransaction>> map2 =
        transactions2.getDatanodeTransactionMap();

    for (Map.Entry<UUID, List<DeletedBlocksTransaction>> entry :
        map1.entrySet()) {
      UUID dnId = entry.getKey();
      Set<DeletedBlocksTransaction> txSet1 = new HashSet<>(entry.getValue());
      Set<DeletedBlocksTransaction> txSet2 = new HashSet<>(map2.get(dnId));

      assertThat(txSet1).containsAll(txSet2);
    }
  }

  private void commitSCMCommandStatus(Long scmCmdId, UUID dnID,
      StorageContainerDatanodeProtocolProtos.CommandStatus.Status status) {
    List<StorageContainerDatanodeProtocolProtos
        .CommandStatus> deleteBlockStatus = new ArrayList<>();
    deleteBlockStatus.add(CommandStatus.CommandStatusBuilder.newBuilder()
        .setCmdId(scmCmdId)
        .setType(Type.deleteBlocksCommand)
        .setStatus(status)
        .build()
        .getProtoBufMessage());

    deletedBlockLog.getSCMDeletedBlockTransactionStatusManager()
        .commitSCMCommandStatus(deleteBlockStatus, dnID);
  }

  private void createDeleteBlocksCommandAndAction(
      DatanodeDeletedBlockTransactions transactions,
      BiConsumer<UUID, DeleteBlocksCommand> afterCreate) {
    for (Map.Entry<UUID, List<DeletedBlocksTransaction>> entry :
        transactions.getDatanodeTransactionMap().entrySet()) {
      UUID dnId = entry.getKey();
      List<DeletedBlocksTransaction> dnTXs = entry.getValue();
      DeleteBlocksCommand command = new DeleteBlocksCommand(dnTXs);
      afterCreate.accept(dnId, command);
    }
  }

  @Test
  public void testNoDuplicateTransactionsForInProcessingSCMCommand()
      throws Exception {
    // The SCM will not resend these transactions in blow case:
    // - If the command has not been sent;
    // - The DN does not report the status of the command via heartbeat
    //   After the command is sent;
    // - If the DN reports the command status as PENDING;
    addTransactions(generateData(10), true);
    int blockLimit = 2 * BLOCKS_PER_TXN * THREE;
    mockContainerHealthResult(true);

    // If the command has not been sent
    DatanodeDeletedBlockTransactions transactions1 =
        deletedBlockLog.getTransactions(blockLimit, new HashSet<>(dnList));
    createDeleteBlocksCommandAndAction(transactions1,
        this::recordScmCommandToStatusManager);

    // - The DN does not report the status of the command via heartbeat
    //   After the command is sent
    DatanodeDeletedBlockTransactions transactions2 =
        deletedBlockLog.getTransactions(blockLimit, new HashSet<>(dnList));
    assertNoDuplicateTransactions(transactions1, transactions2);
    createDeleteBlocksCommandAndAction(transactions2, (dnId, command) -> {
      recordScmCommandToStatusManager(dnId, command);
      sendSCMDeleteBlocksCommand(dnId, command);
    });

    // - If the DN reports the command status as PENDING
    DatanodeDeletedBlockTransactions transactions3 =
        deletedBlockLog.getTransactions(blockLimit, new HashSet<>(dnList));
    assertNoDuplicateTransactions(transactions1, transactions3);
    createDeleteBlocksCommandAndAction(transactions3, (dnId, command) -> {
      recordScmCommandToStatusManager(dnId, command);
      sendSCMDeleteBlocksCommand(dnId, command);
      commitSCMCommandStatus(command.getId(), dnId,
          StorageContainerDatanodeProtocolProtos.CommandStatus.Status.PENDING);
    });
    assertNoDuplicateTransactions(transactions3, transactions1);
    assertNoDuplicateTransactions(transactions3, transactions2);

    DatanodeDeletedBlockTransactions transactions4 =
        deletedBlockLog.getTransactions(blockLimit, new HashSet<>(dnList));
    assertNoDuplicateTransactions(transactions4, transactions1);
    assertNoDuplicateTransactions(transactions4, transactions2);
    assertNoDuplicateTransactions(transactions4, transactions3);
  }

  @Test
  public void testFailedAndTimeoutSCMCommandCanBeResend() throws Exception {
    // The SCM will be resent these transactions in blow case:
    // - Executed failed commands;
    // - DN does not refresh the PENDING state for more than a period of time;
    deletedBlockLog.setScmCommandTimeoutMs(Long.MAX_VALUE);
    addTransactions(generateData(10), true);
    int blockLimit = 2 * BLOCKS_PER_TXN * THREE;
    mockContainerHealthResult(true);

    // - DN does not refresh the PENDING state for more than a period of time;
    DatanodeDeletedBlockTransactions transactions =
        deletedBlockLog.getTransactions(blockLimit, new HashSet<>(dnList));
    createDeleteBlocksCommandAndAction(transactions, (dnId, command) -> {
      recordScmCommandToStatusManager(dnId, command);
      sendSCMDeleteBlocksCommand(dnId, command);
      commitSCMCommandStatus(command.getId(), dnId,
          StorageContainerDatanodeProtocolProtos.CommandStatus.Status.PENDING);
    });

    // - Executed failed commands;
    DatanodeDeletedBlockTransactions transactions2 =
        deletedBlockLog.getTransactions(blockLimit, new HashSet<>(dnList));
    createDeleteBlocksCommandAndAction(transactions2, (dnId, command) -> {
      recordScmCommandToStatusManager(dnId, command);
      sendSCMDeleteBlocksCommand(dnId, command);
      commitSCMCommandStatus(command.getId(), dnId,
          StorageContainerDatanodeProtocolProtos.CommandStatus.Status.FAILED);
    });

    deletedBlockLog.setScmCommandTimeoutMs(-1L);
    DatanodeDeletedBlockTransactions transactions3 =
        deletedBlockLog.getTransactions(Integer.MAX_VALUE,
            new HashSet<>(dnList));
    assertNoDuplicateTransactions(transactions, transactions2);
    assertContainsAllTransactions(transactions3, transactions);
    assertContainsAllTransactions(transactions3, transactions2);
  }

  @Test
  public void testDNOnlyOneNodeHealthy() throws Exception {
    Map<Long, List<Long>> deletedBlocks = generateData(50);
    addTransactions(deletedBlocks, true);
    mockContainerHealthResult(false);
    DatanodeDeletedBlockTransactions transactions
        = deletedBlockLog.getTransactions(
        30 * BLOCKS_PER_TXN * THREE,
        dnList.subList(0, 1).stream().collect(Collectors.toSet()));
    assertEquals(0, transactions.getDatanodeTransactionMap().size());
  }

  @Test
  public void testInadequateReplicaCommit() throws Exception {
    Map<Long, List<Long>> deletedBlocks = generateData(50);
    addTransactions(deletedBlocks, true);
    long containerID;
    // let the first 30 container only consisting of only two unhealthy replicas
    int count = 0;
    for (Map.Entry<Long, List<Long>> entry : deletedBlocks.entrySet()) {
      containerID = entry.getKey();
      mockInadequateReplicaUnhealthyContainerInfo(containerID, count);
      count += 1;
    }
    // getTransactions will get existing container replicas then add transaction
    // to DN.
    // For the first 30 txn, deletedBlockLog only has the txn from dn1 and dn2
    // For the rest txn, txn will be got from all dns.
    // Committed txn will be: 1-40. 1-40. 31-40
    commitTransactions(getTransactions(30 * BLOCKS_PER_TXN * THREE));

    // The rest txn shall be: 41-50. 41-50. 41-50
    List<DeletedBlocksTransaction> blocks = getAllTransactions();
    // First 30 txns aren't considered for deletion as they don't have required
    // container replica's so getAllTransactions() won't be able to fetch them
    // and rest 20 txns are already committed and removed so in total
    // getAllTransactions() will fetch 0 txns.
    assertEquals(0, blocks.size());
  }

  @Test
  public void testRandomOperateTransactions() throws Exception {
    mockContainerHealthResult(true);
    int added = 0, committed = 0;
    List<DeletedBlocksTransaction> blocks = new ArrayList<>();
    List<Long> txIDs;
    // Randomly add/get/commit/increase transactions.
    for (int i = 0; i < 100; i++) {
      int state = RandomUtils.nextInt(0, 4);
      if (state == 0) {
        addTransactions(generateData(10), true);
        added += 10;
      } else if (state == 1) {
        blocks = getTransactions(20 * BLOCKS_PER_TXN * THREE);
        txIDs = new ArrayList<>();
        for (DeletedBlocksTransaction block : blocks) {
          txIDs.add(block.getTxID());
        }
        incrementCount(txIDs);
      } else if (state == 2) {
        commitTransactions(blocks);
        committed += blocks.size() / THREE;
        blocks = new ArrayList<>();
      } else {
        // verify the number of added and committed.
        try (TableIterator<Long,
            ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
            scm.getScmMetadataStore().getDeletedBlocksTXTable().iterator()) {
          AtomicInteger count = new AtomicInteger();
          iter.forEachRemaining((keyValue) -> count.incrementAndGet());
          assertEquals(added, count.get() + committed);
        }
      }
    }
    blocks = getAllTransactions();
    commitTransactions(blocks);
  }

  @Test
  public void testPersistence() throws Exception {
    addTransactions(generateData(50), true);
    mockContainerHealthResult(true);
    // close db and reopen it again to make sure
    // transactions are stored persistently.
    deletedBlockLog.close();
    deletedBlockLog = new DeletedBlockLogImpl(conf,
        scm,
        containerManager,
        scmHADBTransactionBuffer,
        metrics);
    List<DeletedBlocksTransaction> blocks =
        getTransactions(10 * BLOCKS_PER_TXN * THREE);
    assertEquals(10 * THREE, blocks.size());
    commitTransactions(blocks);
    blocks = getTransactions(40 * BLOCKS_PER_TXN * THREE);
    assertEquals(40 * THREE, blocks.size());
    commitTransactions(blocks);

    // close db and reopen it again to make sure
    // currentTxnID = 50
    deletedBlockLog.close();
    new DeletedBlockLogImpl(conf,
        scm,
        containerManager,
        scmHADBTransactionBuffer,
        metrics);
    blocks = getTransactions(40 * BLOCKS_PER_TXN * THREE);
    assertEquals(0, blocks.size());
    //assertEquals((long)deletedBlockLog.getCurrentTXID(), 50L);
  }

  @Test
  public void testDeletedBlockTransactions()
      throws IOException, TimeoutException {
    deletedBlockLog.setScmCommandTimeoutMs(Long.MAX_VALUE);
    mockContainerHealthResult(true);
    int txNum = 10;
    List<DeletedBlocksTransaction> blocks;
    DatanodeDetails dnId1 = dnList.get(0), dnId2 = dnList.get(1);

    int count = 0;
    long containerID;

    // Creates {TXNum} TX in the log.
    Map<Long, List<Long>> deletedBlocks = generateData(txNum);
    addTransactions(deletedBlocks, true);
    for (Map.Entry<Long, List<Long>> entry :deletedBlocks.entrySet()) {
      count++;
      containerID = entry.getKey();
      // let the container replication factor to be ONE
      if (count % 2 == 0) {
        mockStandAloneContainerInfo(containerID, dnId1);
      } else {
        mockStandAloneContainerInfo(containerID, dnId2);
      }
    }

    // fetch and delete 1 less txn Id
    commitTransactions(getTransactions((txNum - 1) * BLOCKS_PER_TXN * ONE));

    blocks = getTransactions(txNum * BLOCKS_PER_TXN * ONE);
    // There should be one txn remaining
    assertEquals(1, blocks.size());

    // add two transactions for same container
    containerID = blocks.get(0).getContainerID();
    Map<Long, List<Long>> deletedBlocksMap = new HashMap<>();
    long localId = RandomUtils.nextLong();
    deletedBlocksMap.put(containerID, new LinkedList<>(
        Collections.singletonList(localId)));
    addTransactions(deletedBlocksMap, true);
    blocks = getTransactions(txNum * BLOCKS_PER_TXN * ONE);
    // Only newly added Blocks will be sent, as previously sent transactions
    // that have not yet timed out will not be sent.
    assertEquals(1, blocks.size());
    assertEquals(1, blocks.get(0).getLocalIDCount());
    assertEquals(blocks.get(0).getLocalID(0), localId);

    // Lets the SCM delete the transaction and wait for the DN reply
    // to timeout, thus allowing the transaction to resend the
    deletedBlockLog.setScmCommandTimeoutMs(-1L);
    // get should return two transactions for the same container
    blocks = getTransactions(txNum * BLOCKS_PER_TXN * ONE);
    assertEquals(2, blocks.size());
  }

  @Test
  public void testDeletedBlockTransactionsOfDeletedContainer()
      throws IOException, TimeoutException {
    int txNum = 10;
    List<DeletedBlocksTransaction> blocks;

    // Creates {TXNum} TX in the log.
    Map<Long, List<Long>> deletedBlocks = generateData(txNum,
        HddsProtos.LifeCycleState.DELETED);
    addTransactions(deletedBlocks, true);

    blocks = getTransactions(txNum * BLOCKS_PER_TXN);
    // There should be no txn remaining
    assertEquals(0, blocks.size());
  }

  private void mockStandAloneContainerInfo(long containerID, DatanodeDetails dd)
      throws IOException {
    List<DatanodeDetails> dns = Collections.singletonList(dd);
    Pipeline pipeline = Pipeline.newBuilder()
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setState(Pipeline.PipelineState.OPEN)
            .setId(PipelineID.randomId())
            .setNodes(dns)
            .build();

    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setContainerID(containerID)
        .setPipelineID(pipeline.getId())
        .setReplicationConfig(pipeline.getReplicationConfig());

    ContainerInfo containerInfo = builder.build();
    doReturn(containerInfo).when(containerManager)
        .getContainer(ContainerID.valueOf(containerID));

    final Set<ContainerReplica> replicaSet = dns.stream()
        .map(datanodeDetails -> ContainerReplica.newBuilder()
            .setContainerID(containerInfo.containerID())
            .setContainerState(ContainerReplicaProto.State.OPEN)
            .setDatanodeDetails(datanodeDetails)
            .build())
        .collect(Collectors.toSet());
    when(containerManager.getContainerReplicas(
        ContainerID.valueOf(containerID)))
        .thenReturn(replicaSet);
  }

  private void mockInadequateReplicaUnhealthyContainerInfo(long containerID,
      int count) throws IOException {
    List<DatanodeDetails> dns = dnList.subList(0, 2);
    Pipeline pipeline = Pipeline.newBuilder()
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setNodes(dnList)
        .build();

    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setContainerID(containerID)
        .setPipelineID(pipeline.getId())
        .setReplicationConfig(pipeline.getReplicationConfig());

    ContainerInfo containerInfo = builder.build();
    doReturn(containerInfo).when(containerManager)
        .getContainer(ContainerID.valueOf(containerID));

    final Set<ContainerReplica> replicaSet = dns.stream()
        .map(datanodeDetails -> ContainerReplica.newBuilder()
            .setContainerID(containerInfo.containerID())
            .setContainerState(ContainerReplicaProto.State.CLOSED)
            .setDatanodeDetails(datanodeDetails)
            .build())
        .collect(Collectors.toSet());
    ContainerHealthResult healthResult;
    if (count < 30) {
      healthResult = new ContainerHealthResult.UnHealthyResult(containerInfo);
    } else {
      healthResult = new ContainerHealthResult.HealthyResult(containerInfo);
    }
    doReturn(healthResult).when(replicationManager)
        .getContainerReplicationHealth(containerInfo, replicaSet);
    when(containerManager.getContainerReplicas(
        ContainerID.valueOf(containerID)))
        .thenReturn(replicaSet);
  }
}

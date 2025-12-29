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

package org.apache.hadoop.hdds.scm.block;

import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.EMPTY_SUMMARY;
import static org.apache.hadoop.ozone.common.BlockGroup.SIZE_NOT_AVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.TxBlockInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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
  private final Map<ContainerID, ContainerInfo> containers = new HashMap<>();
  private final Map<ContainerID, Set<ContainerReplica>> replicas = new HashMap<>();
  private ScmBlockDeletingServiceMetrics metrics;
  private static final int THREE = ReplicationFactor.THREE_VALUE;
  private static final int ONE = ReplicationFactor.ONE_VALUE;

  private ReplicationManager replicationManager;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
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
    BlockManager blockManager = mock(BlockManager.class);
    when(blockManager.getDeletedBlockLog()).thenReturn(deletedBlockLog);
    metrics = ScmBlockDeletingServiceMetrics.create(blockManager);
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
          return replicas.get(cid);
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
        ContainerInfo info = containers.get(e.getKey());
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
    final ContainerID containerID = container.containerID();
    containers.put(containerID, container);
    containerTable.put(containerID, container);
    replicas.put(containerID, replicaSet);
  }

  @AfterEach
  public void tearDown() throws Exception {
    ScmBlockDeletingServiceMetrics.unRegister();
    deletedBlockLog.close();
    scm.stop();
    scm.join();
  }

  private Map<Long, List<DeletedBlock>> generateData(int dataSize) throws IOException {
    return generateData(dataSize, HddsProtos.LifeCycleState.CLOSED);
  }

  private Map<Long, List<DeletedBlock>> generateData(int txCount,
      HddsProtos.LifeCycleState state) throws IOException {
    Map<Long, List<DeletedBlock>> blockMap = new HashMap<>();
    long continerIDBase = RandomUtils.secure().randomLong(0, 100);
    int localIDBase = RandomUtils.secure().randomInt(0, 1000);
    long blockSize = 1024 * 1024 * 64;
    for (int i = 0; i < txCount; i++) {
      List<DeletedBlock> blocks = new ArrayList<>();
      long containerID = continerIDBase + i;
      updateContainerMetadata(containerID, state);
      for (int j = 0; j < BLOCKS_PER_TXN; j++)  {
        long localID = localIDBase + j;
        blocks.add(new DeletedBlock(new BlockID(containerID, localID), blockSize + j, blockSize + j));
      }
      blockMap.put(containerID, blocks);
    }
    return blockMap;
  }

  private void addTransactions(Map<Long, List<DeletedBlock>> containerBlocksMap,
      boolean shouldFlush) throws IOException {
    deletedBlockLog.addTransactions(containerBlocksMap);
    if (shouldFlush) {
      scmHADBTransactionBuffer.flush();
    }
  }

  private void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults,
      DatanodeDetails... dns) throws IOException {
    for (DatanodeDetails dnDetails : dns) {
      deletedBlockLog.getSCMDeletedBlockTransactionStatusManager()
          .commitTransactions(transactionResults, dnDetails.getID());
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
      int maximumAllowedBlocksNum) throws IOException {
    DatanodeDeletedBlockTransactions transactions =
        deletedBlockLog.getTransactions(maximumAllowedBlocksNum, new HashSet<>(dnList));
    List<DeletedBlocksTransaction> txns = new LinkedList<>();
    for (DatanodeDetails dn : dnList) {
      txns.addAll(Optional.ofNullable(
          transactions.getDatanodeTransactionMap().get(dn.getID()))
          .orElseGet(LinkedList::new));
    }
    // Simulated transactions are sent
    for (Map.Entry<DatanodeID, List<DeletedBlocksTransaction>> entry :
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
  public void testAddTransactionsIsBatched() throws Exception {
    conf.setStorageSize(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT, 1, StorageUnit.KB);

    SCMDeletedBlockTransactionStatusManager mockStatusManager = mock(SCMDeletedBlockTransactionStatusManager.class);
    DeletedBlockLogImpl log = new DeletedBlockLogImpl(conf, scm, containerManager, scmHADBTransactionBuffer, metrics);

    log.setSCMDeletedBlockTransactionStatusManager(mockStatusManager);

    Map<Long, List<DeletedBlock>> containerBlocksMap = generateData(100);
    log.addTransactions(containerBlocksMap);

    verify(mockStatusManager, atLeast(2)).addTransactions(any());
  }

  @Test
  public void testSCMDelIteratorProgress() throws Exception {

    // Create 8 TXs in the log.
    int noOfTransactions = 8;
    addTransactions(generateData(noOfTransactions), true);
    mockContainerHealthResult(true);
    List<DeletedBlocksTransaction> blocks;

    int i = 1;
    while (i < noOfTransactions) {
      // In each iteration read two transactions, API returns all the transactions in order.
      // 1st iteration: {1, 2}
      // 2nd iteration: {3, 4}
      // 3rd iteration: {5, 6}
      // 4th iteration: {7, 8}
      blocks = getTransactions(2 * BLOCKS_PER_TXN * THREE);
      assertEquals(blocks.get(0).getTxID(), i++);
      assertEquals(blocks.get(1).getTxID(), i++);
    }

    // Since all the transactions are in-flight, the getTransaction should return empty list.
    blocks = getTransactions(2 * BLOCKS_PER_TXN * THREE);
    assertTrue(blocks.isEmpty());
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
      DatanodeID dnId, DeleteBlocksCommand command) {
    Set<Long> dnTxSet = command.blocksTobeDeleted()
        .stream().map(DeletedBlocksTransaction::getTxID)
        .collect(Collectors.toSet());
    deletedBlockLog.recordTransactionCreated(dnId, command.getId(), dnTxSet);
  }

  private void sendSCMDeleteBlocksCommand(DatanodeID dnId, SCMCommand<?> scmCommand) {
    deletedBlockLog.onSent(DatanodeDetails.newBuilder().setID(dnId).build(), scmCommand);
  }

  private void assertNoDuplicateTransactions(
      DatanodeDeletedBlockTransactions transactions1,
      DatanodeDeletedBlockTransactions transactions2) {
    Map<DatanodeID, List<DeletedBlocksTransaction>> map1 =
        transactions1.getDatanodeTransactionMap();
    Map<DatanodeID, List<DeletedBlocksTransaction>> map2 =
        transactions2.getDatanodeTransactionMap();

    for (Map.Entry<DatanodeID, List<DeletedBlocksTransaction>> entry :
        map1.entrySet()) {
      DatanodeID dnId = entry.getKey();
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
    Map<DatanodeID, List<DeletedBlocksTransaction>> map1 =
        transactions1.getDatanodeTransactionMap();
    Map<DatanodeID, List<DeletedBlocksTransaction>> map2 =
        transactions2.getDatanodeTransactionMap();

    for (Map.Entry<DatanodeID, List<DeletedBlocksTransaction>> entry :
        map1.entrySet()) {
      DatanodeID dnId = entry.getKey();
      Set<DeletedBlocksTransaction> txSet1 = new HashSet<>(entry.getValue());
      Set<DeletedBlocksTransaction> txSet2 = new HashSet<>(map2.get(dnId));

      assertThat(txSet1).containsAll(txSet2);
    }
  }

  private void commitSCMCommandStatus(Long scmCmdId, DatanodeID dnID,
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
      BiConsumer<DatanodeID, DeleteBlocksCommand> afterCreate) {
    for (Map.Entry<DatanodeID, List<DeletedBlocksTransaction>> entry :
        transactions.getDatanodeTransactionMap().entrySet()) {
      DatanodeID dnId = entry.getKey();
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
    Map<Long, List<DeletedBlock>> deletedBlocks = generateData(50);
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
    Map<Long, List<DeletedBlock>> deletedBlocks = generateData(50);
    addTransactions(deletedBlocks, true);
    long containerID;
    // let the first 30 container only consisting of only two unhealthy replicas
    int count = 0;
    for (Map.Entry<Long, List<DeletedBlock>> entry : deletedBlocks.entrySet()) {
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
      int state = RandomUtils.secure().randomInt(0, 4);
      if (state == 0) {
        addTransactions(generateData(10), true);
        added += 10;
      } else if (state == 1) {
        blocks = getTransactions(20 * BLOCKS_PER_TXN * THREE);
        txIDs = new ArrayList<>();
        for (DeletedBlocksTransaction block : blocks) {
          txIDs.add(block.getTxID());
        }
      } else if (state == 2) {
        commitTransactions(blocks);
        committed += blocks.size() / THREE;
        blocks = new ArrayList<>();
      } else {
        // verify the number of added and committed.
        try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter =
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
  public void testDeletedBlockTransactions() throws IOException {
    deletedBlockLog.setScmCommandTimeoutMs(Long.MAX_VALUE);
    mockContainerHealthResult(true);
    int txNum = 10;
    List<DeletedBlocksTransaction> blocks;
    DatanodeDetails dnId1 = dnList.get(0), dnId2 = dnList.get(1);

    int count = 0;
    long containerID;

    // Creates {TXNum} TX in the log.
    Map<Long, List<DeletedBlock>> deletedBlocks = generateData(txNum);
    addTransactions(deletedBlocks, true);
    for (Map.Entry<Long, List<DeletedBlock>> entry :deletedBlocks.entrySet()) {
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
    Map<Long, List<DeletedBlock>> deletedBlocksMap = new HashMap<>();
    long localId = RandomUtils.secure().randomLong();
    List<DeletedBlock> blockIDList = new ArrayList<>();
    blockIDList.add(new DeletedBlock(new BlockID(containerID, localId), SIZE_NOT_AVAILABLE, SIZE_NOT_AVAILABLE));
    deletedBlocksMap.put(containerID, blockIDList);
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

  @ParameterizedTest
  @ValueSource(ints = {30, 45})
  public void testGetTransactionsWithMaxBlocksPerDatanode(int maxAllowedBlockNum) throws IOException {
    int deleteBlocksFactorPerDatanode = 1;
    deletedBlockLog.setDeleteBlocksFactorPerDatanode(deleteBlocksFactorPerDatanode);
    mockContainerHealthResult(true);
    int txNum = 10;
    DatanodeDetails dnId1 = dnList.get(0), dnId2 = dnList.get(1);

    // Creates {TXNum} TX in the log.
    Map<Long, List<DeletedBlock>> deletedBlocks = generateData(txNum);
    addTransactions(deletedBlocks, true);
    List<Long> containerIds = new ArrayList<>(deletedBlocks.keySet());
    for (int i = 0; i < containerIds.size(); i++) {
      DatanodeDetails assignedDn = (i % 2 == 0) ? dnId1 : dnId2;
      mockStandAloneContainerInfo(containerIds.get(i), assignedDn);
    }

    int blocksPerDataNode = maxAllowedBlockNum / (dnList.size() / deleteBlocksFactorPerDatanode);
    DatanodeDeletedBlockTransactions transactions =
        deletedBlockLog.getTransactions(maxAllowedBlockNum, new HashSet<>(dnList));

    Map<DatanodeID, Integer> datanodeBlockCountMap =  transactions.getDatanodeTransactionMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()
                .stream()
                .mapToInt(tx -> tx.getLocalIDList().size())
                .sum()
        ));
    // Transactions should have blocksPerDataNode for both DNs
    assertEquals(datanodeBlockCountMap.get(dnId1.getID()), blocksPerDataNode);
    assertEquals(datanodeBlockCountMap.get(dnId2.getID()), blocksPerDataNode);
  }

  @Test
  public void testDeletedBlockTransactionsOfDeletedContainer() throws IOException {
    int txNum = 10;
    List<DeletedBlocksTransaction> blocks;

    // Creates {TXNum} TX in the log.
    Map<Long, List<DeletedBlock>> deletedBlocks = generateData(txNum,
        HddsProtos.LifeCycleState.DELETED);
    addTransactions(deletedBlocks, true);

    blocks = getTransactions(txNum * BLOCKS_PER_TXN);
    // There should be no txn remaining
    assertEquals(0, blocks.size());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 25, 50, 100})
  public void testTransactionSerializedSize(int blockCount) {
    long txID = 10000000;
    long containerID = 1000000;
    List<DeletedBlock> blocks = new ArrayList<>();
    for (int i = 0; i < blockCount; i++) {
      blocks.add(new DeletedBlock(new BlockID(containerID, 100000000 + i), 128 * 1024 * 1024, 128 * 1024 * 1024));
    }
    List<Long> localIdList = blocks.stream().map(b -> b.getBlockID().getLocalID()).collect(Collectors.toList());
    DeletedBlocksTransaction tx1 = DeletedBlocksTransaction.newBuilder()
        .setTxID(txID)
        .setContainerID(containerID)
        .addAllLocalID(localIdList)
        .setCount(0)
        .setTotalBlockSize(blocks.stream().mapToLong(DeletedBlock::getSize).sum())
        .setTotalBlockReplicatedSize(blocks.stream().mapToLong(DeletedBlock::getReplicatedSize).sum())
        .build();
    DeletedBlocksTransaction tx2 = DeletedBlocksTransaction.newBuilder()
        .setTxID(txID)
        .setContainerID(containerID)
        .addAllLocalID(localIdList)
        .setCount(0)
        .build();
    /*
     *  1 blocks tx with totalBlockSize size is 26
     *  1 blocks tx without totalBlockSize size is 16
     *  10 blocks tx with totalBlockSize size is 73
     *  10 blocks tx without totalBlockSize size is 61
     *  25 blocks tx with totalBlockSize size is 148
     *  25 blocks tx without totalBlockSize size is 136
     *  50 blocks tx with totalBlockSize size is 273
     *  50 blocks tx without totalBlockSize size is 261
     *  100 blocks tx with totalBlockSize size is 523
     *  100 blocks tx without totalBlockSize size is 511
     */
    System.out.println(blockCount + " blocks tx with totalBlockSize size is " + tx1.getSerializedSize());
    System.out.println(blockCount + " blocks tx without totalBlockSize size is " + tx2.getSerializedSize());
  }

  public static Stream<Arguments> values() {
    return Stream.of(
        arguments(100, false),
        arguments(100, true),
        arguments(1000, false),
        arguments(1000, true),
        arguments(10000, false),
        arguments(10000, true),
        arguments(100000, false),
        arguments(100000, true)
    );
  }

  @ParameterizedTest
  @MethodSource("values")
  public void testAddRemoveTransactionPerformance(int txCount, boolean dataDistributionFinalized)
      throws Exception {
    Map<Long, List<DeletedBlock>> data = generateData(txCount);
    SCMDeletedBlockTransactionStatusManager statusManager =
        deletedBlockLog.getSCMDeletedBlockTransactionStatusManager();
    HddsProtos.DeletedBlocksTransactionSummary summary = statusManager.getTransactionSummary();
    assertEquals(EMPTY_SUMMARY, summary);

    SCMDeletedBlockTransactionStatusManager.setDisableDataDistributionForTest(!dataDistributionFinalized);
    long startTime = System.nanoTime();
    deletedBlockLog.addTransactions(data);
    scmHADBTransactionBuffer.flush();
    /**
     * Before DataDistribution is enabled
     *  - 979 ms to add 100 txs to DB
     *  - 275 ms to add 1000 txs to DB
     *  - 1106 ms to add 10000 txs to DB
     *  - 11103 ms to add 100000 txs to DB
     * After DataDistribution is enabled
     *  - 908 ms to add 100 txs to DB
     *  - 351 ms to add 1000 txs to DB
     *  - 2875 ms to add 10000 txs to DB
     *  - 12446 ms to add 100000 txs to DB
     */
    System.out.println((System.nanoTime() - startTime) / 100000 + " ms to add " + txCount + " txs to DB, " +
        "dataDistributionFinalized " + dataDistributionFinalized);
    summary = statusManager.getTransactionSummary();
    if (dataDistributionFinalized) {
      assertEquals(txCount, summary.getTotalTransactionCount());
    } else {
      assertEquals(0, summary.getTotalTransactionCount());
    }

    ArrayList txIdList = data.keySet().stream().collect(Collectors.toCollection(ArrayList::new));

    if (dataDistributionFinalized) {
      Map<Long, TxBlockInfo> txSizeMap = statusManager.getTxSizeMap();
      for (Map.Entry<Long, List<DeletedBlock>> entry : data.entrySet()) {
        List<DeletedBlock> deletedBlockList = entry.getValue();
        TxBlockInfo txBlockInfo = new TxBlockInfo(deletedBlockList.size(),
            deletedBlockList.stream().map(DeletedBlock::getSize).reduce(0L, Long::sum),
            deletedBlockList.stream().map(DeletedBlock::getReplicatedSize).reduce(0L, Long::sum));
        txSizeMap.put(entry.getKey(), txBlockInfo);
      }
    }
    startTime = System.nanoTime();
    statusManager.removeTransactions(txIdList);
    scmHADBTransactionBuffer.flush();
    /**
     * Before DataDistribution is enabled
     *  - 19 ms to remove 100 txs from DB
     *  - 26 ms to remove 1000 txs from DB
     *  - 142 ms to remove 10000 txs from DB
     *  - 2571 ms to remove 100000 txs from DB
     * After DataDistribution is enabled (all cache miss)
     *  - 62 ms to remove 100 txs from DB
     *  - 186 ms to remove 1000 txs from DB
     *  - 968 ms to remove 10000 txs from DB
     *  - 8635 ms to remove 100000 txs from DB
     * After DataDistribution is enabled (all cache hit)
     *  - 40 ms to remove 100 txs from DB
     *  - 112 ms to remove 1000 txs from DB
     *  - 412 ms to remove 10000 txs from DB
     *  - 3499 ms to remove 100000 txs from DB
     */
    System.out.println((System.nanoTime() - startTime) / 100000 + " ms to remove " + txCount + " txs from DB, " +
        "dataDistributionFinalized " + dataDistributionFinalized);
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

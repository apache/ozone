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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferStub;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
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
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Tests for DeletedBlockLog.
 */
public class TestDeletedBlockLog {

  private  DeletedBlockLogImpl deletedBlockLog;
  private static final int BLOCKS_PER_TXN = 5;
  private OzoneConfiguration conf;
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

  @BeforeEach
  public void setup() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestDeletedBlockLog.class.getSimpleName());
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.setInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    scm = HddsTestUtils.getScm(conf);
    containerManager = Mockito.mock(ContainerManager.class);
    containerTable = scm.getScmMetadataStore().getContainerTable();
    scmHADBTransactionBuffer =
        new SCMHADBTransactionBufferStub(scm.getScmMetadataStore().getStore());
    metrics = Mockito.mock(ScmBlockDeletingServiceMetrics.class);
    deletedBlockLog = new DeletedBlockLogImpl(conf,
        containerManager,
        scm.getScmHAManager().getRatisServer(),
        scm.getScmMetadataStore().getDeletedBlocksTXTable(),
        scmHADBTransactionBuffer,
        scm.getScmContext(),
        scm.getSequenceIdGen(),
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

    when(containerManager.getContainerReplicas(anyObject()))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return replicas.get(cid.getId());
        });
    when(containerManager.getContainer(anyObject()))
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
          Assertions.assertTrue(e.getValue() > info.getDeleteTransactionId());
        } catch (AssertionError err) {
          throw new Exception("New TxnId " + e.getValue() + " < " + info
              .getDeleteTransactionId());
        }
        info.updateDeleteTransactionId(e.getValue());
        scmHADBTransactionBuffer.addToBuffer(containerTable, e.getKey(), info);
      }
      return null;
    }).when(containerManager).updateDeleteTransactionId(anyObject());
  }

  private void updateContainerMetadata(long cid) throws IOException {
    final ContainerInfo container =
        new ContainerInfo.Builder()
            .setContainerID(cid)
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                ReplicationFactor.THREE))
            .setState(HddsProtos.LifeCycleState.CLOSED)
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
    FileUtils.deleteDirectory(testDir);
  }

  private Map<Long, List<Long>> generateData(int dataSize) throws IOException {
    Map<Long, List<Long>> blockMap = new HashMap<>();
    Random random = new Random(1);
    int continerIDBase = random.nextInt(100);
    int localIDBase = random.nextInt(1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      updateContainerMetadata(containerID);
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
      deletedBlockLog
          .commitTransactions(transactionResults, dnDetails.getUuid());
    }
    scmHADBTransactionBuffer.flush();
  }

  private void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults)
      throws IOException {
    commitTransactions(transactionResults,
        dnList.toArray(new DatanodeDetails[3]));
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

  private void commitTransactions(DatanodeDeletedBlockTransactions
      transactions) {
    transactions.getDatanodeTransactionMap().forEach((uuid,
        deletedBlocksTransactions) -> deletedBlockLog
        .commitTransactions(deletedBlocksTransactions.stream()
            .map(this::createDeleteBlockTransactionResult)
            .collect(Collectors.toList()), uuid));
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
        deletedBlockLog.getTransactions(maximumAllowedBlocksNum);
    List<DeletedBlocksTransaction> txns = new LinkedList<>();
    for (DatanodeDetails dn : dnList) {
      txns.addAll(Optional.ofNullable(
          transactions.getDatanodeTransactionMap().get(dn.getUuid()))
          .orElseGet(LinkedList::new));
    }
    return txns;
  }

  @Test
  public void testContainerManagerTransactionId() throws Exception {
    // Initially all containers should have deleteTransactionId as 0
    for (ContainerInfo containerInfo : containerManager.getContainers()) {
      Assertions.assertEquals(0, containerInfo.getDeleteTransactionId());
    }

    // Create 30 TXs
    addTransactions(generateData(30), false);
    // Since transactions are not yet flushed deleteTransactionId should be
    // 0 for all containers
    Assertions.assertEquals(0, getAllTransactions().size());
    for (ContainerInfo containerInfo : containerManager.getContainers()) {
      Assertions.assertEquals(0, containerInfo.getDeleteTransactionId());
    }

    scmHADBTransactionBuffer.flush();
    // After flush there should be 30 transactions in deleteTable
    // All containers should have positive deleteTransactionId
    Assertions.assertEquals(30 * THREE, getAllTransactions().size());
    for (ContainerInfo containerInfo : containerManager.getContainers()) {
      Assertions.assertTrue(containerInfo.getDeleteTransactionId() > 0);
    }
  }

  @Test
  public void testIncrementCount() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    // Create 30 TXs in the log.
    addTransactions(generateData(30), true);

    // This will return all TXs, total num 30.
    List<DeletedBlocksTransaction> blocks = getAllTransactions();
    List<Long> txIDs = blocks.stream().map(DeletedBlocksTransaction::getTxID)
        .collect(Collectors.toList());

    for (int i = 0; i < maxRetry; i++) {
      incrementCount(txIDs);
    }

    // Increment another time so it exceed the maxRetry.
    // On this call, count will be set to -1 which means TX eventually fails.
    incrementCount(txIDs);
    blocks = getAllTransactions();
    for (DeletedBlocksTransaction block : blocks) {
      Assertions.assertEquals(-1, block.getCount());
    }

    // If all TXs are failed, getTransactions call will always return nothing.
    blocks = getAllTransactions();
    Assertions.assertEquals(0, blocks.size());
  }

  @Test
  public void testResetCount() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    // Create 30 TXs in the log.
    addTransactions(generateData(30), true);

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
      Assertions.assertEquals(-1, block.getCount());
    }

    // If all TXs are failed, getTransactions call will always return nothing.
    blocks = getAllTransactions();
    Assertions.assertEquals(0, blocks.size());

    // Reset the retry count, these transactions should be accessible.
    resetCount(txIDs);
    blocks = getAllTransactions();
    for (DeletedBlocksTransaction block : blocks) {
      Assertions.assertEquals(0, block.getCount());
    }
    Assertions.assertEquals(30 * THREE, blocks.size());
  }

  @Test
  public void testCommitTransactions() throws Exception {
    addTransactions(generateData(50), true);
    List<DeletedBlocksTransaction> blocks =
        getTransactions(20 * BLOCKS_PER_TXN * THREE);
    // Add an invalid txn.
    blocks.add(
        DeletedBlocksTransaction.newBuilder().setContainerID(1).setTxID(70)
            .setCount(0).addLocalID(0).build());
    commitTransactions(blocks);

    blocks = getTransactions(50 * BLOCKS_PER_TXN * THREE);
    Assertions.assertEquals(30 * THREE, blocks.size());
    commitTransactions(blocks, dnList.get(1), dnList.get(2),
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());

    blocks = getTransactions(50 * BLOCKS_PER_TXN * THREE);
    // only uncommitted dn have transactions
    Assertions.assertEquals(30, blocks.size());
    commitTransactions(blocks, dnList.get(0));

    blocks = getTransactions(50 * BLOCKS_PER_TXN * THREE);
    Assertions.assertEquals(0, blocks.size());
  }

  @Test
  public void testInadequateReplicaCommit() throws Exception {
    Map<Long, List<Long>> deletedBlocks = generateData(50);
    addTransactions(deletedBlocks, true);
    long containerID;
    // let the first 30 container only consisting of only two unhealthy replicas
    int count = 30;
    for (Map.Entry<Long, List<Long>> entry :deletedBlocks.entrySet()) {
      if (count <= 0) {
        break;
      }
      containerID = entry.getKey();
      mockInadequateReplicaUnhealthyContainerInfo(containerID);
      count -= 1;
    }
    // getTransactions will get existing container replicas then add transaction
    // to DN.
    // For the first 30 txn, deletedBlockLog only has the txn from dn1 and dn2
    // For the rest txn, txn will be got from all dns.
    // Committed txn will be: 1-40. 1-40. 31-40
    commitTransactions(deletedBlockLog.getTransactions(
        30 * BLOCKS_PER_TXN * THREE));

    // The rest txn shall be: 41-50. 41-50. 41-50
    List<DeletedBlocksTransaction> blocks = getAllTransactions();
    Assertions.assertEquals(30, blocks.size());
  }

  @Test
  public void testRandomOperateTransactions() throws Exception {
    Random random = new Random();
    int added = 0, committed = 0;
    List<DeletedBlocksTransaction> blocks = new ArrayList<>();
    List<Long> txIDs;
    // Randomly add/get/commit/increase transactions.
    for (int i = 0; i < 100; i++) {
      int state = random.nextInt(4);
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
          Assertions.assertEquals(added, count.get() + committed);
        }
      }
    }
    blocks = getAllTransactions();
    commitTransactions(blocks);
  }

  @Test
  public void testPersistence() throws Exception {
    addTransactions(generateData(50), true);
    // close db and reopen it again to make sure
    // transactions are stored persistently.
    deletedBlockLog.close();
    deletedBlockLog = new DeletedBlockLogImpl(conf,
        containerManager,
        scm.getScmHAManager().getRatisServer(),
        scm.getScmMetadataStore().getDeletedBlocksTXTable(),
        scmHADBTransactionBuffer,
        scm.getScmContext(),
        scm.getSequenceIdGen(),
        metrics);
    List<DeletedBlocksTransaction> blocks =
        getTransactions(10 * BLOCKS_PER_TXN * THREE);
    Assertions.assertEquals(10 * THREE, blocks.size());
    commitTransactions(blocks);
    blocks = getTransactions(40 * BLOCKS_PER_TXN * THREE);
    Assertions.assertEquals(40 * THREE, blocks.size());
    commitTransactions(blocks);

    // close db and reopen it again to make sure
    // currentTxnID = 50
    deletedBlockLog.close();
    new DeletedBlockLogImpl(conf,
        containerManager,
        scm.getScmHAManager().getRatisServer(),
        scm.getScmMetadataStore().getDeletedBlocksTXTable(),
        scmHADBTransactionBuffer,
        scm.getScmContext(),
        scm.getSequenceIdGen(),
        metrics);
    blocks = getTransactions(40 * BLOCKS_PER_TXN * THREE);
    Assertions.assertEquals(0, blocks.size());
    //Assertions.assertEquals((long)deletedBlockLog.getCurrentTXID(), 50L);
  }

  @Test
  public void testDeletedBlockTransactions()
      throws IOException, TimeoutException {
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
    Assertions.assertEquals(1, blocks.size());

    // add two transactions for same container
    containerID = blocks.get(0).getContainerID();
    Map<Long, List<Long>> deletedBlocksMap = new HashMap<>();
    deletedBlocksMap.put(containerID, new LinkedList<>());
    addTransactions(deletedBlocksMap, true);

    // get should return two transactions for the same container
    blocks = getTransactions(txNum * BLOCKS_PER_TXN * ONE);
    Assertions.assertEquals(2, blocks.size());
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
    Mockito.doReturn(containerInfo).when(containerManager)
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

  private void mockInadequateReplicaUnhealthyContainerInfo(long containerID)
      throws IOException {
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
    Mockito.doReturn(containerInfo).when(containerManager)
        .getContainer(ContainerID.valueOf(containerID));

    final Set<ContainerReplica> replicaSet = dns.stream()
        .map(datanodeDetails -> ContainerReplica.newBuilder()
            .setContainerID(containerInfo.containerID())
            .setContainerState(ContainerReplicaProto.State.CLOSED)
            .setDatanodeDetails(datanodeDetails)
            .build())
        .collect(Collectors.toSet());
    when(containerManager.getContainerReplicas(
        ContainerID.valueOf(containerID)))
        .thenReturn(replicaSet);
  }
}

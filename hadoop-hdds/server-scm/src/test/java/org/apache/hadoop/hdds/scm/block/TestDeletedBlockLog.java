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
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.scm.container.ContainerMapping;
import org.apache.hadoop.hdds.scm.container.Mapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.mockito.Mockito.mock;

/**
 * Tests for DeletedBlockLog.
 */
public class TestDeletedBlockLog {

  private static DeletedBlockLogImpl deletedBlockLog;
  private OzoneConfiguration conf;
  private File testDir;

  @Before
  public void setup() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestDeletedBlockLog.class.getSimpleName());
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    deletedBlockLog = new DeletedBlockLogImpl(conf);
  }

  @After
  public void tearDown() throws Exception {
    deletedBlockLog.close();
    FileUtils.deleteDirectory(testDir);
  }

  private Map<Long, List<Long>> generateData(int dataSize) {
    Map<Long, List<Long>> blockMap = new HashMap<>();
    Random random = new Random(1);
    int continerIDBase = random.nextInt(100);
    int localIDBase = random.nextInt(1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      List<Long> blocks = new ArrayList<>();
      int blockSize = random.nextInt(30) + 1;
      for (int j = 0; j < blockSize; j++)  {
        long localID = localIDBase + j;
        blocks.add(localID);
      }
      blockMap.put(containerID, blocks);
    }
    return blockMap;
  }

  @Test
  public void testGetTransactions() throws Exception {
    List<DeletedBlocksTransaction> blocks =
        deletedBlockLog.getTransactions(30);
    Assert.assertEquals(0, blocks.size());

    // Creates 40 TX in the log.
    for (Map.Entry<Long, List<Long>> entry : generateData(40).entrySet()){
      deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
    }

    // Get first 30 TXs.
    blocks = deletedBlockLog.getTransactions(30);
    Assert.assertEquals(30, blocks.size());
    for (int i = 0; i < 30; i++) {
      Assert.assertEquals(i + 1, blocks.get(i).getTxID());
    }

    // Get another 30 TXs.
    // The log only 10 left, so this time it will only return 10 TXs.
    blocks = deletedBlockLog.getTransactions(30);
    Assert.assertEquals(10, blocks.size());
    for (int i = 30; i < 40; i++) {
      Assert.assertEquals(i + 1, blocks.get(i - 30).getTxID());
    }

    // Get another 50 TXs.
    // By now the position should have moved to the beginning,
    // this call will return all 40 TXs.
    blocks = deletedBlockLog.getTransactions(50);
    Assert.assertEquals(40, blocks.size());
    for (int i = 0; i < 40; i++) {
      Assert.assertEquals(i + 1, blocks.get(i).getTxID());
    }
    List<Long> txIDs = new ArrayList<>();
    for (DeletedBlocksTransaction block : blocks) {
      txIDs.add(block.getTxID());
    }
    deletedBlockLog.commitTransactions(txIDs);
  }

  @Test
  public void testIncrementCount() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    // Create 30 TXs in the log.
    for (Map.Entry<Long, List<Long>> entry : generateData(30).entrySet()){
      deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
    }

    // This will return all TXs, total num 30.
    List<DeletedBlocksTransaction> blocks =
        deletedBlockLog.getTransactions(40);
    List<Long> txIDs = blocks.stream().map(DeletedBlocksTransaction::getTxID)
        .collect(Collectors.toList());

    for (int i = 0; i < maxRetry; i++) {
      deletedBlockLog.incrementCount(txIDs);
    }

    // Increment another time so it exceed the maxRetry.
    // On this call, count will be set to -1 which means TX eventually fails.
    deletedBlockLog.incrementCount(txIDs);
    blocks = deletedBlockLog.getTransactions(40);
    for (DeletedBlocksTransaction block : blocks) {
      Assert.assertEquals(-1, block.getCount());
    }

    // If all TXs are failed, getTransactions call will always return nothing.
    blocks = deletedBlockLog.getTransactions(40);
    Assert.assertEquals(blocks.size(), 0);
  }

  @Test
  public void testCommitTransactions() throws Exception {
    for (Map.Entry<Long, List<Long>> entry : generateData(50).entrySet()){
      deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
    }
    List<DeletedBlocksTransaction> blocks =
        deletedBlockLog.getTransactions(20);
    List<Long> txIDs = new ArrayList<>();
    for (DeletedBlocksTransaction block : blocks) {
      txIDs.add(block.getTxID());
    }
    // Add an invalid txID.
    txIDs.add(70L);
    deletedBlockLog.commitTransactions(txIDs);
    blocks = deletedBlockLog.getTransactions(50);
    Assert.assertEquals(30, blocks.size());
  }

  @Test
  public void testRandomOperateTransactions() throws Exception {
    Random random = new Random();
    int added = 0, committed = 0;
    List<DeletedBlocksTransaction> blocks = new ArrayList<>();
    List<Long> txIDs = new ArrayList<>();
    byte[] latestTxid = DFSUtil.string2Bytes("#LATEST_TXID#");
    MetadataKeyFilters.MetadataKeyFilter avoidLatestTxid =
        (preKey, currentKey, nextKey) ->
            !Arrays.equals(latestTxid, currentKey);
    MetadataStore store = deletedBlockLog.getDeletedStore();
    // Randomly add/get/commit/increase transactions.
    for (int i = 0; i < 100; i++) {
      int state = random.nextInt(4);
      if (state == 0) {
        for (Map.Entry<Long, List<Long>> entry :
            generateData(10).entrySet()){
          deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
        }
        added += 10;
      } else if (state == 1) {
        blocks = deletedBlockLog.getTransactions(20);
        txIDs = new ArrayList<>();
        for (DeletedBlocksTransaction block : blocks) {
          txIDs.add(block.getTxID());
        }
        deletedBlockLog.incrementCount(txIDs);
      } else if (state == 2) {
        txIDs = new ArrayList<>();
        for (DeletedBlocksTransaction block : blocks) {
          txIDs.add(block.getTxID());
        }
        blocks = new ArrayList<>();
        committed += txIDs.size();
        deletedBlockLog.commitTransactions(txIDs);
      } else {
        // verify the number of added and committed.
        List<Map.Entry<byte[], byte[]>> result =
            store.getRangeKVs(null, added, avoidLatestTxid);
        Assert.assertEquals(added, result.size() + committed);
      }
    }
  }

  @Test
  public void testPersistence() throws Exception {
    for (Map.Entry<Long, List<Long>> entry : generateData(50).entrySet()){
      deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
    }
    // close db and reopen it again to make sure
    // transactions are stored persistently.
    deletedBlockLog.close();
    deletedBlockLog = new DeletedBlockLogImpl(conf);
    List<DeletedBlocksTransaction> blocks =
        deletedBlockLog.getTransactions(10);
    List<Long> txIDs = new ArrayList<>();
    for (DeletedBlocksTransaction block : blocks) {
      txIDs.add(block.getTxID());
    }
    deletedBlockLog.commitTransactions(txIDs);
    blocks = deletedBlockLog.getTransactions(10);
    Assert.assertEquals(10, blocks.size());
  }

  @Test
  public void testDeletedBlockTransactions() throws IOException {
    int txNum = 10;
    int maximumAllowedTXNum = 5;
    List<DeletedBlocksTransaction> blocks = null;
    List<Long> containerIDs = new LinkedList<>();

    int count = 0;
    long containerID = 0L;
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails dnId1 = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setIpAddress("127.0.0.1")
        .setHostName("localhost")
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort)
        .build();
    DatanodeDetails dnId2 = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setIpAddress("127.0.0.1")
        .setHostName("localhost")
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort)
        .build();
    Mapping mappingService = mock(ContainerMapping.class);
    // Creates {TXNum} TX in the log.
    for (Map.Entry<Long, List<Long>> entry : generateData(txNum)
        .entrySet()) {
      count++;
      containerID = entry.getKey();
      containerIDs.add(containerID);
      deletedBlockLog.addTransaction(containerID, entry.getValue());

      // make TX[1-6] for datanode1; TX[7-10] for datanode2
      if (count <= (maximumAllowedTXNum + 1)) {
        mockContainerInfo(mappingService, containerID, dnId1);
      } else {
        mockContainerInfo(mappingService, containerID, dnId2);
      }
    }

    DatanodeDeletedBlockTransactions transactions =
        new DatanodeDeletedBlockTransactions(mappingService,
            maximumAllowedTXNum, 2);
    deletedBlockLog.getTransactions(transactions);

    List<Long> txIDs = new LinkedList<>();
    for (UUID id : transactions.getDatanodeIDs()) {
      List<DeletedBlocksTransaction> txs = transactions
          .getDatanodeTransactions(id);
      for (DeletedBlocksTransaction tx : txs) {
        txIDs.add(tx.getTxID());
      }
    }

    // delete TX ID
    deletedBlockLog.commitTransactions(txIDs);
    blocks = deletedBlockLog.getTransactions(txNum);
    // There should be one block remained since dnID1 reaches
    // the maximum value (5).
    Assert.assertEquals(1, blocks.size());

    Assert.assertFalse(transactions.isFull());
    // The number of TX in dnID1 won't more than maximum value.
    Assert.assertEquals(maximumAllowedTXNum,
        transactions.getDatanodeTransactions(dnId1.getUuid()).size());

    int size = transactions.getDatanodeTransactions(dnId2.getUuid()).size();
    // add duplicated container in dnID2, this should be failed.
    DeletedBlocksTransaction.Builder builder =
        DeletedBlocksTransaction.newBuilder();
    builder.setTxID(11);
    builder.setContainerID(containerID);
    builder.setCount(0);
    transactions.addTransaction(builder.build());

    // The number of TX in dnID2 should not be changed.
    Assert.assertEquals(size,
        transactions.getDatanodeTransactions(dnId2.getUuid()).size());

    // Add new TX in dnID2, then dnID2 will reach maximum value.
    containerID = RandomUtils.nextLong();
    builder = DeletedBlocksTransaction.newBuilder();
    builder.setTxID(12);
    builder.setContainerID(containerID);
    builder.setCount(0);
    mockContainerInfo(mappingService, containerID, dnId2);
    transactions.addTransaction(builder.build());
    // Since all node are full, then transactions is full.
    Assert.assertTrue(transactions.isFull());
  }

  private void mockContainerInfo(Mapping mappingService, long containerID,
      DatanodeDetails dd) throws IOException {
    Pipeline pipeline =
        new Pipeline("fake", LifeCycleState.OPEN,
            ReplicationType.STAND_ALONE, ReplicationFactor.ONE, "fake");
    pipeline.addMember(dd);

    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setPipelineName(pipeline.getPipelineName())
        .setReplicationType(pipeline.getType())
        .setReplicationFactor(pipeline.getFactor());

    ContainerInfo containerInfo = builder.build();
    ContainerWithPipeline containerWithPipeline = new ContainerWithPipeline(
        containerInfo, pipeline);
    Mockito.doReturn(containerInfo).when(mappingService)
        .getContainer(containerID);
    Mockito.doReturn(containerWithPipeline).when(mappingService)
        .getContainerWithPipeline(containerID);
  }
}

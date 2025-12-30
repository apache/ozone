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

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.admin.scm.GetDeletedBlockSummarySubcommand;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for DeletedBlocksTxnSubcommand Cli.
 */
public class TestDeletedBlocksTxnShell {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestDeletedBlocksTxnShell.class);

  private final PrintStream originalOut = System.out;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private int numOfSCMs = 3;

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private static final int BLOCKS_PER_TX = 5;
  private static final int BLOCK_SIZE = 100;
  private static final int BLOCK_REPLICATED_SIZE = 300;

  @TempDir
  private Path tempDir;

  /**
   * Create a MiniOzoneHACluster for testing.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    String scmServiceId = "scm-service-test1";

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfActiveSCMs(numOfSCMs)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();

    File txnFile = tempDir.resolve("txn.txt").toFile();
    LOG.info("txnFile path: {}", txnFile.getAbsolutePath());
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    System.setOut(originalOut);
  }

  //<containerID,  List<blockID>>
  private Map<Long, List<DeletedBlock>> generateData(int dataSize) throws Exception {
    Map<Long, List<DeletedBlock>> blockMap = new HashMap<>();
    int continerIDBase = RandomUtils.secure().randomInt(0, 100);
    int localIDBase = RandomUtils.secure().randomInt(0, 1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      updateContainerMetadata(containerID);
      List<DeletedBlock> blocks = new ArrayList<>();
      for (int j = 0; j < BLOCKS_PER_TX; j++)  {
        long localID = localIDBase + j;
        blocks.add(new DeletedBlock(new BlockID(containerID, localID), BLOCK_SIZE, BLOCK_REPLICATED_SIZE));
      }
      blockMap.put(containerID, blocks);
    }
    return blockMap;
  }

  private void updateContainerMetadata(long cid) throws Exception {
    final ContainerInfo container =
        new ContainerInfo.Builder()
            .setContainerID(cid)
            .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
            .setState(HddsProtos.LifeCycleState.CLOSED)
            .setOwner("TestDeletedBlockLog")
            .setPipelineID(PipelineID.randomId())
            .build();
    final Set<ContainerReplica> replicaSet = cluster.getHddsDatanodes()
        .subList(0, 3)
        .stream()
        .map(dn -> ContainerReplica.newBuilder()
            .setContainerID(container.containerID())
            .setContainerState(State.CLOSED)
            .setDatanodeDetails(dn.getDatanodeDetails())
            .build())
        .collect(Collectors.toSet());
    ContainerStateManager containerStateManager = getSCMLeader().
        getContainerManager().getContainerStateManager();
    containerStateManager.addContainer(container.getProtobuf());
    for (ContainerReplica replica: replicaSet) {
      containerStateManager.updateContainerReplica(replica);
    }
  }

  private StorageContainerManager getSCMLeader() {
    return cluster.getStorageContainerManagersList()
        .stream().filter(a -> a.getScmContext().isLeaderReady())
        .collect(Collectors.toList()).get(0);
  }
  
  private void flush() throws Exception {
    // only flush leader here, avoid the follower concurrent flush and write
    getSCMLeader().getScmHAManager().asSCMHADBTransactionBuffer().flush();
  }

  @Test
  public void testGetDeletedBlockSummarySubcommand() throws Exception {
    int currentValidTxnNum;
    // add 30 block deletion transactions
    DeletedBlockLog deletedBlockLog = getSCMLeader().
        getScmBlockManager().getDeletedBlockLog();
    deletedBlockLog.addTransactions(generateData(30));
    flush();
    currentValidTxnNum = deletedBlockLog.getNumOfValidTransactions();
    LOG.info("Valid num of txns: {}", currentValidTxnNum);
    assertEquals(30, currentValidTxnNum);
    DeletedBlocksTransactionSummary summary = deletedBlockLog.getTransactionSummary();
    assertEquals(30, summary.getTotalTransactionCount());
    assertEquals(30 * BLOCKS_PER_TX, summary.getTotalBlockCount());
    assertEquals(30 * BLOCKS_PER_TX * BLOCK_SIZE, summary.getTotalBlockSize());
    assertEquals(30 * BLOCKS_PER_TX * BLOCK_REPLICATED_SIZE, summary.getTotalBlockReplicatedSize());

    GetDeletedBlockSummarySubcommand getDeletedBlockSummarySubcommand =
        new GetDeletedBlockSummarySubcommand();
    outContent.reset();
    ContainerOperationClient scmClient = new ContainerOperationClient(conf);
    getDeletedBlockSummarySubcommand.execute(scmClient);
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Total number of transactions: 30"));
    assertTrue(output.contains("Total number of blocks: 150"));
    assertTrue(output.contains("Total size of blocks: 15000"));
    assertTrue(output.contains("Total replicated size of blocks: 45000"));
  }
}

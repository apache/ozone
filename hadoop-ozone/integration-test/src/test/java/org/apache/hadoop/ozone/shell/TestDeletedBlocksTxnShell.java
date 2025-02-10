/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.shell;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.admin.scm.GetFailedDeletedBlocksTxnSubcommand;
import org.apache.hadoop.ozone.admin.scm.ResetDeletedBlockRetryCountSubcommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
  private String scmServiceId;
  private File txnFile;
  private int numOfSCMs = 3;

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

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
    scmServiceId = "scm-service-test1";

    conf.setInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfActiveSCMs(numOfSCMs)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();

    txnFile = tempDir.resolve("txn.txt").toFile();
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
  private Map<Long, List<Long>> generateData(int dataSize) throws Exception {
    Map<Long, List<Long>> blockMap = new HashMap<>();
    int continerIDBase = RandomUtils.nextInt(0, 100);
    int localIDBase = RandomUtils.nextInt(0, 1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      updateContainerMetadata(containerID);
      List<Long> blocks = new ArrayList<>();
      for (int j = 0; j < 5; j++)  {
        long localID = localIDBase + j;
        blocks.add(localID);
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
      containerStateManager.updateContainerReplica(
          ContainerID.valueOf(cid), replica);
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
  public void testDeletedBlocksTxnSubcommand() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);
    int currentValidTxnNum;
    // add 30 block deletion transactions
    DeletedBlockLog deletedBlockLog = getSCMLeader().
        getScmBlockManager().getDeletedBlockLog();
    deletedBlockLog.addTransactions(generateData(30));
    flush();
    currentValidTxnNum = deletedBlockLog.getNumOfValidTransactions();
    LOG.info("Valid num of txns: {}", currentValidTxnNum);
    assertEquals(30, currentValidTxnNum);

    // let the first 20 txns be failed
    List<Long> txIds = new ArrayList<>();
    for (int i = 1; i < 21; i++) {
      txIds.add((long) i);
    }
    // increment retry count than threshold, count will be set to -1
    for (int i = 0; i < maxRetry + 1; i++) {
      deletedBlockLog.incrementCount(txIds);
    }
    flush();
    currentValidTxnNum = deletedBlockLog.getNumOfValidTransactions();
    LOG.info("Valid num of txns: {}", currentValidTxnNum);
    assertEquals(10, currentValidTxnNum);

    ContainerOperationClient scmClient = new ContainerOperationClient(conf);
    CommandLine cmd;
    // getFailedDeletedBlocksTxn cmd will print all the failed txns
    GetFailedDeletedBlocksTxnSubcommand getCommand =
        new GetFailedDeletedBlocksTxnSubcommand();
    cmd = new CommandLine(getCommand);
    cmd.parseArgs("-a");
    getCommand.execute(scmClient);
    int matchCount = 0;
    Pattern p = Pattern.compile("\"txID\" : \\d+", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    while (m.find()) {
      matchCount += 1;
    }
    assertEquals(20, matchCount);

    // print the first 10 failed txns info into file
    cmd.parseArgs("-o", txnFile.getAbsolutePath(), "-c", "10");
    getCommand.execute(scmClient);
    assertThat(txnFile).exists();

    ResetDeletedBlockRetryCountSubcommand resetCommand =
        new ResetDeletedBlockRetryCountSubcommand();
    cmd = new CommandLine(resetCommand);

    // reset the txns in file
    cmd.parseArgs("-i", txnFile.getAbsolutePath());
    resetCommand.execute(scmClient);
    flush();
    currentValidTxnNum = deletedBlockLog.getNumOfValidTransactions();
    LOG.info("Valid num of txns: {}", currentValidTxnNum);
    assertEquals(20, currentValidTxnNum);

    // reset the given txIds list
    cmd.parseArgs("-l", "11,12,13,14,15");
    resetCommand.execute(scmClient);
    flush();
    currentValidTxnNum = deletedBlockLog.getNumOfValidTransactions();
    LOG.info("Valid num of txns: {}", currentValidTxnNum);
    assertEquals(25, currentValidTxnNum);

    // reset the non-existing txns and valid txns, should do nothing
    cmd.parseArgs("-l", "1,2,3,4,5,100,101,102,103,104,105");
    resetCommand.execute(scmClient);
    flush();
    currentValidTxnNum = deletedBlockLog.getNumOfValidTransactions();
    LOG.info("Valid num of txns: {}", currentValidTxnNum);
    assertEquals(25, currentValidTxnNum);

    // reset all the result expired txIds, all transactions should be available
    cmd.parseArgs("-a");
    resetCommand.execute(scmClient);
    flush();
    currentValidTxnNum = deletedBlockLog.getNumOfValidTransactions();
    LOG.info("Valid num of txns: {}", currentValidTxnNum);
    assertEquals(30, currentValidTxnNum);

    // Fail first 20 txns be failed
    // increment retry count than threshold, count will be set to -1
    for (int i = 0; i < maxRetry + 1; i++) {
      deletedBlockLog.incrementCount(txIds);
    }
    flush();

    GetFailedDeletedBlocksTxnSubcommand getFailedBlockCommand =
        new GetFailedDeletedBlocksTxnSubcommand();
    outContent.reset();
    cmd = new CommandLine(getFailedBlockCommand);
    // set start transaction as 15
    cmd.parseArgs("-c", "5", "-s", "15");
    getFailedBlockCommand.execute(scmClient);
    matchCount = 0;
    p = Pattern.compile("\"txID\" : \\d+", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    while (m.find()) {
      matchCount += 1;
    }
    assertEquals(5, matchCount);
  }
}

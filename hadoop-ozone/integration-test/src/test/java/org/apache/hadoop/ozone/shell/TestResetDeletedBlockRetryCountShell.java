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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.admin.scm.ResetDeletedBlockRetryCountSubcommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;

/**
 * Test for ResetDeletedBlockRetryCountSubcommand Cli.
 */
public class TestResetDeletedBlockRetryCountShell {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestResetDeletedBlockRetryCountShell.class);
  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String scmServiceId;
  private int numOfSCMs = 3;

  /**
   * Create a MiniOzoneHACluster for testing.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    scmServiceId = "scm-service-test1";

    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.setInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfActiveSCMs(numOfSCMs)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  //<containerID,  List<blockID>>
  private Map<Long, List<Long>> generateData(int dataSize) {
    Map<Long, List<Long>> blockMap = new HashMap<>();
    Random random = new Random(1);
    int continerIDBase = random.nextInt(100);
    int localIDBase = random.nextInt(1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      List<Long> blocks = new ArrayList<>();
      for (int j = 0; j < 5; j++)  {
        long localID = localIDBase + j;
        blocks.add(localID);
      }
      blockMap.put(containerID, blocks);
    }
    return blockMap;
  }

  private StorageContainerManager getSCMLeader() {
    return cluster.getStorageContainerManagersList()
        .stream().filter(a -> a.getScmContext().isLeaderReady())
        .collect(Collectors.toList()).get(0);
  }

  @Test
  public void testResetCmd() throws IOException, TimeoutException {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);
    // add some block deletion transactions
    DeletedBlockLog deletedBlockLog = getSCMLeader().
        getScmBlockManager().getDeletedBlockLog();
    deletedBlockLog.addTransactions(generateData(30));
    getSCMLeader().getScmHAManager().asSCMHADBTransactionBuffer().flush();
    LOG.info("Valid num of txns: {}", deletedBlockLog.
        getNumOfValidTransactions());
    Assertions.assertEquals(30, deletedBlockLog.getNumOfValidTransactions());

    List<Long> txIds = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      txIds.add((long) i);
    }
    // increment retry count than threshold, count will be set to -1
    for (int i = 0; i < maxRetry + 1; i++) {
      deletedBlockLog.incrementCount(txIds);
    }
    for (StorageContainerManager scm:
        cluster.getStorageContainerManagersList()) {
      scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
    }
    LOG.info("Valid num of txns: {}", deletedBlockLog.
        getNumOfValidTransactions());
    Assertions.assertEquals(0, deletedBlockLog.getNumOfValidTransactions());

    ResetDeletedBlockRetryCountSubcommand subcommand =
        new ResetDeletedBlockRetryCountSubcommand();
    ContainerOperationClient scmClient = new ContainerOperationClient(conf);
    CommandLine c = new CommandLine(subcommand);
    // reset the given txIds list, only these transactions should be available
    c.parseArgs("-l", "1,2,3,4,5");
    subcommand.execute(scmClient);
    getSCMLeader().getScmHAManager().asSCMHADBTransactionBuffer().flush();
    LOG.info("Valid num of txns: {}", deletedBlockLog.
        getNumOfValidTransactions());
    Assertions.assertEquals(5, deletedBlockLog.getNumOfValidTransactions());

    // reset all the result expired txIds, all transactions should be available
    c.parseArgs("-a");
    subcommand.execute(scmClient);
    getSCMLeader().getScmHAManager().asSCMHADBTransactionBuffer().flush();
    LOG.info("Valid num of txns: {}", deletedBlockLog.
        getNumOfValidTransactions());
    Assertions.assertEquals(30, deletedBlockLog.getNumOfValidTransactions());
  }
}

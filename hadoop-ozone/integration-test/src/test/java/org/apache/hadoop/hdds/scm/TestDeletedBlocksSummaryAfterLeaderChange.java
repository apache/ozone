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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration regression test for the bug where the pending deletion block-size
 * summary in SCM goes negative after a leader change.
 *
 * <p>Root cause: a leader version that had STORAGE_SPACE_DISTRIBUTION finalized
 * (so it set totalBlockSize / totalBlockReplicatedSize on each
 * DeletedBlocksTransaction proto) but predated the summary accounting code
 * (so it never wrote a summary to statefulConfigTable). When a new leader with
 * the accounting code took over it found null summary, left counters at 0, then
 * decremented them as datanodes committed transactions — driving the totals
 * deeply negative.  Those negative values were Raft-replicated and reloaded on
 * every subsequent restart.
 *
 * <p>This test simulates that exact scenario using
 * {@code setDisableDataDistributionForTest(true)} to suppress summary writes
 * while allowing size fields to be set on each transaction proto.
 *
 * <p><strong>To verify the test guards the fix:</strong> revert either change in
 * {@code SCMDeletedBlockTransactionStatusManager} ({@code isSummaryValid} or
 * {@code descDeletedBlocksSummary}) and the assertions will fail.
 */
public class TestDeletedBlocksSummaryAfterLeaderChange {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDeletedBlocksSummaryAfterLeaderChange.class);

  private static final int NUM_SCMS = 3;
  private static final int NUM_DATANODES = 3;
  private static final int TX_COUNT = 20;
  private static final long BLOCK_SIZE = 1024 * 1024L; // 1 MB per block
  private static final int BLOCKS_PER_TX = 5;

  private MiniOzoneHAClusterImpl cluster;

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 50, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL, 500, TimeUnit.MILLISECONDS);

    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);
    conf.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");

    // Build the cluster — call setters on the typed builder variable first,
    // then call build() separately so the return type stays MiniOzoneHAClusterImpl.
    MiniOzoneHAClusterImpl.Builder clusterBuilder = MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder
        .setSCMServiceId("scm-service-test")
        .setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS)
        .setNumOfOzoneManagers(1)
        .setNumDatanodes(NUM_DATANODES);
    cluster = clusterBuilder.build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void shutdown() {
    SCMDeletedBlockTransactionStatusManager.setDisableDataDistributionForTest(false);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Phase 1: simulate "old leader" — accounting disabled so TXs are written
   * with size fields but no summary entry reaches statefulConfigTable.
   * Phase 2: trigger leader transfer; the new leader must fall back to a DB
   * scan and report a positive summary immediately.
   * Phase 3: force-commit all transactions; the summary counters must stay >= 0.
   */
  @Test
  public void testSummaryNonNegativeAfterLeaderChangeWithNoPersistedSummary()
      throws Exception {

    SCMDeletedBlockTransactionStatusManager.setDisableDataDistributionForTest(true);

    StorageContainerManager oldLeader = getLeader();
    assertNotNull(oldLeader, "Cluster must have an SCM leader at test start");

    Map<Long, List<DeletedBlock>> txData = generateData(TX_COUNT);
    DeletedBlockLogImpl oldLog =
        (DeletedBlockLogImpl) oldLeader.getScmBlockManager().getDeletedBlockLog();
    oldLog.addTransactions(txData);
    flushBuffer(oldLeader);

    HddsProtos.DeletedBlocksTransactionSummary oldSummary =
        oldLog.getSCMDeletedBlockTransactionStatusManager().getTransactionSummary();
    assertEquals(0L, oldSummary.getTotalTransactionCount(),
        "Old leader must not have written a summary");
    assertEquals(0L, oldSummary.getTotalBlockSize(),
        "Old leader block-size summary must be zero");
    assertEquals(TX_COUNT, oldLog.getNumOfValidTransactions(),
        "All transactions must be in the deletedBlocks CF");

    SCMDeletedBlockTransactionStatusManager.setDisableDataDistributionForTest(false);

    String newLeaderScmId = pickDifferentScmId(oldLeader);
    cluster.getStorageContainerLocationClient().transferLeadership(newLeaderScmId);

    StorageContainerManager newLeader = waitForNewLeader(oldLeader);
    assertNotNull(newLeader, "A new leader must be elected after transfer");
    assertNotEquals(oldLeader.getScmId(), newLeader.getScmId(),
        "The new leader must be a different SCM node");

    DeletedBlockLogImpl newLog =
        (DeletedBlockLogImpl) newLeader.getScmBlockManager().getDeletedBlockLog();
    SCMDeletedBlockTransactionStatusManager newStatusManager =
        newLog.getSCMDeletedBlockTransactionStatusManager();

    HddsProtos.DeletedBlocksTransactionSummary summaryAfterTransfer =
        newStatusManager.getTransactionSummary();

    assertThat(summaryAfterTransfer.getTotalBlockSize())
        .as("Block size must be > 0 after leader change (DB scan must have found the TXs)")
        .isGreaterThan(0L);
    assertThat(summaryAfterTransfer.getTotalBlockReplicatedSize())
        .as("Replicated block size must be > 0 after leader change")
        .isGreaterThan(0L);
    assertEquals(TX_COUNT, summaryAfterTransfer.getTotalTransactionCount(),
        "Transaction count must match the number of TXs added");

    long expectedBlockSize = (long) TX_COUNT * BLOCKS_PER_TX * BLOCK_SIZE;
    long expectedReplicatedSize = expectedBlockSize * 3;
    assertEquals(expectedBlockSize, summaryAfterTransfer.getTotalBlockSize(),
        "Block size must exactly match the DB-scanned total");
    assertEquals(expectedReplicatedSize, summaryAfterTransfer.getTotalBlockReplicatedSize(),
        "Replicated block size must exactly match the DB-scanned total");

    // Exercise descDeletedBlocksSummary's clamping by directly committing all
    // pending transactions via removeTransactions().
    Map<Long, SCMDeletedBlockTransactionStatusManager.TxBlockInfo> txSizeMap =
        newStatusManager.getTxSizeMap();
    ArrayList<Long> allTxIds = new ArrayList<>();

    try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter =
             newLog.getDeletedBlockLogStateManager().getReadOnlyIterator()) {
      while (iter.hasNext()) {
        Table.KeyValue<Long, DeletedBlocksTransaction> kv = iter.next();
        DeletedBlocksTransaction tx = kv.getValue();
        allTxIds.add(tx.getTxID());
        if (tx.hasTotalBlockSize() && tx.hasTotalBlockReplicatedSize()) {
          txSizeMap.put(tx.getTxID(),
              new SCMDeletedBlockTransactionStatusManager.TxBlockInfo(
                  tx.getLocalIDCount(),
                  tx.getTotalBlockSize(),
                  tx.getTotalBlockReplicatedSize()));
        }
      }
    }
    assertEquals(TX_COUNT, allTxIds.size(),
        "All transactions must be present in the deletedBlocks CF before removal");

    newStatusManager.removeTransactions(allTxIds);

    HddsProtos.DeletedBlocksTransactionSummary summaryAfterDeletion =
        newStatusManager.getTransactionSummary();

    assertThat(summaryAfterDeletion.getTotalBlockSize())
        .as("Block size must be >= 0 after all transactions processed")
        .isGreaterThanOrEqualTo(0L);
    assertThat(summaryAfterDeletion.getTotalBlockReplicatedSize())
        .as("Replicated block size must be >= 0 after all transactions processed")
        .isGreaterThanOrEqualTo(0L);

    LOG.info("Final summary: txCount={}, blockSize={}, replicatedSize={}",
        summaryAfterDeletion.getTotalTransactionCount(),
        summaryAfterDeletion.getTotalBlockSize(),
        summaryAfterDeletion.getTotalBlockReplicatedSize());
  }

  /**
   * When the persisted summary is valid (> 0), the new leader must take the
   * fast O(1) path (load from statefulConfigTable) and restore the exact
   * counters that the old leader had written.
   */
  @Test
  public void testSummaryFastPathOnNormalLeaderTransfer() throws Exception {
    StorageContainerManager oldLeader = getLeader();
    assertNotNull(oldLeader);

    Map<Long, List<DeletedBlock>> txData = generateData(TX_COUNT);
    DeletedBlockLogImpl oldLog =
        (DeletedBlockLogImpl) oldLeader.getScmBlockManager().getDeletedBlockLog();
    oldLog.addTransactions(txData);
    flushBuffer(oldLeader);

    HddsProtos.DeletedBlocksTransactionSummary before =
        oldLog.getSCMDeletedBlockTransactionStatusManager().getTransactionSummary();
    assertThat(before.getTotalBlockSize()).isGreaterThan(0L);
    assertEquals(TX_COUNT, before.getTotalTransactionCount());

    String newLeaderScmId = pickDifferentScmId(oldLeader);
    cluster.getStorageContainerLocationClient().transferLeadership(newLeaderScmId);

    StorageContainerManager newLeader = waitForNewLeader(oldLeader);
    assertNotNull(newLeader);

    HddsProtos.DeletedBlocksTransactionSummary after =
        ((DeletedBlockLogImpl) newLeader.getScmBlockManager().getDeletedBlockLog())
            .getSCMDeletedBlockTransactionStatusManager().getTransactionSummary();

    assertEquals(before.getTotalTransactionCount(), after.getTotalTransactionCount(),
        "Transaction count must be identical after normal leader transfer");
    assertEquals(before.getTotalBlockSize(), after.getTotalBlockSize(),
        "Block size must be identical after normal leader transfer (fast path)");
    assertEquals(before.getTotalBlockReplicatedSize(), after.getTotalBlockReplicatedSize(),
        "Replicated block size must be identical after normal leader transfer");
  }

  private StorageContainerManager getLeader() {
    return cluster.getStorageContainerManagersList().stream()
        .filter(scm -> scm.getScmContext().isLeaderReady())
        .findFirst()
        .orElse(null);
  }

  private StorageContainerManager waitForNewLeader(StorageContainerManager oldLeader)
      throws Exception {
    final StorageContainerManager[] holder = new StorageContainerManager[1];
    GenericTestUtils.waitFor((BooleanSupplier) () -> {
      StorageContainerManager candidate = getLeader();
      if (candidate != null && !candidate.getScmId().equals(oldLeader.getScmId())) {
        holder[0] = candidate;
        return true;
      }
      return false;
    }, 200, 30_000);
    return holder[0];
  }

  private String pickDifferentScmId(StorageContainerManager current) {
    for (StorageContainerManager scm : cluster.getStorageContainerManagersList()) {
      if (!scm.getScmId().equals(current.getScmId())) {
        return scm.getScmId();
      }
    }
    throw new IllegalStateException("No other SCM found in cluster");
  }

  private void flushBuffer(StorageContainerManager scm) throws IOException {
    DBTransactionBuffer buf = scm.getScmHAManager().getDBTransactionBuffer();
    if (buf instanceof SCMHADBTransactionBuffer) {
      ((SCMHADBTransactionBuffer) buf).flush();
    }
  }

  /**
   * Registers a synthetic container in the current leader's ContainerStateManager
   * with CLOSED state and one replica on each datanode so the block-deleting
   * service dispatches deletion commands rather than immediately purging the TX
   * via ContainerNotFoundException.
   */
  private void registerContainer(long containerID) throws Exception {
    StorageContainerManager leader = getLeader();
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setOwner("TestDeletedBlocksSummary")
        .setPipelineID(PipelineID.randomId())
        .build();
    Set<ContainerReplica> replicas = cluster.getHddsDatanodes()
        .subList(0, NUM_DATANODES)
        .stream()
        .map(dn -> ContainerReplica.newBuilder()
            .setContainerID(container.containerID())
            .setContainerState(State.CLOSED)
            .setDatanodeDetails(dn.getDatanodeDetails())
            .build())
        .collect(Collectors.toSet());
    ContainerStateManager csm = leader.getContainerManager().getContainerStateManager();
    csm.addContainer(container.getProtobuf());
    for (ContainerReplica replica : replicas) {
      csm.updateContainerReplica(replica);
    }
  }

  /**
   * Generates synthetic deletion-transaction data with explicit block sizes and
   * registers each container in the leader's ContainerStateManager.
   */
  private Map<Long, List<DeletedBlock>> generateData(int txCount) throws Exception {
    Map<Long, List<DeletedBlock>> data = new HashMap<>();
    int containerBase = RandomUtils.secure().randomInt(10_000, 50_000);
    int localBase = RandomUtils.secure().randomInt(0, 1_000);
    for (int i = 0; i < txCount; i++) {
      long containerID = containerBase + i;
      registerContainer(containerID);
      List<DeletedBlock> blocks = new ArrayList<>(BLOCKS_PER_TX);
      for (int j = 0; j < BLOCKS_PER_TX; j++) {
        blocks.add(new DeletedBlock(
            new BlockID(containerID, localBase + j),
            BLOCK_SIZE,
            BLOCK_SIZE * 3,
            BLOCK_SIZE));
      }
      data.put(containerID, blocks);
    }
    return data;
  }
}

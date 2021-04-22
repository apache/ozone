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
package org.apache.hadoop.ozone.scm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHAConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMStateMachine;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;

import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Tests the Ratis snapshot feature in SCM.
 */
@Timeout(500)
public class TestSCMInstallSnapshotWithHA {

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private String scmServiceId;
  private int numOfOMs = 1;
  private int numOfSCMs = 3;

  private static final long SNAPSHOT_THRESHOLD = 5;
 // private static final int LOG_PURGE_GAP = 5;

  /**
   * Create a MiniOzoneCluster for testing.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";
    SCMHAConfiguration scmhaConfiguration =
        conf.getObject(SCMHAConfiguration.class);
  //  scmhaConfiguration.setRaftLogPurgeEnabled(true);
  //  scmhaConfiguration.setRaftLogPurgeGap(LOG_PURGE_GAP);
    scmhaConfiguration.setRatisSnapshotThreshold(SNAPSHOT_THRESHOLD);
    conf.setFromObject(scmhaConfiguration);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfActiveSCMs(2)
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

  /**
   * This test is disabled for now as there seems to be an issue with
   * Ratis install Snapshot code. In ratis while a new node gets added,
   * unless and until the node gets added to the voter list, the follower state
   * is not updated with leader info. So, while an install snapshot notification
   * is received in the leader, the leader info is not set and hence, out of
   * ratis transfer using the same leader info doesn't work.
   *
   * TODO: Fix this
   * */
  @Test
  @Disabled
  public void testInstallSnapshot() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = getLeader(cluster);
    String leaderNodeId = leaderSCM.getScmNodeDetails().getNodeId();
    Assert.assertNotNull(leaderSCM);
    // Find the inactive SCM
    String followerId = getInactiveSCM(cluster).getScmId();

    StorageContainerManager follower = cluster.getSCM(followerId);
    // Do some transactions so that the log index increases
    List<ContainerInfo> containers = writeToIncreaseLogIndex(leaderSCM, 200);

    // Get the latest db checkpoint from the leader SCM.
    TransactionInfo transactionInfo =
        leaderSCM.getScmHAManager().asSCMHADBTransactionBuffer()
            .getLatestTrxInfo();
    TermIndex leaderTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderSnaphsotIndex = leaderTermIndex.getIndex();
    long leaderSnapshotTermIndex = leaderTermIndex.getTerm();

    DBCheckpoint leaderDbCheckpoint =
        leaderSCM.getScmMetadataStore().getStore().getCheckpoint(false);

    // Start the inactive
    cluster.startInactiveSCM(followerId);

    // The recently started  should be lagging behind the leader .
    long followerLastAppliedIndex =
        follower.getScmHAManager().getRatisServer().getSCMStateMachine()
            .getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerLastAppliedIndex < leaderSnaphsotIndex);

    SCMHAManagerImpl scmhaManager =
        (SCMHAManagerImpl) (follower.getScmHAManager());
    // Install leader 's db checkpoint on the lagging .
    scmhaManager.installCheckpoint(leaderNodeId, leaderDbCheckpoint);

    SCMStateMachine followerStateMachine =
        follower.getScmHAManager().getRatisServer().getSCMStateMachine();
    // After the new checkpoint is installed, the follower
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerLastAppliedIndex = followerStateMachine
            .getLastAppliedTermIndex().getIndex();
    assertTrue(followerLastAppliedIndex >= leaderSnaphsotIndex);
    assertTrue(followerStateMachine
        .getLastAppliedTermIndex().getTerm() >= leaderSnapshotTermIndex);

    // Verify that the follower 's DB contains the transactions which were
    // made while it was inactive.
    SCMMetadataStore followerMetaStore = follower.getScmMetadataStore();
    for (ContainerInfo containerInfo : containers) {
      Assert.assertNotNull(followerMetaStore.getContainerTable()
          .get(containerInfo.containerID()));
    }
  }

  @Test
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = getLeader(cluster);
    String leaderNodeId = leaderSCM.getScmNodeDetails().getNodeId();
    String followerId = getInactiveSCM(cluster).getScmId();
    // Find the inactive SCM

    StorageContainerManager follower = cluster.getSCM(followerId);
    cluster.startInactiveSCM(followerId);
    follower.exitSafeMode();
    DBCheckpoint leaderDbCheckpoint = leaderSCM.getScmMetadataStore().getStore()
        .getCheckpoint(false);

    SCMStateMachine leaderSM =
        leaderSCM.getScmHAManager().getRatisServer().getSCMStateMachine();
    TermIndex lastTermIndex = leaderSM.getLastAppliedTermIndex();

    SCMStateMachine followerSM =
        follower.getScmHAManager().getRatisServer().getSCMStateMachine();
    follower.getScmMetadataStore().getTransactionInfoTable().
        put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.builder()
        .setCurrentTerm(lastTermIndex.getTerm())
            .setTransactionIndex(lastTermIndex.getIndex() + 100).build());
    // Advance the follower
    followerSM.notifyTermIndexUpdated(lastTermIndex.getTerm(),
        lastTermIndex.getIndex() + 100);

    GenericTestUtils.setLogLevel(SCMHAManagerImpl.getLogger(), Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(SCMHAManagerImpl.getLogger());

    // Install the old checkpoint on the follower . This should fail as the
    // follower is already ahead of that transactionLogIndex and the
    // state should be reloaded.
    TermIndex followerTermIndex = followerSM.getLastAppliedTermIndex();
    SCMHAManagerImpl scmhaManager =
        (SCMHAManagerImpl) (follower.getScmHAManager());
    TermIndex newTermIndex =
        scmhaManager.installCheckpoint(leaderNodeId, leaderDbCheckpoint);

    String errorMsg = "Reloading old state of SCM";
    Assert.assertTrue(logCapture.getOutput().contains(errorMsg));
    Assert.assertNull(" installed checkpoint even though checkpoint " +
        "logIndex is less than it's lastAppliedIndex", newTermIndex);
    Assert.assertEquals(followerTermIndex,
        followerSM.getLastAppliedTermIndex());
    Assert.assertFalse(followerSM.getLifeCycleState().isPausingOrPaused());
  }

  @Test
  public void testInstallCorruptedCheckpointFailure() throws Exception {
    StorageContainerManager leaderSCM = getLeader(cluster);
    String leaderNodeId = leaderSCM.getScmId();
    // Find the inactive SCM
    String followerId = getInactiveSCM(cluster).getScmId();
    StorageContainerManager follower = cluster.getSCM(followerId);
    // Do some transactions so that the log index increases
    writeToIncreaseLogIndex(leaderSCM, 100);
    File oldDBLocation =
        follower.getScmMetadataStore().getStore().getDbLocation();

    SCMStateMachine sm =
        follower.getScmHAManager().getRatisServer().getSCMStateMachine();
    TermIndex termIndex = sm.getLastAppliedTermIndex();
    DBCheckpoint leaderDbCheckpoint = leaderSCM.getScmMetadataStore().getStore()
        .getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    TransactionInfo leaderCheckpointTrxnInfo = HAUtils
        .getTrxnInfoFromCheckpoint(conf, leaderCheckpointLocation,
            new SCMDBDefinition());

    Assert.assertNotNull(leaderCheckpointLocation);
    // Take a backup of the current DB
    String dbBackupName =
        "SCM_CHECKPOINT_BACKUP" + termIndex.getIndex() + "_" + System
            .currentTimeMillis();
    File dbDir = oldDBLocation.getParentFile();
    File checkpointBackup = new File(dbDir, dbBackupName);

    // Take a backup of the leader checkpoint
    Files.copy(leaderCheckpointLocation.toAbsolutePath(),
        checkpointBackup.toPath());
    // Corrupt the leader checkpoint and install that on the follower. The
    // operation should fail and  should shutdown.
    boolean delete = true;
    for (File file : leaderCheckpointLocation.toFile()
        .listFiles()) {
      if (file.getName().contains(".sst")) {
        if (delete) {
          file.delete();
          delete = false;
        } else {
          delete = true;
        }
      }
    }

    SCMHAManagerImpl scmhaManager =
        (SCMHAManagerImpl) (follower.getScmHAManager());
    GenericTestUtils.setLogLevel(SCMHAManagerImpl.getLogger(), Level.ERROR);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(SCMHAManagerImpl.getLogger());
    scmhaManager.setExitManagerForTesting(new DummyExitManager());

    scmhaManager.installCheckpoint(leaderNodeId, leaderCheckpointLocation,
        leaderCheckpointTrxnInfo);

    Assert.assertTrue(logCapture.getOutput()
        .contains("Failed to reload SCM state and instantiate services."));
    Assert.assertTrue(sm.getLifeCycleState().isPausingOrPaused());

    // Verify correct reloading
    HAUtils
        .replaceDBWithCheckpoint(leaderCheckpointTrxnInfo.getTransactionIndex(),
            oldDBLocation, checkpointBackup.toPath(),
            OzoneConsts.SCM_DB_BACKUP_PREFIX);
    scmhaManager.startServices();
    sm.unpause(leaderCheckpointTrxnInfo.getTerm(),
        leaderCheckpointTrxnInfo.getTransactionIndex());
    Assert.assertTrue(sm.getLastAppliedTermIndex()
        .equals(leaderCheckpointTrxnInfo.getTermIndex()));
  }

  private List<ContainerInfo> writeToIncreaseLogIndex(
      StorageContainerManager scm, long targetLogIndex)
      throws IOException, InterruptedException {
    List<ContainerInfo> containers = new ArrayList<>();
    SCMStateMachine stateMachine =
        scm.getScmHAManager().getRatisServer().getSCMStateMachine();
    long logIndex = scm.getScmHAManager().getRatisServer().getSCMStateMachine()
        .getLastAppliedTermIndex().getIndex();
    while (logIndex < targetLogIndex) {
      containers.add(scm.getContainerManager()
          .allocateContainer(
              new RatisReplicationConfig(ReplicationFactor.THREE),
              TestSCMInstallSnapshotWithHA.class.getName()));
      Thread.sleep(100);
      logIndex = stateMachine.getLastAppliedTermIndex().getIndex();
    }
    return containers;
  }

  private static class DummyExitManager extends ExitManager {
    @Override
    public void exitSystem(int status, String message, Throwable throwable,
        Logger log) {
      log.error("System Exit: " + message, throwable);
    }
  }


  static StorageContainerManager getLeader(MiniOzoneHAClusterImpl impl) {
    for (StorageContainerManager scm : impl.getStorageContainerManagers()) {
      if (scm.checkLeader()) {
        return scm;
      }
    }
    return null;
  }

  static StorageContainerManager getInactiveSCM(MiniOzoneHAClusterImpl impl) {
    for (StorageContainerManager scm : impl.getStorageContainerManagers()) {
      if (!impl.isSCMActive(scm.getScmId())) {
        return scm;
      }
    }
    return null;
  }
}


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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMStateMachine;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Tests the Ratis snapshot feature in SCM.
 */
@Flaky("HDDS-5631")
public class TestSCMInstallSnapshotWithHA {

  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String SCM_SERVICE_ID = "scm-service-test1";
  private static final int NUM_OF_OMS = 1;
  private static final int NUM_OF_SCMS = 3;

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;

  private static final long SNAPSHOT_THRESHOLD = 5;
  private static final int LOG_PURGE_GAP = 5;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();

    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
            SNAPSHOT_THRESHOLD);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setSCMServiceId(SCM_SERVICE_ID)
        .setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumOfStorageContainerManagers(NUM_OF_SCMS)
        .setNumOfActiveSCMs(2)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testInstallSnapshot() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = getLeader(cluster);
    assertNotNull(leaderSCM);
    // Find the inactive SCM
    String followerId = getInactiveSCM(cluster).getSCMNodeId();

    StorageContainerManager followerSCM = cluster.getSCM(followerId);
    // Do some transactions so that the log index increases
    List<ContainerInfo> containers = writeToIncreaseLogIndex(leaderSCM, 200);

    // Start the inactive SCM. Install Snapshot will happen as part
    // of setConfiguration() call to ratis leader and the follower will catch
    // up
    cluster.startInactiveSCM(followerId);

    // The recently started  should be lagging behind the leader .
    SCMStateMachine followerSM =
        followerSCM.getScmHAManager().getRatisServer().getSCMStateMachine();
    
    // Wait & retry for follower to update transactions to leader
    // snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerSM.getLastAppliedTermIndex().getIndex() >= 200;
    }, 100, 3000);
    long followerLastAppliedIndex =
        followerSM.getLastAppliedTermIndex().getIndex();
    assertThat(followerLastAppliedIndex).isGreaterThanOrEqualTo(200);
    assertFalse(followerSM.getLifeCycleState().isPausingOrPaused());

    // Verify that the follower 's DB contains the transactions which were
    // made while it was inactive.
    SCMMetadataStore followerMetaStore = followerSCM.getScmMetadataStore();
    for (ContainerInfo containerInfo : containers) {
      assertNotNull(followerMetaStore.getContainerTable()
          .get(containerInfo.containerID()));
    }
  }

  @Test
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = getLeader(cluster);
    String followerId = getInactiveSCM(cluster).getSCMNodeId();
    // Find the inactive SCM

    StorageContainerManager followerSCM = cluster.getSCM(followerId);
    cluster.startInactiveSCM(followerId);
    followerSCM.exitSafeMode();
    DBCheckpoint leaderDbCheckpoint = leaderSCM.getScmMetadataStore().getStore()
        .getCheckpoint(false);

    SCMStateMachine leaderSM =
        leaderSCM.getScmHAManager().getRatisServer().getSCMStateMachine();
    TermIndex lastTermIndex = leaderSM.getLastAppliedTermIndex();

    SCMStateMachine followerSM =
        followerSCM.getScmHAManager().getRatisServer().getSCMStateMachine();
    followerSCM.getScmMetadataStore().getTransactionInfoTable().
        put(OzoneConsts.TRANSACTION_INFO_KEY,
            TransactionInfo.valueOf(lastTermIndex.getTerm(), lastTermIndex.getIndex() + 100));
    // Advance the follower
    followerSM.notifyTermIndexUpdated(lastTermIndex.getTerm(),
        lastTermIndex.getIndex() + 100);

    GenericTestUtils.setLogLevel(SCMHAManagerImpl.class, Level.INFO);
    LogCapturer logCapture = LogCapturer.captureLogs(SCMHAManagerImpl.class);

    // Install the old checkpoint on the follower . This should fail as the
    // follower is already ahead of that transactionLogIndex and the
    // state should be reloaded.
    TermIndex followerTermIndex = followerSM.getLastAppliedTermIndex();
    SCMHAManagerImpl scmhaManager =
        (SCMHAManagerImpl) (followerSCM.getScmHAManager());

    TermIndex newTermIndex = null;
    try {
      newTermIndex = scmhaManager.installCheckpoint(leaderDbCheckpoint);
    } catch (IOException ioe) {
      // throw IOException as expected
    }

    String errorMsg = "Reloading old state of SCM";
    assertThat(logCapture.getOutput()).contains(errorMsg);
    assertNull(newTermIndex, " installed checkpoint even though checkpoint " +
        "logIndex is less than it's lastAppliedIndex");
    assertEquals(followerTermIndex, followerSM.getLastAppliedTermIndex());
    assertFalse(followerSM.getLifeCycleState().isPausingOrPaused());
  }

  @Test
  public void testInstallCorruptedCheckpointFailure() throws Exception {
    StorageContainerManager leaderSCM = getLeader(cluster);
    // Find the inactive SCM
    String followerId = getInactiveSCM(cluster).getSCMNodeId();
    StorageContainerManager followerSCM = cluster.getSCM(followerId);
    // Do some transactions so that the log index increases
    writeToIncreaseLogIndex(leaderSCM, 100);
    File oldDBLocation =
        followerSCM.getScmMetadataStore().getStore().getDbLocation();

    SCMStateMachine followerSM =
        followerSCM.getScmHAManager().getRatisServer().getSCMStateMachine();
    TermIndex termIndex = followerSM.getLastAppliedTermIndex();
    DBCheckpoint leaderDbCheckpoint = leaderSCM.getScmMetadataStore().getStore()
        .getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    final TransactionInfo leaderCheckpointTrxnInfo = HAUtils.getTrxnInfoFromCheckpoint(
        conf, leaderCheckpointLocation, SCMDBDefinition.get());

    assertNotNull(leaderCheckpointLocation);
    // Take a backup of the current DB
    String dbBackupName =
        "SCM_CHECKPOINT_BACKUP" + termIndex.getIndex() + "_" + System
            .currentTimeMillis();
    File dbDir = oldDBLocation.getParentFile();
    File checkpointBackup = new File(dbDir, dbBackupName);

    // Take a backup of the leader checkpoint
    FileUtils.copyDirectory(leaderCheckpointLocation.toFile(),
        checkpointBackup, false);
    // Corrupt the leader checkpoint and install that on the follower. The
    // operation should fail and  should shutdown.
    boolean delete = true;
    File[] files = leaderCheckpointLocation.toFile().listFiles();
    assertNotNull(files);
    for (File file : files) {
      if (file.getName().contains(".sst")) {
        if (delete) {
          FileUtils.deleteQuietly(file);
          delete = false;
        } else {
          delete = true;
        }
      }
    }

    SCMHAManagerImpl scmhaManager =
        (SCMHAManagerImpl) (followerSCM.getScmHAManager());
    GenericTestUtils.setLogLevel(SCMHAManagerImpl.class, Level.ERROR);
    LogCapturer logCapture = LogCapturer.captureLogs(SCMHAManagerImpl.class);
    scmhaManager.setExitManagerForTesting(new DummyExitManager());

    followerSM.pause();
    scmhaManager.installCheckpoint(leaderCheckpointLocation,
        leaderCheckpointTrxnInfo);

    assertThat(logCapture.getOutput())
        .contains("Failed to reload SCM state and instantiate services.");
    final LifeCycle.State s = followerSM.getLifeCycleState();
    assertTrue(s == LifeCycle.State.NEW || s.isPausingOrPaused(), "Unexpected lifeCycle state: " + s);

    // Verify correct reloading
    followerSM.setInstallingSnapshotData(
        new RocksDBCheckpoint(checkpointBackup.toPath()), null);
    followerSM.reinitialize();
    assertEquals(followerSM.getLastAppliedTermIndex(),
        leaderCheckpointTrxnInfo.getTermIndex());
  }

  private List<ContainerInfo> writeToIncreaseLogIndex(
      StorageContainerManager scm, long targetLogIndex)
      throws IOException, InterruptedException, TimeoutException {
    List<ContainerInfo> containers = new ArrayList<>();
    SCMStateMachine stateMachine =
        scm.getScmHAManager().getRatisServer().getSCMStateMachine();
    long logIndex = scm.getScmHAManager().getRatisServer().getSCMStateMachine()
        .getLastAppliedTermIndex().getIndex();
    while (logIndex <= targetLogIndex) {
      containers.add(scm.getContainerManager()
          .allocateContainer(
              RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
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

  private static StorageContainerManager getInactiveSCM(
      MiniOzoneHAClusterImpl cluster) {
    Iterator<StorageContainerManager> inactiveScms = cluster.getInactiveSCM();
    return inactiveScms.hasNext() ? inactiveScms.next() : null;
  }
}


/*
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMStateMachine;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.DBCheckpointMetrics;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.server.protocol.TermIndex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests the Ratis snapshot feature in SCM.
 */
@Timeout(500)
@Flaky("HDDS-5631")
public class TestSCMInstallSnapshotWithHA {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestSCMInstallSnapshotWithHA.class);

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private String scmServiceId;
  private int numOfOMs = 1;
  private int numOfSCMs = 3;

  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;

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

    conf.setStorageSize(ScmConfigKeys.OZONE_SCM_HA_RAFT_SEGMENT_SIZE,
        8, StorageUnit.KB);
    conf.setStorageSize(
        ScmConfigKeys.OZONE_SCM_HA_RAFT_SEGMENT_PRE_ALLOCATED_SIZE,
        8, StorageUnit.KB);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
            SNAPSHOT_THRESHOLD);

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

  @Test
  @Timeout(300)
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

    assertTrue(followerSCM.getScmHAManager().
        getSCMSnapshotProvider().getNumDownloaded() >= 1);
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
  @Timeout(300)
  public void testInstallIncrementalSnapshot() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = getLeader(cluster);
    assertNotNull(leaderSCM);

    // Find the inactive SCM
    String followerId = getInactiveSCM(cluster).getSCMNodeId();
    StorageContainerManager followerSCM = cluster.getSCM(followerId);

    // Start the inactive SCM. Install Snapshot will happen as part
    // of setConfiguration() call to ratis leader and the follower will catch
    // up
    cluster.startInactiveSCM(followerId);

    // Wait the follower bootstrap finish
    GenericTestUtils.waitFor(() -> {
      return ((SCMHAManagerImpl) followerSCM.getScmHAManager()).
          getGrpcServerState();
    }, 1000, 5000);

    followerSCM.stop();

    // Do some transactions so that the log index increases
    List<ContainerInfo> firstContainers =
        writeToIncreaseLogIndex(leaderSCM, 100);

    // ReInstantiate the follower SCM
    StorageContainerManager restartedSCM =
        cluster.reInstantiateStorageContainerManager(followerSCM);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    restartedSCM.getScmHAManager().getSCMSnapshotProvider().
        setInjector(faultInjector);

    // Restart the follower SCM
    restartedSCM.start();
    cluster.waitForClusterToBeReady();

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      LOG.info("Current NumDownloaded {}", restartedSCM.getScmHAManager().
          getSCMSnapshotProvider().getNumDownloaded());
      return restartedSCM.getScmHAManager().getSCMSnapshotProvider().
          getNumDownloaded() >= 1;
    }, 1000, 10000);

    // Do some transactions so that the log index increases
    List<ContainerInfo> secondContainers =
        writeToIncreaseLogIndex(leaderSCM, 200);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    SCMStateMachine followerSM = restartedSCM.getScmHAManager().
        getRatisServer().getSCMStateMachine();

    // Wait & retry for follower to update transactions to leader
    // snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerSM.getLastAppliedTermIndex().getIndex() >= 200;
    }, 100, 3000);

    assertFalse(followerSM.getLifeCycleState().isPausingOrPaused());
    assertTrue(restartedSCM.getScmHAManager().getSCMSnapshotProvider().
        getNumDownloaded() >= 2);

    // Verify that the follower's DB contains the transactions which were
    // made while it was inactive.
    SCMMetadataStore followerMetaStore = restartedSCM.getScmMetadataStore();
    for (ContainerInfo containerInfo : firstContainers) {
      assertNotNull(followerMetaStore.getContainerTable()
          .get(containerInfo.containerID()));
    }
    for (ContainerInfo containerInfo : secondContainers) {
      assertNotNull(followerMetaStore.getContainerTable()
          .get(containerInfo.containerID()));
    }

    // Verify the metrics recording the incremental checkpoint at leader side
    DBCheckpointMetrics dbMetrics = leaderSCM.getMetrics().
        getDBCheckpointMetrics();
    assertTrue(dbMetrics.getLastCheckpointStreamingNumSSTExcluded() > 0);
    assertEquals(1, dbMetrics.getNumIncrementalCheckpoints());
    assertEquals(1, dbMetrics.getNumCheckpoints());

    // Trigger notifySnapshotInstalled event
    writeToIncreaseLogIndex(leaderSCM, 220);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = restartedSCM.getScmHAManager().
        getSCMSnapshotProvider().getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);
  }

  @Test
  @Timeout(300)
  public void testInstallIncrementalSnapshotWithFailure() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = getLeader(cluster);
    assertNotNull(leaderSCM);

    // Find the inactive SCM
    String followerId = getInactiveSCM(cluster).getSCMNodeId();
    StorageContainerManager followerSCM = cluster.getSCM(followerId);

    // Start the inactive SCM. Install Snapshot will happen as part
    // of setConfiguration() call to ratis leader and the follower will catch
    // up
    cluster.startInactiveSCM(followerId);

    // Wait the follower bootstrap finish
    GenericTestUtils.waitFor(() -> {
      return ((SCMHAManagerImpl) followerSCM.getScmHAManager()).
          getGrpcServerState();
    }, 1000, 5000);

    followerSCM.stop();

    // Do some transactions so that the log index increases
    List<ContainerInfo> firstContainers =
        writeToIncreaseLogIndex(leaderSCM, 100);

    // ReInstantiate the follower SCM
    StorageContainerManager restartedSCM =
        cluster.reInstantiateStorageContainerManager(followerSCM);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    restartedSCM.getScmHAManager().getSCMSnapshotProvider().
        setInjector(faultInjector);

    // Restart the follower SCM
    restartedSCM.start();
    cluster.waitForClusterToBeReady();

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      LOG.info("Current NumDownloaded {}", restartedSCM.getScmHAManager().
          getSCMSnapshotProvider().getNumDownloaded());
      return restartedSCM.getScmHAManager().getSCMSnapshotProvider().
          getNumDownloaded() >= 1;
    }, 1000, 10000);

    // Do some transactions so that the log index increases
    List<ContainerInfo> secondContainers =
        writeToIncreaseLogIndex(leaderSCM, 200);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Pause the follower thread again to block the second-time install
    faultInjector.reset();

    // Wait the follower download the incremental snapshot, but get stuck
    // by injector
    GenericTestUtils.waitFor(() -> {
      return restartedSCM.getScmHAManager().getSCMSnapshotProvider().
          getNumDownloaded() == 2;
    }, 1000, 10000);

    SCMStateMachine followerSM = restartedSCM.getScmHAManager().
        getRatisServer().getSCMStateMachine();

    // Corrupt the mixed checkpoint in the candidate DB dir
    File followerCandidateDir = restartedSCM.getScmHAManager().
        getSCMSnapshotProvider().getCandidateDir();
    List<String> sstList = HAUtils.getExistingSstFiles(followerCandidateDir);
    assertTrue(sstList.size() > 0);
    Collections.shuffle(sstList);
    List<String> victimSstList = sstList.subList(0, sstList.size() / 3);
    for (String sst: victimSstList) {
      File victimSst = new File(followerCandidateDir, sst);
      assertTrue(victimSst.delete());
    }

    // Resume the follower thread, it would download the full snapshot again
    // as the installation will fail for the corruption detected.
    faultInjector.resume();

    // Wait & retry for follower to update transactions to leader
    // snapshot index.
    // Timeout error if follower does not load update within 5s
    GenericTestUtils.waitFor(() -> {
      return followerSM.getLastAppliedTermIndex().getIndex() >= 200;
    }, 100, 5000);

    assertFalse(followerSM.getLifeCycleState().isPausingOrPaused());

    // Verify that the follower's DB contains the transactions which were
    // made while it was inactive.
    SCMMetadataStore followerMetaStore = restartedSCM.getScmMetadataStore();
    for (ContainerInfo containerInfo : firstContainers) {
      assertNotNull(followerMetaStore.getContainerTable()
          .get(containerInfo.containerID()));
    }
    for (ContainerInfo containerInfo : secondContainers) {
      assertNotNull(followerMetaStore.getContainerTable()
          .get(containerInfo.containerID()));
    }

    // Verify the metrics recording the checkpoint at leader side
    // should have twice full checkpoint download
    DBCheckpointMetrics dbMetrics = leaderSCM.getMetrics().
        getDBCheckpointMetrics();
    assertEquals(0, dbMetrics.getLastCheckpointStreamingNumSSTExcluded());
    assertTrue(dbMetrics.getNumIncrementalCheckpoints() >= 1);
    assertTrue(dbMetrics.getNumCheckpoints() >= 2);

    // Trigger notifySnapshotInstalled event
    writeToIncreaseLogIndex(leaderSCM, 220);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = restartedSCM.getScmHAManager().
        getSCMSnapshotProvider().getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);
  }

  @Test
  @Timeout(300)
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = getLeader(cluster);
    String followerId = getInactiveSCM(cluster).getSCMNodeId();
    // Find the inactive SCM

    StorageContainerManager followerSCM = cluster.getSCM(followerId);
    cluster.startInactiveSCM(followerId);
    followerSCM.exitSafeMode();

    // Manual flush SCMHADBTransactionBuffer to write TRANSACTION_INFO_KEY
    leaderSCM.getScmHAManager().asSCMHADBTransactionBuffer().flush();
    DBCheckpoint leaderDbCheckpoint = leaderSCM.getScmMetadataStore().
        getStore().getCheckpoint(false);
    SCMStateMachine leaderSM =
        leaderSCM.getScmHAManager().getRatisServer().getSCMStateMachine();
    TermIndex lastTermIndex = leaderSM.getLastAppliedTermIndex();

    SCMStateMachine followerSM =
        followerSCM.getScmHAManager().getRatisServer().getSCMStateMachine();
    followerSCM.getScmMetadataStore().getTransactionInfoTable().
        put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.builder()
        .setCurrentTerm(lastTermIndex.getTerm())
            .setTransactionIndex(lastTermIndex.getIndex() + 100).build());
    // Advance the follower
    followerSM.notifyTermIndexUpdated(lastTermIndex.getTerm(),
        lastTermIndex.getIndex() + 100);
    GenericTestUtils.setLogLevel(SCMHAManagerImpl.getLogger(), Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(SCMHAManagerImpl.getLogger());

    // Install the old checkpoint on the follower. This should fail as the
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
      LOG.info("Got an Exception", ioe);
    }

    String errorMsg = "Reloading old state of SCM";
    assertTrue(logCapture.getOutput().contains(errorMsg));
    assertNull(newTermIndex, " installed checkpoint even though " +
        "checkpoint logIndex is less than it's lastAppliedIndex");
    assertEquals(followerTermIndex,
        followerSM.getLastAppliedTermIndex());
    assertFalse(followerSM.getLifeCycleState().isPausingOrPaused());
  }

  @Test
  @Timeout(300)
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
    TransactionInfo leaderCheckpointTrxnInfo = HAUtils
        .getTrxnInfoFromCheckpoint(conf, leaderCheckpointLocation,
            new SCMDBDefinition());

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
        (SCMHAManagerImpl) (followerSCM.getScmHAManager());
    GenericTestUtils.setLogLevel(SCMHAManagerImpl.getLogger(), Level.ERROR);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(SCMHAManagerImpl.getLogger());
    scmhaManager.setExitManagerForTesting(new DummyExitManager());

    followerSM.pause();
    scmhaManager.installCheckpoint(leaderCheckpointLocation,
        leaderCheckpointTrxnInfo);

    assertTrue(logCapture.getOutput()
        .contains("Failed to reload SCM state and instantiate services."));
    final LifeCycle.State s = followerSM.getLifeCycleState();
    assertTrue(s == LifeCycle.State.NEW || s.isPausingOrPaused(),
        "Unexpected lifeCycle state: " + s);

    // Verify correct reloading
    followerSM.setInstallingDBCheckpoint(
        new RocksDBCheckpoint(checkpointBackup.toPath()));
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
      LOG.info("Current SM log index {}", logIndex);
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

  private static class SnapshotPauseInjector extends FaultInjector {
    private CountDownLatch ready;
    private CountDownLatch wait;

    SnapshotPauseInjector() {
      init();
    }

    @Override
    public void init() {
      this.ready = new CountDownLatch(1);
      this.wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      LOG.info("FaultInjector pauses");
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void resume() throws IOException {
      LOG.info("FaultInjector resumes");
      wait.countDown();
    }

    @Override
    public void reset() throws IOException {
      LOG.info("FaultInjector resets");
      init();
    }
  }
}


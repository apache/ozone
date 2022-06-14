package org.apache.hadoop.hdds.upgrade;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;

public class TestScmHAFinalization {
  private static final String clientID = UUID.randomUUID().toString();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmHAFinalization.class);
  
  private StorageContainerLocationProtocol scmClient;
  // Used to unpause finalization at the halting point once test conditions
  // have been set up during finalization.
  private CountDownLatch unpauseSignal;
  private MiniOzoneHAClusterImpl cluster;
  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;
  private Future<?> finalizationFuture;

  public void init(UpgradeTestInjectionPoints haltingPoint) throws Exception {
    // Make upgrade finalization halt at the specified halting point until
    // the countdown latch is decremented. This allows triggering conditions
    // on the SCM during finalization.
    unpauseSignal = new CountDownLatch(1);
    InjectedUpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor =
        new InjectedUpgradeFinalizationExecutor<>();
    executor.configureTestInjectionFunction(haltingPoint, () -> {
      LOG.info("Halting upgrade finalization at point: {}", haltingPoint);
      try {
        unpauseSignal.await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IOException("SCM test finalization interrupted.", ex);
      }
      LOG.info("Upgrade finalization resumed from point: {}", haltingPoint);
      return false;
    });

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(executor);

    // For testing snapshot install.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, 5);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        5);

    MiniOzoneCluster cluster =
        new MiniOzoneHAClusterImpl.Builder(conf)
        .setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS - 1)
        .setScmLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
        .setSCMServiceId("foo")
        .setSCMConfigurator(configurator)
        .setNumOfOzoneManagers(1)
        .setNumDatanodes(NUM_DATANODES)
        .setDnLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
        .build();
    this.cluster = (MiniOzoneHAClusterImpl) cluster;

    scmClient = cluster.getStorageContainerLocationClient();
    cluster.waitForClusterToBeReady();

    // Launch finalization from the client. In the current implementation,
    // this call will block until finalization completes. If the test
    // involves restarts or leader changes the client may be disconnected,
    // but finalization should still proceed.
    finalizationFuture = Executors.newSingleThreadExecutor().submit(
        () -> {
          try {
            scmClient.finalizeScmUpgrade(clientID);
          } catch (Exception ex) {
            LOG.info("finalization client failed.", ex);
          }
        });

    // finalization executor has now paused at the halting point, waiting for
    // the tests to pick up finalization.
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @EnumSource(UpgradeTestInjectionPoints.class)
  public void testSnapshotFinalization(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    init(haltingPoint);

    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(FinalizationStateManagerImpl.LOG);

//    // Stop a follower SCM, requiring it to finalize from a Ratis snapshot.
//    StorageContainerManager stoppedScm = null;
//    List<StorageContainerManager> scms =
//        cluster.getStorageContainerManagersList();
//    for (StorageContainerManager scm : scms) {
//      if (!scm.getScmContext().isLeader()) {
//        scm.stop();
//        stoppedScm = scm;
//        break;
//      }
//    }
//    Assertions.assertNotNull(stoppedScm);

    StorageContainerManager inactiveScm = cluster.getInactiveSCM().next();
    LOG.info("Inactive SCM node ID: {}", inactiveScm.getSCMNodeId());

    List<StorageContainerManager> scms =
        cluster.getStorageContainerManagersList();
    List<StorageContainerManager> activeScms = new ArrayList<>();
    for (StorageContainerManager scm : scms) {
      if (!scm.getSCMNodeId().equals(inactiveScm.getSCMNodeId())) {
        activeScms.add(scm);
      }
    }

    // Finalize the two active SCMs.
    unpauseFinalization();
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalization(scmClient, clientID);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        activeScms,0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);

    // Move log index farther ahead.
    for (int i = 0; i < 10; i++) {
          ContainerWithPipeline container =
              scmClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, "foo");
          scmClient.closeContainer(
              container.getContainerInfo().getContainerID());
    }

    cluster.startInactiveSCM(inactiveScm.getSCMNodeId());
    GenericTestUtils.waitFor(() -> !inactiveScm.isInSafeMode(), 500, 5000);
    GenericTestUtils.waitFor(() ->
        inactiveScm.getScmContext()
            .isFinalizationCheckpointCrossed(FinalizationCheckpoint.FINALIZATION_COMPLETE),
        500, 10000);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        inactiveScm,0, NUM_DATANODES);

    // Use log to verify a snapshot was installed.
    Assertions.assertTrue(logCapture.getOutput().contains("New SCM snapshot " +
        "received with higher layout version"));
  }

  private void unpauseFinalization() {
    unpauseSignal.countDown();
  }
}

package org.apache.hadoop.hdds.upgrade;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, 1);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        1);

    MiniOzoneCluster cluster =
        new MiniOzoneHAClusterImpl.Builder(new OzoneConfiguration())
        .setNumOfStorageContainerManagers(NUM_SCMS)
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

  @ParameterizedTest
  @EnumSource(UpgradeTestInjectionPoints.class)
  public void testRestart(UpgradeTestInjectionPoints haltingPoint)
      throws Exception {
    init(haltingPoint);
    // Restart SCMs.
    List<StorageContainerManager> scms =
        cluster.getStorageContainerManagersList();
    for (StorageContainerManager scm : scms) {
      cluster.restartStorageContainerManager(scm, false);
    }
    // TODO: Seems to stop here before implementation for some reason.
    cluster.waitForClusterToBeReady();

    for (StorageContainerManager scm: scms) {
      Assertions.assertTrue(
          scm.getPipelineManager().isPipelineCreationFrozen());
    }

    unpauseFinalization();
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalization(scmClient, clientID);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(scms, 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(cluster.getHddsDatanodes(),
        CLOSED);
  }

  @ParameterizedTest
  @EnumSource(UpgradeTestInjectionPoints.class)
  public void testLeaderChangeAndSnapshotFinalization(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    init(haltingPoint);
    // Stop SCM leader to trigger a leader change.
    StorageContainerManager stoppedScm = null;
    List<StorageContainerManager> scms =
        cluster.getStorageContainerManagersList();
    for (StorageContainerManager scm : scms) {
      if (scm.getScmContext().isLeader()) {
        scm.stop();
        stoppedScm = scm;
      }
    }
    Assertions.assertNotNull(stoppedScm);
//    cluster.waitForClusterToBeReady();

    unpauseFinalization();
    finalizationFuture.get();
    // TODO: Never passes this point (as expected) before leader change is
    //  implemented.
    TestHddsUpgradeUtils.waitForFinalization(scmClient, clientID);
    // Since more than one Ratis transaction has passed since this SCM was
    // taken down, it will have to finish finalizing from a snapshot.
    stoppedScm.start();

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(scms, 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(cluster.getHddsDatanodes(),
        CLOSED);
  }

  private void unpauseFinalization() {
    unpauseSignal.countDown();
  }
}

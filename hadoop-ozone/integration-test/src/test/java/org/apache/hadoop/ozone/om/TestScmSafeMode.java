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

package org.apache.hadoop.ozone.om;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMClientProtocolServer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
@Unhealthy("HDDS-3260")
public class TestScmSafeMode {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestScmSafeMode.class);
  private MiniOzoneCluster cluster = null;
  private OzoneClient client;
  private MiniOzoneCluster.Builder builder = null;
  private OzoneConfiguration conf;
  private OzoneManager om;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_STALENODE_INTERVAL, "10s");
    conf.set(OZONE_SCM_DEADNODE_INTERVAL, "25s");
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1000, MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 500, MILLISECONDS);
    builder = MiniOzoneCluster.newBuilder(conf)
        .setStartDataNodes(false);
    cluster = builder.build();
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    om = cluster.getOzoneManager();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      try {
        cluster.shutdown();
      } catch (Exception e) {
        // do nothing.
      }
    }
  }

  @Test
  void testSafeModeOperations() throws Exception {
    TestDataUtil.createKeys(cluster, 100);
    final List<ContainerInfo> containers = cluster
        .getStorageContainerManager().getContainerManager().getContainers();
    GenericTestUtils.waitFor(() -> containers.size() >= 3, 100, 1000);

    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);
    String keyName = "key" + RandomStringUtils.secure().nextNumeric(5);

    ObjectStore store = client.getObjectStore();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.createKey(keyName, 1000, RATIS, ONE, new HashMap<>());

    cluster.stop();

    cluster = builder.build();

    StorageContainerManager scm;

    scm = cluster.getStorageContainerManager();
    assertTrue(scm.isInSafeMode());

    om = cluster.getOzoneManager();


    final OzoneBucket bucket1 =
        client.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

// As cluster is restarted with out datanodes restart
    IOException ioException = assertThrows(IOException.class,
        () -> bucket1.createKey(keyName, 1000, RATIS, ONE,
            new HashMap<>()));
    assertThat(ioException.getMessage())
        .contains("SafeModePrecheck failed for allocateBlock");
  }

  /**
   * Tests inSafeMode & forceExitSafeMode api calls.
   */
  @Test
  void testIsScmInSafeModeAndForceExit() throws Exception {
    // Test 1: SCM should be out of safe mode.
    assertFalse(storageContainerLocationClient.inSafeMode());
    cluster.stop();
    // Restart the cluster with same metadata dir.

    cluster = builder.build();

    // Test 2: Scm should be in safe mode as datanodes are not started yet.
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    assertTrue(storageContainerLocationClient.inSafeMode());
    // Force scm out of safe mode.
    cluster.getStorageContainerManager().getClientProtocolServer()
        .forceExitSafeMode();
    // Test 3: SCM should be out of safe mode.
    GenericTestUtils.waitFor(() -> {
      try {
        return !cluster.getStorageContainerManager().getClientProtocolServer()
            .inSafeMode();
      } catch (IOException e) {
        fail("Cluster");
        return false;
      }
    }, 10, 1000 * 5);

  }

  @Test
  void testSCMSafeMode() throws Exception {
    // Test1: Test safe mode  when there are no containers in system.
    cluster.stop();

    cluster = builder.build();

    assertTrue(cluster.getStorageContainerManager().isInSafeMode());
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    assertFalse(cluster.getStorageContainerManager().isInSafeMode());

    // Test2: Test safe mode  when containers are there in system.
    TestDataUtil.createKeys(cluster, 100 * 2);
    final List<ContainerInfo> containers = cluster
        .getStorageContainerManager().getContainerManager().getContainers();
    GenericTestUtils.waitFor(() -> containers.size() >= 3, 100, 1000 * 30);

    // Removing some container to keep them open.
    containers.remove(0);
    containers.remove(0);

    // Close remaining containers
    ContainerManager mapping = cluster
        .getStorageContainerManager().getContainerManager();
    containers.forEach(c -> {
      try {
        mapping.updateContainerState(c.containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
        mapping.updateContainerState(c.containerID(),
            LifeCycleEvent.CLOSE);
      } catch (IOException | InvalidStateTransitionException e) {
        LOG.info("Failed to change state of open containers.", e);
      }
    });
    cluster.stop();

    LogCapturer logCapturer = LogCapturer.captureLogs(SCMSafeModeManager.getLogger());
    logCapturer.clearOutput();

    cluster = builder.build();

    StorageContainerManager scm;

    scm = cluster.getStorageContainerManager();
    assertTrue(scm.isInSafeMode());
    assertFalse(logCapturer.getOutput().contains("SCM exiting safe mode."));
    assertEquals(0, scm.getCurrentContainerThreshold());
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      dn.start();
    }
    GenericTestUtils
        .waitFor(() -> scm.getCurrentContainerThreshold() == 1.0, 100, 20000);

    EventQueue eventQueue =
        (EventQueue) cluster.getStorageContainerManager().getEventQueue();
    eventQueue.processAll(5000L);

    double safeModeCutoff = conf
        .getDouble(HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
            HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    assertThat(scm.getCurrentContainerThreshold()).isGreaterThanOrEqualTo(safeModeCutoff);
    assertThat(logCapturer.getOutput()).contains("SCM exiting safe mode.");
    assertFalse(scm.isInSafeMode());
  }

  @Test
  public void testSCMSafeModeRestrictedOp() throws Exception {
    cluster.stop();
    cluster = builder.build();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    assertTrue(scm.isInSafeMode());

    SCMException scmException = assertThrows(SCMException.class,
        () -> scm.getClientProtocolServer()
            .allocateContainer(ReplicationType.STAND_ALONE,
                ReplicationFactor.ONE, ""));
    assertThat(scmException.getMessage())
        .contains("SafeModePrecheck failed for allocateContainer");
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    assertFalse(scm.isInSafeMode());

    TestDataUtil.createKeys(cluster, 10);
    SCMClientProtocolServer clientProtocolServer = cluster
        .getStorageContainerManager().getClientProtocolServer();
    assertFalse((scm.getClientProtocolServer()).getSafeModeStatus());
    final List<ContainerInfo> containers = scm.getContainerManager()
        .getContainers();
    GenericTestUtils.waitFor(clientProtocolServer::getSafeModeStatus,
        50, 1000 * 30);
    assertTrue(clientProtocolServer.getSafeModeStatus());

    cluster.shutdownHddsDatanodes();
    Thread.sleep(30000);
    scmException = assertThrows(SCMException.class,
        () -> clientProtocolServer.getContainerWithPipeline(containers.get(0)
            .getContainerID()));

    assertEquals("Open container " + containers.get(0).getContainerID() +
        " doesn't have enough replicas to service this operation in Safe" +
        " mode.", scmException.getMessage());
  }

  @Test
  public void testSCMSafeModeDisabled() throws Exception {
    cluster.shutdown();

    // If safe mode is disabled, cluster should not be in safe mode even if
    // min number of datanodes are not started.
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, false);
    conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, 3);
    builder = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3);
    cluster = builder.build();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    assertFalse(scm.isInSafeMode());

    // Even on SCM restart, cluster should be out of safe mode immediately.
    cluster.restartStorageContainerManager(true);
    assertFalse(scm.isInSafeMode());
  }

  @Test
  public void testCreateRetryWhileSCMSafeMode() throws Exception {
    // Test1: Test safe mode  when there are no containers in system.
    cluster.stop();
    cluster = builder.build();

    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    OMMetrics omMetrics = cluster.getOzoneManager().getMetrics();
    long allocateBlockReqCount = omMetrics.getNumBlockAllocateFails();

    try (FileSystem fs = FileSystem.get(conf)) {
      assertTrue(((SafeMode)fs).setSafeMode(SafeModeAction.GET));

      Thread t = new Thread(() -> {
        try {
          LOG.info("Wait for allocate block fails at least once");
          GenericTestUtils.waitFor(() -> omMetrics.getNumBlockAllocateFails() > allocateBlockReqCount,
              100, 10000);

          cluster.startHddsDatanodes();
          cluster.waitForClusterToBeReady();
          cluster.waitTobeOutOfSafeMode();
        } catch (InterruptedException | TimeoutException e) {
          throw new RuntimeException(e);
        }
      });
      t.start();

      final Path file = new Path("file");
      try (FSDataOutputStream outputStream = fs.create(file, true)) {
        LOG.info("Successfully created a file");
      }
      t.join();
    }

    assertFalse(cluster.getStorageContainerManager().isInSafeMode());
  }
}

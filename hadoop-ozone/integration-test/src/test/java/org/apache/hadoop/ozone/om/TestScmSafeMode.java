/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
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
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.scm.TestStorageContainerManagerHelper;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.UnhealthyTest;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
@Timeout(300)
@Category(UnhealthyTest.class) @Unhealthy("HDDS-3260")
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

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_STALENODE_INTERVAL, "10s");
    conf.set(OZONE_SCM_DEADNODE_INTERVAL, "25s");
    builder = MiniOzoneCluster.newBuilder(conf)
        .setHbInterval(1000)
        .setHbProcessorInterval(500)
        .setStartDataNodes(false);
    cluster = builder.build();
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    om = cluster.getOzoneManager();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
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
  public void testSafeModeOperations() throws Exception {
    // Create {numKeys} random names keys.
    TestStorageContainerManagerHelper helper =
        new TestStorageContainerManagerHelper(cluster, conf);
    Map<String, OmKeyInfo> keyLocations = helper.createKeys(100, 4096);
    final List<ContainerInfo> containers = cluster
        .getStorageContainerManager().getContainerManager().getContainers();
    GenericTestUtils.waitFor(() -> containers.size() >= 3, 100, 1000);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    ObjectStore store = client.getObjectStore();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.createKey(keyName, 1000, RATIS, ONE, new HashMap<>());

    cluster.stop();

    try {
      cluster = builder.build();
    } catch (IOException e) {
      fail("failed");
    }


    StorageContainerManager scm;

    scm = cluster.getStorageContainerManager();
    Assertions.assertTrue(scm.isInSafeMode());

    om = cluster.getOzoneManager();


    final OzoneBucket bucket1 =
        client.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

// As cluster is restarted with out datanodes restart
    IOException ioException = assertThrows(IOException.class,
        () -> bucket1.createKey(keyName, 1000, RATIS, ONE,
            new HashMap<>()));
    assertTrue(ioException.getMessage()
        .contains("SafeModePrecheck failed for allocateBlock"));
  }

  /**
   * Tests inSafeMode & forceExitSafeMode api calls.
   */
  @Test
  public void testIsScmInSafeModeAndForceExit() throws Exception {
    // Test 1: SCM should be out of safe mode.
    Assertions.assertFalse(storageContainerLocationClient.inSafeMode());
    cluster.stop();
    // Restart the cluster with same metadata dir.

    try {
      cluster = builder.build();
    } catch (IOException e) {
      Assertions.fail("Cluster startup failed.");
    }

    // Test 2: Scm should be in safe mode as datanodes are not started yet.
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    Assertions.assertTrue(storageContainerLocationClient.inSafeMode());
    // Force scm out of safe mode.
    cluster.getStorageContainerManager().getClientProtocolServer()
        .forceExitSafeMode();
    // Test 3: SCM should be out of safe mode.
    GenericTestUtils.waitFor(() -> {
      try {
        return !cluster.getStorageContainerManager().getClientProtocolServer()
            .inSafeMode();
      } catch (IOException e) {
        Assertions.fail("Cluster");
        return false;
      }
    }, 10, 1000 * 5);

  }

  @Test
  public void testSCMSafeMode() throws Exception {
    // Test1: Test safe mode  when there are no containers in system.
    cluster.stop();

    try {
      cluster = builder.build();
    } catch (IOException e) {
      Assertions.fail("Cluster startup failed.");
    }
    assertTrue(cluster.getStorageContainerManager().isInSafeMode());
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    assertFalse(cluster.getStorageContainerManager().isInSafeMode());

    // Test2: Test safe mode  when containers are there in system.
    // Create {numKeys} random names keys.
    TestStorageContainerManagerHelper helper =
        new TestStorageContainerManagerHelper(cluster, conf);
    Map<String, OmKeyInfo> keyLocations = helper.createKeys(100 * 2, 4096);
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

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(SCMSafeModeManager.getLogger());
    logCapturer.clearOutput();

    try {
      cluster = builder.build();
    } catch (IOException ex) {
      fail("failed");
    }

    StorageContainerManager scm;

    scm = cluster.getStorageContainerManager();
    assertTrue(scm.isInSafeMode());
    assertFalse(logCapturer.getOutput().contains("SCM exiting safe mode."));
    assertTrue(scm.getCurrentContainerThreshold() == 0);
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
    assertTrue(scm.getCurrentContainerThreshold() >= safeModeCutoff);
    assertTrue(logCapturer.getOutput().contains("SCM exiting safe mode."));
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
    assertTrue(scmException.getMessage()
        .contains("SafeModePrecheck failed for allocateContainer"));
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    assertFalse(scm.isInSafeMode());

    TestStorageContainerManagerHelper helper =
        new TestStorageContainerManagerHelper(cluster, conf);
    helper.createKeys(10, 4096);
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
        .setHbInterval(1000)
        .setHbProcessorInterval(500)
        .setNumDatanodes(3);
    cluster = builder.build();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    assertFalse(scm.isInSafeMode());

    // Even on SCM restart, cluster should be out of safe mode immediately.
    cluster.restartStorageContainerManager(true);
    assertFalse(scm.isInSafeMode());
  }
}

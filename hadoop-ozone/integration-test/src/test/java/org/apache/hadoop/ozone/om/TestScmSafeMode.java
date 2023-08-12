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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
@Ignore("HDDS-3260")
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


  @Rule
  public Timeout timeout = Timeout.seconds(200);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @Before
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
  @After
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

  @Test(timeout = 300_000)
  public void testSafeModeOperations() throws Exception {
    // Create {numKeys} random names keys.
    final List<ContainerInfo> containers = cluster.getStorageContainerManager()
        .getContainerManager()
        .getContainers();

    await().atMost(Duration.ofSeconds(1))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> containers.size() >= 3);

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
    Assert.assertTrue(scm.isInSafeMode());

    om = cluster.getOzoneManager();

    final OzoneBucket bucket1 =
        client.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    // As cluster is restarted without datanodes restart
    IOException ioException = assertThrows(IOException.class,
        () -> bucket1.createKey(keyName, 1000, RATIS, ONE,
            new HashMap<>()));
    assertTrue(ioException.getMessage()
        .contains("SafeModePrecheck failed for allocateBlock"));
  }

  /**
   * Tests inSafeMode & forceExitSafeMode api calls.
   */
  @Test(timeout = 300_000)
  public void testIsScmInSafeModeAndForceExit() throws Exception {
    // Test 1: SCM should be out of safe mode.
    Assert.assertFalse(storageContainerLocationClient.inSafeMode());
    cluster.stop();
    // Restart the cluster with same metadata dir.

    try {
      cluster = builder.build();
    } catch (IOException e) {
      Assert.fail("Cluster startup failed.");
    }

    // Test 2: Scm should be in safe mode as datanodes are not started yet.
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    Assert.assertTrue(storageContainerLocationClient.inSafeMode());
    // Force scm out of safe mode.
    cluster.getStorageContainerManager().getClientProtocolServer()
        .forceExitSafeMode();
    // Test 3: SCM should be out of safe mode.
    await().atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(100))
        .ignoreException(IOException.class)
        .until(() -> !cluster.getStorageContainerManager()
            .getClientProtocolServer()
            .inSafeMode());
  }

  @Test(timeout = 300_000)
  public void testSCMSafeMode() throws Exception {
    // Test1: Test safe mode  when there are no containers in system.
    cluster.stop();

    try {
      cluster = builder.build();
    } catch (IOException e) {
      Assert.fail("Cluster startup failed.");
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

    await().atMost(Duration.ofSeconds(3))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> containers.size() >= 3);

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
    assertEquals(0, scm.getCurrentContainerThreshold(), 0.0);
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      dn.start();
    }
    await().atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() ->
            assertEquals(1, scm.getCurrentContainerThreshold(), 0.0));

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

  @Test(timeout = 300_000)
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
    await().atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() ->
            assertTrue(clientProtocolServer.getSafeModeStatus()));

    cluster.shutdownHddsDatanodes();
    Thread.sleep(30000);
    scmException = assertThrows(SCMException.class,
        () -> clientProtocolServer.getContainerWithPipeline(containers.get(0)
            .getContainerID()));

    assertEquals("Open container " + containers.get(0).getContainerID() +
        " doesn't have enough replicas to service this operation in Safe" +
        " mode.", scmException.getMessage());
  }

  @Test(timeout = 300_000)
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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.server.leader.FollowerInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.event.Level;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_DUMMY_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHA.createKey;

/**
 * Test for OM bootstrap process.
 */
public class TestOzoneManagerBootstrap {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(500_000);

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private final String clusterId = UUID.randomUUID().toString();
  private final String scmId = UUID.randomUUID().toString();

  private static final String OM_SERVICE_ID = "om-bootstrap";
  private static final String VOLUME_NAME;
  private static final String BUCKET_NAME;

  private long lastTransactionIndex;

  static {
    VOLUME_NAME = "volume" + RandomStringUtils.randomNumeric(5);
    BUCKET_NAME = "bucket" + RandomStringUtils.randomNumeric(5);
  }

  private void setupCluster(int numInitialOMs) throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 5);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setSCMServiceId(SCM_DUMMY_SERVICE_ID)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(numInitialOMs)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf)
        .getObjectStore();

    // Perform some transactions
    objectStore.createVolume(VOLUME_NAME);
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    volume.createBucket(BUCKET_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    createKey(bucket);

    lastTransactionIndex = cluster.getOMLeader().getOmRatisServer()
        .getOmStateMachine().getLastAppliedTermIndex().getIndex();
  }

  @After
  public void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void assertNewOMExistsInPeerList(String nodeId) throws Exception {
    // Check that new peer exists in all OMs peers list and also in their Ratis
    // server's peer list
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      Assert.assertTrue("New OM node " + nodeId + " not present in Peer list " +
              "of OM " + om.getOMNodeId(), om.doesPeerExist(nodeId));
      Assert.assertTrue("New OM node " + nodeId + " not present in Peer list " +
          "of OM " + om.getOMNodeId() + " RatisServer",
          om.getOmRatisServer().doesPeerExist(nodeId));
      Assert.assertTrue("New OM node " + nodeId + " not present in " +
          "OM " + om.getOMNodeId() + "RatisServer's RaftConf",
          om.getOmRatisServer().getCurrentPeersFromRaftConf().contains(nodeId));
    }

    OzoneManager newOM = cluster.getOzoneManager(nodeId);
    GenericTestUtils.waitFor(() ->
        newOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
            >= lastTransactionIndex, 100, 100000);

    // Check Ratis Dir for log files
    File[] logFiles = getRatisLogFiles(newOM);
    Assert.assertTrue("There are no ratis logs in new OM ",
        logFiles.length > 0);
  }

  private File[] getRatisLogFiles(OzoneManager om) {
    OzoneManagerRatisServer newOMRatisServer = om.getOmRatisServer();
    File ratisDir = new File(newOMRatisServer.getRatisStorageDir(),
        newOMRatisServer.getRaftGroupId().getUuid().toString());
    File ratisLogDir = new File(ratisDir, Storage.STORAGE_DIR_CURRENT);
    return ratisLogDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().startsWith("log");
      }
    });
  }

  private List<String> testBootstrapOMs(int numNewOMs) throws Exception {
    List<String> newOMNodeIds = new ArrayList<>(numNewOMs);
    for (int i = 1; i <= numNewOMs; i++) {
      String nodeId =  "omNode-bootstrap-" + i;
      cluster.bootstrapOzoneManager(nodeId);
      assertNewOMExistsInPeerList(nodeId);
      newOMNodeIds.add(nodeId);
    }
    return newOMNodeIds;
  }

  /**
   * 1. Add 2 new OMs to an existing 1 node OM cluster.
   * 2. Verify that one of the new OMs must becomes the leader by stopping
   * the old OM.
   */
  @Test
  public void testBootstrap() throws Exception {
    setupCluster(1);
    OzoneManager oldOM = cluster.getOzoneManager();

    // 1. Add 2 new OMs to an existing 1 node OM cluster.
    List<String> newOMNodeIds = testBootstrapOMs(2);

    // 2. Verify that one of the new OMs becomes the leader by stopping the
    // old OM.
    cluster.stopOzoneManager(oldOM.getOMNodeId());

    // Wait for Leader Election timeout
    Thread.sleep(OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT
        .toLong(TimeUnit.MILLISECONDS) * 3);

    // Verify that one of the new OMs is the leader
    GenericTestUtils.waitFor(() -> cluster.getOMLeader() != null, 500, 30000);
    OzoneManager omLeader = cluster.getOMLeader();

    Assert.assertTrue("New Bootstrapped OM not elected Leader even though " +
        "other OMs are down", newOMNodeIds.contains(omLeader.getOMNodeId()));

    // Perform some read and write operations with new OM leader
    objectStore = OzoneClientFactory.getRpcClient(OM_SERVICE_ID,
        cluster.getConf()).getObjectStore();

    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    String key = createKey(bucket);

    Assert.assertNotNull(bucket.getKey(key));
  }

  /**
   * Tests the following scenarios:
   * 1. Bootstrap without updating config on any existing OM -> fail
   * 2. Force bootstrap without upating config on any OM -> fail
   */
  @Test
  public void testBootstrapWithoutConfigUpdate() throws Exception {
    // Setup 1 node cluster
    setupCluster(1);
    cluster.setupExitManagerForTesting();
    OzoneManager existingOM = cluster.getOzoneManager(0);
    String existingOMNodeId = existingOM.getOMNodeId();

    GenericTestUtils.LogCapturer omLog =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);
    GenericTestUtils.LogCapturer miniOzoneClusterLog =
        GenericTestUtils.LogCapturer.captureLogs(MiniOzoneHAClusterImpl.LOG);

    /***************************************************************************
     * 1. Bootstrap without updating config on any existing OM -> fail
     **************************************************************************/

    // Bootstrap a new node without updating the configs on existing OMs.
    // This should result in the bootstrap failing.
    String newNodeId = "omNode-bootstrap-1";
    try {
      cluster.bootstrapOzoneManager(newNodeId, false, false);
      Assert.fail("Bootstrap should have failed as configs are not updated on" +
          " all OMs.");
    } catch (Exception e) {
      Assert.assertEquals(OmUtils.getOMAddressListPrintString(
          Lists.newArrayList(existingOM.getNodeDetails())) + " do not have or" +
          " have incorrect information of the bootstrapping OM. Update their " +
          "ozone-site.xml before proceeding.", e.getMessage());
      Assert.assertTrue(omLog.getOutput().contains("Remote OM config check " +
          "failed on OM " + existingOMNodeId));
      Assert.assertTrue(miniOzoneClusterLog.getOutput().contains(newNodeId +
          " - System Exit"));
    }

    /***************************************************************************
     * 2. Force bootstrap without updating config on any OM -> fail
     **************************************************************************/

    // Force Bootstrap a new node without updating the configs on existing OMs.
    // This should avoid the bootstrap check but the bootstrap should fail
    // eventually as the SetConfiguration request cannot succeed.

    miniOzoneClusterLog.clearOutput();
    omLog.clearOutput();

    newNodeId = "omNode-bootstrap-2";
    try {
      cluster.bootstrapOzoneManager(newNodeId, false, true);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(omLog.getOutput().contains("Couldn't add OM " +
          newNodeId + " to peer list."));
      Assert.assertTrue(miniOzoneClusterLog.getOutput().contains(
          existingOMNodeId + " - System Exit: There is no OM configuration " +
              "for node ID " + newNodeId + " in ozone-site.xml."));

      // Verify that the existing OM has stopped.
      Assert.assertFalse(cluster.getOzoneManager(existingOMNodeId).isRunning());
    }
  }

  /**
   * Tests the following scenarios:
   * 1. Stop 1 OM and update configs on rest, bootstrap new node -> fail
   * 2. Force bootstrap (with 1 node down and updated configs on rest) -> pass
   */
  @Test
  public void testForceBootstrap() throws Exception {
    GenericTestUtils.setLogLevel(GrpcLogAppender.LOG, Level.ERROR);
    GenericTestUtils.setLogLevel(FollowerInfo.LOG, Level.ERROR);
    // Setup a 3 node cluster and stop 1 OM.
    setupCluster(3);
    OzoneManager downOM = cluster.getOzoneManager(2);
    String downOMNodeId = downOM.getOMNodeId();
    cluster.stopOzoneManager(downOMNodeId);

    // Set a smaller value for OM Metadata and Client protocol retry attempts
    OzoneConfiguration config = cluster.getConf();
    config.setInt(OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_KEY, 2);
    config.setInt(
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_WAIT_BETWEEN_RETRIES_KEY, 100);
    cluster.setConf(config);

    GenericTestUtils.LogCapturer omLog =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);
    GenericTestUtils.LogCapturer miniOzoneClusterLog =
        GenericTestUtils.LogCapturer.captureLogs(MiniOzoneHAClusterImpl.LOG);

    /***************************************************************************
     * 1. Force bootstrap (with 1 node down and updated configs on rest) -> pass
     **************************************************************************/

    // Update configs on all active OMs and Bootstrap a new node
    String newNodeId = "omNode-bootstrap-1";
    try {
      cluster.bootstrapOzoneManager(newNodeId, true, false);
      Assert.fail("Bootstrap should have failed as configs are not updated on" +
          " all OMs.");
    } catch (IOException e) {
      Assert.assertEquals(OmUtils.getOMAddressListPrintString(
          Lists.newArrayList(downOM.getNodeDetails())) + " do not have or " +
          "have incorrect information of the bootstrapping OM. Update their " +
          "ozone-site.xml before proceeding.", e.getMessage());
      Assert.assertTrue(omLog.getOutput().contains("Remote OM " + downOMNodeId +
          " configuration returned null"));
      Assert.assertTrue(omLog.getOutput().contains("Remote OM config check " +
          "failed on OM " + downOMNodeId));
      Assert.assertTrue(miniOzoneClusterLog.getOutput().contains(newNodeId +
          " - System Exit"));
    }

    /***************************************************************************
     * 2. Force bootstrap (with 1 node down and updated configs on rest) -> pass
     **************************************************************************/

    miniOzoneClusterLog.clearOutput();
    omLog.clearOutput();

    // Update configs on all active OMs and Force Bootstrap a new node
    newNodeId = "omNode-bootstrap-2";
    cluster.bootstrapOzoneManager(newNodeId, true, true);
    OzoneManager newOM = cluster.getOzoneManager(newNodeId);

    // Verify that the newly bootstrapped OM is running
    Assert.assertTrue(newOM.isRunning());
  }
}

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

import static org.apache.hadoop.ozone.OzoneConsts.SCM_DUMMY_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DECOMMISSIONED_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHA.createKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.leader.FollowerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Test for OM bootstrap process.
 */
public class TestAddRemoveOzoneManager {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private long lastTransactionIndex;
  private UserGroupInformation user;

  private static final String OM_SERVICE_ID = "om-add-remove";
  private static final String VOLUME_NAME;
  private static final String BUCKET_NAME;

  static {
    VOLUME_NAME = "volume" + RandomStringUtils.secure().nextNumeric(5);
    BUCKET_NAME = "bucket" + RandomStringUtils.secure().nextNumeric(5);
  }

  private OzoneClient client;

  private void setupCluster(int numInitialOMs) throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 5);
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId(SCM_DUMMY_SERVICE_ID)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(numInitialOMs)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf);
    objectStore = client.getObjectStore();

    // Perform some transactions
    objectStore.createVolume(VOLUME_NAME);
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    volume.createBucket(BUCKET_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    createKey(bucket);

    lastTransactionIndex = cluster.getOMLeader().getOmRatisServer()
        .getOmStateMachine().getLastAppliedTermIndex().getIndex();
  }

  @AfterEach
  public void shutdown() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static List<String> getCurrentPeersFromRaftConf(OzoneManagerRatisServer omRatisServer) {
    return omRatisServer.getServerDivision().getRaftConf().getCurrentPeers().stream()
        .map(RaftPeer::getId)
        .map(RaftPeerId::toString)
        .collect(Collectors.toList());
  }

  private void assertNewOMExistsInPeerList(String nodeId) throws Exception {
    // Check that new peer exists in all OMs peers list and also in their Ratis
    // server's peer list
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      assertTrue(om.doesPeerExist(nodeId), "New OM node " + nodeId
          + " not present in Peer list of OM " + om.getOMNodeId());
      assertTrue(om.getOmRatisServer().doesPeerExist(nodeId), "New OM node " + nodeId
          + " not present in Peer list of OM " + om.getOMNodeId() + " RatisServer");
      assertThat(getCurrentPeersFromRaftConf(om.getOmRatisServer()))
          .withFailMessage("New OM node " + nodeId + " not present in " + om.getOMNodeId() + "'s RaftConf")
          .contains(nodeId);
    }

    OzoneManager newOM = cluster.getOzoneManager(nodeId);
    GenericTestUtils.waitFor(() ->
        newOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
            >= lastTransactionIndex, 100, 100000);

    // Check Ratis Dir for log files
    File[] logFiles = getRatisLogFiles(newOM);
    assertThat(logFiles.length).withFailMessage("There are no ratis logs in new OM ")
        .isGreaterThan(0);
  }

  private void assertNewOMExistsInListenerList(String nodeId) throws Exception {
    // Check that new peer exists in all OMs Peer list and also in their Ratis
    // server's listener list
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      assertTrue(om.doesPeerExist(nodeId), "New OM node " + nodeId + " not present in Peer list " +
          "of OM " + om.getOMNodeId());
      assertTrue(om.getOmRatisServer().doesPeerExist(nodeId),
          "New OM node " + nodeId + " not present in Peer list of OM " + om.getOMNodeId() + " RatisServer");
      assertTrue(om.getOmRatisServer().getCurrentListenersFromRaftConf().contains(nodeId),
          "New OM node " + nodeId + " not present in OM " + om.getOMNodeId() + "RatisServer's RaftConf");
    }

    OzoneManager newOM = cluster.getOzoneManager(nodeId);
    GenericTestUtils.waitFor(() ->
        newOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
            >= lastTransactionIndex, 100, 100000);

    // Check Ratis Dir for log files
    File[] logFiles = getRatisLogFiles(newOM);
    assertTrue(logFiles.length > 0, "There are no ratis logs in new OM ");
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

  private List<String> testBootstrapListenerOMs(int numNewOMs) throws Exception {
    List<String> newOMNodeIds = new ArrayList<>(numNewOMs);
    for (int i = 1; i <= numNewOMs; i++) {
      String nodeId =  "omNode-bootstrap-listener-" + i;
      cluster.bootstrapOzoneManager(nodeId, true, false, true);
      assertNewOMExistsInListenerList(nodeId);
      newOMNodeIds.add(nodeId);
    }
    return newOMNodeIds;
  }

  /**
   * 1. Add 2 new OMs to an existing 1 node OM cluster.
   * 2. Verify that one of the new OMs becomes the leader by stopping the old
   * OM.
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
    cluster.waitForLeaderOM();
    OzoneManager omLeader = cluster.getOMLeader();

    assertThat(newOMNodeIds)
        .withFailMessage("New Bootstrapped OM not elected Leader even though" + " other OMs are down")
        .contains(omLeader.getOMNodeId());

    // Perform some read and write operations with new OM leader
    IOUtils.closeQuietly(client);
    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf());
    objectStore = client.getObjectStore();

    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    String key = createKey(bucket);

    assertNotNull(bucket.getKey(key));
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

    LogCapturer omLog = LogCapturer.captureLogs(OzoneManager.class);
    LogCapturer miniOzoneClusterLog = LogCapturer.captureLogs(MiniOzoneHAClusterImpl.class);

    /***************************************************************************
     * 1. Bootstrap without updating config on any existing OM -> fail
     **************************************************************************/

    // Bootstrap a new node without updating the configs on existing OMs.
    // This should result in the bootstrap failing.
    final String newNodeId = "omNode-bootstrap-1";
    Exception e =
        assertThrows(Exception.class, () -> cluster.bootstrapOzoneManager(newNodeId, false, false),
            "Bootstrap should have failed as configs are not updated on all OMs.");
    assertEquals(OmUtils.getOMAddressListPrintString(
        Lists.newArrayList(existingOM.getNodeDetails())) + " do not have or" +
        " have incorrect information of the bootstrapping OM. Update their " +
        "ozone-site.xml before proceeding.", e.getMessage());
    assertThat(omLog.getOutput()).contains("Remote OM config check " +
        "failed on OM " + existingOMNodeId);
    assertThat(miniOzoneClusterLog.getOutput()).contains(newNodeId +
        " - System Exit");

    /***************************************************************************
     * 2. Force bootstrap without updating config on any OM -> fail
     **************************************************************************/

    // Force Bootstrap a new node without updating the configs on existing OMs.
    // This should avoid the bootstrap check but the bootstrap should fail
    // eventually as the SetConfiguration request cannot succeed.

    miniOzoneClusterLog.clearOutput();
    omLog.clearOutput();

    String newNodeId1 = "omNode-bootstrap-2";
    try {
      cluster.bootstrapOzoneManager(newNodeId1, false, true);
    } catch (IOException ex) {
      assertThat(omLog.getOutput()).contains("Couldn't add OM " +
          newNodeId1 + " to peer list.");
      assertThat(miniOzoneClusterLog.getOutput()).contains(
          existingOMNodeId + " - System Exit: There is no OM configuration " +
              "for node ID " + newNodeId1 + " in ozone-site.xml.");

      // Verify that the existing OM has stopped.
      assertFalse(cluster.getOzoneManager(existingOMNodeId).isRunning());
    }
  }

  /**
   * Tests the following scenarios:
   * 1. Stop 1 OM and update configs on rest, bootstrap new node -> fail
   * 2. Force bootstrap (with 1 node down and updated configs on rest) -> pass
   */
  @Flaky("HDDS-11358")
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

    LogCapturer omLog = LogCapturer.captureLogs(OzoneManager.class);
    LogCapturer miniOzoneClusterLog = LogCapturer.captureLogs(MiniOzoneHAClusterImpl.class);

    /***************************************************************************
     * 1. Force bootstrap (with 1 node down and updated configs on rest) -> pass
     **************************************************************************/

    // Update configs on all active OMs and Bootstrap a new node
    final String newNodeId = "omNode-bootstrap-1";
    IOException e =
        assertThrows(IOException.class, () -> cluster.bootstrapOzoneManager(newNodeId, true, false),
            "Bootstrap should have failed as configs are not updated on all OMs.");
    assertEquals(OmUtils.getOMAddressListPrintString(
        Lists.newArrayList(downOM.getNodeDetails())) + " do not have or " +
        "have incorrect information of the bootstrapping OM. Update their " +
        "ozone-site.xml before proceeding.", e.getMessage());
    assertThat(omLog.getOutput()).contains("Remote OM " + downOMNodeId +
        " configuration returned null");
    assertThat(omLog.getOutput()).contains("Remote OM config check " +
        "failed on OM " + downOMNodeId);
    assertThat(miniOzoneClusterLog.getOutput()).contains(newNodeId +
        " - System Exit");

    /***************************************************************************
     * 2. Force bootstrap (with 1 node down and updated configs on rest) -> pass
     **************************************************************************/

    miniOzoneClusterLog.clearOutput();
    omLog.clearOutput();

    // Update configs on all active OMs and Force Bootstrap a new node
    String newNodeId1 = "omNode-bootstrap-2";
    cluster.bootstrapOzoneManager(newNodeId1, true, true);
    OzoneManager newOM = cluster.getOzoneManager(newNodeId1);

    // Verify that the newly bootstrapped OM is running
    assertTrue(newOM.isRunning());
  }

  /**
   * Tests:
   * 1. Start a Listener OM.
   * 2. Decommission the Listener OM.
   */
  @Test
  public void testBootstrapListenerOM() throws Exception {
    setupCluster(3);
    user = UserGroupInformation.getCurrentUser();

    List<String> newOMNodeIds = testBootstrapListenerOMs(1);

    for (String omId: newOMNodeIds) {
      OzoneManager newOM = cluster.getOzoneManager(omId);
      assertTrue(newOM.isRunning());
    }

    // Verify that we can read/ write to the cluster with only 1 OM.
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    String key = createKey(bucket);

    assertNotNull(bucket.getKey(key));

    for (String omId: newOMNodeIds) {
      cluster.stopOzoneManager(omId);
      decommissionOM(omId);
    }
  }

  /**
   * Decommissioning Tests:
   * 1. Stop an OM and decommission it from a 3 node cluster
   * 2. Decommission another OM without stopping it.
   * 3.
   */
  @Test
  public void testDecommission() throws Exception {
    setupCluster(3);

    user = UserGroupInformation.createUserForTesting("user", new String[]{});
    // Stop the 3rd OM and decommission it using non-privileged user
    String omNodeId3 = cluster.getOzoneManager(2).getOMNodeId();
    cluster.stopOzoneManager(omNodeId3);
    // decommission should fail
    assertThrows(IOException.class, () -> decommissionOM(omNodeId3));

    // Switch to admin user
    user = UserGroupInformation.getCurrentUser();
    // Stop the 3rd OM and decommission it
    cluster.stopOzoneManager(omNodeId3);
    decommissionOM(omNodeId3);

    // Decommission the non leader OM and then stop it. Stopping OM before will
    // lead to no quorum and there will not be a elected leader OM to process
    // the decommission request.
    String omNodeId2;
    if (cluster.getOMLeader().getOMNodeId().equals(
        cluster.getOzoneManager(1).getOMNodeId())) {
      omNodeId2 = cluster.getOzoneManager(0).getOMNodeId();
    } else {
      omNodeId2 = cluster.getOzoneManager(1).getOMNodeId();
    }
    decommissionOM(omNodeId2);
    cluster.stopOzoneManager(omNodeId2);

    // Verify that we can read/ write to the cluster with only 1 OM.
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    String key = createKey(bucket);

    assertNotNull(bucket.getKey(key));

  }

  /**
   * Decommission given OM and verify that the other OM's peer nodes are
   * updated after decommissioning.
   */
  private void decommissionOM(String decommNodeId) throws Exception {
    Collection<String> decommNodes = conf.getTrimmedStringCollection(
        OZONE_OM_DECOMMISSIONED_NODES_KEY);
    decommNodes.add(decommNodeId);
    conf.set(OZONE_OM_DECOMMISSIONED_NODES_KEY, StringUtils.join(",", decommNodes));
    List<OzoneManager> activeOMs = new ArrayList<>();
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      String omNodeId = om.getOMNodeId();
      if (cluster.isOMActive(omNodeId)) {
        om.setConfiguration(conf);
        activeOMs.add(om);
      }
    }

    // Create OMAdmin protocol client to send decommission request
    OMAdminProtocolClientSideImpl omAdminProtocolClient =
        OMAdminProtocolClientSideImpl.createProxyForOMHA(conf, user,
            OM_SERVICE_ID);
    OMNodeDetails decommNodeDetails = new OMNodeDetails.Builder()
        .setOMNodeId(decommNodeId)
        .setHostAddress("localhost")
        .build();
    omAdminProtocolClient.decommission(decommNodeDetails);

    // Verify decomm node is removed from the HA ring
    GenericTestUtils.waitFor(() -> {
      for (OzoneManager om : activeOMs) {
        if (om.getPeerNodes().contains(decommNodeId)) {
          return false;
        }
      }
      return true;
    }, 100, 100000);

    // Wait for new leader election if required
    cluster.waitForLeaderOM();
  }

  /**
   * Test that listener OMs cannot become leaders even when all voting OMs are
   * down.
   * This test verifies the core safety property of listener nodes.
   */
  @Test
  public void testListenerCannotBecomeLeader() throws Exception {
    // Setup cluster with 2 voting OMs
    setupCluster(2);
    user = UserGroupInformation.getCurrentUser();

    // Add 2 listener OMs
    List<String> listenerNodeIds = testBootstrapListenerOMs(2);

    // Verify all listeners are running
    for (String omId : listenerNodeIds) {
      OzoneManager listenerOM = cluster.getOzoneManager(omId);
      assertTrue(listenerOM.isRunning());
      // Verify the node is actually a listener
      assertTrue(listenerOM.getOmRatisServer()
          .getCurrentListenersFromRaftConf().contains(omId));
    }

    // Stop all voting OMs
    List<String> votingOMs = new ArrayList<>();
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      if (!listenerNodeIds.contains(om.getOMNodeId())) {
        votingOMs.add(om.getOMNodeId());
        cluster.stopOzoneManager(om.getOMNodeId());
      }
    }

    // Wait for election timeout
    Thread.sleep(OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT
        .toLong(TimeUnit.MILLISECONDS) * 3);

    // Verify no listener became leader (cluster should have no leader)
    for (String listenerId : listenerNodeIds) {
      OzoneManager listenerOM = cluster.getOzoneManager(listenerId);
      assertFalse(listenerOM.isLeaderReady(),
          "Listener OM " + listenerId + " should not become leader");
    }
  }

  /**
   * Test mixed cluster behavior with both followers and listeners.
   * Verifies that the cluster operates correctly with mixed node types.
   */
  @Test
  public void testMixedFollowersAndListeners() throws Exception {
    // Setup cluster with 3 voting OMs
    setupCluster(3);
    user = UserGroupInformation.getCurrentUser();

    // Add 1 voting OM and 2 listener OMs
    List<String> newVotingOMs = testBootstrapOMs(1);
    List<String> listenerOMs = testBootstrapListenerOMs(2);

    // Verify total cluster size
    assertEquals(6, cluster.getOzoneManagersList().size(),
        "Cluster should have 6 OMs total (4 voting + 2 listeners)");

    // Verify listeners are in the listener list
    for (String listenerId : listenerOMs) {
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        if (om.isRunning()) {
          List<String> listeners = om.getOmRatisServer()
              .getCurrentListenersFromRaftConf();
          assertTrue(listeners.contains(listenerId),
              "OM " + om.getOMNodeId() + " should have " + listenerId +
                  " in its listener list");
        }
      }
    }

    // Verify voting OMs are NOT in the listener list
    for (String votingId : newVotingOMs) {
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        if (om.isRunning()) {
          List<String> listeners = om.getOmRatisServer()
              .getCurrentListenersFromRaftConf();
          assertFalse(listeners.contains(votingId),
              "Voting OM " + votingId + " should not be in listener list");
        }
      }
    }

    // Perform operations to ensure cluster works
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    String key = createKey(bucket);
    assertNotNull(bucket.getKey(key));

    // Verify listeners are receiving updates by checking their last applied index
    long leaderLastIndex = cluster.getOMLeader().getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    for (String listenerId : listenerOMs) {
      OzoneManager listenerOM = cluster.getOzoneManager(listenerId);
      GenericTestUtils.waitFor(() -> {
        long listenerIndex = listenerOM.getOmRatisServer()
            .getLastAppliedTermIndex().getIndex();
        // Listener should be close to leader's index (allowing some lag)
        return listenerIndex == leaderLastIndex;
      }, 500, 10000);
    }
  }

  /**
   * Test removing a listener OM from the cluster.
   * Verifies that listeners can be safely removed.
   */
  @Test
  public void testRemoveListenerOM() throws Exception {
    // Setup cluster with 3 voting OMs
    setupCluster(3);
    user = UserGroupInformation.getCurrentUser();

    // Add 2 listener OMs
    List<String> listenerNodeIds = testBootstrapListenerOMs(2);
    String listenerToRemove = listenerNodeIds.get(0);

    // Verify listener is present in all OMs
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      if (om.isRunning()) {
        assertTrue(om.getOmRatisServer()
            .getCurrentListenersFromRaftConf().contains(listenerToRemove));
      }
    }

    // Decommission the listener OM
    decommissionOM(listenerToRemove);

    // Verify listener is removed from all OMs
    GenericTestUtils.waitFor(() -> {
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        if (om.isRunning() && !om.getOMNodeId().equals(listenerToRemove)) {
          try {
            if (om.getOmRatisServer()
                .getCurrentListenersFromRaftConf().contains(listenerToRemove)) {
              return false;
            }
          } catch (IOException e) {
            return false;
          }
        }
      }
      return true;
    }, 100, 30000);

    // Verify remaining listener is still functioning
    String remainingListener = listenerNodeIds.get(1);
    OzoneManager remainingListenerOM = cluster.getOzoneManager(remainingListener);
    assertTrue(remainingListenerOM.isRunning());

    // Verify cluster still works
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    String key = createKey(bucket);
    assertNotNull(bucket.getKey(key));
  }
}

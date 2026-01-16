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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Ozone Manager HA follower read tests that stop/restart one or more OM nodes.
 * @see TestOzoneManagerHAFollowerReadWithAllRunning
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestOzoneManagerHAFollowerReadWithStoppedNodes extends TestOzoneManagerHAFollowerRead {

  /**
   * After restarting OMs we need to wait
   * for a leader to be elected and ready.
   */
  @BeforeEach
  void setup() throws Exception {
    waitForLeaderToBeReady();
  }

  /**
   * Restart all OMs after each test.
   */
  @AfterEach
  void resetCluster() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    if (cluster != null) {
      cluster.restartOzoneManager();
    }
  }

  /**
   * Test client request succeeds when one OM node is down.
   */
  @Test
  void oneOMDown() throws Exception {
    changeFollowerReadInitialProxy(1);

    getCluster().stopOzoneManager(1);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);

    createVolumeTest(true);
    createKeyTest(true);
  }

  /**
   * Test client request fails when 2 OMs are down.
   */
  @Test
  void twoOMDown() throws Exception {
    changeFollowerReadInitialProxy(1);

    getCluster().stopOzoneManager(1);
    getCluster().stopOzoneManager(2);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);

    createVolumeTest(false);
    createKeyTest(false);
  }

  @Test
  void testMultipartUpload() throws Exception {

    // Happy scenario when all OM's are up.
    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    testMultipartUploadWithOneOmNodeDown();
  }

  private void testMultipartUploadWithOneOmNodeDown() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    // After initiate multipartupload, shutdown leader OM.
    // Stop leader OM, to see when the OM leader changes
    // multipart upload is happening successfully or not.

    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFailoverProxyProvider(getObjectStore().getClientProxy());
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFollowerReadFailoverProxyProvider(getObjectStore().getClientProxy());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Also change the initial follower read proxy to the current leader OM node
    changeFollowerReadInitialProxy(leaderOMNodeId);

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    getCluster().stopOzoneManager(leaderOMNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    assertNotEquals(leaderOMNodeId, newLeaderOMNodeId);
    assertNotEquals(leaderOMNodeId, followerReadFailoverProxyProvider.getCurrentProxy().getNodeId());
  }

  private String initiateMultipartUpload(OzoneBucket ozoneBucket,
      String keyName) throws Exception {

    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName,
            ReplicationType.RATIS,
            ReplicationFactor.ONE);

    String uploadID = omMultipartInfo.getUploadID();
    assertNotNull(uploadID);
    return uploadID;
  }

  private void createMultipartKeyAndReadKey(OzoneBucket ozoneBucket,
      String keyName, String uploadID) throws Exception {

    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), 1, uploadID);
    ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(value));
    ozoneOutputStream.close();


    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, ozoneOutputStream.getCommitUploadPartInfo().getETag());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        ozoneBucket.completeMultipartUpload(keyName, uploadID, partsMap);

    assertNotNull(omMultipartUploadCompleteInfo);
    assertNotNull(omMultipartUploadCompleteInfo.getHash());


    try (OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName)) {
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      IOUtils.readFully(ozoneInputStream, fileContent);
      assertEquals(value, new String(fileContent, UTF_8));
    }
  }

  @Test
  void testLeaderOmProxyProviderFailoverOnConnectionFailure() throws Exception {
    ObjectStore objectStore = getObjectStore();
    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFailoverProxyProvider(objectStore.getClientProxy());
    String firstProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    createVolumeTest(true);

    // On stopping the current OM Proxy, the next connection attempt should
    // failover to a another OM proxy.
    getCluster().stopOzoneManager(firstProxyNodeId);
    Thread.sleep(OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT * 4);

    // Next request to the proxy provider should result in a failover
    createVolumeTest(true);
    Thread.sleep(OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);

    // Get the new OM Proxy NodeId
    String newProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Verify that a failover occurred. the new proxy nodeId should be
    // different from the old proxy nodeId.
    assertNotEquals(firstProxyNodeId, newProxyNodeId);
  }

  @Test
  void testFollowerReadOmProxyProviderFailoverOnConnectionFailure() throws Exception {
    ObjectStore objectStore = getObjectStore();
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFollowerReadFailoverProxyProvider(objectStore.getClientProxy());
    String firstProxyNodeId = followerReadFailoverProxyProvider.getCurrentProxy().getNodeId();

    objectStore.getClientProxy().listVolumes(null, null, 1000);

    // On stopping the current OM Proxy, the next connection attempt should
    // failover to another OM proxy.
    getCluster().stopOzoneManager(firstProxyNodeId);

    // Next request to the proxy provider should result in a failover
    objectStore.getClientProxy().listVolumes(null, null, 1000);

    // Get the new OM Proxy NodeId
    String newProxyNodeId = followerReadFailoverProxyProvider.getCurrentProxy().getNodeId();

    // Verify that a failover occurred. the new proxy nodeId should be
    // different from the old proxy nodeId.
    assertNotEquals(firstProxyNodeId, newProxyNodeId);
    assertTrue(followerReadFailoverProxyProvider.isFollowerReadEnabled());
  }

  @Test
  void testFollowerReadSkipsStoppedFollower() throws Exception {
    ObjectStore objectStore = getObjectStore();
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFollowerReadFailoverProxyProvider(objectStore.getClientProxy());

    String leaderOMNodeId = getCluster().getOMLeader().getOMNodeId();
    List<String> followerOmNodeIds = new ArrayList<>();
    for (OzoneManager om : getCluster().getOzoneManagersList()) {
      if (!om.getOMNodeId().equals(leaderOMNodeId)) {
        followerOmNodeIds.add(om.getOMNodeId());
      }
    }
    assertFalse(followerOmNodeIds.isEmpty());

    String stoppedFollowerNodeId = followerOmNodeIds.get(0);
    getCluster().stopOzoneManager(stoppedFollowerNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);

    followerReadFailoverProxyProvider.changeInitialProxyForTest(stoppedFollowerNodeId);
    objectStore.getClientProxy().listVolumes(null, null, 10);

    OMProxyInfo lastProxy =
        (OMProxyInfo) followerReadFailoverProxyProvider.getLastProxy();
    assertNotNull(lastProxy);
    assertNotEquals(stoppedFollowerNodeId, lastProxy.getNodeId());
  }

  @Test
  @Order(Integer.MAX_VALUE - 1)
  void testIncrementalWaitTimeWithSameNodeFailover() throws Exception {
    long waitBetweenRetries = getConf().getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFailoverProxyProvider(getObjectStore().getClientProxy());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    getCluster().stopOzoneManager(leaderOMNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);
    createKeyTest(true); // failover should happen to new node

    long numTimesTriedToSameNode = omFailoverProxyProvider.getWaitTime()
        / waitBetweenRetries;
    omFailoverProxyProvider.setNextOmProxy(omFailoverProxyProvider.
        getCurrentProxyOMNodeId());
    assertEquals((numTimesTriedToSameNode + 1) * waitBetweenRetries,
        omFailoverProxyProvider.getWaitTime());
  }

  @Test
  void testOMRetryProxy() {
    int maxFailoverAttempts = getOzoneClientFailoverMaxAttempts();
    // Stop all the OMs.
    for (int i = 0; i < getNumOfOMs(); i++) {
      getCluster().stopOzoneManager(i);
    }

    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);

    // After making N (set maxRetries value) connection attempts to OMs,
    // the RpcClient should give up.
    assertThrows(ConnectException.class, () -> createVolumeTest(true));
    assertEquals(1,
        appender.countLinesWithMessage("Failed to connect to OMs:"));
    assertEquals(maxFailoverAttempts,
        appender.countLinesWithMessage("Trying to failover"));
    assertEquals(1, appender.countLinesWithMessage("Attempted " +
        maxFailoverAttempts + " failovers."));
  }

  private void changeFollowerReadInitialProxy(int omIndex) {
    // Change the initial proxy to the OM to be stopped to test follower read failover
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmFailoverProxyUtil.getFollowerReadFailoverProxyProvider(getObjectStore().getClientProxy());
    followerReadFailoverProxyProvider.changeInitialProxyForTest(getCluster().getOzoneManager(omIndex).getOMNodeId());
  }

  private void changeFollowerReadInitialProxy(String omNodeId) {
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmFailoverProxyUtil.getFollowerReadFailoverProxyProvider(getObjectStore().getClientProxy());
    followerReadFailoverProxyProvider.changeInitialProxyForTest(omNodeId);
  }

}

/**
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
package org.apache.hadoop.ozone.om;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMHAMetrics;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ozone Manager HA tests that stop/restart one or more OM nodes.
 * @see TestOzoneManagerHAWithAllRunning
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestOzoneManagerHAWithStoppedNodes extends TestOzoneManagerHA {

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

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    getCluster().stopOzoneManager(leaderOMNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    assertNotEquals(leaderOMNodeId, newLeaderOMNodeId);
  }

  private String initiateMultipartUpload(OzoneBucket ozoneBucket,
      String keyName) throws Exception {

    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName,
            ReplicationType.RATIS,
            ReplicationFactor.ONE);

    String uploadID = omMultipartInfo.getUploadID();
    Assertions.assertNotNull(uploadID);
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

    Assertions.assertNotNull(omMultipartUploadCompleteInfo);
    Assertions.assertNotNull(omMultipartUploadCompleteInfo.getHash());


    try (OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName)) {
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      ozoneInputStream.read(fileContent);
      assertEquals(value, new String(fileContent, UTF_8));
    }
  }

  /**
   * Test HadoopRpcOMFailoverProxyProvider failover on connection exception
   * to OM client.
   */
  @Test
  public void testOMProxyProviderFailoverOnConnectionFailure()
      throws Exception {
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
  @Order(Integer.MAX_VALUE)
  void testOMRestart() throws Exception {
    // start fresh cluster
    shutdown();
    init();

    ObjectStore objectStore = getObjectStore();
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = getCluster().getOzoneManager(leaderOMNodeId);

    // Get follower OMs
    OzoneManager followerOM1 = getCluster().getOzoneManager(
        leaderOM.getPeerNodes().get(0).getNodeId());
    OzoneManager followerOM2 = getCluster().getOzoneManager(
        leaderOM.getPeerNodes().get(1).getNodeId());

    // Do some transactions so that the log index increases
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);

    ozoneVolume.createBucket(bucketName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }

    final long followerOM1LastAppliedIndex =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();

    // Stop one follower OM
    followerOM1.stop();

    // Do more transactions. Stopped OM should miss these transactions and
    // the logs corresponding to at least some missed transactions
    // should be purged. This will force the OM to install snapshot when
    // restarted.
    long minNewTxIndex = followerOM1LastAppliedIndex + getLogPurgeGap() * 10L;
    while (leaderOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
        < minNewTxIndex) {
      createKey(ozoneBucket);
    }

    // Get the latest snapshotIndex from the leader OM.
    final long leaderOMSnaphsotIndex = leaderOM.getRatisSnapshotIndex();

    // The stopped OM should be lagging behind the leader OM.
    assertTrue(followerOM1LastAppliedIndex < leaderOMSnaphsotIndex);

    // Restart the stopped OM.
    followerOM1.restart();

    // Wait for the follower OM to catch up
    GenericTestUtils.waitFor(() -> followerOM1.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex() >= leaderOMSnaphsotIndex,
        100, 200000);

    // Do more transactions. The restarted OM should receive the
    // new transactions. It's last applied tx index should increase from the
    // last snapshot index after more transactions are applied.
    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }

    final long followerOM1LastAppliedIndexNew =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOM1LastAppliedIndexNew > leaderOMSnaphsotIndex);
  }

  @Test
  void testListParts() throws Exception {

    OzoneBucket ozoneBucket = setupBucket();
    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, createMultipartUploadPartKey(ozoneBucket, 1, keyName,
        uploadID));
    partsMap.put(2, createMultipartUploadPartKey(ozoneBucket, 2, keyName,
        uploadID));
    partsMap.put(3, createMultipartUploadPartKey(ozoneBucket, 3, keyName,
        uploadID));

    validateListParts(ozoneBucket, keyName, uploadID, partsMap);

    // Stop leader OM, and then validate list parts.
    stopLeaderOM();
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);

    validateListParts(ozoneBucket, keyName, uploadID, partsMap);

  }

  /**
   * Validate parts uploaded to a MPU Key.
   */
  private void validateListParts(OzoneBucket ozoneBucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        ozoneBucket.listParts(keyName, uploadID, 0, 1000);

    List<OzoneMultipartUploadPartListParts.PartInfo> partInfoList =
        ozoneMultipartUploadPartListParts.getPartInfoList();

    assertEquals(partInfoList.size(), partsMap.size());

    for (int i = 0; i < partsMap.size(); i++) {
      assertEquals(partsMap.get(partInfoList.get(i).getPartNumber()),
          partInfoList.get(i).getETag());

    }

    Assertions.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  /**
   * Create a Multipart upload part Key with specified partNumber and uploadID.
   * @return Part name for the uploaded part.
   */
  private String createMultipartUploadPartKey(OzoneBucket ozoneBucket,
      int partNumber, String keyName, String uploadID) throws Exception {
    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), partNumber, uploadID);
    ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(value));
    ozoneOutputStream.close();

    return ozoneOutputStream.getCommitUploadPartInfo().getETag();
  }

  @Test
  public void testConf() {
    final RaftProperties p = getCluster()
        .getOzoneManager()
        .getOmRatisServer()
        .getServer()
        .getProperties();
    final TimeDuration t = RaftServerConfigKeys.Log.Appender.waitTimeMin(p);
    assertEquals(TimeDuration.ZERO, t,
        RaftServerConfigKeys.Log.Appender.WAIT_TIME_MIN_KEY);
  }

  @Test
  public void testKeyDeletion() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String data = "random data";
    String keyName1 = "dir/file1";
    String keyName2 = "dir/file2";
    String keyName3 = "dir/file3";
    String keyName4 = "dir/file4";

    testCreateFile(ozoneBucket, keyName1, data, true, false);
    testCreateFile(ozoneBucket, keyName2, data, true, false);
    testCreateFile(ozoneBucket, keyName3, data, true, false);
    testCreateFile(ozoneBucket, keyName4, data, true, false);

    ozoneBucket.deleteKey(keyName1);
    ozoneBucket.deleteKey(keyName2);
    ozoneBucket.deleteKey(keyName3);
    ozoneBucket.deleteKey(keyName4);

    // Now check delete table has entries been removed.

    OzoneManager ozoneManager = getCluster().getOMLeader();

    KeyDeletingService keyDeletingService =
        (KeyDeletingService) ozoneManager.getKeyManager().getDeletingService();

    // Check on leader OM Count.
    GenericTestUtils.waitFor(() ->
        keyDeletingService.getRunCount().get() >= 2, 10000, 120000);
    GenericTestUtils.waitFor(() ->
        keyDeletingService.getDeletedKeyCount().get() == 4, 10000, 120000);

    // Check delete table is empty or not on all OMs.
    getCluster().getOzoneManagersList().forEach((om) -> {
      try {
        GenericTestUtils.waitFor(() -> {
          Table<String, RepeatedOmKeyInfo> deletedTable =
              om.getMetadataManager().getDeletedTable();
          try (TableIterator<?, ?> iterator = deletedTable.iterator()) {
            return !iterator.hasNext();
          } catch (Exception ex) {
            return false;
          }
        },
            10000, 120000);
      } catch (Exception ex) {
        Assertions.fail("TestOzoneManagerHAKeyDeletion failed");
      }
    });
  }
  /**
   * 1. Stop one of the OM
   * 2. make a call to OM, this will make failover attempts to find new node.
   * a) if LE finishes but leader not ready, it retries to same node
   * b) if LE not done, it will failover to new node and check
   * 3. Try failover to same OM explicitly.
   * Now #3 should wait additional waitBetweenRetries time.
   * LE: Leader Election.
   */
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
  void testOMHAMetrics() throws Exception {
    // Get leader OM
    OzoneManager leaderOM = getCluster().getOMLeader();
    // Store current leader's node ID,
    // to use it after restarting the OM
    String leaderOMId = leaderOM.getOMNodeId();
    // Get a list of all OMs
    List<OzoneManager> omList = getCluster().getOzoneManagersList();
    // Check metrics for all OMs
    checkOMHAMetricsForAllOMs(omList, leaderOMId);

    // Restart leader OM
    getCluster().shutdownOzoneManager(leaderOM);
    getCluster().restartOzoneManager(leaderOM, true);
    waitForLeaderToBeReady();

    // Get the new leader
    OzoneManager newLeaderOM = getCluster().getOMLeader();
    String newLeaderOMId = newLeaderOM.getOMNodeId();
    // Get a list of all OMs again
    omList = getCluster().getOzoneManagersList();
    // New state for the old leader
    int newState = leaderOMId.equals(newLeaderOMId) ? 1 : 0;

    // Get old leader
    OzoneManager oldLeader = getCluster().getOzoneManager(leaderOMId);
    // Get old leader's metrics
    OMHAMetrics omhaMetrics = oldLeader.getOmhaMetrics();

    assertEquals(newState,
        omhaMetrics.getOmhaInfoOzoneManagerHALeaderState());

    // Check that metrics for all OMs have been updated
    checkOMHAMetricsForAllOMs(omList, newLeaderOMId);
  }

  private void checkOMHAMetricsForAllOMs(List<OzoneManager> omList,
      String leaderOMId) {
    for (OzoneManager om : omList) {
      // Get OMHAMetrics for the current OM
      OMHAMetrics omhaMetrics = om.getOmhaMetrics();
      String nodeId = om.getOMNodeId();

      // If current OM is leader, state should be 1
      int expectedState = nodeId
          .equals(leaderOMId) ? 1 : 0;
      assertEquals(expectedState,
          omhaMetrics.getOmhaInfoOzoneManagerHALeaderState());

      assertEquals(nodeId, omhaMetrics.getOmhaInfoNodeId());
    }
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

  @Test
  void testListVolumes() throws Exception {
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    ObjectStore objectStore = getObjectStore();

    String prefix = "vol-" + RandomStringUtils.randomNumeric(10) + "-";
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(userName)
        .build();

    Set<String> expectedVolumes = new TreeSet<>();
    for (int i = 0; i < 100; i++) {
      String volumeName = prefix + i;
      expectedVolumes.add(volumeName);
      objectStore.createVolume(volumeName, createVolumeArgs);
    }

    validateVolumesList(expectedVolumes,
        objectStore.listVolumesByUser(userName, prefix, ""));

    // Stop leader OM, and then validate list volumes for user.
    stopLeaderOM();
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    validateVolumesList(expectedVolumes,
        objectStore.listVolumesByUser(userName, prefix, ""));
  }

  private void validateVolumesList(Set<String> expectedVolumes,
      Iterator<? extends OzoneVolume> volumeIterator) {
    int expectedCount = 0;

    while (volumeIterator.hasNext()) {
      OzoneVolume next = volumeIterator.next();
      assertTrue(expectedVolumes.contains(next.getName()));
      expectedCount++;
    }

    assertEquals(expectedVolumes.size(), expectedCount);
  }

}

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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_DELETE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestOzoneManagerHAWithData extends TestOzoneManagerHA {

  /**
   * Test a client request when all OM nodes are running. The request should
   * succeed. Repeat with one OM node down.
   */
  @Test
  void testAllOMNodesRunningAndOneDown() throws Exception {
    createVolumeTest(true);
    createKeyTest(true);

    // Repeat the test with one OM down
    getCluster().stopOzoneManager(1);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);

    createVolumeTest(true);

    createKeyTest(true);
  }

  /**
   * Test client request fails when 2 OMs are down.
   */
  @Test
  void testTwoOMNodesDown() throws Exception {
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

  @Test
  void testFileOperationsAndDelete() throws Exception {
    testFileOperationsWithRecursive();
    testFileOperationsWithNonRecursive();
    testKeysDelete();
  }

  private void testFileOperationsWithRecursive() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();

    String data = "random data";

    // one level key name
    String keyName = UUID.randomUUID().toString();
    testCreateFile(ozoneBucket, keyName, data, true, false);

    // multi level key name
    keyName = "dir1/dir2/dir3/file1";
    testCreateFile(ozoneBucket, keyName, data, true, false);


    data = "random data random data";

    // multi level key name with over write set.
    testCreateFile(ozoneBucket, keyName, data, true, true);


    try {
      testCreateFile(ozoneBucket, keyName, data, true, false);
      Assertions.fail("testFileOperationsWithRecursive");
    } catch (OMException ex) {
      Assertions.assertEquals(FILE_ALREADY_EXISTS, ex.getResult());
    }

    // Try now with a file name which is same as a directory.
    try {
      keyName = "folder/folder2";
      ozoneBucket.createDirectory(keyName);
      testCreateFile(ozoneBucket, keyName, data, true, false);
      Assertions.fail("testFileOperationsWithNonRecursive");
    } catch (OMException ex) {
      Assertions.assertEquals(NOT_A_FILE, ex.getResult());
    }

  }

  private void testKeysDelete() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String data = "random data";
    String keyName1 = "dir/file1";
    String keyName2 = "dir/file2";
    String keyName3 = "dir/file3";
    String keyName4 = "dir/file4";
    List<String> keyList1 = new ArrayList<>();
    keyList1.add(keyName2);
    keyList1.add(keyName3);

    testCreateFile(ozoneBucket, keyName1, data, true, false);
    testCreateFile(ozoneBucket, keyName2, data, true, false);
    testCreateFile(ozoneBucket, keyName3, data, true, false);
    testCreateFile(ozoneBucket, keyName4, data, true, false);

    // Delete keyName1 use deleteKey api.
    ozoneBucket.deleteKey(keyName1);

    // Delete keyName2 and keyName3 in keyList1 using the deleteKeys api.
    ozoneBucket.deleteKeys(keyList1);

    // In keyList2 keyName3 was previously deleted and KeyName4 exists .
    List<String> keyList2 = new ArrayList<>();
    keyList2.add(keyName3);
    keyList2.add(keyName4);

    // Because keyName3 has been deleted, there should be a KEY_NOT_FOUND
    // exception. In this case, we test for deletion failure.
    try {
      ozoneBucket.deleteKeys(keyList2);
      Assertions.fail("testFilesDelete");
    } catch (OMException ex) {
      // The expected exception PARTIAL_DELETE, as if not able to delete, we
      // return error codee PARTIAL_DElETE.
      Assertions.assertEquals(PARTIAL_DELETE, ex.getResult());
    }
  }


  private void testFileOperationsWithNonRecursive() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();

    String data = "random data";

    // one level key name
    String keyName = UUID.randomUUID().toString();
    testCreateFile(ozoneBucket, keyName, data, false, false);

    // multi level key name
    keyName = "dir1/dir2/dir3/file1";

    // Should fail, as this is non-recursive and no parent directories exist
    try {
      testCreateFile(ozoneBucket, keyName, data, false, false);
    } catch (OMException ex) {
      Assertions.assertEquals(DIRECTORY_NOT_FOUND, ex.getResult());
    }

    // create directory, now this should pass.
    ozoneBucket.createDirectory("dir1/dir2/dir3");
    testCreateFile(ozoneBucket, keyName, data, false, false);
    data = "random data random data";

    // multi level key name with over write set.
    testCreateFile(ozoneBucket, keyName, data, false, true);

    try {
      testCreateFile(ozoneBucket, keyName, data, false, false);
      Assertions.fail("testFileOperationsWithRecursive");
    } catch (OMException ex) {
      Assertions.assertEquals(FILE_ALREADY_EXISTS, ex.getResult());
    }


    // Try now with a file which already exists under the path
    ozoneBucket.createDirectory("folder1/folder2/folder3/folder4");

    keyName = "folder1/folder2/folder3/folder4/file1";
    testCreateFile(ozoneBucket, keyName, data, false, false);

    keyName = "folder1/folder2/folder3/file1";
    testCreateFile(ozoneBucket, keyName, data, false, false);

    // Try now with a file under path already. This should fail.
    try {
      keyName = "folder/folder2";
      ozoneBucket.createDirectory(keyName);
      testCreateFile(ozoneBucket, keyName, data, false, false);
      Assertions.fail("testFileOperationsWithNonRecursive");
    } catch (OMException ex) {
      Assertions.assertEquals(NOT_A_FILE, ex.getResult());
    }

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

    Assertions.assertNotEquals(leaderOMNodeId, newLeaderOMNodeId);
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
    ozoneOutputStream.close();


    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, ozoneOutputStream.getCommitUploadPartInfo().getPartName());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        ozoneBucket.completeMultipartUpload(keyName, uploadID, partsMap);

    Assertions.assertNotNull(omMultipartUploadCompleteInfo);
    Assertions.assertNotNull(omMultipartUploadCompleteInfo.getHash());


    try (OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName)) {
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      ozoneInputStream.read(fileContent);
      Assertions.assertEquals(value, new String(fileContent, UTF_8));
    }
  }

  @Test
  void testOMRatisSnapshot() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName);
    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager ozoneManager = getCluster().getOzoneManager(leaderOMNodeId);

    // Send commands to ratis to increase the log index so that ratis
    // triggers a snapshot on the state machine.

    long appliedLogIndex = 0;
    while (appliedLogIndex <= getSnapshotThreshold()) {
      createKey(ozoneBucket);
      appliedLogIndex = ozoneManager.getOmRatisServer()
          .getLastAppliedTermIndex().getIndex();
    }

    GenericTestUtils.waitFor(() -> {
      try {
        if (ozoneManager.getRatisSnapshotIndex() > 0) {
          return true;
        }
      } catch (IOException ex) {
        Assertions.fail("test failed during transactionInfo read");
      }
      return false;
    }, 1000, 100000);

    // The current lastAppliedLogIndex on the state machine should be greater
    // than or equal to the saved snapshot index.
    long smLastAppliedIndex =
        ozoneManager.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    long ratisSnapshotIndex = ozoneManager.getRatisSnapshotIndex();
    Assertions.assertTrue(smLastAppliedIndex >= ratisSnapshotIndex,
        "LastAppliedIndex on OM State Machine ("
        + smLastAppliedIndex + ") is less than the saved snapshot index("
        + ratisSnapshotIndex + ").");

    // Add more transactions to Ratis to trigger another snapshot
    while (appliedLogIndex <= (smLastAppliedIndex + getSnapshotThreshold())) {
      createKey(ozoneBucket);
      appliedLogIndex = ozoneManager.getOmRatisServer()
          .getLastAppliedTermIndex().getIndex();
    }

    GenericTestUtils.waitFor(() -> {
      try {
        if (ozoneManager.getRatisSnapshotIndex() > 0) {
          return true;
        }
      } catch (IOException ex) {
        Assertions.fail("test failed during transactionInfo read");
      }
      return false;
    }, 1000, 100000);

    // The new snapshot index must be greater than the previous snapshot index
    long ratisSnapshotIndexNew = ozoneManager.getRatisSnapshotIndex();
    Assertions.assertTrue(ratisSnapshotIndexNew > ratisSnapshotIndex,
        "Latest snapshot index must be greater than previous " +
            "snapshot indices");

  }

  @Test
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
    Assertions.assertTrue(followerOM1LastAppliedIndex < leaderOMSnaphsotIndex);

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
    Assertions.assertTrue(
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
   * @param ozoneBucket
   * @param keyName
   * @param uploadID
   * @param partsMap
   * @throws Exception
   */
  private void validateListParts(OzoneBucket ozoneBucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        ozoneBucket.listParts(keyName, uploadID, 0, 1000);

    List<OzoneMultipartUploadPartListParts.PartInfo> partInfoList =
        ozoneMultipartUploadPartListParts.getPartInfoList();

    Assertions.assertEquals(partInfoList.size(), partsMap.size());

    for (int i = 0; i < partsMap.size(); i++) {
      Assertions.assertEquals(partsMap.get(partInfoList.get(i).getPartNumber()),
          partInfoList.get(i).getPartName());

    }

    Assertions.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  /**
   * Create an Multipart upload part Key with specified partNumber and uploadID.
   * @param ozoneBucket
   * @param partNumber
   * @param keyName
   * @param uploadID
   * @return Part name for the uploaded part.
   * @throws Exception
   */
  private String createMultipartUploadPartKey(OzoneBucket ozoneBucket,
      int partNumber, String keyName, String uploadID) throws Exception {
    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), partNumber, uploadID);
    ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
    ozoneOutputStream.close();

    return ozoneOutputStream.getCommitUploadPartInfo().getPartName();
  }

  @Test
  public void testConf() {
    final RaftProperties p = getCluster()
        .getOzoneManager()
        .getOmRatisServer()
        .getServer()
        .getProperties();
    final TimeDuration t = RaftServerConfigKeys.Log.Appender.waitTimeMin(p);
    Assertions.assertEquals(TimeDuration.ZERO, t,
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
    List<String> keyList1 = new ArrayList<>();
    keyList1.add(keyName2);
    keyList1.add(keyName3);

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
  @Order(Integer.MAX_VALUE)
  void testIncrementalWaitTimeWithSameNodeFailover() throws Exception {
    long waitBetweenRetries = getConf().getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
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
    Assertions.assertEquals((numTimesTriedToSameNode + 1) * waitBetweenRetries,
        omFailoverProxyProvider.getWaitTime());
  }

  @Test
  void testRunAllTests() throws Exception {
    testAddBucketAcl();
    testRemoveBucketAcl();
    testSetBucketAcl();

    testAddKeyAcl();
    testRemoveKeyAcl();
    testSetKeyAcl();

    testAddPrefixAcl();
    testRemovePrefixAcl();
    testSetPrefixAcl();

    testLinkBucketAddBucketAcl();
    testLinkBucketRemoveBucketAcl();
    testLinkBucketSetBucketAcl();

    testLinkBucketAddKeyAcl();
    testLinkBucketRemoveKeyAcl();
    testLinkBucketSetKeyAcl();

  }

  void testAddBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  void testRemoveBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testRemoveAcl(remoteUserName, ozoneObj, defaultUserAcl);

  }

  void testSetBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testSetAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  private boolean containsAcl(OzoneAcl ozoneAcl, List<OzoneAcl> ozoneAcls) {
    for (OzoneAcl acl : ozoneAcls) {
      boolean result = compareAcls(ozoneAcl, acl);
      if (result) {
        // We found a match, return.
        return result;
      }
    }
    return false;
  }

  private boolean compareAcls(OzoneAcl givenAcl, OzoneAcl existingAcl) {
    if (givenAcl.getType().equals(existingAcl.getType())
        && givenAcl.getName().equals(existingAcl.getName())
        && givenAcl.getAclScope().equals(existingAcl.getAclScope())) {
      BitSet bitSet = (BitSet) givenAcl.getAclBitSet().clone();
      bitSet.and(existingAcl.getAclBitSet());
      if (bitSet.equals(existingAcl.getAclBitSet())) {
        return true;
      }
    }
    return false;
  }

  void testAddKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testAddAcl(remoteUserName, ozoneObj, userAcl);
  }

  void testRemoveKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testRemoveAcl(remoteUserName, ozoneObj, userAcl);

  }

  void testSetKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testSetAcl(remoteUserName, ozoneObj, userAcl);

  }

  void testAddPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) + "/";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  void testRemovePrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) + "/";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, ACCESS);
    OzoneAcl userAcl1 = new OzoneAcl(USER, "remote",
        READ, ACCESS);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    ObjectStore objectStore = getObjectStore();

    boolean result = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertTrue(result);

    result = objectStore.addAcl(ozoneObj, userAcl1);
    Assertions.assertTrue(result);

    result = objectStore.removeAcl(ozoneObj, userAcl);
    Assertions.assertTrue(result);

    // try removing already removed acl.
    result = objectStore.removeAcl(ozoneObj, userAcl);
    Assertions.assertFalse(result);

    result = objectStore.removeAcl(ozoneObj, userAcl1);
    Assertions.assertTrue(result);

  }

  void testSetPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) + "/";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    testSetAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  void testLinkBucketAddBucketAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);

    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);

    // Add ACL to the LINK and verify that it is added to the source bucket
    OzoneAcl acl1 = new OzoneAcl(USER, "remoteUser1", READ, DEFAULT);
    boolean addAcl = getObjectStore().addAcl(linkObj, acl1);
    Assertions.assertTrue(addAcl);
    assertEqualsAcls(srcObj, linkObj);

    // Add ACL to the SOURCE and verify that it from link
    OzoneAcl acl2 = new OzoneAcl(USER, "remoteUser2", WRITE, DEFAULT);
    boolean addAcl2 = getObjectStore().addAcl(srcObj, acl2);
    Assertions.assertTrue(addAcl2);
    assertEqualsAcls(srcObj, linkObj);

  }

  void testLinkBucketRemoveBucketAcl() throws Exception {
    // case1 : test remove link acl
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);
    // As by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls = getObjectStore().getAcl(linkObj);
    Assertions.assertTrue(acls.size() > 0);
    // Remove an existing acl.
    boolean removeAcl = getObjectStore().removeAcl(linkObj, acls.get(0));
    Assertions.assertTrue(removeAcl);
    assertEqualsAcls(srcObj, linkObj);

    // case2 : test remove src acl
    OzoneBucket srcBucket2 = setupBucket();
    OzoneBucket linkedBucket2 = linkBucket(srcBucket2);
    OzoneObj linkObj2 = buildBucketObj(linkedBucket2);
    OzoneObj srcObj2 = buildBucketObj(srcBucket2);
    // As by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls2 = getObjectStore().getAcl(srcObj2);
    Assertions.assertTrue(acls2.size() > 0);
    // Remove an existing acl.
    boolean removeAcl2 = getObjectStore().removeAcl(srcObj2, acls.get(0));
    Assertions.assertTrue(removeAcl2);
    assertEqualsAcls(srcObj2, linkObj2);

  }

  void testLinkBucketSetBucketAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);

    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);

    // Set ACL to the LINK and verify that it is set to the source bucket
    List<OzoneAcl> acl1 = Collections.singletonList(
        new OzoneAcl(USER, "remoteUser1", READ, DEFAULT));
    boolean setAcl1 = getObjectStore().setAcl(linkObj, acl1);
    Assertions.assertTrue(setAcl1);
    assertEqualsAcls(srcObj, linkObj);

    // Set ACL to the SOURCE and verify that it from link
    List<OzoneAcl> acl2 = Collections.singletonList(
        new OzoneAcl(USER, "remoteUser2", WRITE, DEFAULT));
    boolean setAcl2 = getObjectStore().setAcl(srcObj, acl2);
    Assertions.assertTrue(setAcl2);
    assertEqualsAcls(srcObj, linkObj);

  }

  void testLinkBucketAddKeyAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = new OzoneAcl(USER, user1, READ, DEFAULT);
    testAddAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = new OzoneAcl(USER, user2, READ, DEFAULT);
    testAddAcl(user2, srcObj, acl2);  // case2: set src acl
    assertEqualsAcls(srcObj, linkObj);

  }

  void testLinkBucketRemoveKeyAcl() throws Exception {

    // CASE 1: from link bucket
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);
    String user = "remoteUser1";
    OzoneAcl acl = new OzoneAcl(USER, user, READ, DEFAULT);
    testRemoveAcl(user, linkObj, acl);
    assertEqualsAcls(srcObj, linkObj);

    // CASE 2: from src bucket
    OzoneBucket srcBucket2 = setupBucket();
    OzoneBucket linkedBucket2 = linkBucket(srcBucket2);
    String key2 = createKey(srcBucket2);
    OzoneObj linkObj2 = buildKeyObj(linkedBucket2, key2);
    OzoneObj srcObj2 = buildKeyObj(srcBucket2, key2);
    String user2 = "remoteUser2";
    OzoneAcl acl2 = new OzoneAcl(USER, user2, READ, DEFAULT);
    testRemoveAcl(user2, srcObj2, acl2);
    assertEqualsAcls(srcObj2, linkObj2);

  }

  void testLinkBucketSetKeyAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = new OzoneAcl(USER, user1, READ, DEFAULT);
    testSetAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = new OzoneAcl(USER, user2, READ, DEFAULT);
    testSetAcl(user2, srcObj, acl2);  // case2: set src acl
    assertEqualsAcls(srcObj, linkObj);

  }

  private OzoneObj buildBucketObj(OzoneBucket bucket) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName()).build();
  }

  private OzoneObj buildKeyObj(OzoneBucket bucket, String key) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(key).build();
  }

  private OzoneObj buildPrefixObj(OzoneBucket bucket, String prefix) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setPrefixName(prefix).build();
  }

  private void assertEqualsAcls(OzoneObj srcObj, OzoneObj linkObj)
      throws IOException {
    if (linkObj.getResourceType() == OzoneObj.ResourceType.BUCKET) {
      linkObj = getSourceBucketObj(linkObj);
    }
    Assertions.assertEquals(getObjectStore().getAcl(srcObj),
        getObjectStore().getAcl(linkObj));
  }

  private OzoneObj getSourceBucketObj(OzoneObj obj)
      throws IOException {
    assert obj.getResourceType() == OzoneObj.ResourceType.BUCKET;
    OzoneBucket bucket = getObjectStore()
        .getVolume(obj.getVolumeName())
        .getBucket(obj.getBucketName());
    if (!bucket.isLink()) {
      return obj;
    }
    obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucket.getSourceBucket())
        .setVolumeName(bucket.getSourceVolume())
        .setKeyName(obj.getKeyName())
        .setResType(obj.getResourceType())
        .setStoreType(obj.getStoreType())
        .build();
    return getSourceBucketObj(obj);
  }

  private void testSetAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    // As by default create will add some default acls in RpcClient.

    ObjectStore objectStore = getObjectStore();
    if (!ozoneObj.getResourceType().name().equals(
        OzoneObj.ResourceType.PREFIX.name())) {
      List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

      Assertions.assertTrue(acls.size() > 0);
    }

    OzoneAcl modifiedUserAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);

    List<OzoneAcl> newAcls = Collections.singletonList(modifiedUserAcl);
    boolean setAcl = objectStore.setAcl(ozoneObj, newAcls);
    Assertions.assertTrue(setAcl);

    // Get acls and check whether they are reset or not.
    List<OzoneAcl> getAcls = objectStore.getAcl(ozoneObj);

    Assertions.assertEquals(newAcls.size(), getAcls.size());
    int i = 0;
    for (OzoneAcl ozoneAcl : newAcls) {
      Assertions.assertTrue(compareAcls(getAcls.get(i++), ozoneAcl));
    }

  }

  private void testAddAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();
    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertTrue(addAcl);

    List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

    Assertions.assertTrue(containsAcl(userAcl, acls));

    // Add an already existing acl.
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertFalse(addAcl);

    // Add an acl by changing acl type with same type, name and scope.
    userAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertTrue(addAcl);
  }

  private void testAddLinkAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();
    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertTrue(addAcl);

    List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

    Assertions.assertTrue(containsAcl(userAcl, acls));

    // Add an already existing acl.
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertFalse(addAcl);

    // Add an acl by changing acl type with same type, name and scope.
    userAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertTrue(addAcl);
  }

  private void testRemoveAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();

    // As by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

    Assertions.assertTrue(acls.size() > 0);

    // Remove an existing acl.
    boolean removeAcl = objectStore.removeAcl(ozoneObj, acls.get(0));
    Assertions.assertTrue(removeAcl);

    // Trying to remove an already removed acl.
    removeAcl = objectStore.removeAcl(ozoneObj, acls.get(0));
    Assertions.assertFalse(removeAcl);

    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assertions.assertTrue(addAcl);

    // Just changed acl type here to write, rest all is same as defaultUserAcl.
    OzoneAcl modifiedUserAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);
    addAcl = objectStore.addAcl(ozoneObj, modifiedUserAcl);
    Assertions.assertTrue(addAcl);

    removeAcl = objectStore.removeAcl(ozoneObj, modifiedUserAcl);
    Assertions.assertTrue(removeAcl);

    removeAcl = objectStore.removeAcl(ozoneObj, userAcl);
    Assertions.assertTrue(removeAcl);
  }
}

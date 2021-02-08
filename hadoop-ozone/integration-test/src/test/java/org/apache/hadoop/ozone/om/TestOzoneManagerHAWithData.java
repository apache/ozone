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
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_DELETE;
import static org.junit.Assert.fail;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
public class TestOzoneManagerHAWithData extends TestOzoneManagerHA {

  /**
   * Test a client request when all OM nodes are running. The request should
   * succeed.
   * @throws Exception
   */
  @Test
  public void testAllOMNodesRunning() throws Exception {
    createVolumeTest(true);
    createKeyTest(true);
  }

  /**
   * Test client request succeeds even if one OM is down.
   */
  @Test
  public void testOneOMNodeDown() throws Exception {
    getCluster().stopOzoneManager(1);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    createVolumeTest(true);

    createKeyTest(true);
  }

  /**
   * Test client request fails when 2 OMs are down.
   */
  @Ignore("This test is failing randomly. It will be enabled after fixing it.")
  @Test
  public void testTwoOMNodesDown() throws Exception {
    getCluster().stopOzoneManager(1);
    getCluster().stopOzoneManager(2);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    createVolumeTest(false);

    createKeyTest(false);

  }

  @Test
  public void testMultipartUpload() throws Exception {

    // Happy scenario when all OM's are up.
    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

  }


  @Test
  public void testFileOperationsWithRecursive() throws Exception {
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
      fail("testFileOperationsWithRecursive");
    } catch (OMException ex) {
      Assert.assertEquals(FILE_ALREADY_EXISTS, ex.getResult());
    }

    // Try now with a file name which is same as a directory.
    try {
      keyName = "folder/folder2";
      ozoneBucket.createDirectory(keyName);
      testCreateFile(ozoneBucket, keyName, data, true, false);
      fail("testFileOperationsWithNonRecursive");
    } catch (OMException ex) {
      Assert.assertEquals(NOT_A_FILE, ex.getResult());
    }

  }

  @Test
  public void testKeysDelete() throws Exception {
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
      fail("testFilesDelete");
    } catch (OMException ex) {
      // The expected exception PARTIAL_DELETE, as if not able to delete, we
      // return error codee PARTIAL_DElETE.
      Assert.assertEquals(PARTIAL_DELETE, ex.getResult());
    }
  }


  @Test
  public void testFileOperationsWithNonRecursive() throws Exception {
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
      Assert.assertEquals(DIRECTORY_NOT_FOUND, ex.getResult());
    }

    // create directory, now this should pass.
    ozoneBucket.createDirectory("dir1/dir2/dir3");
    testCreateFile(ozoneBucket, keyName, data, false, false);
    data = "random data random data";

    // multi level key name with over write set.
    testCreateFile(ozoneBucket, keyName, data, false, true);

    try {
      testCreateFile(ozoneBucket, keyName, data, false, false);
      fail("testFileOperationsWithRecursive");
    } catch (OMException ex) {
      Assert.assertEquals(FILE_ALREADY_EXISTS, ex.getResult());
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
      fail("testFileOperationsWithNonRecursive");
    } catch (OMException ex) {
      Assert.assertEquals(NOT_A_FILE, ex.getResult());
    }

  }

  @Test
  public void testMultipartUploadWithOneOmNodeDown() throws Exception {

    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    // After initiate multipartupload, shutdown leader OM.
    // Stop leader OM, to see when the OM leader changes
    // multipart upload is happening successfully or not.

    OMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFailoverProxyProvider(getObjectStore().getClientProxy());

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    getCluster().stopOzoneManager(leaderOMNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    Assert.assertTrue(leaderOMNodeId != newLeaderOMNodeId);
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
  public void testIncrementalWaitTimeWithSameNodeFailover() throws Exception {
    long waitBetweenRetries = getConf().getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
    OMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFailoverProxyProvider(getObjectStore().getClientProxy());

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    getCluster().stopOzoneManager(leaderOMNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);
    createKeyTest(true); // failover should happen to new node

    long numTimesTriedToSameNode = omFailoverProxyProvider.getWaitTime()
        / waitBetweenRetries;
    omFailoverProxyProvider.performFailoverIfRequired(omFailoverProxyProvider.
        getCurrentProxyOMNodeId());
    Assert.assertEquals((numTimesTriedToSameNode + 1) * waitBetweenRetries,
        omFailoverProxyProvider.getWaitTime());
  }


  private String initiateMultipartUpload(OzoneBucket ozoneBucket,
      String keyName) throws Exception {

    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName,
            ReplicationType.RATIS,
            ReplicationFactor.ONE);

    String uploadID = omMultipartInfo.getUploadID();
    Assert.assertTrue(uploadID != null);
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

    Assert.assertTrue(omMultipartUploadCompleteInfo != null);
    Assert.assertTrue(omMultipartUploadCompleteInfo.getHash() != null);


    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName);

    byte[] fileContent = new byte[value.getBytes(UTF_8).length];
    ozoneInputStream.read(fileContent);
    Assert.assertEquals(value, new String(fileContent, UTF_8));
  }


  private void createKeyTest(boolean checkSuccess) throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    try {
      getObjectStore().createVolume(volumeName, createVolumeArgs);

      OzoneVolume retVolumeinfo = getObjectStore().getVolume(volumeName);

      Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
      Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
      Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));

      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();
      retVolumeinfo.createBucket(bucketName);

      OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

      Assert.assertTrue(ozoneBucket.getName().equals(bucketName));
      Assert.assertTrue(ozoneBucket.getVolumeName().equals(volumeName));

      String value = "random data";
      OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
          value.length(), ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE, new HashMap<>());
      ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
      ozoneOutputStream.close();

      OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName);

      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      ozoneInputStream.read(fileContent);
      Assert.assertEquals(value, new String(fileContent, UTF_8));

    } catch (ConnectException | RemoteException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          GenericTestUtils.assertExceptionContains(
              "OMNotLeaderException", e);
        }
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testOMRatisSnapshot() throws Exception {
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
        fail("test failed during transactionInfo read");
      }
      return false;
    }, 1000, 100000);

    // The current lastAppliedLogIndex on the state machine should be greater
    // than or equal to the saved snapshot index.
    long smLastAppliedIndex =
        ozoneManager.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    long ratisSnapshotIndex = ozoneManager.getRatisSnapshotIndex();
    Assert.assertTrue("LastAppliedIndex on OM State Machine ("
            + smLastAppliedIndex + ") is less than the saved snapshot index("
            + ratisSnapshotIndex + ").",
        smLastAppliedIndex >= ratisSnapshotIndex);

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
        fail("test failed during transactionInfo read");
      }
      return false;
    }, 1000, 100000);

    // The new snapshot index must be greater than the previous snapshot index
    long ratisSnapshotIndexNew = ozoneManager.getRatisSnapshotIndex();
    Assert.assertTrue("Latest snapshot index must be greater than previous " +
        "snapshot indices", ratisSnapshotIndexNew > ratisSnapshotIndex);

  }

  @Test
  public void testOMRestart() throws Exception {
    ObjectStore objectStore = getObjectStore();
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = getCluster().getOzoneManager(leaderOMNodeId);

    // Get follower OMs
    OzoneManager followerOM1 = getCluster().getOzoneManager(
        leaderOM.getPeerNodes().get(0).getOMNodeId());
    OzoneManager followerOM2 = getCluster().getOzoneManager(
        leaderOM.getPeerNodes().get(1).getOMNodeId());

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
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName);
    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }

    long lastAppliedTxOnFollowerOM =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();

    // Stop one follower OM
    followerOM1.stop();

    // Do more transactions. Stopped OM should miss these transactions and
    // the logs corresponding to atleast some of the missed transactions
    // should be purged. This will force the OM to install snapshot when
    // restarted.
    long minNewTxIndex = lastAppliedTxOnFollowerOM + (getLogPurgeGap() * 10);
    long leaderOMappliedLogIndex = leaderOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();

    List<String> missedKeys = new ArrayList<>();
    while (leaderOMappliedLogIndex < minNewTxIndex) {
      missedKeys.add(createKey(ozoneBucket));
      leaderOMappliedLogIndex = leaderOM.getOmRatisServer()
          .getLastAppliedTermIndex().getIndex();
    }

    // Restart the stopped OM.
    followerOM1.restart();

    // Get the latest snapshotIndex from the leader OM.
    long leaderOMSnaphsotIndex = leaderOM.getRatisSnapshotIndex();

    // The recently started OM should be lagging behind the leader OM.
    long followerOMLastAppliedIndex =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    Assert.assertTrue(
        followerOMLastAppliedIndex < leaderOMSnaphsotIndex);

    // Wait for the follower OM to catch up
    GenericTestUtils.waitFor(() -> {
      long lastAppliedIndex =
          followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();
      if (lastAppliedIndex >= leaderOMSnaphsotIndex) {
        return true;
      }
      return false;
    }, 100, 200000);

    // Do more transactions. The restarted OM should receive the
    // new transactions. It's last applied tx index should increase from the
    // last snapshot index after more transactions are applied.
    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }
    long followerOM1lastAppliedIndex = followerOM1.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    Assert.assertTrue(followerOM1lastAppliedIndex >
        leaderOMSnaphsotIndex);

  }

  @Test
  public void testListParts() throws Exception {

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
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

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

    Assert.assertTrue(partInfoList.size() == partsMap.size());

    for (int i=0; i< partsMap.size(); i++) {
      Assert.assertEquals(partsMap.get(partInfoList.get(i).getPartNumber()),
          partInfoList.get(i).getPartName());

    }

    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
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
}

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

import static java.util.UUID.randomUUID;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_DELETE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ServiceException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeRequest.Scope;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.junit.jupiter.api.Test;

/**
 * Ozone Manager HA follower read tests where all OMs are running throughout all tests.
 * @see TestOzoneManagerHAFollowerReadWithAllRunning
 */
public class TestOzoneManagerHAFollowerReadWithAllRunning extends TestOzoneManagerHAFollowerRead {

  @Test
  void testOMFollowerReadProxyProviderInitialization() {
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmTestUtil.getFollowerReadFailoverProxyProvider(getObjectStore());

    List<OMProxyInfo<OzoneManagerProtocolPB>> omProxies =
        followerReadFailoverProxyProvider.getOMProxies();

    assertEquals(getNumOfOMs(), omProxies.size());

    for (int i = 0; i < getNumOfOMs(); i++) {
      OzoneManager om = getCluster().getOzoneManager(i);
      InetSocketAddress omRpcServerAddr = om.getOmRpcServerAddr();
      boolean omClientProxyExists = false;
      for (OMProxyInfo<OzoneManagerProtocolPB> omProxyInfo : omProxies) {
        if (omProxyInfo.getAddress().equals(omRpcServerAddr)) {
          omClientProxyExists = true;
          break;
        }
      }
      assertTrue(omClientProxyExists,
          () -> "No Client Proxy for node " + om.getOMNodeId());
    }
  }

  @Test
  void testFollowerReadTargetsFollower() throws Exception {
    ObjectStore objectStore = getObjectStore();
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmTestUtil.getFollowerReadFailoverProxyProvider(objectStore);

    String leaderOMNodeId = getCluster().getOMLeader().getOMNodeId();
    String followerOMNodeId = null;
    for (OzoneManager om : getCluster().getOzoneManagersList()) {
      if (!om.getOMNodeId().equals(leaderOMNodeId)) {
        followerOMNodeId = om.getOMNodeId();
        break;
      }
    }
    assertNotNull(followerOMNodeId);

    followerReadFailoverProxyProvider.changeInitialProxyForTest(followerOMNodeId);
    objectStore.getClientProxy().listVolumes(null, null, 10);

    OMProxyInfo<OzoneManagerProtocolPB> lastProxy =
        (OMProxyInfo<OzoneManagerProtocolPB>) followerReadFailoverProxyProvider.getLastProxy();
    assertNotNull(lastProxy);
    assertEquals(followerOMNodeId, lastProxy.getNodeId());
  }

  /**
   * Choose a follower to send the request, the returned exception should
   * include the suggested leader node.
   */
  @Test
  public void testFailoverWithSuggestedLeader() throws Exception {
    HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider =
        OmTestUtil.getFailoverProxyProvider(getObjectStore());

    // Make sure All OMs are ready.
    createVolumeTest(true);

    String leaderOMNodeId = null;
    OzoneManager followerOM = null;
    for (OzoneManager om: getCluster().getOzoneManagersList()) {
      if (om.isLeaderReady()) {
        leaderOMNodeId = om.getOMNodeId();
      } else if (followerOM == null) {
        followerOM = om;
      }
    }
    assertNotNull(followerOM);
    assertNotNull(leaderOMNodeId);
    String leaderOMAddress = ((OMProxyInfo)
        omFailoverProxyProvider.getOMProxyMap().get(leaderOMNodeId))
        .getAddress().getAddress().toString();
    assertSame(OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER,
        followerOM.getOmRatisServer().getLeaderStatus());

    CreateVolumeRequest.Builder req =
        CreateVolumeRequest.newBuilder();
    VolumeInfo volumeInfo = VolumeInfo.newBuilder()
        .setVolume("testvolume")
        .setAdminName("admin")
        .setOwnerName("admin")
        .build();
    req.setVolumeInfo(volumeInfo);

    OzoneManagerProtocolProtos.OMRequest writeRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(Type.CreateVolume)
            .setCreateVolumeRequest(req)
            .setVersion(ClientVersion.CURRENT_VERSION)
            .setClientId(randomUUID().toString())
            .build();

    OzoneManagerProtocolServerSideTranslatorPB omServerProtocol =
        followerOM.getOmServerProtocol();
    ServiceException ex = assertThrows(ServiceException.class,
        () -> omServerProtocol.submitRequest(null, writeRequest));
    assertThat(ex).hasCauseInstanceOf(OMNotLeaderException.class)
        .hasMessageEndingWith("Suggested leader is OM:" + leaderOMNodeId + "[" + leaderOMAddress + "].");
  }

  /**
   * Test strong read-after-write consistency across clients. This means that
   * after a single client has finished writing, another client should be able
   * to immediately see the changes.
   */
  @Test
  void testLinearizableReadConsistency() throws Exception {
    // Setup another client
    OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
    clientConf.setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    OzoneClient anotherClient = null;
    try {
      anotherClient = OzoneClientFactory.getRpcClient(getOmServiceId(), clientConf);
      ObjectStore anotherObjectStore = anotherClient.getObjectStore();

      // Ensure that the proxy provider of the two clients are not shared
      assertNotSame(
          OmTestUtil.getFailoverProxyProvider(getObjectStore()),
          OmTestUtil.getFailoverProxyProvider(anotherObjectStore));
      HadoopRpcOMFollowerReadFailoverProxyProvider otherClientFollowerReadProxyProvider =
          OmTestUtil.getFollowerReadFailoverProxyProvider(anotherObjectStore);
      assertNotSame(
          OmTestUtil.getFollowerReadFailoverProxyProvider(getObjectStore()),
          otherClientFollowerReadProxyProvider);
      String initialProxyOmNodeId = otherClientFollowerReadProxyProvider.getCurrentProxy().getNodeId();

      // Setup the bucket and create a key with the default client
      OzoneBucket ozoneBucket = setupBucket();
      String key = createKey(ozoneBucket);

      // Immediately read using another client, this might or might
      // not be sent to the leader. Regardless, the other client should be
      // able to see the read immediately.
      OzoneKey keyReadFromAnotherClient = anotherObjectStore.getClientProxy().headObject(
          ozoneBucket.getVolumeName(), ozoneBucket.getName(), key);
      assertEquals(key, keyReadFromAnotherClient.getName());

      // Create a more keys
      for (int i = 0; i < 100; i++) {
        createKey(ozoneBucket);
      }

      List<OzoneKey> ozoneKeys = anotherObjectStore.getClientProxy().listKeys(
          ozoneBucket.getVolumeName(), ozoneBucket.getName(),
          null, null, 1000);
      assertEquals(101, ozoneKeys.size());
      // Since the OM node is normal, it should not failover
      assertEquals(initialProxyOmNodeId, otherClientFollowerReadProxyProvider.getCurrentProxy().getNodeId());
    } finally {
      IOUtils.closeQuietly(anotherClient);
    }
  }

  @Test
  void testFileOperationsWithRecursive() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();

    String data = "random data";

    // one level key name
    testCreateFile(ozoneBucket, randomUUID().toString(), data, true, false);

    // multi level key name
    String keyName = "dir1/dir2/dir3/file1";
    testCreateFile(ozoneBucket, keyName, data, true, false);

    String newData = "random data random data";

    // multi level key name with overwrite set.
    testCreateFile(ozoneBucket, keyName, newData, true, true);

    OMException ex = assertThrows(OMException.class,
        () -> testCreateFile(ozoneBucket, keyName, "any", true, false));
    assertEquals(FILE_ALREADY_EXISTS, ex.getResult());

    // Try now with a file name which is same as a directory.
    String dir = "folder/folder2";
    ozoneBucket.createDirectory(dir);
    ex = assertThrows(OMException.class,
        () -> testCreateFile(ozoneBucket, dir, "any", true, false));
    assertEquals(NOT_A_FILE, ex.getResult());
  }

  @Test
  void testKeysDelete() throws Exception {
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
    OMException ex = assertThrows(OMException.class,
        () -> ozoneBucket.deleteKeys(keyList2));
    // The expected exception PARTIAL_DELETE, as if not able to delete, we
    // return error codee PARTIAL_DElETE.
    assertEquals(PARTIAL_DELETE, ex.getResult());
  }

  @Test
  void testFileOperationsWithNonRecursive() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();

    String data = "random data";

    // one level key name
    testCreateFile(ozoneBucket, randomUUID().toString(), data, false, false);

    // multi level key name
    String keyName = "dir1/dir2/dir3/file1";

    // Should fail, as this is non-recursive and no parent directories exist
    try {
      testCreateFile(ozoneBucket, keyName, data, false, false);
    } catch (OMException ex) {
      assertEquals(DIRECTORY_NOT_FOUND, ex.getResult());
    }

    // create directory, now this should pass.
    ozoneBucket.createDirectory("dir1/dir2/dir3");
    testCreateFile(ozoneBucket, keyName, data, false, false);
    data = "random data random data";

    // multi level key name with overwrite set.
    testCreateFile(ozoneBucket, keyName, data, false, true);

    OMException ex = assertThrows(OMException.class,
        () -> testCreateFile(ozoneBucket, keyName, "any", false, false));
    assertEquals(FILE_ALREADY_EXISTS, ex.getResult());

    // Try now with a file which already exists under the path
    ozoneBucket.createDirectory("folder1/folder2/folder3/folder4");

    testCreateFile(ozoneBucket, "folder1/folder2/folder3/folder4/file1", data,
        false, false);

    testCreateFile(ozoneBucket, "folder1/folder2/folder3/file1", data, false,
        false);

    // Try now with a file under path already. This should fail.
    String dir = "folder/folder2";
    ozoneBucket.createDirectory(dir);
    ex = assertThrows(OMException.class,
        () -> testCreateFile(ozoneBucket, dir, "any", false, false)
    );
    assertEquals(NOT_A_FILE, ex.getResult());
  }

  private OzoneVolume createAndCheckVolume(String volumeName)
      throws Exception {
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(volumeName, createVolumeArgs);

    OzoneVolume retVolume = objectStore.getVolume(volumeName);

    assertEquals(volumeName, retVolume.getName());
    assertEquals(userName, retVolume.getOwner());
    assertEquals(adminName, retVolume.getAdmin());

    return retVolume;
  }

  @Test
  public void testAllVolumeOperations() throws Exception {
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);

    createAndCheckVolume(volumeName);

    ObjectStore objectStore = getObjectStore();
    objectStore.deleteVolume(volumeName);

    OzoneTestUtils.expectOmException(OMException.ResultCodes.VOLUME_NOT_FOUND,
        () -> objectStore.getVolume(volumeName));

    OzoneTestUtils.expectOmException(OMException.ResultCodes.VOLUME_NOT_FOUND,
        () -> objectStore.deleteVolume(volumeName));
  }

  @Test
  public void testAllBucketOperations() throws Exception {
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "volume" + RandomStringUtils.secure().nextNumeric(5);

    OzoneVolume retVolume = createAndCheckVolume(volumeName);

    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setStorageType(StorageType.DISK)
            .setVersioning(true).build();


    retVolume.createBucket(bucketName, bucketArgs);


    OzoneBucket ozoneBucket = retVolume.getBucket(bucketName);

    assertEquals(volumeName, ozoneBucket.getVolumeName());
    assertEquals(bucketName, ozoneBucket.getName());
    assertTrue(ozoneBucket.getVersioning());
    assertEquals(StorageType.DISK, ozoneBucket.getStorageType());
    assertFalse(ozoneBucket.getCreationTime().isAfter(Instant.now()));


    // Change versioning to false
    ozoneBucket.setVersioning(false);

    ozoneBucket = retVolume.getBucket(bucketName);
    assertFalse(ozoneBucket.getVersioning());

    retVolume.deleteBucket(bucketName);

    OzoneTestUtils.expectOmException(OMException.ResultCodes.BUCKET_NOT_FOUND,
        () -> retVolume.deleteBucket(bucketName));
  }

  @Test
  void testOMResponseLeaderOmNodeId() throws Exception {
    HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider =
        OmTestUtil.getFailoverProxyProvider(getObjectStore());
    HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
        OmTestUtil.getFollowerReadFailoverProxyProvider(getObjectStore());

    // Make sure All OMs are ready
    createVolumeTest(true);

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();
    String initialNextProxyOmNodeId = omFailoverProxyProvider.getNextProxyOMNodeId();
    OzoneManager followerOM = null;
    for (OzoneManager om: getCluster().getOzoneManagersList()) {
      if (!om.isLeaderReady()) {
        followerOM = om;
        break;
      }
    }
    assertNotNull(followerOM);
    assertSame(OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER,
        followerOM.getOmRatisServer().getLeaderStatus());


    ListVolumeRequest req =
        ListVolumeRequest.newBuilder()
            .setScope(Scope.VOLUMES_BY_USER)
            .build();

    OzoneManagerProtocolProtos.OMRequest readRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(Type.ListVolume)
            .setListVolumeRequest(req)
            .setVersion(ClientVersion.CURRENT_VERSION)
            .setClientId(randomUUID().toString())
            .build();

    OmTransport omTransport = ((OzoneManagerProtocolClientSideTranslatorPB)
        getObjectStore().getClientProxy().getOzoneManagerClient()).getTransport();
    followerReadFailoverProxyProvider.changeInitialProxyForTest(followerOM.getOMNodeId());
    OMResponse omResponse = omTransport.submitRequest(readRequest);

    // The returned OM response should be the same as the actual leader OM node ID
    assertEquals(leaderOMNodeId, omResponse.getLeaderOMNodeId());
    // There should not be any change in the leader proxy's next proxy OM node ID
    assertEquals(initialNextProxyOmNodeId, omFailoverProxyProvider.getNextProxyOMNodeId());
  }

  @Test
  void testClientWithFollowerReadDisabled() throws Exception {
    // Setup a client with follower read disabled
    OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
    clientConf.setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, false);
    OzoneClient leaderOnlyClient = null;
    try {
      // This will trigger getServiceId so the client should point to the leader
      leaderOnlyClient = OzoneClientFactory.getRpcClient(getOmServiceId(), clientConf);
      ObjectStore leaderOnlyObjectStore = leaderOnlyClient.getObjectStore();
      assertNull(OmTestUtil.getFollowerReadFailoverProxyProvider(leaderOnlyObjectStore));
      HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxyProvider =
          OmTestUtil.getFailoverProxyProvider(leaderOnlyObjectStore);

      // The OMFailoverProxyProvider will point to the current leader OM node.
      OzoneManager currentLeader = getCluster().getOMLeader();
      assertNotNull(currentLeader);
      String leaderOMNodeId = currentLeader.getOMNodeId();

      assertNotNull(leaderOMNodeId);
      assertEquals(leaderOMNodeId, leaderProxyProvider.getCurrentProxyOMNodeId());

      // Try to transfer leadership
      OzoneManager newLeader = getCluster().transferOMLeadershipToAnotherNode(currentLeader);
      assertNotEquals(currentLeader.getOMNodeId(), newLeader.getOMNodeId());

      // Do some reads and ensure that the client will failover to the new leader
      leaderOnlyObjectStore.getS3Volume();
      assertEquals(newLeader.getOMNodeId(), leaderProxyProvider.getCurrentProxyOMNodeId());
    } finally {
      IOUtils.closeQuietly(leaderOnlyClient);
    }

  }

  @Test
  void testClientWithLinearizableLeaderRead() throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
    clientConf.setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, false);
    clientConf.set(OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_KEY, "LINEARIZABLE_LEADER_ONLY");

    OzoneClient ozoneClient = null;
    try {
      ozoneClient = OzoneClientFactory.getRpcClient(clientConf);
      ObjectStore objectStore = ozoneClient.getObjectStore();
      HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderFailoverProxyProvider =
          OmTestUtil.getFailoverProxyProvider(objectStore);
      OzoneManager leaderOM = getCluster().getOMLeader();
      long previousLinearizableRead = leaderOM.getMetrics().getNumLinearizableRead();

      // Trigger linearizable leader read
      objectStore.listVolumes("");

      // Get the leader OM node ID
      String leaderOMNodeId = leaderFailoverProxyProvider.getCurrentProxyOMNodeId();

      assertEquals(leaderOM.getOMNodeId(), leaderOMNodeId);
      leaderOM.getMetrics().getNumLinearizableRead();
      long currentLinearizableRead = leaderOM.getMetrics().getNumLinearizableRead();
      assertThat(currentLinearizableRead).isGreaterThan(previousLinearizableRead);
    } finally {
      IOUtils.closeQuietly(ozoneClient);
    }
  }

  @Test
  void testClientWithLocalLeaseEnabled() throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
    clientConf.setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    clientConf.set(OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_KEY, "LOCAL_LEASE");
    OzoneClient ozoneClient = null;
    try {
      ozoneClient = OzoneClientFactory.getRpcClient(clientConf);
      ObjectStore objectStore = ozoneClient.getObjectStore();
      HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
          OmTestUtil.getFollowerReadFailoverProxyProvider(objectStore);
      assertNotNull(followerReadFailoverProxyProvider);
      assertTrue(followerReadFailoverProxyProvider.isUseFollowerRead());

      String currentOMNodeId = followerReadFailoverProxyProvider.getCurrentProxy().getNodeId();
      OzoneManager ozoneManager = getCluster().getOzoneManager(currentOMNodeId);
      assertNotNull(ozoneManager);
      assertEquals(currentOMNodeId, ozoneManager.getOMNodeId());

      long previousLocalLeaseSuccess = ozoneManager.getMetrics().getNumFollowerReadLocalLeaseSuccess();

      objectStore.listVolumes("");
      long currentLocalLeaseSuccess = ozoneManager.getMetrics().getNumFollowerReadLocalLeaseSuccess();
      assertThat(currentLocalLeaseSuccess).isGreaterThan(previousLocalLeaseSuccess);
    } finally {
      IOUtils.closeQuietly(ozoneClient);
    }
  }
}

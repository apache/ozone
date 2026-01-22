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
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_DELETE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.ratis.metrics.RatisMetrics.RATIS_APPLICATION_NAME_METRICS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;

/**
 * Ozone Manager HA tests where all OMs are running throughout all tests.
 * @see TestOzoneManagerHAWithStoppedNodes
 */
class TestOzoneManagerHAWithAllRunning extends TestOzoneManagerHA {

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

  /**
   * Test that HadoopRpcOMFailoverProxyProvider creates an OM proxy
   * for each OM in the cluster.
   */
  @Test
  void testOMProxyProviderInitialization() {
    final List<OMProxyInfo<OzoneManagerProtocolPB>> omProxies
        = OmTestUtil.getFailoverProxyProvider(getClient().getObjectStore()).getOMProxies();
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

  /**
   * Test HadoopRpcOMFailoverProxyProvider failover when current OM proxy is not
   * the current OM Leader.
   */
  @Test
  public void testOMProxyProviderFailoverToCurrentLeader() throws Exception {
    ObjectStore objectStore = getObjectStore();
    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(objectStore);

    // Run couple of createVolume tests to discover the current Leader OM
    createVolumeTest(true);
    createVolumeTest(true);

    // The oMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Perform a manual failover of the proxy provider to move the
    // currentProxyIndex to a node other than the leader OM.
    omFailoverProxyProvider.selectNextOmProxy();
    omFailoverProxyProvider.performFailover(null);

    String newProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();
    assertNotEquals(leaderOMNodeId, newProxyNodeId);

    // Once another request is sent to this new proxy node, the leader
    // information must be returned via the response and a failover must
    // happen to the leader proxy node.
    createVolumeTest(true);
    Thread.sleep(2000);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // The old and new Leader OM NodeId must match since there was no new
    // election in the Ratis ring.
    assertEquals(leaderOMNodeId, newLeaderOMNodeId);
  }

  /**
   * Choose a follower to send the request, the returned exception should
   * include the suggested leader node.
   */
  @Test
  public void testFailoverWithSuggestedLeader() throws Exception {
    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(getObjectStore());

    // Make sure All OMs are ready.
    createVolumeTest(true);

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();
    String leaderOMAddress = omFailoverProxyProvider.getOMProxyMap().get(leaderOMNodeId)
        .getAddress().getAddress().toString();
    OzoneManager followerOM = null;
    for (OzoneManager om: getCluster().getOzoneManagersList()) {
      if (!om.isLeaderReady()) {
        followerOM = om;
        break;
      }
    }
    assertNotNull(followerOM);
    assertSame(followerOM.getOmRatisServer().getLeaderStatus(),
        OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER);

    OzoneManagerProtocolProtos.OMRequest readRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.ListVolume)
            .setVersion(ClientVersion.CURRENT_VERSION)
            .setClientId(randomUUID().toString())
            .build();

    OzoneManagerProtocolServerSideTranslatorPB omServerProtocol =
        followerOM.getOmServerProtocol();
    ServiceException ex = assertThrows(ServiceException.class,
        () -> omServerProtocol.submitRequest(null, readRequest));
    assertThat(ex).hasCauseInstanceOf(OMNotLeaderException.class)
        .hasMessageEndingWith("Suggested leader is OM:" + leaderOMNodeId + "[" + leaderOMAddress + "].");
  }

  @Test
  public void testReadRequest() throws Exception {
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(volumeName);

    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(objectStore);

    String leaderId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // A read request from any proxy should failover to the current leader OM
    for (int i = 0; i < getNumOfOMs(); i++) {
      // Failover omFailoverProxyProvider to OM at index i
      OzoneManager ozoneManager = getCluster().getOzoneManager(i);

      // Get the ObjectStore and FailoverProxyProvider for OM at index i
      final ObjectStore store = getClient().getObjectStore();
      final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> proxyProvider
          = OmTestUtil.getFailoverProxyProvider(store);

      // Failover to the OM node that the objectStore points to
      omFailoverProxyProvider.setNextOmProxy(
          ozoneManager.getOMNodeId());

      // A read request should result in the proxyProvider failing over to
      // leader node.
      OzoneVolume volume = store.getVolume(volumeName);
      assertEquals(volumeName, volume.getName());

      assertEquals(leaderId, proxyProvider.getCurrentProxyOMNodeId());
    }
  }

  @Test
  public void testJMXMetrics() throws Exception {
    // Verify any one ratis metric is exposed by JMX MBeanServer
    OzoneManagerRatisServer ratisServer =
        getCluster().getOzoneManager(0).getOmRatisServer();
    ObjectName oname = new ObjectName(RATIS_APPLICATION_NAME_METRICS, "name",
        RATIS_APPLICATION_NAME_METRICS + ".log_worker." +
            ratisServer.getRaftPeerId().toString() +
            "@" + ratisServer.getRaftGroup().getGroupId() + ".flushCount");
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    MBeanInfo mBeanInfo = mBeanServer.getMBeanInfo(oname);
    assertNotNull(mBeanInfo);
    Object flushCount = mBeanServer.getAttribute(oname, "Count");
    assertTrue((long) flushCount >= 0);
  }

  @Test
  public void testOMRetryCache() throws Exception {
    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(randomUUID().toString());


    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(objectStore);

    String currentLeaderNodeId = omFailoverProxyProvider
        .getCurrentProxyOMNodeId();

    OzoneManagerRatisServer ozoneManagerRatisServer =
        getCluster().getOzoneManager(currentLeaderNodeId).getOmRatisServer();

    final RaftServer raftServer = ozoneManagerRatisServer.getServerDivision().getRaftServer();

    ClientId clientId = ClientId.randomId();
    long callId = 2000L;
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    String volumeName = randomUUID().toString();


    LogCapturer logCapturer = LogCapturer.captureLogs(OMVolumeCreateRequest.class);

    OzoneManagerProtocolProtos.UserInfo userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("host")
            .setRemoteAddress("0.0.0.0")
            .build();

    OMRequest omRequest =
        OMRequest.newBuilder().setCreateVolumeRequest(
            CreateVolumeRequest.newBuilder().setVolumeInfo(
                VolumeInfo.newBuilder().setOwnerName(userName)
                    .setAdminName(userName).setVolume(volumeName).build())
                .build()).setClientId(randomUUID().toString())
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
            .setUserInfo(userInfo)
            .build();

    RaftClientReply raftClientReply =
        raftServer.submitClientRequest(RaftClientRequest.newBuilder()
            .setClientId(clientId)
            .setServerId(raftServer.getId())
            .setGroupId(ozoneManagerRatisServer.getRaftGroup().getGroupId())
            .setCallId(callId)
            .setMessage(
                Message.valueOf(
                    OMRatisHelper.convertRequestToByteString(omRequest)))
            .setType(RaftClientRequest.writeRequestType())
            .build());

    assertTrue(raftClientReply.isSuccess());

    assertTrue(logCapturer.getOutput().contains("created volume:"
        + volumeName));

    logCapturer.clearOutput();

    raftClientReply =
        raftServer.submitClientRequest(RaftClientRequest.newBuilder()
            .setClientId(clientId)
            .setServerId(raftServer.getId())
            .setGroupId(ozoneManagerRatisServer.getRaftGroup().getGroupId())
            .setCallId(callId)
            .setMessage(
                Message.valueOf(
                    OMRatisHelper.convertRequestToByteString(omRequest)))
            .setType(RaftClientRequest.writeRequestType())
            .build());

    assertTrue(raftClientReply.isSuccess());

    // As second time with same client id and call id, this request should
    // not be executed ratis server should return from cache.
    // If 2nd time executed, it will fail with Volume creation failed. check
    // for that.
    assertFalse(logCapturer.getOutput().contains(
        "Volume creation failed"));

    //Sleep for little above retry cache duration to get cache clear.
    Thread.sleep(getRetryCacheDuration().toMillis() + 5000);

    raftClientReply =
        raftServer.submitClientRequest(RaftClientRequest.newBuilder()
            .setClientId(clientId)
            .setServerId(raftServer.getId())
            .setGroupId(ozoneManagerRatisServer.getRaftGroup().getGroupId())
            .setCallId(callId)
            .setMessage(
                Message.valueOf(
                    OMRatisHelper.convertRequestToByteString(omRequest)))
            .setType(RaftClientRequest.writeRequestType())
            .build());

    assertTrue(raftClientReply.isSuccess());

    // As second time with same client id and call id, this request should
    // be executed by ratis server as we are sending this request after cache
    // expiry duration.
    assertTrue(logCapturer.getOutput().contains(
        "Volume creation failed"));

  }

  @Test
  void testAddBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  @Test
  void testRemoveBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testRemoveAcl(remoteUserName, ozoneObj, defaultUserAcl);

  }

  @Test
  void testSetBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

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
      return givenAcl.equals(existingAcl);
    }
    return false;
  }

  @Test
  void testAddKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testAddAcl(remoteUserName, ozoneObj, userAcl);
  }

  @Test
  void testRemoveKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testRemoveAcl(remoteUserName, ozoneObj, userAcl);

  }

  @Test
  void testSetKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testSetAcl(remoteUserName, ozoneObj, userAcl);

  }

  @Test
  void testAddPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.secure().nextAlphabetic(5) + "/";
    OzoneAcl defaultUserAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  @Test
  void testRemovePrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.secure().nextAlphabetic(5) + "/";
    OzoneAcl userAcl = OzoneAcl.of(USER, remoteUserName,
        ACCESS, READ);
    OzoneAcl userAcl1 = OzoneAcl.of(USER, "remote",
        ACCESS, READ);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    ObjectStore objectStore = getObjectStore();

    boolean result = objectStore.addAcl(ozoneObj, userAcl);
    assertTrue(result);

    result = objectStore.addAcl(ozoneObj, userAcl1);
    assertTrue(result);

    result = objectStore.removeAcl(ozoneObj, userAcl);
    assertTrue(result);

    // try removing already removed acl.
    result = objectStore.removeAcl(ozoneObj, userAcl);
    assertFalse(result);

    result = objectStore.removeAcl(ozoneObj, userAcl1);
    assertTrue(result);

  }

  @Test
  void testSetPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.secure().nextAlphabetic(5) + "/";
    OzoneAcl defaultUserAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, READ);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    testSetAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  @Test
  void testLinkBucketAddBucketAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);

    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);

    // Add ACL to the LINK and verify that it is added to the source bucket
    OzoneAcl acl1 = OzoneAcl.of(USER, "remoteUser1", DEFAULT, READ);
    boolean addAcl = getObjectStore().addAcl(linkObj, acl1);
    assertTrue(addAcl);
    assertEqualsAcls(srcObj, linkObj);

    // Add ACL to the SOURCE and verify that it from link
    OzoneAcl acl2 = OzoneAcl.of(USER, "remoteUser2", DEFAULT, WRITE);
    boolean addAcl2 = getObjectStore().addAcl(srcObj, acl2);
    assertTrue(addAcl2);
    assertEqualsAcls(srcObj, linkObj);

  }

  @Test
  void testLinkBucketRemoveBucketAcl() throws Exception {
    // case1 : test remove link acl
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);
    // choose from src's ACLs to avoid trying to remove OzoneAcl#LINK_BUCKET_DEFAULT_ACL
    List<OzoneAcl> acls = getObjectStore().getAcl(srcObj);
    assertFalse(acls.isEmpty());
    // Remove an existing acl.
    boolean removeAcl = getObjectStore().removeAcl(linkObj, acls.get(0));
    assertTrue(removeAcl);
    assertEqualsAcls(srcObj, linkObj);

    // case2 : test remove src acl
    OzoneBucket srcBucket2 = setupBucket();
    OzoneBucket linkedBucket2 = linkBucket(srcBucket2);
    OzoneObj linkObj2 = buildBucketObj(linkedBucket2);
    OzoneObj srcObj2 = buildBucketObj(srcBucket2);
    // As by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls2 = getObjectStore().getAcl(srcObj2);
    assertFalse(acls2.isEmpty());
    // Remove an existing acl.
    boolean removeAcl2 = getObjectStore().removeAcl(srcObj2, acls.get(0));
    assertTrue(removeAcl2);
    assertEqualsAcls(srcObj2, linkObj2);

  }

  @Test
  void testLinkBucketSetBucketAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);

    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);

    // Set ACL to the LINK and verify that it is set to the source bucket
    List<OzoneAcl> acl1 = Collections.singletonList(
        OzoneAcl.of(USER, "remoteUser1", DEFAULT, READ));
    boolean setAcl1 = getObjectStore().setAcl(linkObj, acl1);
    assertTrue(setAcl1);
    assertEqualsAcls(srcObj, linkObj);

    // Set ACL to the SOURCE and verify that it from link
    List<OzoneAcl> acl2 = Collections.singletonList(
        OzoneAcl.of(USER, "remoteUser2", DEFAULT, WRITE));
    boolean setAcl2 = getObjectStore().setAcl(srcObj, acl2);
    assertTrue(setAcl2);
    assertEqualsAcls(srcObj, linkObj);

  }

  @Test
  void testLinkBucketAddKeyAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = OzoneAcl.of(USER, user1, DEFAULT, READ);
    testAddAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = OzoneAcl.of(USER, user2, DEFAULT, READ);
    testAddAcl(user2, srcObj, acl2);  // case2: set src acl
    assertEqualsAcls(srcObj, linkObj);

  }

  @Test
  void testLinkBucketRemoveKeyAcl() throws Exception {

    // CASE 1: from link bucket
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);
    String user = "remoteUser1";
    OzoneAcl acl = OzoneAcl.of(USER, user, DEFAULT, READ);
    testRemoveAcl(user, linkObj, acl);
    assertEqualsAcls(srcObj, linkObj);

    // CASE 2: from src bucket
    OzoneBucket srcBucket2 = setupBucket();
    OzoneBucket linkedBucket2 = linkBucket(srcBucket2);
    String key2 = createKey(srcBucket2);
    OzoneObj linkObj2 = buildKeyObj(linkedBucket2, key2);
    OzoneObj srcObj2 = buildKeyObj(srcBucket2, key2);
    String user2 = "remoteUser2";
    OzoneAcl acl2 = OzoneAcl.of(USER, user2, DEFAULT, READ);
    testRemoveAcl(user2, srcObj2, acl2);
    assertEqualsAcls(srcObj2, linkObj2);

  }

  @Test
  void testLinkBucketSetKeyAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = OzoneAcl.of(USER, user1, DEFAULT, READ);
    testSetAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = OzoneAcl.of(USER, user2, DEFAULT, READ);
    testSetAcl(user2, srcObj, acl2);  // case2: set src acl
    assertEqualsAcls(srcObj, linkObj);

  }

  @Test
  void testLinkBucketAddPrefixAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String prefix = createPrefixName();
    OzoneObj linkObj = buildPrefixObj(linkedBucket, prefix);
    OzoneObj srcObj = buildPrefixObj(srcBucket, prefix);
    createPrefix(linkObj);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = OzoneAcl.of(USER, user1, DEFAULT, READ);
    testAddAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = OzoneAcl.of(USER, user2, DEFAULT, READ);
    testAddAcl(user2, srcObj, acl2);  // case2: set src acl
    assertEqualsAcls(srcObj, linkObj);

  }

  @Test
  void testLinkBucketRemovePrefixAcl() throws Exception {

    // CASE 1: from link bucket
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String prefix = createPrefixName();
    OzoneObj linkObj = buildPrefixObj(linkedBucket, prefix);
    OzoneObj srcObj = buildPrefixObj(srcBucket, prefix);
    createPrefix(linkObj);

    String user = "remoteUser1";
    OzoneAcl acl = OzoneAcl.of(USER, user, DEFAULT, READ);
    testRemoveAcl(user, linkObj, acl);
    assertEqualsAcls(srcObj, linkObj);

    // CASE 2: from src bucket
    OzoneBucket srcBucket2 = setupBucket();
    OzoneBucket linkedBucket2 = linkBucket(srcBucket2);
    String prefix2 = createPrefixName();
    OzoneObj linkObj2 = buildPrefixObj(linkedBucket2, prefix2);
    OzoneObj srcObj2 = buildPrefixObj(srcBucket2, prefix2);
    createPrefix(srcObj2);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = OzoneAcl.of(USER, user2, DEFAULT, READ);
    testRemoveAcl(user2, srcObj2, acl2);
    assertEqualsAcls(srcObj2, linkObj2);

  }

  @Test
  void testLinkBucketSetPrefixAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String prefix = createPrefixName();
    OzoneObj linkObj = buildPrefixObj(linkedBucket, prefix);
    OzoneObj srcObj = buildPrefixObj(srcBucket, prefix);
    createPrefix(linkObj);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = OzoneAcl.of(USER, user1, DEFAULT, READ);
    testSetAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = OzoneAcl.of(USER, user2, DEFAULT, READ);
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
    assertEquals(getObjectStore().getAcl(srcObj),
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

      assertFalse(acls.isEmpty());
    }

    OzoneAcl modifiedUserAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, WRITE);

    List<OzoneAcl> newAcls = Collections.singletonList(modifiedUserAcl);
    boolean setAcl = objectStore.setAcl(ozoneObj, newAcls);
    assertTrue(setAcl);

    // Get acls and check whether they are reset or not.
    List<OzoneAcl> getAcls = objectStore.getAcl(ozoneObj);

    assertEquals(newAcls.size(), getAcls.size());
    int i = 0;
    for (OzoneAcl ozoneAcl : newAcls) {
      assertTrue(compareAcls(getAcls.get(i++), ozoneAcl));
    }

  }

  private void testAddAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();
    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    assertTrue(addAcl);

    List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

    assertTrue(containsAcl(userAcl, acls));

    // Add an already existing acl.
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    assertFalse(addAcl);

    // Add an acl by changing acl type with same type, name and scope.
    userAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, WRITE);
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    assertTrue(addAcl);
  }

  private void testRemoveAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();

    // Other than prefix, by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls;
    if (ozoneObj.getResourceType().equals(OzoneObj.ResourceType.PREFIX)) {
      objectStore.addAcl(ozoneObj, userAcl);
      // Add another arbitrary group ACL since the prefix will be removed when removing
      // the last ACL for the prefix and PREFIX_NOT_FOUND will be thrown
      OzoneAcl groupAcl = OzoneAcl.of(GROUP, "arbitrary-group", ACCESS, READ);
      objectStore.addAcl(ozoneObj, groupAcl);
    }
    acls = objectStore.getAcl(ozoneObj);

    assertFalse(acls.isEmpty());

    // Remove an existing acl.
    boolean removeAcl = objectStore.removeAcl(ozoneObj, acls.get(0));
    assertTrue(removeAcl);

    // Trying to remove an already removed acl.
    removeAcl = objectStore.removeAcl(ozoneObj, acls.get(0));
    assertFalse(removeAcl);

    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    assertTrue(addAcl);

    // Just changed acl type here to write, rest all is same as defaultUserAcl.
    OzoneAcl modifiedUserAcl = OzoneAcl.of(USER, remoteUserName,
        DEFAULT, WRITE);
    addAcl = objectStore.addAcl(ozoneObj, modifiedUserAcl);
    assertTrue(addAcl);

    removeAcl = objectStore.removeAcl(ozoneObj, modifiedUserAcl);
    assertTrue(removeAcl);

    removeAcl = objectStore.removeAcl(ozoneObj, userAcl);
    assertTrue(removeAcl);
  }

  @Test
  void testOMRatisSnapshot() throws Exception {
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName);
    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);

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

    ozoneManager.awaitDoubleBufferFlush();
    // The current lastAppliedLogIndex on the state machine should be greater
    // than or equal to the saved snapshot index.
    long smLastAppliedIndex =
        ozoneManager.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    long ratisSnapshotIndex = ozoneManager.getRatisSnapshotIndex();
    assertTrue(smLastAppliedIndex >= ratisSnapshotIndex,
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
        fail("test failed during transactionInfo read");
      }
      return false;
    }, 1000, 100000);

    // The new snapshot index must be greater than the previous snapshot index
    long ratisSnapshotIndexNew = ozoneManager.getRatisSnapshotIndex();
    assertTrue(ratisSnapshotIndexNew > ratisSnapshotIndex,
        "Latest snapshot index must be greater than previous " +
            "snapshot indices");

  }

  @Test
  void testOMFollowerReadWithClusterDisabled() throws Exception {
    OzoneConfiguration clientConf = OzoneConfiguration.of(getConf());
    clientConf.setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);

    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();
    OzoneClient ozoneClient = null;
    try {
      ozoneClient = OzoneClientFactory.getRpcClient(clientConf);

      HadoopRpcOMFailoverProxyProvider leaderFailoverProxyProvider =
          OmFailoverProxyUtil
              .getFailoverProxyProvider(ozoneClient.getProxy());
      HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
          OmFailoverProxyUtil.getFollowerReadFailoverProxyProvider(
              ozoneClient.getProxy()
          );
      assertNotNull(followerReadFailoverProxyProvider);
      assertTrue(followerReadFailoverProxyProvider.isUseFollowerRead());

      ObjectStore objectStore = ozoneClient.getObjectStore();

      // Trigger write so that the leader failover proxy provider points to the leader
      objectStore.createVolume(volumeName, createVolumeArgs);

      // Get the leader OM node ID
      String leaderOMNodeId = leaderFailoverProxyProvider.getCurrentProxyOMNodeId();

      // Pick a follower and tigger read so that read failover proxy provider fall back to the leader-only read
      // on encountering OMNotLeaderException
      String followerOMNodeId = null;
      for (OzoneManager om : getCluster().getOzoneManagersList()) {
        if (!om.getOMNodeId().equals(leaderOMNodeId)) {
          followerOMNodeId = om.getOMNodeId();
          break;
        }
      }
      assertNotNull(followerOMNodeId);
      followerReadFailoverProxyProvider.changeInitialProxyForTest(followerOMNodeId);
      objectStore.getVolume(volumeName);

      // Client follower read is disabled since it detected that the cluster does not
      // support follower read
      assertFalse(followerReadFailoverProxyProvider.isUseFollowerRead());
      OMProxyInfo<OzoneManagerProtocolPB> lastProxy =
          (OMProxyInfo<OzoneManagerProtocolPB>) followerReadFailoverProxyProvider.getLastProxy();
      // The last read will be done on the leader
      assertEquals(leaderFailoverProxyProvider.getCurrentProxyOMNodeId(), lastProxy.getNodeId());
    } finally {
      IOUtils.closeQuietly(ozoneClient);
    }

  }
}

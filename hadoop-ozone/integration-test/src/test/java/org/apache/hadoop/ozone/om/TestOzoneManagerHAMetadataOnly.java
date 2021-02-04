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
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Logger;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;

import static org.apache.ratis.metrics.RatisMetrics.RATIS_APPLICATION_NAME_METRICS;
import static org.junit.Assert.fail;

/**
 * Test Ozone Manager Metadata operation in distributed handler scenario.
 */
public class TestOzoneManagerHAMetadataOnly extends TestOzoneManagerHA {

  private OzoneVolume createAndCheckVolume(String volumeName)
      throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(volumeName, createVolumeArgs);

    OzoneVolume retVolume = objectStore.getVolume(volumeName);

    Assert.assertTrue(retVolume.getName().equals(volumeName));
    Assert.assertTrue(retVolume.getOwner().equals(userName));
    Assert.assertTrue(retVolume.getAdmin().equals(adminName));

    return retVolume;
  }
  @Test
  public void testAllVolumeOperations() throws Exception {

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

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

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "volume" + RandomStringUtils.randomNumeric(5);

    OzoneVolume retVolume = createAndCheckVolume(volumeName);

    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setStorageType(StorageType.DISK)
            .setVersioning(true).build();


    retVolume.createBucket(bucketName, bucketArgs);


    OzoneBucket ozoneBucket = retVolume.getBucket(bucketName);

    Assert.assertEquals(volumeName, ozoneBucket.getVolumeName());
    Assert.assertEquals(bucketName, ozoneBucket.getName());
    Assert.assertTrue(ozoneBucket.getVersioning());
    Assert.assertEquals(StorageType.DISK, ozoneBucket.getStorageType());
    Assert.assertFalse(ozoneBucket.getCreationTime().isAfter(Instant.now()));


    // Change versioning to false
    ozoneBucket.setVersioning(false);

    ozoneBucket = retVolume.getBucket(bucketName);
    Assert.assertFalse(ozoneBucket.getVersioning());

    retVolume.deleteBucket(bucketName);

    OzoneTestUtils.expectOmException(OMException.ResultCodes.BUCKET_NOT_FOUND,
        () -> retVolume.deleteBucket(bucketName));
  }

  /**
   * Test that OMFailoverProxyProvider creates an OM proxy for each OM in the
   * cluster.
   */
  @Test
  public void testOMProxyProviderInitialization() throws Exception {
    OzoneClient rpcClient = getCluster().getRpcClient();

    OMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil.getFailoverProxyProvider(
            rpcClient.getObjectStore().getClientProxy());

    List<OMProxyInfo> omProxies =
        omFailoverProxyProvider.getOMProxyInfos();

    Assert.assertEquals(getNumOfOMs(), omProxies.size());

    for (int i = 0; i < getNumOfOMs(); i++) {
      InetSocketAddress omRpcServerAddr =
          getCluster().getOzoneManager(i).getOmRpcServerAddr();
      boolean omClientProxyExists = false;
      for (OMProxyInfo omProxyInfo : omProxies) {
        if (omProxyInfo.getAddress().equals(omRpcServerAddr)) {
          omClientProxyExists = true;
          break;
        }
      }
      Assert.assertTrue("There is no OM Client Proxy corresponding to OM " +
              "node" + getCluster().getOzoneManager(i).getOMNodeId(),
          omClientProxyExists);
    }
  }

  /**
   * Test OMFailoverProxyProvider failover on connection exception to OM client.
   */
  @Ignore("This test randomly failing. Let's enable once its fixed.")
  @Test
  public void testOMProxyProviderFailoverOnConnectionFailure()
      throws Exception {
    ObjectStore objectStore = getObjectStore();
    OMFailoverProxyProvider omFailoverProxyProvider =
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

    // Verify that a failover occured. the new proxy nodeId should be
    // different from the old proxy nodeId.
    Assert.assertNotEquals("Failover did not occur as expected",
        firstProxyNodeId, newProxyNodeId);
  }

  /**
   * Test OMFailoverProxyProvider failover when current OM proxy is not
   * the current OM Leader.
   */
  @Test
  public void testOMProxyProviderFailoverToCurrentLeader() throws Exception {
    ObjectStore objectStore = getObjectStore();
    OMFailoverProxyProvider omFailoverProxyProvider = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy());

    // Run couple of createVolume tests to discover the current Leader OM
    createVolumeTest(true);
    createVolumeTest(true);

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Perform a manual failover of the proxy provider to move the
    // currentProxyIndex to a node other than the leader OM.
    omFailoverProxyProvider.performFailoverToNextProxy();

    String newProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();
    Assert.assertNotEquals(leaderOMNodeId, newProxyNodeId);

    // Once another request is sent to this new proxy node, the leader
    // information must be returned via the response and a failover must
    // happen to the leader proxy node.
    createVolumeTest(true);
    Thread.sleep(2000);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // The old and new Leader OM NodeId must match since there was no new
    // election in the Ratis ring.
    Assert.assertEquals(leaderOMNodeId, newLeaderOMNodeId);
  }

  @Test
  public void testOMRetryProxy() throws Exception {
    int maxFailoverAttempts = getOzoneClientFailoverMaxAttempts();
    // Stop all the OMs.
    for (int i = 0; i < getNumOfOMs(); i++) {
      getCluster().stopOzoneManager(i);
    }

    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);

    try {
      createVolumeTest(true);
      // After making N (set maxRetries value) connection attempts to OMs,
      // the RpcClient should give up.
      fail("TestOMRetryProxy should fail when there are no OMs running");
    } catch (ConnectException e) {
      Assert.assertEquals(1,
          appender.countLinesWithMessage("Failed to connect to OMs:"));
      Assert.assertEquals(maxFailoverAttempts,
          appender.countLinesWithMessage("Trying to failover"));
      Assert.assertEquals(1,
          appender.countLinesWithMessage("Attempted " +
              maxFailoverAttempts + " failovers."));
    }
  }

  @Test
  public void testReadRequest() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(volumeName);

    OMFailoverProxyProvider omFailoverProxyProvider = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy());

    String currentLeaderNodeId = omFailoverProxyProvider
        .getCurrentProxyOMNodeId();

    // A read request from any proxy should failover to the current leader OM
    for (int i = 0; i < getNumOfOMs(); i++) {
      // Failover OMFailoverProxyProvider to OM at index i
      OzoneManager ozoneManager = getCluster().getOzoneManager(i);

      // Get the ObjectStore and FailoverProxyProvider for OM at index i
      final ObjectStore store = OzoneClientFactory.getRpcClient(
          getOmServiceId(), getConf()).getObjectStore();
      final OMFailoverProxyProvider proxyProvider =
          OmFailoverProxyUtil.getFailoverProxyProvider(store.getClientProxy());

      // Failover to the OM node that the objectStore points to
      omFailoverProxyProvider.performFailoverIfRequired(
          ozoneManager.getOMNodeId());

      // A read request should result in the proxyProvider failing over to
      // leader node.
      OzoneVolume volume = store.getVolume(volumeName);
      Assert.assertEquals(volumeName, volume.getName());

      Assert.assertEquals(currentLeaderNodeId,
          proxyProvider.getCurrentProxyOMNodeId());
    }
  }

  @Ignore("This test randomly failing. Let's enable once its fixed.")
  @Test
  public void testListVolumes() throws Exception {
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    String adminName = userName;
    ObjectStore objectStore = getObjectStore();

    Set<String> expectedVolumes = new TreeSet<>();
    for (int i=0; i < 100; i++) {
      String volumeName = "vol" + i;
      expectedVolumes.add(volumeName);
      VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setAdmin(adminName)
          .build();
      objectStore.createVolume(volumeName, createVolumeArgs);
    }

    validateVolumesList(userName, expectedVolumes);

    // Stop leader OM, and then validate list volumes for user.
    stopLeaderOM();
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    validateVolumesList(userName, expectedVolumes);

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
    Assert.assertNotNull(mBeanInfo);
    Object flushCount = mBeanServer.getAttribute(oname, "Count");
    Assert.assertTrue((long) flushCount >= 0);
  }

  @Test
  public void testOMRetryCache() throws Exception {
    ObjectStore objectStore = getObjectStore();
    objectStore.createVolume(UUID.randomUUID().toString());


    OMFailoverProxyProvider omFailoverProxyProvider = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy());

    String currentLeaderNodeId = omFailoverProxyProvider
        .getCurrentProxyOMNodeId();

    OzoneManagerRatisServer ozoneManagerRatisServer =
        getCluster().getOzoneManager(currentLeaderNodeId).getOmRatisServer();

    RaftServer raftServer = ozoneManagerRatisServer.getServer();

    ClientId clientId = ClientId.randomId();
    long callId = 2000L;
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    String volumeName = UUID.randomUUID().toString();


    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(OMVolumeCreateRequest.getLogger());
    OMRequest omRequest =
        OMRequest.newBuilder().setCreateVolumeRequest(
            CreateVolumeRequest.newBuilder().setVolumeInfo(
                VolumeInfo.newBuilder().setOwnerName(userName)
                    .setAdminName(userName).setVolume(volumeName).build())
                .build()).setClientId(UUID.randomUUID().toString())
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume).build();

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

    Assert.assertTrue(raftClientReply.isSuccess());

    Assert.assertTrue(logCapturer.getOutput().contains("created volume:"
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

    Assert.assertTrue(raftClientReply.isSuccess());

    // As second time with same client id and call id, this request should
    // not be executed ratis server should return from cache.
    // If 2nd time executed, it will fail with Volume creation failed. check
    // for that.
    Assert.assertFalse(logCapturer.getOutput().contains(
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

    Assert.assertTrue(raftClientReply.isSuccess());

    // As second time with same client id and call id, this request should
    // be executed by ratis server as we are sending this request after cache
    // expiry duration.
    Assert.assertTrue(logCapturer.getOutput().contains(
        "Volume creation failed"));

  }

  private void validateVolumesList(String userName,
      Set<String> expectedVolumes) throws Exception {
    ObjectStore objectStore = getObjectStore();

    int expectedCount = 0;
    Iterator<? extends OzoneVolume> volumeIterator =
        objectStore.listVolumesByUser(userName, "", "");

    while (volumeIterator.hasNext()) {
      OzoneVolume next = volumeIterator.next();
      Assert.assertTrue(expectedVolumes.contains(next.getName()));
      expectedCount++;
    }

    Assert.assertEquals(expectedVolumes.size(),  expectedCount);
  }
}

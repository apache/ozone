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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
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
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;

import static org.apache.ratis.server.metrics.RatisMetrics.RATIS_APPLICATION_NAME_METRICS;
import static org.junit.Assert.fail;

/**
 * Test Ozone Manager Metadata operation in distributed handler scenario.
 */
public class TestOzoneManagerHAMetadataOnly {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;
  /* Reduce max number of retries to speed up unit test. */
  private static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS = 5;
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES = 4;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(300_000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS);
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        IPC_CLIENT_CONNECT_MAX_RETRIES);
    /* Reduce IPC retry interval to speed up unit test. */
    conf.setInt(IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 200);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  private OzoneVolume createAndCheckVolume(String volumeName)
      throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

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
   * Create a volume and test its attribute.
   */
  private void createVolumeTest(boolean checkSuccess) throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    try {
      objectStore.createVolume(volumeName, createVolumeArgs);

      OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

      if (checkSuccess) {
        Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
        Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
        Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));
      } else {
        // Verify that the request failed
        fail("There is no quorum. Request should have failed");
      }
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

  /**
   * Test that OMFailoverProxyProvider creates an OM proxy for each OM in the
   * cluster.
   */
  @Test
  public void testOMProxyProviderInitialization() throws Exception {
    OzoneClient rpcClient = cluster.getRpcClient();
    OMFailoverProxyProvider omFailoverProxyProvider =
        rpcClient.getObjectStore().getClientProxy().getOMProxyProvider();
    List<OMProxyInfo> omProxies =
        omFailoverProxyProvider.getOMProxyInfos();

    Assert.assertEquals(numOfOMs, omProxies.size());

    for (int i = 0; i < numOfOMs; i++) {
      InetSocketAddress omRpcServerAddr =
          cluster.getOzoneManager(i).getOmRpcServerAddr();
      boolean omClientProxyExists = false;
      for (OMProxyInfo omProxyInfo : omProxies) {
        if (omProxyInfo.getAddress().equals(omRpcServerAddr)) {
          omClientProxyExists = true;
          break;
        }
      }
      Assert.assertTrue("There is no OM Client Proxy corresponding to OM " +
              "node" + cluster.getOzoneManager(i).getOMNodeId(),
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
    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();
    String firstProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    createVolumeTest(true);

    // On stopping the current OM Proxy, the next connection attempt should
    // failover to a another OM proxy.
    cluster.stopOzoneManager(firstProxyNodeId);
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
    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();

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
    // Stop all the OMs.
    for (int i = 0; i < numOfOMs; i++) {
      cluster.stopOzoneManager(i);
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
      Assert.assertEquals(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS,
          appender.countLinesWithMessage("Trying to failover"));
      Assert.assertEquals(1,
          appender.countLinesWithMessage("Attempted " +
              OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS + " failovers."));
    }
  }

  @Test
  public void testReadRequest() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    objectStore.createVolume(volumeName);

    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();
    String currentLeaderNodeId = omFailoverProxyProvider
        .getCurrentProxyOMNodeId();

    // A read request from any proxy should failover to the current leader OM
    for (int i = 0; i < numOfOMs; i++) {
      // Failover OMFailoverProxyProvider to OM at index i
      OzoneManager ozoneManager = cluster.getOzoneManager(i);

      // Get the ObjectStore and FailoverProxyProvider for OM at index i
      final ObjectStore store = OzoneClientFactory.getRpcClient(
          omServiceId, conf).getObjectStore();
      final OMFailoverProxyProvider proxyProvider =
          store.getClientProxy().getOMProxyProvider();

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
        cluster.getOzoneManager(0).getOmRatisServer();
    ObjectName oname = new ObjectName(RATIS_APPLICATION_NAME_METRICS, "name",
        RATIS_APPLICATION_NAME_METRICS + ".log_worker." +
            ratisServer.getRaftPeerId().toString() + ".flushCount");
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    MBeanInfo mBeanInfo = mBeanServer.getMBeanInfo(oname);
    Assert.assertNotNull(mBeanInfo);
    Object flushCount = mBeanServer.getAttribute(oname, "Count");
    Assert.assertTrue((long) flushCount >= 0);
  }

  private void validateVolumesList(String userName,
      Set<String> expectedVolumes) throws Exception {

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


  /**
   * Stop the current leader OM.
   * @throws Exception
   */
  private void stopLeaderOM() {
    //Stop the leader OM.
    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    cluster.stopOzoneManager(leaderOMNodeId);
  }
}

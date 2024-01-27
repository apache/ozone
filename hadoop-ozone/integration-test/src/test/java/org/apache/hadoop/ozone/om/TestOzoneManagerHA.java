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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.OzoneClientFactory;

import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Iterator;
import java.util.UUID;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for Ozone Manager HA tests.
 */
@Timeout(300)
public abstract class TestOzoneManagerHA {

  private static MiniOzoneHAClusterImpl cluster = null;
  private static MiniOzoneCluster.Builder clusterBuilder = null;
  private static ObjectStore objectStore;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static String omServiceId;
  private static int numOfOMs = 3;
  private static final int LOG_PURGE_GAP = 50;
  /* Reduce max number of retries to speed up unit test. */
  private static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS = 5;
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES = 4;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final Duration RETRY_CACHE_DURATION = Duration.ofSeconds(30);
  private static OzoneClient client;


  public MiniOzoneHAClusterImpl getCluster() {
    return cluster;
  }

  public ObjectStore getObjectStore() {
    return objectStore;
  }

  public static OzoneClient getClient() {
    return client;
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  public MiniOzoneCluster.Builder getClusterBuilder() {
    return clusterBuilder;
  }

  public String getOmServiceId() {
    return omServiceId;
  }

  public static int getLogPurgeGap() {
    return LOG_PURGE_GAP;
  }

  public static long getSnapshotThreshold() {
    return SNAPSHOT_THRESHOLD;
  }

  public static int getNumOfOMs() {
    return numOfOMs;
  }

  public static int getOzoneClientFailoverMaxAttempts() {
    return OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS;
  }

  public static Duration getRetryCacheDuration() {
    return RETRY_CACHE_DURATION;
  }

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS);
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        IPC_CLIENT_CONNECT_MAX_RETRIES);
    /* Reduce IPC retry interval to speed up unit test. */
    conf.setInt(IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 200);
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);

    // Some subclasses check RocksDB directly as part of their tests. These
    // depend on OBS layout.
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_OBJECT_STORE);

    OzoneManagerRatisServerConfig omHAConfig =
        conf.getObject(OzoneManagerRatisServerConfig.class);

    omHAConfig.setRetryCacheTimeout(RETRY_CACHE_DURATION);

    conf.setFromObject(omHAConfig);

    // config for key deleting service.
    conf.set(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, "10s");
    conf.set(OZONE_KEY_DELETING_LIMIT_PER_TASK, "2");

    clusterBuilder = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setOmId(omId)
        .setNumOfOzoneManagers(numOfOMs);

    cluster = (MiniOzoneHAClusterImpl) clusterBuilder.build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(omServiceId, conf);
    objectStore = client.getObjectStore();
  }

  /**
   * Shutdown MiniDFSCluster after all tests of a class have run.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Create a key in the bucket.
   *
   * @return the key name.
   */
  public static String createKey(OzoneBucket ozoneBucket) throws IOException {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    createKey(ozoneBucket, keyName);
    return keyName;
  }

  public static void createKey(OzoneBucket ozoneBucket, String keyName) throws IOException {
    String data = "data" + RandomStringUtils.randomNumeric(5);
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName, data.length(), ReplicationType.RATIS,
        ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();
  }

  protected OzoneBucket setupBucket() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + UUID.randomUUID();

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    assertEquals(volumeName, retVolumeinfo.getName());
    assertEquals(userName, retVolumeinfo.getOwner());
    assertEquals(adminName, retVolumeinfo.getAdmin());

    String bucketName = UUID.randomUUID().toString();
    retVolumeinfo.createBucket(bucketName);

    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    assertEquals(bucketName, ozoneBucket.getName());
    assertEquals(volumeName, ozoneBucket.getVolumeName());

    return ozoneBucket;
  }

  protected OzoneBucket linkBucket(OzoneBucket srcBuk) throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String linkedVolName = "volume-link-" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    BucketArgs createBucketArgs = new BucketArgs.Builder()
        .setSourceVolume(srcBuk.getVolumeName())
        .setSourceBucket(srcBuk.getName())
        .build();

    objectStore.createVolume(linkedVolName, createVolumeArgs);
    OzoneVolume linkedVolumeInfo = objectStore.getVolume(linkedVolName);

    assertEquals(linkedVolName, linkedVolumeInfo.getName());
    assertEquals(userName, linkedVolumeInfo.getOwner());
    assertEquals(adminName, linkedVolumeInfo.getAdmin());

    String linkedBucketName = UUID.randomUUID().toString();
    linkedVolumeInfo.createBucket(linkedBucketName, createBucketArgs);

    OzoneBucket linkedBucket = linkedVolumeInfo.getBucket(linkedBucketName);

    assertEquals(linkedBucketName, linkedBucket.getName());
    assertEquals(linkedVolName, linkedBucket.getVolumeName());
    assertTrue(linkedBucket.isLink());

    return linkedBucket;
  }

  /**
   * Stop the current leader OM.
   */
  protected void stopLeaderOM() {
    //Stop the leader OM.
    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil.getFailoverProxyProvider(
            (RpcClient) objectStore.getClientProxy());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    cluster.stopOzoneManager(leaderOMNodeId);
  }

  /**
   * Create a volume and test its attribute.
   */
  protected void createVolumeTest(boolean checkSuccess) throws Exception {
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
        assertEquals(volumeName, retVolumeinfo.getName());
        assertEquals(userName, retVolumeinfo.getOwner());
        assertEquals(adminName, retVolumeinfo.getAdmin());
      } else {
        // Verify that the request failed
        fail("There is no quorum. Request should have failed");
      }
    } catch (IOException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          GenericTestUtils.assertExceptionContains("OMNotLeaderException", e);
        } else if (e instanceof ConnectException) {
          GenericTestUtils.assertExceptionContains("Connection refused", e);
        } else {
          GenericTestUtils.assertExceptionContains(
              "Could not determine or connect to OM Leader", e);
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * This method createFile and verifies the file is successfully created or
   * not.
   *
   * @param ozoneBucket
   * @param keyName
   * @param data
   * @param recursive
   * @param overwrite
   * @throws Exception
   */
  protected void testCreateFile(OzoneBucket ozoneBucket, String keyName,
                                String data, boolean recursive,
                                boolean overwrite)
      throws Exception {

    OzoneOutputStream ozoneOutputStream = ozoneBucket.createFile(keyName,
        data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
        overwrite, recursive);

    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();

    OzoneKeyDetails ozoneKeyDetails = ozoneBucket.getKey(keyName);

    assertEquals(keyName, ozoneKeyDetails.getName());
    assertEquals(ozoneBucket.getName(), ozoneKeyDetails.getBucketName());
    assertEquals(ozoneBucket.getVolumeName(),
        ozoneKeyDetails.getVolumeName());
    assertEquals(data.length(), ozoneKeyDetails.getDataSize());
    assertTrue(ozoneKeyDetails.isFile());

    try (OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName)) {
      byte[] fileContent = new byte[data.getBytes(UTF_8).length];
      ozoneInputStream.read(fileContent);
      assertEquals(data, new String(fileContent, UTF_8));
    }

    Iterator<? extends OzoneKey> iterator = ozoneBucket.listKeys("/");
    while (iterator.hasNext()) {
      OzoneKey ozoneKey = iterator.next();
      if (!ozoneKey.getName().endsWith(OM_KEY_PREFIX)) {
        assertTrue(ozoneKey.isFile());
      } else {
        assertFalse(ozoneKey.isFile());
      }
    }
  }

  protected void createKeyTest(boolean checkSuccess) throws Exception {
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

      assertEquals(volumeName, retVolumeinfo.getName());
      assertEquals(userName, retVolumeinfo.getOwner());
      assertEquals(adminName, retVolumeinfo.getAdmin());

      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();
      retVolumeinfo.createBucket(bucketName);

      OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

      assertEquals(bucketName, ozoneBucket.getName());
      assertEquals(volumeName, ozoneBucket.getVolumeName());

      String value = "random data";
      OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
          value.length(), ReplicationType.RATIS,
          ReplicationFactor.ONE, new HashMap<>());
      ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
      ozoneOutputStream.close();

      try (OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName)) {
        byte[] fileContent = new byte[value.getBytes(UTF_8).length];
        ozoneInputStream.read(fileContent);
        assertEquals(value, new String(fileContent, UTF_8));
      }

    } catch (IOException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          GenericTestUtils.assertExceptionContains("OMNotLeaderException", e);
        } else if (e instanceof ConnectException) {
          GenericTestUtils.assertExceptionContains("Connection refused", e);
        } else {
          GenericTestUtils.assertExceptionContains(
              "Could not determine or connect to OM Leader", e);
        }
      } else {
        throw e;
      }
    }
  }

  protected void waitForLeaderToBeReady()
      throws InterruptedException, TimeoutException {
    // Wait for Leader Election timeout
    int timeout = OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT
        .toIntExact(TimeUnit.MILLISECONDS);
    GenericTestUtils.waitFor(() ->
        getCluster().getOMLeader() != null, 500, timeout);
  }
}

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
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneOMHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.OzoneClientFactory;

import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.Assert;

import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.UUID;
import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.junit.Assert.fail;

/**
 * Base class for Ozone Manager HA tests.
 */
public abstract class TestOzoneManagerHA {

  private MiniOzoneOMHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omId;
  private String omServiceId;
  private static int numOfOMs = 3;
  private static final int LOG_PURGE_GAP = 50;
  /* Reduce max number of retries to speed up unit test. */
  private static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS = 5;
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES = 4;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final Duration RETRY_CACHE_DURATION = Duration.ofSeconds(30);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = Timeout.seconds(300);;

  public MiniOzoneOMHAClusterImpl getCluster() {
    return cluster;
  }

  public ObjectStore getObjectStore() {
    return objectStore;
  }

  public OzoneConfiguration getConf() {
    return conf;
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
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    omId = UUID.randomUUID().toString();
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
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);

    OzoneManagerRatisServerConfig omHAConfig =
        conf.getObject(OzoneManagerRatisServerConfig.class);

    omHAConfig.setRetryCacheTimeout(RETRY_CACHE_DURATION);

    conf.setFromObject(omHAConfig);

    /**
     * config for key deleting service.
     */
    conf.set(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, "10s");
    conf.set(OZONE_KEY_DELETING_LIMIT_PER_TASK, "2");
    cluster = (MiniOzoneOMHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setOmId(omId)
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

  /**
   * Create a key in the bucket.
   * @return the key name.
   */
  public static String createKey(OzoneBucket ozoneBucket) throws IOException {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    String data = "data" + RandomStringUtils.randomNumeric(5);
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
        data.length(), ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();
    return keyName;
  }

  protected OzoneBucket setupBucket() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
    Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
    Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));

    String bucketName = UUID.randomUUID().toString();
    retVolumeinfo.createBucket(bucketName);

    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));
    Assert.assertTrue(ozoneBucket.getVolumeName().equals(volumeName));

    return ozoneBucket;
  }

  /**
   * Stop the current leader OM.
   * @throws Exception
   */
  protected void stopLeaderOM() {
    //Stop the leader OM.
    OMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil.getFailoverProxyProvider(
            (RpcClient) objectStore.getClientProxy());

    // The OMFailoverProxyProvider will point to the current leader OM node.
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
   * This method createFile and verifies the file is successfully created or
   * not.
   * @param ozoneBucket
   * @param keyName
   * @param data
   * @param recursive
   * @param overwrite
   * @throws Exception
   */
  protected void testCreateFile(OzoneBucket ozoneBucket, String keyName,
      String data, boolean recursive, boolean overwrite)
      throws Exception {

    OzoneOutputStream ozoneOutputStream = ozoneBucket.createFile(keyName,
        data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
        overwrite, recursive);

    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();

    OzoneKeyDetails ozoneKeyDetails = ozoneBucket.getKey(keyName);

    Assert.assertEquals(keyName, ozoneKeyDetails.getName());
    Assert.assertEquals(ozoneBucket.getName(), ozoneKeyDetails.getBucketName());
    Assert.assertEquals(ozoneBucket.getVolumeName(),
        ozoneKeyDetails.getVolumeName());
    Assert.assertEquals(data.length(), ozoneKeyDetails.getDataSize());

    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName);

    byte[] fileContent = new byte[data.getBytes(UTF_8).length];
    ozoneInputStream.read(fileContent);
    Assert.assertEquals(data, new String(fileContent, UTF_8));
  }

}

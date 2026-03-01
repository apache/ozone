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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for Ozone Manager HA tests.
 */
public abstract class TestOzoneManagerHAFollowerRead {

  private static MiniOzoneHAClusterImpl cluster = null;
  private static ObjectStore objectStore;
  private static OzoneConfiguration conf;
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

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    omServiceId = "om-service-test1";
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

    // Enable the OM follower read
    omHAConfig.setReadOption("LINEARIZABLE");
    omHAConfig.setReadLeaderLeaseEnabled(true);

    conf.setFromObject(omHAConfig);

    // Enable local lease
    OmConfig omConfig = conf.getObject(OmConfig.class);
    omConfig.setFollowerReadLocalLeaseEnabled(true);

    conf.setFromObject(omConfig);

    // config for key deleting service.
    conf.set(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, "10s");
    conf.set(OZONE_KEY_DELETING_LIMIT_PER_TASK, "2");

    MiniOzoneHAClusterImpl.Builder clusterBuilder = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs);

    cluster = clusterBuilder.build();
    cluster.waitForClusterToBeReady();

    OzoneConfiguration clientConf = OzoneConfiguration.of(conf);
    clientConf.setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    client = OzoneClientFactory.getRpcClient(omServiceId, clientConf);
    objectStore = client.getObjectStore();
  }

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
    String keyName = "key" + RandomStringUtils.secure().nextNumeric(5);
    createKey(ozoneBucket, keyName);
    return keyName;
  }

  public static void createKey(OzoneBucket ozoneBucket, String keyName) throws IOException {
    String data = "data" + RandomStringUtils.secure().nextNumeric(5);
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName, data.length(), ReplicationType.RATIS,
        ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();
  }

  public static String createPrefixName() {
    return "prefix" + RandomStringUtils.secure().nextNumeric(5) + OZONE_URI_DELIMITER;
  }

  public static void createPrefix(OzoneObj prefixObj) throws IOException {
    assertTrue(objectStore.setAcl(prefixObj, Collections.emptyList()));
  }

  protected OzoneBucket setupBucket() throws Exception {
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
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
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String linkedVolName = "volume-link-" + RandomStringUtils.secure().nextNumeric(5);

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
   * Create a volume and test its attribute.
   */
  protected void createVolumeTest(boolean checkSuccess) throws Exception {
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);

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
          assertThat(e).hasMessageContaining("is not the leader");
        } else if (e instanceof ConnectException) {
          assertThat(e).hasMessageContaining("Connection refused");
        } else {
          assertThat(e).hasMessageContaining("Could not determine or connect to OM Leader");
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
      IOUtils.readFully(ozoneInputStream, fileContent);
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
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);

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
        IOUtils.readFully(ozoneInputStream, fileContent);
        assertEquals(value, new String(fileContent, UTF_8));
      }

    } catch (IOException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          assertThat(e).hasMessageContaining("is not the leader");
        } else if (e instanceof ConnectException) {
          assertThat(e).hasMessageContaining("Connection refused");
        } else {
          assertThat(e).hasMessageContaining("Could not determine or connect to OM Leader");
        }
      } else {
        throw e;
      }
    }
  }

  protected void listVolumes(boolean checkSuccess)
      throws Exception {
    try {
      getObjectStore().getClientProxy().listVolumes(null, null, 100);
    } catch (IOException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          // Linearizable read will fail with ReadIndexException if the follower does not recognize any leader
          // or leader is uncontactable. It will throw ReadException if the read submitted to Ratis encounters
          // timeout.
          assertThat(e).hasMessageFindingMatch("OMRead(Index)?Exception");
        } else if (e instanceof ConnectException) {
          assertThat(e).hasMessageContaining("Connection refused");
        } else {
          assertThat(e).hasMessageContaining("Could not determine or connect to OM Leader");
        }
      } else {
        throw e;
      }
    }
  }

  protected void waitForLeaderToBeReady()
      throws InterruptedException, TimeoutException {
    // Wait for Leader Election timeout
    cluster.waitForLeaderOM();
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
public class TestSecureOzoneRpcClient extends TestOzoneRpcClient {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static File testDir;
  private static OzoneConfiguration conf;

  /**
   * Create a MiniOzoneCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestSecureOzoneRpcClient.class.getSimpleName());
    OzoneManager.setTestSecureOmFlag(true);
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    CertificateClientTestImpl certificateClientTest =
        new CertificateClientTestImpl(conf);
    // These tests manually insert keys into RocksDB. This is easier to do
    // with object store layout so keys with path IDs do not need to be
    // constructed.
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_OBJECT_STORE);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(14)
        .setScmId(SCM_ID)
        .setClusterId(CLUSTER_ID)
        .setCertificateClient(certificateClientTest)
        .setSecretKeyClient(new SecretKeyTestClient())
        .build();
    cluster.getOzoneManager().startSecretManager();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
    TestOzoneRpcClient.setCluster(cluster);
    TestOzoneRpcClient.setOzClient(ozClient);
    TestOzoneRpcClient.setOzoneManager(ozoneManager);
    TestOzoneRpcClient.setStorageContainerLocationClient(
        storageContainerLocationClient);
    TestOzoneRpcClient.setStore(store);
    TestOzoneRpcClient.setClusterId(CLUSTER_ID);
  }

  /**
   * Tests successful completion of following operations when grpc block
   * token is used.
   * 1. getKey
   * 2. writeChunk
   * */
  @Test
  public void testPutKeySuccessWithBlockToken() throws Exception {
    testPutKeySuccessWithBlockTokenWithBucketLayout(BucketLayout.OBJECT_STORE);
    testPutKeySuccessWithBlockTokenWithBucketLayout(
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  private void testPutKeySuccessWithBlockTokenWithBucketLayout(
      BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName,
        new BucketArgs.Builder().setBucketLayout(bucketLayout).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      long committedBytes = ozoneManager.getMetrics().getDataCommittedBytes();
      try (OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS,
          ReplicationFactor.ONE, new HashMap<>())) {
        out.write(value.getBytes(UTF_8));
      }

      assertEquals(committedBytes + value.getBytes(UTF_8).length,
          ozoneManager.getMetrics().getDataCommittedBytes());

      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
      byte[] fileContent;
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        fileContent = new byte[value.getBytes(UTF_8).length];
        is.read(fileContent);
      }

      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      String bucketId = String.valueOf(
          omMetadataManager.getBucketTable().get(bucketKey).getObjectID());
      String keyPrefix =
          bucketLayout.isFileSystemOptimized() ? bucketId : bucketKey;
      Table table = omMetadataManager.getKeyTable(bucketLayout);

      // Check table entry.
      try (
          TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyIterator = table.iterator()) {
        Table.KeyValue<String, OmKeyInfo> kv =
            keyIterator.seek(keyPrefix + "/" + keyName);

        CacheValue<OmKeyInfo> cacheValue =
            table.getCacheValue(new CacheKey(kv.getKey()));
        if (cacheValue != null) {
          assertTokenIsNull(cacheValue.getCacheValue());
        } else {
          assertTokenIsNull(kv.getValue());
        }
      }


      assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS,
          ReplicationFactor.ONE));
      assertEquals(value, new String(fileContent, UTF_8));
      assertFalse(key.getCreationTime().isBefore(testStartTime));
      assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
  }

  private void assertTokenIsNull(OmKeyInfo value) {
    value.getKeyLocationVersions()
        .forEach(
            keyLocationInfoGroup -> keyLocationInfoGroup.getLocationList()
                .forEach(
                    keyLocationInfo -> assertNull(keyLocationInfo
                        .getToken())));
  }

  @Test
  public void testS3Auth() throws Exception {

    String volumeName = UUID.randomUUID().toString();

    String strToSign = "AWS4-HMAC-SHA256\n" +
        "20150830T123600Z\n" +
        "20150830/us-east-1/iam/aws4_request\n" +
        "f536975d06c0309214f805bb90ccff089219ecd68b2" +
        "577efef23edd43b7e1a59";

    String signature =  "5d672d79c15b13162d9279b0855cfba" +
        "6789a8edb4c82c400e06b5924a6f2b5d7";

    String secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";


    String accessKey = UserGroupInformation.getCurrentUser().getUserName();

    S3SecretManager s3SecretManager = cluster.getOzoneManager()
        .getS3SecretManager();

    // Add secret to S3Secret table.
    s3SecretManager.storeSecret(accessKey,
        S3SecretValue.of(accessKey, secret));

    OMRequest writeRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(UUID.randomUUID().toString())
        .setCreateVolumeRequest(CreateVolumeRequest.newBuilder().
            setVolumeInfo(VolumeInfo.newBuilder().setVolume(volumeName)
                .setAdminName(accessKey).setOwnerName(accessKey).build())
            .build())
        .setS3Authentication(S3Authentication.newBuilder()
            .setAccessId(accessKey)
            .setSignature(signature).setStringToSign(strToSign))
        .build();

    GenericTestUtils.waitFor(() -> cluster.getOzoneManager().isLeaderReady(),
        100, 120000);
    OMResponse omResponse = cluster.getOzoneManager().getOmServerProtocol()
        .submitRequest(null, writeRequest);

    // Verify response.
    assertEquals(Status.OK, omResponse.getStatus());

    // Read Request
    OMRequest readRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.InfoVolume)
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(UUID.randomUUID().toString())
        .setInfoVolumeRequest(InfoVolumeRequest.newBuilder()
            .setVolumeName(volumeName).build())
        .setS3Authentication(S3Authentication.newBuilder()
            .setAccessId(accessKey)
            .setSignature(signature).setStringToSign(strToSign))
        .build();

    omResponse = cluster.getOzoneManager().getOmServerProtocol()
        .submitRequest(null, readRequest);

    // Verify response.
    assertEquals(Status.OK, omResponse.getStatus());

    VolumeInfo volumeInfo = omResponse.getInfoVolumeResponse().getVolumeInfo();
    assertNotNull(volumeInfo);
    assertEquals(volumeName, volumeInfo.getVolume());
    assertEquals(accessKey, volumeInfo.getAdminName());
    assertEquals(accessKey, volumeInfo.getOwnerName());

    // Override secret to S3Secret store with some dummy value
    s3SecretManager
        .storeSecret(accessKey, S3SecretValue.of(accessKey, "dummy"));

    // Write request with invalid credentials.
    omResponse = cluster.getOzoneManager().getOmServerProtocol()
        .submitRequest(null, writeRequest);
    assertEquals(Status.INVALID_TOKEN, omResponse.getStatus());

    // Read request with invalid credentials.
    omResponse = cluster.getOzoneManager().getOmServerProtocol()
        .submitRequest(null, readRequest);
    assertEquals(Status.INVALID_TOKEN, omResponse.getStatus());
  }

  private boolean verifyRatisReplication(String volumeName, String bucketName,
      String keyName, ReplicationType type, ReplicationFactor factor)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    HddsProtos.ReplicationType replicationType =
        HddsProtos.ReplicationType.valueOf(type.toString());
    HddsProtos.ReplicationFactor replicationFactor =
        HddsProtos.ReplicationFactor.valueOf(factor.getValue());
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info:
        keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container =
          storageContainerLocationClient.getContainer(info.getContainerID());
      if (!ReplicationConfig.getLegacyFactor(container.getReplicationConfig())
          .equals(replicationFactor) || (
          container.getReplicationType() != replicationType)) {
        return false;
      }
    }
    return true;
  }

  @Test
  @Override
  // Restart DN doesn't work with security enabled.
  public void testZReadKeyWithUnhealthyContainerReplica() {
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }
}

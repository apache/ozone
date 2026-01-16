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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.FORCE_LEASE_RECOVERY_ENV;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.TestDataUtil.cleanupDeletedTable;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.ozone.test.GenericTestUtils.getTestStartTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.RootedOzoneFileSystem;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test Ozone Client with block tokens enabled.
 */
class TestSecureOzoneRpcClient extends OzoneRpcClientTests {

  @TempDir
  private static File testDir;
  private static String keyProviderUri = "kms://http@localhost:9600/kms";

  @BeforeAll
  public static void init() throws Exception {
    OzoneManager.setTestSecureOmFlag(true);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    CertificateClientTestImpl certificateClientTest =
        new CertificateClientTestImpl(conf);
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        keyProviderUri);
    conf.setBoolean(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED, true);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .setCertificateClient(certificateClientTest)
        .setSecretKeyClient(new SecretKeyTestClient());
    startCluster(conf, builder);
    getCluster().getOzoneManager().startSecretManager();
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
        FILE_SYSTEM_OPTIMIZED);
  }

  private void testPutKeySuccessWithBlockTokenWithBucketLayout(
      BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = getTestStartTime();
    OzoneManager ozoneManager = getCluster().getOzoneManager();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    String value = "sample value";
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName,
        new BucketArgs.Builder().setBucketLayout(bucketLayout).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    RatisReplicationConfig replication = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      long committedBytes = ozoneManager.getMetrics().getDataCommittedBytes();
      TestDataUtil.createKey(bucket, keyName, replication, value.getBytes(UTF_8));

      assertEquals(committedBytes + value.getBytes(UTF_8).length,
          ozoneManager.getMetrics().getDataCommittedBytes());

      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
      byte[] fileContent;
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        fileContent = new byte[value.getBytes(UTF_8).length];
        IOUtils.readFully(is, fileContent);
      }

      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      String bucketId = String.valueOf(
          omMetadataManager.getBucketTable().get(bucketKey).getObjectID());
      String keyPrefix =
          bucketLayout.isFileSystemOptimized() ? bucketId : bucketKey;
      Table<String, OmKeyInfo> table = omMetadataManager.getKeyTable(bucketLayout);

      // Check table entry.
      try (Table.KeyValueIterator<String, OmKeyInfo>
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

      verifyReplication(volumeName, bucketName, keyName, replication);
      assertEquals(value, new String(fileContent, UTF_8));
      assertFalse(key.getCreationTime().isBefore(testStartTime));
      assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFileRecovery(boolean forceRecovery) throws Exception {
    OzoneConfiguration conf = getCluster().getConf();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName,
        new BucketArgs.Builder().setBucketLayout(FILE_SYSTEM_OPTIMIZED).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();
    final String dir = OZONE_ROOT + bucket.getVolumeName() + OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, keyName);

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // force recovery file
    System.setProperty(FORCE_LEASE_RECOVERY_ENV, String.valueOf(forceRecovery));
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", OZONE_OFS_URI_SCHEME), true);
    try (RootedOzoneFileSystem fs = (RootedOzoneFileSystem) FileSystem.get(conf)) {
      OzoneOutputStream out = null;
      try {
        out = bucket.createKey(keyName, value.getBytes(UTF_8).length, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());
        out.write(value.getBytes(UTF_8));
        out.hsync();

        if (forceRecovery) {
          fs.recoverLease(file);
        } else {
          assertThrows(OMException.class, () -> fs.recoverLease(file));
        }
      } finally {
        if (out != null) {
          if (forceRecovery) {
            // close failure because the key is already committed
            assertThrows(OMException.class, out::close);
          } else {
            out.close();
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1 << 24, (1 << 24) + 1, (1 << 24) - 1})
  public void testPreallocateFileRecovery(long dataSize) throws Exception {
    OzoneConfiguration conf = getCluster().getConf();
    cleanupDeletedTable(getCluster().getOzoneManager());
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    final byte[] data = new byte[(int) dataSize];
    ThreadLocalRandom.current().nextBytes(data);

    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    long nsQuota = 100;
    long spaceQuota = 1 * 1024 * 1024 * 1024;
    volume.createBucket(bucketName, new BucketArgs.Builder().setBucketLayout(FILE_SYSTEM_OPTIMIZED)
        .setQuotaInNamespace(nsQuota).setQuotaInBytes(spaceQuota).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();
    final String dir = OZONE_ROOT + bucket.getVolumeName() + OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, keyName);

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    try (RootedOzoneFileSystem fs = (RootedOzoneFileSystem) FileSystem.get(conf)) {
      OzoneOutputStream out = null;
      long totalBlock = 10;
      long usedBlock = (dataSize - 1) / fs.getDefaultBlockSize() + 1;
      long fileSize = fs.getDefaultBlockSize() * totalBlock;
      OMMetrics metrics = getCluster().getOzoneManager().getMetrics();
      long committedBytes = metrics.getDataCommittedBytes();
      try {
        out = bucket.createKey(keyName, fileSize, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());
        // init used quota check
        bucket = volume.getBucket(bucketName);
        assertEquals(0, bucket.getUsedNamespace());
        assertEquals(0, bucket.getUsedBytes());

        out.write(data);
        out.hsync();
        fs.recoverLease(file);

        // check file length
        FileStatus fileStatus = fs.getFileStatus(file);
        assertEquals(dataSize, fileStatus.getLen());
        // check committed bytes
        assertEquals(committedBytes + dataSize,
            getCluster().getOzoneManager().getMetrics().getDataCommittedBytes());
        // check used quota
        GenericTestUtils.waitFor(
            (CheckedSupplier<Boolean, ? extends Exception>) () -> 1 == volume.getBucket(bucketName).getUsedNamespace(),
            1000, 30000);
        GenericTestUtils.waitFor(
            (CheckedSupplier<Boolean, ? extends Exception>) () -> dataSize * ReplicationFactor.THREE.getValue()
                == volume.getBucket(bucketName).getUsedBytes(), 1000, 30000);
        // check unused pre-allocated blocks are reclaimed
        Table<String, RepeatedOmKeyInfo> deletedTable =
            getCluster().getOzoneManager().getMetadataManager().getDeletedTable();
        try (Table.KeyValueIterator<String, RepeatedOmKeyInfo>
                 keyIter = deletedTable.iterator()) {
          while (keyIter.hasNext()) {
            Table.KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
            OmKeyInfo key = kv.getValue().getOmKeyInfoList().get(0);
            assertEquals(totalBlock - usedBlock, key.getKeyLocationVersions().get(0).getLocationListCount());
          }
        }
      } finally {
        if (out != null) {
          // close failure because the key is already committed
          assertThrows(OMException.class, out::close);
        }
      }
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

    S3SecretManager s3SecretManager = getCluster().getOzoneManager()
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

    GenericTestUtils.waitFor(() -> getCluster().getOzoneManager().isLeaderReady(),
        100, 120000);
    OMResponse omResponse = getCluster().getOzoneManager().getOmServerProtocol()
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

    omResponse = getCluster().getOzoneManager().getOmServerProtocol()
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
    omResponse = getCluster().getOzoneManager().getOmServerProtocol()
        .submitRequest(null, writeRequest);
    assertEquals(Status.INVALID_TOKEN, omResponse.getStatus());

    // Read request with invalid credentials.
    omResponse = getCluster().getOzoneManager().getOmServerProtocol()
        .submitRequest(null, readRequest);
    assertEquals(Status.INVALID_TOKEN, omResponse.getStatus());
  }

  @Test
  public void testRemoteException() {
    UserGroupInformation realUser = UserGroupInformation.createRemoteUser("realUser");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("user", realUser);

    assertThrows(AccessControlException.class, () -> {
      proxyUser.doAs((PrivilegedExceptionAction<Void>) () -> {
        try (OzoneClient ozoneClient = OzoneClientFactory.getRpcClient(getCluster().getConf())) {
          ozoneClient.getObjectStore().listVolumes("/");
        }
        return null;
      });
    });
  }

  @Test
  @Override
  // Restart DN doesn't work with security enabled.
  public void testZReadKeyWithUnhealthyContainerReplica() {
  }

  @Override
  @Test
  public void testGetServerDefaults() throws IOException {
    assertNotNull(getClient().getProxy().getServerDefaults());
    assertEquals(keyProviderUri,
        getClient().getProxy().getServerDefaults().getKeyProviderUri());
  }

  @AfterAll
  public static void shutdown() throws IOException {
    shutdownCluster();
  }
}

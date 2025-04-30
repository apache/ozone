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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.utils.ClusterContainersUtil.getContainerByID;
import static org.apache.hadoop.hdds.utils.ClusterContainersUtil.verifyOnDiskData;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.apache.ozone.test.GenericTestUtils.getTestStartTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.cache.Cache;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.MultipartInputStream;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestOzoneAtRestEncryption {

  private static MiniOzoneCluster cluster = null;
  private static MiniKMS miniKMS;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  @TempDir
  private static File testDir;
  private static OzoneConfiguration conf;
  private static final String TEST_KEY = "key1";
  private static final Random RANDOM = new Random();

  private static final int MPU_PART_MIN_SIZE = 256 * 1024; // 256KB
  private static final int BLOCK_SIZE = 64 * 1024; // 64KB
  private static final int CHUNK_SIZE = 16 * 1024; // 16KB
  private static final int DEFAULT_CRYPTO_BUFFER_SIZE = 8 * 1024; // 8KB
  // (this is the default Crypto Buffer size as determined by the config
  // hadoop.security.crypto.buffer.size)
  private static MessageDigest eTagProvider;

  @BeforeAll
  static void init() throws Exception {
    File kmsDir = new File(testDir, UUID.randomUUID().toString());
    assertTrue(kmsDir.mkdirs());
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
    miniKMS = miniKMSBuilder.setKmsConfDir(kmsDir).build();
    miniKMS.start();

    OzoneManager.setTestSecureOmFlag(true);
    conf = new OzoneConfiguration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI(miniKMS));
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    CertificateClientTestImpl certificateClientTest =
        new CertificateClientTestImpl(conf);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(10)
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
    ozoneManager.setMinMultipartUploadPartSize(MPU_PART_MIN_SIZE);

    // create test key
    createKey(TEST_KEY, cluster.getOzoneManager().getKmsProvider(), conf);
    eTagProvider = MessageDigest.getInstance(OzoneConsts.MD5_HASH);

    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    conf.setInt(OZONE_REPLICATION, 1);
  }

  @AfterAll
  static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }

    if (miniKMS != null) {
      miniKMS.stop();
    }
  }

  static void reInitClient() throws IOException {
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
  }

  @Test
  public void testWarmupEDEKCacheOnStartup() throws Exception {

    createVolumeAndBucket("vol", "buck", BucketLayout.OBJECT_STORE);

    @SuppressWarnings("unchecked") KMSClientProvider spy = getKMSClientProvider();
    assertTrue(spy.getEncKeyQueueSize(TEST_KEY) > 0);

    conf.setInt(OMConfigKeys.OZONE_OM_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY, 0);
    cluster.restartOzoneManager();

    GenericTestUtils.waitFor(new BooleanSupplier() {
      @Override
      public boolean getAsBoolean() {
        final KMSClientProvider kspy = getKMSClientProvider();
        return kspy.getEncKeyQueueSize(TEST_KEY) > 0;
      }
    }, 1000, 60000);
  }

  private KMSClientProvider getKMSClientProvider() {
    LoadBalancingKMSClientProvider lbkmscp =
        (LoadBalancingKMSClientProvider) HddsWhiteboxTestUtils.getInternalState(
            cluster.getOzoneManager().getKmsProvider(), "extension");
    assert lbkmscp.getProviders().length == 1;
    return lbkmscp.getProviders()[0];
  }

  @ParameterizedTest
  @EnumSource
  void testPutKeyWithEncryption(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(bucketLayout)
        .setBucketEncryptionKey(TEST_KEY).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    createAndVerifyKeyData(bucket);
    createAndVerifyStreamKeyData(bucket);
    createAndVerifyFileSystemData(bucket);
  }

  @ParameterizedTest
  @EnumSource
  void testLinkEncryptedBuckets(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    // Create source volume/bucket.
    createVolumeAndBucket(volumeName, bucketName, bucketLayout);

    // Create link volume/bucket.
    String linkVolumeName = UUID.randomUUID().toString();
    String linkBucketName = UUID.randomUUID().toString();
    OzoneBucket linkBucket = createLinkVolumeAndBucket(volumeName, bucketName,
        linkVolumeName, linkBucketName);

    createAndVerifyKeyData(linkBucket);
    createAndVerifyStreamKeyData(linkBucket);
  }

  static void createAndVerifyStreamKeyData(OzoneBucket bucket)
      throws Exception {
    Instant testStartTime = getTestStartTime();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    try (OzoneDataStreamOutput out = bucket.createStreamKey(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, value, testStartTime);
  }

  static void createAndVerifyKeyData(OzoneBucket bucket) throws Exception {
    Instant testStartTime = getTestStartTime();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";

    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        value.getBytes(StandardCharsets.UTF_8));

    verifyKeyData(bucket, keyName, value, testStartTime);
    OzoneKeyDetails key1 = bucket.getKey(keyName);

    // Overwrite the key
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        value.getBytes(StandardCharsets.UTF_8));

    OzoneKeyDetails key2 = bucket.getKey(keyName);
    assertNotEquals(key1.getFileEncryptionInfo().toString(), key2.getFileEncryptionInfo().toString());
  }

  static void createAndVerifyFileSystemData(
      OzoneBucket bucket) throws Exception {
    // OBS does not support file system semantics.
    if (bucket.getBucketLayout() == BucketLayout.OBJECT_STORE) {
      return;
    }
    Instant testStartTime = getTestStartTime();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, keyName);
    try (FileSystem fs = FileSystem.get(conf)) {
      try (FSDataOutputStream out = fs.create(file, true)) {
        out.write(value.getBytes(StandardCharsets.UTF_8));
      }
    }
    verifyKeyData(bucket, keyName, value, testStartTime);
  }

  static void verifyKeyData(OzoneBucket bucket, String keyName, String value,
      Instant testStartTime) throws Exception {
    // Verify content.
    OzoneKeyDetails key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());

    // Check file encryption info is set,
    // if set key will use this encryption info and encrypt data.
    assertNotNull(key.getFileEncryptionInfo());

    byte[] fileContent;
    int len = 0;

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      fileContent = new byte[value.getBytes(StandardCharsets.UTF_8).length];
      len = is.read(fileContent);
    }

    assertEquals(len, value.length());
    assertTrue(verifyRatisReplication(bucket.getVolumeName(),
        bucket.getName(), keyName, RATIS,
        ONE));
    assertEquals(value, new String(fileContent, StandardCharsets.UTF_8));
    assertFalse(key.getCreationTime().isBefore(testStartTime));
    assertFalse(key.getModificationTime().isBefore(testStartTime));

    long containerID = key.getOzoneKeyLocations().get(0)
        .getContainerID();
    Container container = getContainerByID(cluster, containerID);
    // the data stored on disk should not be the same as the input.
    assertFalse(verifyOnDiskData(cluster, container, key, value),
        "On disk block is written in clear text!");
  }

  private OzoneBucket createVolumeAndBucket(String volumeName,
      String bucketName, BucketLayout bucketLayout) throws Exception {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketEncryptionKey(TEST_KEY)
        .setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
    return volume.getBucket(bucketName);
  }

  private OzoneBucket createLinkVolumeAndBucket(String sourceVol,
      String sourceBucket, String linkVol, String linkBucket) throws Exception {
    store.createVolume(linkVol);
    OzoneVolume linkVolume = store.getVolume(linkVol);
    BucketArgs linkBucketArgs = BucketArgs.newBuilder()
        .setSourceVolume(sourceVol)
        .setSourceBucket(sourceBucket)
        .build();
    linkVolume.createBucket(linkBucket, linkBucketArgs);
    return linkVolume.getBucket(linkBucket);
  }

  /**
   * Test PutKey & DeleteKey with Encryption and GDPR.
   * 1. Create a GDPR enforced bucket
   * 2. PutKey with Encryption in above bucket and verify.
   * 3. DeleteKey and confirm the metadata does not have encryption key.
   */
  @ParameterizedTest
  @EnumSource
  void testKeyWithEncryptionAndGdpr(BucketLayout bucketLayout)
      throws Exception {
    //Step 1
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = getTestStartTime();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    //Bucket with Encryption & GDPR enforced
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketEncryptionKey(TEST_KEY)
        .addMetadata(OzoneConsts.GDPR_FLAG, "true")
        .setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertNotNull(bucket.getMetadata());
    assertEquals("true", bucket.getMetadata().get(OzoneConsts.GDPR_FLAG));

    //Step 2
    String keyName = UUID.randomUUID().toString();
    Map<String, String> keyMetadata = new HashMap<>();
    keyMetadata.put(OzoneConsts.GDPR_FLAG, "true");
    try (OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        keyMetadata)) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }

    OzoneKeyDetails key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    byte[] fileContent;
    int len = 0;

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      fileContent = new byte[value.getBytes(StandardCharsets.UTF_8).length];
      len = is.read(fileContent);
    }

    assertEquals(len, value.length());
    assertTrue(verifyRatisReplication(volumeName, bucketName,
        keyName, RATIS,
        ONE));
    assertEquals(value, new String(fileContent, StandardCharsets.UTF_8));
    assertFalse(key.getCreationTime().isBefore(testStartTime));
    assertFalse(key.getModificationTime().isBefore(testStartTime));
    assertEquals("true", key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    //As TDE is enabled, the TDE encryption details should not be null.
    assertNotNull(key.getFileEncryptionInfo());

    //Step 3
    bucket.deleteKey(key.getName());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    GenericTestUtils.waitFor(() -> {
      try {
        return getMatchedKeyInfo(keyName, omMetadataManager) != null;
      } catch (IOException e) {
        return false;
      }
    }, 500, 100000);
    RepeatedOmKeyInfo deletedKeys =
        getMatchedKeyInfo(keyName, omMetadataManager);
    assertNotNull(deletedKeys);
    Map<String, String> deletedKeyMetadata =
        deletedKeys.getOmKeyInfoList().get(0).getMetadata();
    assertThat(deletedKeyMetadata).doesNotContainKey(OzoneConsts.GDPR_FLAG);
    assertThat(deletedKeyMetadata).doesNotContainKey(OzoneConsts.GDPR_SECRET);
    assertThat(deletedKeyMetadata).doesNotContainKey(OzoneConsts.GDPR_ALGORITHM);
    assertNull(deletedKeys.getOmKeyInfoList().get(0).getFileEncryptionInfo());
  }

  static boolean verifyRatisReplication(String volumeName, String bucketName,
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

  private static String getKeyProviderURI(MiniKMS kms) {
    return KMSClientProvider.SCHEME_NAME + "://" +
        kms.getKMSUrl().toExternalForm().replace("://", "@");
  }

  private static void createKey(String keyName, KeyProvider
      provider, OzoneConfiguration config)
      throws NoSuchAlgorithmException, IOException {
    final KeyProvider.Options options = KeyProvider.options(config);
    options.setDescription(keyName);
    options.setBitLength(128);
    provider.createKey(keyName, options);
    provider.flush();
  }

  @ParameterizedTest
  @EnumSource
  void mpuOnePart(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    testMultipartUploadWithEncryption(
        createVolumeAndBucket(volumeName, bucketName, bucketLayout), 1);
  }

  @ParameterizedTest
  @EnumSource
  void mpuOnePartInvalidUploadID(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OMException e = assertThrows(OMException.class, () ->
        testMultipartUploadWithEncryption(
        createVolumeAndBucket(volumeName, bucketName, bucketLayout), 1, false, true)
    );
    assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR, e.getResult());
  }

  @ParameterizedTest
  @EnumSource
  void mpuTwoParts(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    testMultipartUploadWithEncryption(
        createVolumeAndBucket(volumeName, bucketName, bucketLayout), 2);
  }

  @ParameterizedTest
  @EnumSource
  void mpuThreePartsOverride(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName,
        bucketLayout);
    testMultipartUploadWithEncryption(bucket, 3);

    // override the key and check content
    testMultipartUploadWithEncryption(bucket, 3);
  }

  @ParameterizedTest
  @EnumSource
  @Flaky("HDDS-8947")
  void mpuStreamOnePart(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    testMultipartUploadWithEncryption(
        createVolumeAndBucket(volumeName, bucketName, bucketLayout), 1, true);
  }

  @ParameterizedTest
  @EnumSource
  @Flaky("HDDS-8947")
  void mpuStreamTwoParts(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    testMultipartUploadWithEncryption(
        createVolumeAndBucket(volumeName, bucketName, bucketLayout), 2, true);
  }

  @ParameterizedTest
  @EnumSource
  @Flaky("HDDS-8947")
  void mpuStreamThreePartsOverride(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName,
        bucketLayout);
    testMultipartUploadWithEncryption(bucket, 3);

    // override the key and check content
    testMultipartUploadWithEncryption(bucket, 3, true);
  }

  @ParameterizedTest
  @EnumSource
  void mpuWithLink(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    createVolumeAndBucket(volumeName, bucketName, bucketLayout);

    String linkVolumeName = UUID.randomUUID().toString();
    String linkBucketName = UUID.randomUUID().toString();
    OzoneBucket linkBucket = createLinkVolumeAndBucket(volumeName, bucketName,
        linkVolumeName, linkBucketName);
    testMultipartUploadWithEncryption(linkBucket, 2);
  }

  private void testMultipartUploadWithEncryption(OzoneBucket bucket,
      int numParts) throws Exception {
    testMultipartUploadWithEncryption(bucket, numParts, false);
  }

  private void testMultipartUploadWithEncryption(OzoneBucket bucket,
      int numParts, boolean isStream) throws Exception {
    testMultipartUploadWithEncryption(bucket, numParts, isStream, false);
  }

  private void testMultipartUploadWithEncryption(OzoneBucket bucket,
      int numParts, boolean isStream, boolean invalidUploadID) throws Exception {
    String keyName = "mpu_test_key_" + numParts;

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE));

    if (invalidUploadID) {
      uploadID += "random1234";
    }

    // Upload Parts
    Map<Integer, String> partsMap = new TreeMap<>();
    List<byte[]> partsData = new ArrayList<>();
    int keySize = 0;
    for (int i = 1; i <= numParts; i++) {
      // Generate random data with different sizes for each part.
      // Adding a random int with a cap at 8K (the default crypto buffer
      // size) to get parts whose last byte does not coincide with crypto
      // buffer boundary.
      int partSize = (MPU_PART_MIN_SIZE * i) +
          RANDOM.nextInt(DEFAULT_CRYPTO_BUFFER_SIZE - 1) + 1;
      byte[] data = generateRandomData(partSize);

      String partName;
      if (isStream) {
        partName = uploadStreamPart(bucket, keyName, uploadID, i, data);
      } else {
        partName = uploadPart(bucket, keyName, uploadID, i, data);
      }

      partsMap.put(i, partName);
      partsData.add(data);
      keySize += data.length;
    }

    // Combine the parts data into 1 byte array for verification
    byte[] inputData = new byte[keySize];
    int dataCopied = 0;
    for (int i = 1; i <= numParts; i++) {
      byte[] partBytes = partsData.get(i - 1);
      System.arraycopy(partBytes, 0, inputData, dataCopied, partBytes.length);
      dataCopied += partBytes.length;
    }

    // Complete MPU
    completeMultipartUpload(bucket, keyName, uploadID, partsMap);

    // Create an input stream to read the data
    try (OzoneInputStream inputStream = bucket.readKey(keyName)) {

      assertInstanceOf(MultipartInputStream.class, inputStream.getInputStream());

      // Test complete read
      byte[] completeRead = new byte[keySize];
      int bytesRead = inputStream.read(completeRead, 0, keySize);
      assertEquals(bytesRead, keySize);
      assertArrayEquals(inputData, completeRead);

      // Read different data lengths and starting from different offsets and
      // verify the data matches.
      int randomSize = RandomUtils.secure().randomInt(0, keySize / 2);
      int randomOffset = RandomUtils.secure().randomInt(0, keySize - randomSize);

      int[] readDataSizes = {keySize, keySize / 3 + 1, BLOCK_SIZE,
          BLOCK_SIZE * 2 + 1, CHUNK_SIZE, CHUNK_SIZE / 4 - 1,
          DEFAULT_CRYPTO_BUFFER_SIZE, DEFAULT_CRYPTO_BUFFER_SIZE / 2, 1,
          randomSize};

      int[] readFromPositions = {0, DEFAULT_CRYPTO_BUFFER_SIZE + 10, CHUNK_SIZE,
          BLOCK_SIZE - DEFAULT_CRYPTO_BUFFER_SIZE + 1, BLOCK_SIZE, keySize / 3,
          keySize - 1, randomOffset};

      for (int readDataLen : readDataSizes) {
        for (int readFromPosition : readFromPositions) {
          // Check that offset + buffer size does not exceed the key size
          if (readFromPosition + readDataLen > keySize) {
            continue;
          }

          byte[] readData = new byte[readDataLen];
          inputStream.seek(readFromPosition);
          int actualReadLen = inputStream.read(readData, 0, readDataLen);

          assertReadContent(inputData, readData, readFromPosition);
          assertEquals(readFromPosition + readDataLen, inputStream.getPos());
          assertEquals(readDataLen, actualReadLen);
        }
      }
    }
  }

  private static byte[] generateRandomData(int length) {
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      ReplicationConfig replicationConfig)
      throws Exception {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationConfig);

    String uploadID = multipartInfo.getUploadID();
    assertNotNull(uploadID);
    return uploadID;
  }

  private String uploadStreamPart(OzoneBucket bucket, String keyName,
      String uploadID, int partNumber, byte[] data) throws Exception {
    final int length = data.length;

    OzoneDataStreamOutput multipartStreamKey =
        bucket.createMultipartStreamKey(keyName,
            length, partNumber, uploadID);

    ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    multipartStreamKey.write(dataBuffer, 0, length);
    multipartStreamKey.getMetadata().put(OzoneConsts.ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    multipartStreamKey.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        multipartStreamKey.getCommitUploadPartInfo();

    assertNotNull(omMultipartCommitUploadPartInfo);
    assertNotNull(omMultipartCommitUploadPartInfo.getETag());
    return omMultipartCommitUploadPartInfo.getETag();
  }

  private String uploadPart(OzoneBucket bucket, String keyName,
      String uploadID, int partNumber, byte[] data) throws Exception {
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    assertNotNull(omMultipartCommitUploadPartInfo);
    assertNotNull(omMultipartCommitUploadPartInfo.getETag());
    return omMultipartCommitUploadPartInfo.getETag();
  }

  private void completeMultipartUpload(OzoneBucket bucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = bucket
        .completeMultipartUpload(keyName, uploadID, partsMap);

    assertNotNull(omMultipartUploadCompleteInfo);
    assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private static void assertReadContent(byte[] inputData, byte[] readData,
      int offset) {
    byte[] inputDataForComparison = Arrays.copyOfRange(inputData, offset,
        offset + readData.length);
    assertArrayEquals(inputDataForComparison, readData,
        "Read data does not match input data at offset " +
            offset + " and length " + readData.length);
  }

  @Test
  void testGetKeyProvider() throws Exception {
    KeyProvider kp1 = store.getKeyProvider();
    KeyProvider kpSpy = spy(kp1);
    assertNotEquals(kpSpy, kp1);
    Cache<URI, KeyProvider> cacheSpy =
        ((RpcClient)store.getClientProxy()).getKeyProviderCache();
    cacheSpy.put(store.getKeyProviderUri(), kpSpy);
    KeyProvider kp2 = store.getKeyProvider();
    assertEquals(kpSpy, kp2);

    // Verify the spied key provider is closed upon ozone client close
    ozClient.close();
    verify(kpSpy).close();

    KeyProvider kp3 = ozClient.getObjectStore().getKeyProvider();
    assertNotEquals(kp3, kpSpy);
    reInitClient();
  }

  private static RepeatedOmKeyInfo getMatchedKeyInfo(
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(
        null, 100, "/");
    for (Table.KeyValue<String, RepeatedOmKeyInfo> rangeKV : rangeKVs) {
      if (rangeKV.getKey().contains(keyName)) {
        return rangeKV.getValue();
      }
    }
    return null;
  }
}

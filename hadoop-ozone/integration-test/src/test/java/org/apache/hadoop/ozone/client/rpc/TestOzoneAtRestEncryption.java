/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.rpc;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import java.util.Collection;

import com.google.common.cache.Cache;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.hdds.scm.storage.MultipartInputStream;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.GenericTestUtils;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
@RunWith(Parameterized.class)
public class TestOzoneAtRestEncryption {

  @Parameterized.Parameters
  public static Collection<BucketLayout> data() {
    return Arrays.asList(
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        BucketLayout.OBJECT_STORE);
  }

  private static MiniOzoneCluster cluster = null;
  private static MiniKMS miniKMS;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
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
  private final BucketLayout bucketLayout;

  public TestOzoneAtRestEncryption(BucketLayout layout) {
    bucketLayout = layout;
  }

  @BeforeClass
  public static void init() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestSecureOzoneRpcClient.class.getSimpleName());

    File kmsDir = new File(testDir, UUID.randomUUID().toString());
    Assert.assertTrue(kmsDir.mkdirs());
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
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(10)
        .setScmId(SCM_ID)
        .setClusterId(CLUSTER_ID)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .setCertificateClient(certificateClientTest)
        .build();
    cluster.getOzoneManager().startSecretManager();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
    ozoneManager.setMinMultipartUploadPartSize(MPU_PART_MIN_SIZE);
    TestOzoneRpcClient.setCluster(cluster);
    TestOzoneRpcClient.setOzClient(ozClient);
    TestOzoneRpcClient.setOzoneManager(ozoneManager);
    TestOzoneRpcClient.setStorageContainerLocationClient(
        storageContainerLocationClient);
    TestOzoneRpcClient.setStore(store);
    TestOzoneRpcClient.setClusterId(CLUSTER_ID);

    // create test key
    createKey(TEST_KEY, cluster.getOzoneManager().getKmsProvider(), conf);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
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

    if (miniKMS != null) {
      miniKMS.stop();
    }
  }

  @Test
  public void testPutKeyWithEncryption() throws Exception {
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
  }

  @Test
  public void testLinkEncryptedBuckets() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    // Create source volume/bucket.
    createVolumeAndBucket(volumeName, bucketName);

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
    Instant testStartTime = Instant.now();
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
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    try (OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, value, testStartTime);
  }

  static void verifyKeyData(OzoneBucket bucket, String keyName, String value,
      Instant testStartTime) throws Exception {
    // Verify content.
    OzoneKeyDetails key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());

    // Check file encryption info is set,
    // if set key will use this encryption info and encrypt data.
    Assert.assertNotNull(key.getFileEncryptionInfo());

    byte[] fileContent;
    int len = 0;

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      fileContent = new byte[value.getBytes(StandardCharsets.UTF_8).length];
      len = is.read(fileContent);
    }


    Assert.assertEquals(len, value.length());
    Assert.assertTrue(verifyRatisReplication(bucket.getVolumeName(),
        bucket.getName(), keyName, ReplicationType.RATIS,
        ReplicationFactor.ONE));
    Assert.assertEquals(value, new String(fileContent, StandardCharsets.UTF_8));
    Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
    Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
  }

  private OzoneBucket createVolumeAndBucket(String volumeName,
      String bucketName) throws Exception {
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
        .setSourceVolume(sourceVol).setSourceBucket(sourceBucket)
        .setBucketLayout(bucketLayout).build();
    linkVolume.createBucket(linkBucket, linkBucketArgs);
    return linkVolume.getBucket(linkBucket);
  }

  /**
   * Test PutKey & DeleteKey with Encryption and GDPR.
   * 1. Create a GDPR enforced bucket
   * 2. PutKey with Encryption in above bucket and verify.
   * 3. DeleteKey and confirm the metadata does not have encryption key.
   * @throws Exception
   */
  @Test
  public void testKeyWithEncryptionAndGdpr() throws Exception {
    //Step 1
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();

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
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertNotNull(bucket.getMetadata());
    Assert.assertEquals("true",
        bucket.getMetadata().get(OzoneConsts.GDPR_FLAG));

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
    Assert.assertEquals(keyName, key.getName());
    byte[] fileContent;
    int len = 0;

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      fileContent = new byte[value.getBytes(StandardCharsets.UTF_8).length];
      len = is.read(fileContent);
    }

    Assert.assertEquals(len, value.length());
    Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
        keyName, ReplicationType.RATIS,
        ReplicationFactor.ONE));
    Assert.assertEquals(value, new String(fileContent, StandardCharsets.UTF_8));
    Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
    Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    Assert.assertEquals("true", key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    //As TDE is enabled, the TDE encryption details should not be null.
    Assert.assertNotNull(key.getFileEncryptionInfo());

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
    Assert.assertNotNull(deletedKeys);
    Map<String, String> deletedKeyMetadata =
        deletedKeys.getOmKeyInfoList().get(0).getMetadata();
    Assert.assertFalse(deletedKeyMetadata.containsKey(OzoneConsts.GDPR_FLAG));
    Assert.assertFalse(deletedKeyMetadata.containsKey(OzoneConsts.GDPR_SECRET));
    Assert.assertFalse(
        deletedKeyMetadata.containsKey(OzoneConsts.GDPR_ALGORITHM));
    Assert.assertNull(
        deletedKeys.getOmKeyInfoList().get(0).getFileEncryptionInfo());
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

  @Test
  public void testMPUwithOnePart() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName);
    testMultipartUploadWithEncryption(bucket, 1);
  }

  @Test
  public void testMPUwithTwoParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName);
    testMultipartUploadWithEncryption(bucket, 2);
  }

  @Test
  public void testMPUwithThreePartsOverride() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName);
    testMultipartUploadWithEncryption(bucket, 3);

    // override the key and check content
    testMultipartUploadWithEncryption(bucket, 3);
  }

  @Test
  public void testMPUwithOneStreamPart() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName);
    testMultipartUploadWithEncryption(bucket, 1, true);
  }

  @Test
  public void testMPUwithTwoStreamParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName);
    testMultipartUploadWithEncryption(bucket, 2, true);
  }

  @Test
  public void testMPUwithThreeStreamPartsOverride() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneBucket bucket = createVolumeAndBucket(volumeName, bucketName);
    testMultipartUploadWithEncryption(bucket, 3);

    // override the key and check content
    testMultipartUploadWithEncryption(bucket, 3, true);
  }

  @Test
  public void testMPUwithLinkBucket() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    createVolumeAndBucket(volumeName, bucketName);

    String linkVolumeName = UUID.randomUUID().toString();
    String linkBucketName = UUID.randomUUID().toString();
    OzoneBucket linkBucket = createLinkVolumeAndBucket(volumeName, bucketName,
        linkVolumeName, linkBucketName);
    testMultipartUploadWithEncryption(linkBucket, 2);
  }

  public void testMultipartUploadWithEncryption(OzoneBucket bucket,
      int numParts) throws Exception {
    testMultipartUploadWithEncryption(bucket, numParts, false);
  }

  public void testMultipartUploadWithEncryption(OzoneBucket bucket,
      int numParts, boolean isStream) throws Exception {
    String keyName = "mpu_test_key_" + numParts;

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE));

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

      Assert.assertTrue(inputStream.getInputStream()
          instanceof MultipartInputStream);

      // Test complete read
      byte[] completeRead = new byte[keySize];
      int bytesRead = inputStream.read(completeRead, 0, keySize);
      Assert.assertEquals(bytesRead, keySize);
      Assert.assertArrayEquals(inputData, completeRead);

      // Read different data lengths and starting from different offsets and
      // verify the data matches.
      Random random = new Random();
      int randomSize = random.nextInt(keySize / 2);
      int randomOffset = random.nextInt(keySize - randomSize);

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
          Assert.assertEquals(readFromPosition + readDataLen,
              inputStream.getPos());
          Assert.assertEquals(readDataLen, actualReadLen);
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
    Assert.assertNotNull(uploadID);
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
    multipartStreamKey.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        multipartStreamKey.getCommitUploadPartInfo();

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);
    Assert.assertNotNull(omMultipartCommitUploadPartInfo.getPartName());
    return omMultipartCommitUploadPartInfo.getPartName();
  }

  private String uploadPart(OzoneBucket bucket, String keyName,
      String uploadID, int partNumber, byte[] data) throws Exception {
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);
    Assert.assertNotNull(omMultipartCommitUploadPartInfo.getPartName());
    return omMultipartCommitUploadPartInfo.getPartName();
  }

  private void completeMultipartUpload(OzoneBucket bucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = bucket
        .completeMultipartUpload(keyName, uploadID, partsMap);

    Assert.assertNotNull(omMultipartUploadCompleteInfo);
    Assert.assertEquals(omMultipartUploadCompleteInfo.getBucket(), bucket
        .getName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getVolume(), bucket
        .getVolumeName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getKey(), keyName);
    Assert.assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private static void assertReadContent(byte[] inputData, byte[] readData,
      int offset) {
    byte[] inputDataForComparison = Arrays.copyOfRange(inputData, offset,
        offset + readData.length);
    Assert.assertArrayEquals("Read data does not match input data at offset " +
        offset + " and length " + readData.length,
        inputDataForComparison, readData);
  }

  @Test
  public void testGetKeyProvider() throws Exception {
    KeyProvider kp1 = store.getKeyProvider();
    KeyProvider kpSpy = Mockito.spy(kp1);
    Assert.assertNotEquals(kpSpy, kp1);
    Cache<URI, KeyProvider> cacheSpy =
        ((RpcClient)store.getClientProxy()).getKeyProviderCache();
    cacheSpy.put(store.getKeyProviderUri(), kpSpy);
    KeyProvider kp2 = store.getKeyProvider();
    Assert.assertEquals(kpSpy, kp2);

    // Verify the spied key provider is closed upon ozone client close
    ozClient.close();
    Mockito.verify(kpSpy).close();

    KeyProvider kp3 = ozClient.getObjectStore().getKeyProvider();
    Assert.assertNotEquals(kp3, kpSpy);
    // Restore ozClient and store
    TestOzoneRpcClient.setOzClient(OzoneClientFactory.getRpcClient(conf));
    TestOzoneRpcClient.setStore(ozClient.getObjectStore());
  }

  private static RepeatedOmKeyInfo getMatchedKeyInfo(
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(
        null, 100, "/");
    for (int i = 0; i < rangeKVs.size(); ++i) {
      if (rangeKVs.get(i).getKey().contains(keyName)) {
        return rangeKVs.get(i).getValue();
      }
    }
    return null;
  }
}

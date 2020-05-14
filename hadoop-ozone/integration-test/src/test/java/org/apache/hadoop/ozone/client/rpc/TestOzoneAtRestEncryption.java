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
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
@Ignore
public class TestOzoneAtRestEncryption extends TestOzoneRpcClient {

  private static MiniOzoneCluster cluster = null;
  private static MiniKMS miniKMS;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private static final String SCM_ID = UUID.randomUUID().toString();
  private static File testDir;
  private static OzoneConfiguration conf;
  private static final String TEST_KEY = "key1";


    /**
     * Create a MiniOzoneCluster for testing.
     * <p>
     * Ozone is made active by setting OZONE_ENABLED = true
     *
     * @throws IOException
     */
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
        .setCertificateClient(certificateClientTest)
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
    TestOzoneRpcClient.setScmId(SCM_ID);

    // create test key
    createKey(TEST_KEY, cluster.getOzoneManager().getKmsProvider(), conf);
  }



    /**
     * Close OzoneClient and shutdown MiniOzoneCluster.
     */
  @AfterClass
  public static void shutdown() throws IOException {
    if(ozClient != null) {
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
    Instant testStartTime = Instant.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketEncryptionKey(TEST_KEY).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 1; i++) {
      String keyName = UUID.randomUUID().toString();

      try (OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes("UTF-8").length, ReplicationType.STAND_ALONE,
          1, new HashMap<>())) {
        out.write(value.getBytes("UTF-8"));
      }

      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      byte[] fileContent;
      int len = 0;

      try(OzoneInputStream is = bucket.readKey(keyName)) {
        fileContent = new byte[value.getBytes("UTF-8").length];
        len = is.read(fileContent);
      }

      Assert.assertEquals(len, value.length());
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.STAND_ALONE,
          1));
      Assert.assertEquals(value, new String(fileContent, "UTF-8"));
      Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
      Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
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
        .addMetadata(OzoneConsts.GDPR_FLAG, "true").build();
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
        value.getBytes("UTF-8").length, ReplicationType.STAND_ALONE,
        1, keyMetadata)) {
      out.write(value.getBytes("UTF-8"));
    }

    OzoneKeyDetails key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    byte[] fileContent;
    int len = 0;

    try(OzoneInputStream is = bucket.readKey(keyName)) {
      fileContent = new byte[value.getBytes("UTF-8").length];
      len = is.read(fileContent);
    }

    Assert.assertEquals(len, value.length());
    Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
        keyName, ReplicationType.STAND_ALONE,
        1));
    Assert.assertEquals(value, new String(fileContent, "UTF-8"));
    Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
    Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    Assert.assertEquals("true", key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    //As TDE is enabled, the TDE encryption details should not be null.
    Assert.assertNotNull(key.getFileEncryptionInfo());

    //Step 3
    bucket.deleteKey(key.getName());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String objectKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    RepeatedOmKeyInfo deletedKeys =
        omMetadataManager.getDeletedTable().get(objectKey);
    Map<String, String> deletedKeyMetadata =
        deletedKeys.getOmKeyInfoList().get(0).getMetadata();
    Assert.assertFalse(deletedKeyMetadata.containsKey(OzoneConsts.GDPR_FLAG));
    Assert.assertFalse(deletedKeyMetadata.containsKey(OzoneConsts.GDPR_SECRET));
    Assert.assertFalse(
        deletedKeyMetadata.containsKey(OzoneConsts.GDPR_ALGORITHM));
    Assert.assertNull(
        deletedKeys.getOmKeyInfoList().get(0).getFileEncryptionInfo());
  }

  private boolean verifyRatisReplication(String volumeName, String bucketName,
      String keyName, ReplicationType type, int replication)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();
    HddsProtos.ReplicationType replicationType =
        HddsProtos.ReplicationType.valueOf(type.toString());

    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info:
        keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container =
          storageContainerLocationClient.getContainer(info.getContainerID());
      if (container.getReplication() != replication || (
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
}

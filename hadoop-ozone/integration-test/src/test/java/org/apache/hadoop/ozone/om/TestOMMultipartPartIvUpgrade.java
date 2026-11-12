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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * Upgrade testing for multipart upload parts with their own IV in encrypted buckets.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize: parts are encrypted with the IV of the key.
 * <p>
 * 2. Post-Finalize: new parts get their own IV. A key with parts from before and after finalization reads back
 * correctly, and clients older than {@link ClientVersion#MULTIPART_PART_IV_SUPPORT} can not look it up.
 * <p>
 * 3. Post-Finalize with {@link OmConfig.Keys#MPU_PART_ENCRYPTION_IV_ENABLED} set to false: parts are encrypted with
 * the IV of the key, and older clients can look the key up.
 *
 * Test cases are run on the same single cluster. Order is important, because "upgrade" changes its state.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestOMMultipartPartIvUpgrade {

  private static final int PRE_UPGRADE = 100;
  private static final int DURING_UPGRADE = 200;
  private static final int POST_UPGRADE = 300;

  private static final String ENCRYPTION_KEY = "key1";
  private static final int PART_SIZE = 4096;
  // Key with all parts written before finalization.
  private static final String OLD_KEY = "old-key";
  // Key with one part written before and one after finalization.
  private static final String MIXED_KEY = "mixed-key";
  // Key with all parts written after finalization, with part IVs disabled.
  private static final String DISABLED_KEY = "disabled-key";

  @TempDir
  private static Path testDir;

  private MiniOzoneCluster cluster;
  private OzoneManager ozoneManager;
  private OzoneClient client;
  private OzoneManagerProtocol omClient;
  private OzoneBucket bucket;

  private String mixedUploadID;
  private final Map<Integer, String> mixedParts = new TreeMap<>();
  private final byte[] part1 = RandomUtils.secure().randomBytes(PART_SIZE);
  private final byte[] part2 = RandomUtils.secure().randomBytes(PART_SIZE);

  @BeforeAll
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY, OzoneManagerVersion.LIST_STATUS_REJECTS_OBS.serialize());
    conf.set(OMConfigKeys.OZONE_OM_UPGRADE_FINALIZATION_CHECK_INTERVAL, "10ms");
    String keyProviderUri = JavaKeyStoreProvider.SCHEME_NAME + "://file" + testDir.resolve("kms.jks").toUri().getPath();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH, keyProviderUri);
    KeyProvider keyProvider = KeyProviderFactory.get(new URI(keyProviderUri), conf);
    KeyProvider.Options options = KeyProvider.options(conf);
    options.setBitLength(128);
    keyProvider.createKey(ENCRYPTION_KEY, options);
    keyProvider.flush();

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
    ozoneManager.setMinMultipartUploadPartSize(PART_SIZE);
    client = cluster.newClient();
    omClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();

    String volumeName = "vol-" + UUID.randomUUID();
    client.getObjectStore().createVolume(volumeName);
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    volume.createBucket("bucket", BucketArgs.newBuilder().setBucketEncryptionKey(ENCRYPTION_KEY).build());
    bucket = volume.getBucket("bucket");
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  @Order(PRE_UPGRADE)
  void writePartsBeforeUpgrade() throws Exception {
    assertFalse(ozoneManager.getVersionManager().isAllowed(OzoneManagerVersion.MULTIPART_PART_IV));

    String oldUploadID = initiateUpload(OLD_KEY);
    Map<Integer, String> oldParts = new TreeMap<>();
    oldParts.put(1, uploadPart(OLD_KEY, oldUploadID, 1, part1));
    oldParts.put(2, uploadPart(OLD_KEY, oldUploadID, 2, part2));
    bucket.completeMultipartUpload(OLD_KEY, oldUploadID, oldParts);
    assertKeyContent(OLD_KEY);
    assertThat(getLocations(OLD_KEY)).isNotEmpty().allSatisfy(location -> assertNull(location.getPartIv()));

    mixedUploadID = initiateUpload(MIXED_KEY);
    mixedParts.put(1, uploadPart(MIXED_KEY, mixedUploadID, 1, part1));
  }

  @Test
  @Order(DURING_UPGRADE)
  void finalizeUpgrade() throws Exception {
    omClient.finalizeUpgrade();
    waitForFinalization(omClient);
    GenericTestUtils.waitFor(() -> ozoneManager.getVersionManager().isAllowed(OzoneManagerVersion.MULTIPART_PART_IV),
        100, 30000);
  }

  @Test
  @Order(POST_UPGRADE)
  void readMixedKeyAfterUpgrade() throws Exception {
    mixedParts.put(2, uploadPart(MIXED_KEY, mixedUploadID, 2, part2));
    bucket.completeMultipartUpload(MIXED_KEY, mixedUploadID, mixedParts);
    assertKeyContent(MIXED_KEY);

    List<OmKeyLocationInfo> locations = getLocations(MIXED_KEY);
    assertThat(locations).filteredOn(location -> location.getPartNumber() == 1).isNotEmpty()
        .allSatisfy(location -> assertNull(location.getPartIv()));
    assertThat(locations).filteredOn(location -> location.getPartNumber() == 2).isNotEmpty()
        .allSatisfy(location -> assertThat(location.getPartIv()).hasSize(16));
  }

  @Test
  @Order(POST_UPGRADE + 1)
  void olderClientCanOnlyLookUpKeyWithoutPartIv() throws Exception {
    assertEquals(Status.OK, lookupKey(OLD_KEY, ClientVersion.BUCKET_LAYOUT_SUPPORT).getStatus());
    assertEquals(Status.OK, lookupKey(MIXED_KEY, ClientVersion.CURRENT).getStatus());

    OMResponse response = lookupKey(MIXED_KEY, ClientVersion.BUCKET_LAYOUT_SUPPORT);
    assertEquals(Status.NOT_SUPPORTED_OPERATION, response.getStatus());
    assertFalse(response.hasLookupKeyResponse());
  }

  @Test
  @Order(POST_UPGRADE + 2)
  void partsUseKeyIvWhenDisabled() throws Exception {
    OzoneConfiguration original = ozoneManager.getConfiguration();
    OzoneConfiguration conf = new OzoneConfiguration(original);
    conf.setBoolean(OmConfig.Keys.MPU_PART_ENCRYPTION_IV_ENABLED, false);
    ozoneManager.setConfiguration(conf);
    try {
      String uploadID = initiateUpload(DISABLED_KEY);
      Map<Integer, String> parts = new TreeMap<>();
      parts.put(1, uploadPart(DISABLED_KEY, uploadID, 1, part1));
      parts.put(2, uploadPart(DISABLED_KEY, uploadID, 2, part2));
      bucket.completeMultipartUpload(DISABLED_KEY, uploadID, parts);
    } finally {
      ozoneManager.setConfiguration(original);
    }

    assertKeyContent(DISABLED_KEY);
    assertThat(getLocations(DISABLED_KEY)).isNotEmpty().allSatisfy(location -> assertNull(location.getPartIv()));
    assertEquals(Status.OK, lookupKey(DISABLED_KEY, ClientVersion.BUCKET_LAYOUT_SUPPORT).getStatus());
  }

  private String initiateUpload(String keyName) throws IOException {
    return bucket.initiateMultipartUpload(keyName, RatisReplicationConfig.getInstance(ONE)).getUploadID();
  }

  private String uploadPart(String keyName, String uploadID, int partNumber, byte[] data) throws IOException {
    OzoneOutputStream out = bucket.createMultipartKey(keyName, data.length, partNumber, uploadID);
    out.write(data);
    out.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(data));
    out.close();
    return out.getCommitUploadPartInfo().getETag();
  }

  private void assertKeyContent(String keyName) throws IOException {
    byte[] expected = new byte[2 * PART_SIZE];
    System.arraycopy(part1, 0, expected, 0, PART_SIZE);
    System.arraycopy(part2, 0, expected, PART_SIZE, PART_SIZE);
    try (OzoneInputStream in = bucket.readKey(keyName)) {
      assertArrayEquals(expected, in.readAllBytes());
    }
  }

  private List<OmKeyLocationInfo> getLocations(String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .build();
    return ozoneManager.lookupKey(keyArgs).getLatestVersionLocations().getBlocksLatestVersionOnly();
  }

  /** Sends LookupKey as a client of the given version would. */
  private OMResponse lookupKey(String keyName, ClientVersion clientVersion) throws IOException {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.LookupKey)
        .setClientId(UUID.randomUUID().toString())
        .setVersion(clientVersion.serialize())
        .setLookupKeyRequest(LookupKeyRequest.newBuilder().setKeyArgs(KeyArgs.newBuilder()
            .setVolumeName(bucket.getVolumeName())
            .setBucketName(bucket.getName())
            .setKeyName(keyName)))
        .build();
    return ((OzoneManagerProtocolClientSideTranslatorPB) omClient).getTransport().submitRequest(request);
  }
}

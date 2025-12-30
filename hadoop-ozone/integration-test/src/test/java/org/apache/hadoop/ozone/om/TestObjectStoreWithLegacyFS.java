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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test verifies object store with OZONE_OM_ENABLE_FILESYSTEM_PATHS enabled.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestObjectStoreWithLegacyFS implements NonHATests.TestCase {

  private OzoneClient client;

  private String volumeName;

  private String bucketName;

  private OzoneVolume volume;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestObjectStoreWithLegacyFS.class);
  private boolean originalFileSystemPathEnabled;

  @BeforeAll
  void initClass() throws Exception {
    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    originalFileSystemPathEnabled = omConfig.isFileSystemPathEnabled();
    omConfig.setFileSystemPathEnabled(true);

    client = cluster().newClient();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    omConfig.setFileSystemPathEnabled(originalFileSystemPathEnabled);
  }

  @BeforeEach
  public void init() throws Exception {
    volumeName = RandomStringUtils.secure().nextAlphabetic(10).toLowerCase();
    bucketName = RandomStringUtils.secure().nextAlphabetic(10).toLowerCase();

    // create a volume and a bucket to be used by OzoneFileSystem
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName,
        BucketLayout.OBJECT_STORE);
    volume = client.getObjectStore().getVolume(volumeName);
  }

  /**
   * Test verifies that OBS bucket keys should create flat key-value
   * structure and intermediate directories shouldn't be created even
   * if the OZONE_OM_ENABLE_FILESYSTEM_PATHS flag is TRUE.
   */
  @Test
  public void testFlatKeyStructureWithOBS() throws Exception {
    OzoneBucket ozoneBucket = volume.getBucket(bucketName);
    String keyName = "dir1/dir2/dir3/key-1";
    OzoneOutputStream stream = ozoneBucket
        .createKey("dir1/dir2/dir3/key-1", 0);
    stream.close();
    Table<String, OmKeyInfo> keyTable =
        cluster().getOzoneManager().getMetadataManager()
            .getKeyTable(BucketLayout.OBJECT_STORE);

    String seekKey = "dir";
    String dbKey = cluster().getOzoneManager().getMetadataManager()
        .getOzoneKey(volumeName, bucketName, seekKey);

    GenericTestUtils
        .waitFor(() -> assertKeyCount(keyTable, dbKey, 1, keyName), 500,
            60000);

    ozoneBucket.renameKey(keyName, "dir1/NewKey-1");

    GenericTestUtils
        .waitFor(() -> assertKeyCount(keyTable, dbKey, 1, "dir1/NewKey-1"), 500,
            60000);
  }

  private boolean assertKeyCount(
      Table<String, OmKeyInfo> keyTable,
      String dbKey, int expectedCnt, String keyName) {
    int countKeys = 0;
    int matchingKeys = 0;
    try (Table.KeyValueIterator<String, OmKeyInfo>
          itr = keyTable.iterator()) {
      itr.seek(dbKey);
      while (itr.hasNext()) {

        Table.KeyValue<String, OmKeyInfo> keyValue = itr.next();
        if (!keyValue.getKey().startsWith(dbKey)) {
          break;
        }
        countKeys++;
        matchingKeys += keyValue.getKey().endsWith(keyName) ? 1 : 0;
      }
    } catch (IOException ex) {
      LOG.info("Test failed with: " + ex.getMessage(), ex);
      fail("Test failed with: " + ex.getMessage());
    }
    if (countKeys > expectedCnt) {
      fail("Test failed with: too many keys found, expected "
          + expectedCnt + " keys, found " + countKeys + " keys");
    }
    if (matchingKeys != expectedCnt) {
      LOG.info("Couldn't find KeyName:{} in KeyTable, retrying...", keyName);
    }
    return countKeys == expectedCnt && matchingKeys == expectedCnt;
  }

  @Test
  public void testMultiPartCompleteUpload() throws Exception {
    // Test-1: Upload MPU to an OBS layout with Directory Exists
    String legacyBuckName = UUID.randomUUID().toString();
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.OBJECT_STORE);
    BucketArgs omBucketArgs = builder.build();
    volume.createBucket(legacyBuckName, omBucketArgs);
    OzoneBucket bucket = volume.getBucket(legacyBuckName);

    String keyName = "abc/def/mpu-key1";

    OmMultipartUploadCompleteInfo
        omMultipartUploadCompleteInfo =
        uploadMPUWithDirectoryExists(bucket, keyName);
    // successfully uploaded MPU key
    assertNotNull(omMultipartUploadCompleteInfo);

    // Test-2: Upload MPU to an LEGACY layout with Directory Exists
    legacyBuckName = UUID.randomUUID().toString();
    builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    omBucketArgs = builder.build();
    volume.createBucket(legacyBuckName, omBucketArgs);
    bucket = volume.getBucket(legacyBuckName);
    OzoneBucket finalBucket = bucket;
    OMException ome =
        assertThrows(OMException.class, () -> uploadMPUWithDirectoryExists(finalBucket, keyName),
            "Must throw error as there is " + "already directory in the given path");
    assertEquals(OMException.ResultCodes.NOT_A_FILE, ome.getResult());
  }

  private OmMultipartUploadCompleteInfo uploadMPUWithDirectoryExists(
      OzoneBucket bucket, String keyName)
      throws IOException, NoSuchAlgorithmException {
    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));

    assertNotNull(omMultipartInfo.getUploadID());

    String uploadID = omMultipartInfo.getUploadID();

    // upload part 1.
    byte[] data = generateData(128, (byte) RandomUtils.secure().randomLong());
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG,
        DatatypeConverter.printHexBinary(MessageDigest.getInstance(OzoneConsts.MD5_HASH)
            .digest(data)).toLowerCase());
    ozoneOutputStream.close();

    if (bucket.getBucketLayout() == BucketLayout.OBJECT_STORE) {
      // Create a path with trailing slash, in LEGACY this represents a dir
      OzoneOutputStream stream = bucket.createKey(
          omMultipartInfo.getKeyName() + OzoneConsts.OZONE_URI_DELIMITER, 0);
      stream.close();
    } else if (bucket.getBucketLayout() == BucketLayout.LEGACY) {
      // Create an intermediate path with trailing slash,
      // in LEGACY this represents a directory
      OzoneOutputStream stream = bucket.createKey(
          keyName + OzoneConsts.OZONE_URI_DELIMITER + "newKey-1", 0);
      stream.close();
    }

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    Map<Integer, String> partsMap = new LinkedHashMap<>();
    partsMap.put(1, omMultipartCommitUploadPartInfo.getETag());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        bucket.completeMultipartUpload(keyName,
            uploadID, partsMap);
    return omMultipartUploadCompleteInfo;
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }
}

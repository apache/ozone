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

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;

import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Test verifies object store with OZONE_OM_ENABLE_FILESYSTEM_PATHS enabled.
 */
public class TestObjectStoreWithLegacyFS {

  @Rule
  public Timeout timeout = Timeout.seconds(200);

  private static MiniOzoneCluster cluster = null;

  private String volumeName;

  private String bucketName;

  private OzoneVolume volume;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestObjectStoreWithLegacyFS.class);

  @BeforeClass
  public static void initClass() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void init() throws Exception {
    volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    // create a volume and a bucket to be used by OzoneFileSystem
    TestDataUtil.createVolumeAndBucket(cluster, volumeName, bucketName,
        BucketLayout.OBJECT_STORE);
    volume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);
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
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(BucketLayout.OBJECT_STORE);

    GenericTestUtils.waitFor(() -> isKeyExist(keyTable, keyName),
        500, 60000);

    ozoneBucket.renameKey(keyName, "dir1/NewKey-1");

    // RenameKey changes keyTable cache, so we need to
    // wait for the transaction to be flushed to db
    GenericTestUtils.waitFor(() -> !isKeyExist(keyTable, keyName),
        500, 60000);
    // When the old key is removed, new key should exist
    Assert.assertTrue(isKeyExist(keyTable, "dir1/NewKey-1"));
  }

  private boolean isKeyExist(Table<String, OmKeyInfo> keyTable,
      String keyName) {
    String dbKey = cluster.getOzoneManager().getMetadataManager()
        .getOzoneKey(volumeName, bucketName, keyName);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = keyTable.iterator()) {
      iterator.seek(dbKey);
      if (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        if (kv.getKey().equals(dbKey)) {
          return true;
        }
      }
    } catch (IOException ex) {
      LOG.error("Error while iterating key table", ex);
    }
    return false;
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
    Assert.assertNotNull(omMultipartUploadCompleteInfo);

    // Test-2: Upload MPU to an LEGACY layout with Directory Exists
    legacyBuckName = UUID.randomUUID().toString();
    builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    omBucketArgs = builder.build();
    volume.createBucket(legacyBuckName, omBucketArgs);
    bucket = volume.getBucket(legacyBuckName);

    try {
      uploadMPUWithDirectoryExists(bucket, keyName);
      Assert.fail("Must throw error as there is " +
          "already directory in the given path");
    } catch (OMException ome) {
      Assert.assertEquals(OMException.ResultCodes.NOT_A_FILE, ome.getResult());
    }
  }

  private OmMultipartUploadCompleteInfo uploadMPUWithDirectoryExists(
      OzoneBucket bucket, String keyName) throws IOException {
    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));

    Assert.assertNotNull(omMultipartInfo.getUploadID());

    String uploadID = omMultipartInfo.getUploadID();

    // upload part 1.
    byte[] data = generateData(128, (byte) RandomUtils.nextLong());
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
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
    partsMap.put(1, omMultipartCommitUploadPartInfo.getPartName());
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

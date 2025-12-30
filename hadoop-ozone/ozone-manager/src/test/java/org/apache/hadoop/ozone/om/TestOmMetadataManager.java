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
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.COMPACTION_LOG_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELEGATION_TOKEN_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.LIFECYCLE_CONFIGURATION_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.META_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PREFIX_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PRINCIPAL_TO_ACCESS_IDS_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_SECRET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_RENAMED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_ACCESS_ID_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_STATE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TRANSACTION_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.USER_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests OzoneManager MetadataManager.
 */
public class TestOmMetadataManager {

  static final String[] ALL_TABLES = new String[] {
      USER_TABLE,
      VOLUME_TABLE,
      BUCKET_TABLE,
      KEY_TABLE,
      DELETED_TABLE,
      OPEN_KEY_TABLE,
      MULTIPART_INFO_TABLE,
      S3_SECRET_TABLE,
      DELEGATION_TOKEN_TABLE,
      PREFIX_TABLE,
      TRANSACTION_INFO_TABLE,
      DIRECTORY_TABLE,
      FILE_TABLE,
      DELETED_DIR_TABLE,
      OPEN_FILE_TABLE,
      META_TABLE,
      TENANT_ACCESS_ID_TABLE,
      PRINCIPAL_TO_ACCESS_IDS_TABLE,
      TENANT_STATE_TABLE,
      SNAPSHOT_INFO_TABLE,
      SNAPSHOT_RENAMED_TABLE,
      COMPACTION_LOG_TABLE,
      LIFECYCLE_CONFIGURATION_TABLE
  };

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration ozoneConfiguration;
  @TempDir
  private File folder;

  @BeforeEach
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_DB_DIRS,
        folder.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
  }

  @Test
  public void testTransactionTable() throws Exception {
    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(1, 100));

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(2, 200));

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(3, 250));

    TransactionInfo transactionInfo =
        omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);

    assertEquals(3, transactionInfo.getTerm());
    assertEquals(250, transactionInfo.getTransactionIndex());
  }

  @Test
  public void testListVolumes() throws Exception {
    String ownerName = "owner";
    OmVolumeArgs.Builder argsBuilder = OmVolumeArgs.newBuilder()
        .setAdminName("admin")
        .setOwnerName(ownerName);

    String volName;
    OmVolumeArgs omVolumeArgs;
    for (int i = 0; i < 50; i++) {
      volName = "vol" + i;
      omVolumeArgs = argsBuilder
          .setVolume(volName)
          .build();

      OMRequestTestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      OMRequestTestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }

    // Test list volumes with setting startVolume that
    // was not part of the result.
    String prefix = "";
    int totalVol = omMetadataManager
        .listVolumes(ownerName, prefix, null, 100)
        .size();
    int startOrder = 10;
    String startVolume = "vol" + startOrder;
    List<OmVolumeArgs> volumeList = omMetadataManager.listVolumes(ownerName,
        prefix, startVolume, 100);
    assertEquals(volumeList.size(), totalVol - startOrder - 1);
  }

  @Test
  public void testListAllVolumes() throws Exception {
    OmVolumeArgs.Builder argsBuilder =
        OmVolumeArgs.newBuilder().setAdminName("admin");
    String volName;
    String ownerName;
    for (int i = 0; i < 50; i++) {
      ownerName = "owner" + i;
      volName = "vola" + i;
      OmVolumeArgs omVolumeArgs = argsBuilder.
          setOwnerName(ownerName).setVolume(volName).build();
      OMRequestTestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      OMRequestTestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }
    for (int i = 0; i < 50; i++) {
      ownerName = "owner" + i;
      volName = "volb" + i;
      OmVolumeArgs omVolumeArgs = argsBuilder.
          setOwnerName(ownerName).setVolume(volName).build();
      OMRequestTestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      OMRequestTestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }

    String prefix = "";
    String startKey = "";

    // Test list all volumes
    List<OmVolumeArgs> volListA = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    assertEquals(volListA.size(), 100);

    // Test list all volumes with prefix
    prefix = "volb";
    List<OmVolumeArgs> volListB = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    assertEquals(volListB.size(), 50);

    // Test list all volumes with setting startVolume
    // that was not part of result.
    prefix = "";
    int totalVol = volListB.size();
    int startOrder = 0;
    startKey = "volb" + startOrder;
    List<OmVolumeArgs> volListC = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    assertEquals(volListC.size(), totalVol - startOrder - 1);
  }

  @Test
  public void testListBuckets() throws Exception {
    String volumeName1 = "volumeA";
    String prefixBucketNameWithOzoneOwner = "ozoneBucket";
    String prefixBucketNameWithHadoopOwner = "hadoopBucket";

    OMRequestTestUtils.addVolumeToDB(volumeName1, omMetadataManager);

    TreeSet<String> volumeABucketsPrefixWithOzoneOwner = new TreeSet<>();

    // Add exact name in prefixBucketNameWithOzoneOwner without postfix.
    volumeABucketsPrefixWithOzoneOwner.add(prefixBucketNameWithOzoneOwner);
    addBucketsToCache(volumeName1, prefixBucketNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 buckets.
        volumeABucketsPrefixWithOzoneOwner.add(
            prefixBucketNameWithOzoneOwner + i);
        addBucketsToCache(volumeName1, prefixBucketNameWithOzoneOwner + i);
      } else {
        addBucketsToCache(volumeName1, prefixBucketNameWithHadoopOwner + i);
      }
    }

    String volumeName2 = "volumeB";
    TreeSet<String> volumeBBucketsPrefixWithHadoopOwner = new TreeSet<>();
    OMRequestTestUtils.addVolumeToDB(volumeName2, omMetadataManager);

    // Add exact name in prefixBucketNameWithOzoneOwner without postfix.
    addBucketsToCache(volumeName2, prefixBucketNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 buckets.
        addBucketsToCache(volumeName2, prefixBucketNameWithOzoneOwner + i);
      } else {
        volumeBBucketsPrefixWithHadoopOwner.add(
            prefixBucketNameWithHadoopOwner + i);
        addBucketsToCache(volumeName2, prefixBucketNameWithHadoopOwner + i);
      }
    }

    // VOLUME A

    // List all buckets which have prefix ozoneBucket
    List<OmBucketInfo> omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            null, prefixBucketNameWithOzoneOwner, 100, false);

    // Cause adding a exact name in prefixBucketNameWithOzoneOwner
    // and another 49 buckets, so if we list buckets with --prefix
    // prefixBucketNameWithOzoneOwner, we should get 50 buckets.
    assertEquals(omBucketInfoList.size(), 50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
    }


    String startBucket = prefixBucketNameWithOzoneOwner + 10;
    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            startBucket, prefixBucketNameWithOzoneOwner,
            100, false);

    assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    startBucket = prefixBucketNameWithOzoneOwner + 38;
    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            startBucket, prefixBucketNameWithOzoneOwner,
            100, false);

    assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
      assertNotEquals(prefixBucketNameWithOzoneOwner + 10, omBucketInfo.getBucketName());
    }

    // VOLUME B

    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        null, prefixBucketNameWithHadoopOwner, 100, false);

    // Cause adding a exact name in prefixBucketNameWithOzoneOwner
    // and another 49 buckets, so if we list buckets with --prefix
    // prefixBucketNameWithOzoneOwner, we should get 50 buckets.
    assertEquals(omBucketInfoList.size(), 50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithHadoopOwner));
    }

    // Try to get buckets by count 10, like that get all buckets in the
    // volumeB with prefixBucketNameWithHadoopOwner.
    startBucket = null;
    TreeSet<String> expectedBuckets = new TreeSet<>();
    for (int i = 0; i < 5; i++) {

      omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
          startBucket, prefixBucketNameWithHadoopOwner, 10, false);

      assertEquals(omBucketInfoList.size(), 10);

      for (OmBucketInfo omBucketInfo : omBucketInfoList) {
        expectedBuckets.add(omBucketInfo.getBucketName());
        assertTrue(omBucketInfo.getBucketName().startsWith(
            prefixBucketNameWithHadoopOwner));
        startBucket =  omBucketInfo.getBucketName();
      }
    }


    assertEquals(volumeBBucketsPrefixWithHadoopOwner, expectedBuckets);
    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        startBucket, prefixBucketNameWithHadoopOwner, 10, false);

    assertEquals(omBucketInfoList.size(), 0);

  }

  private void addBucketsToCache(String volumeName, String bucketName) {

    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();

    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1, omBucketInfo));
  }

  @Test
  public void testListKeys() throws Exception {

    String volumeNameA = "volumeA";
    String volumeNameB = "volumeB";
    String ozoneBucket = "ozoneBucket";
    String hadoopBucket = "hadoopBucket";
    String ozoneTestBucket = "ozoneBucket-Test";

    // Create volumes and buckets.
    OMRequestTestUtils.addVolumeToDB(volumeNameA, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(volumeNameB, omMetadataManager);
    addBucketsToCache(volumeNameA, ozoneBucket);
    addBucketsToCache(volumeNameB, hadoopBucket);
    addBucketsToCache(volumeNameA, ozoneTestBucket);

    String prefixKeyA = "key-a";
    String prefixKeyB = "key-b";
    String prefixKeyC = "key-c";
    TreeSet<String> keysASet = new TreeSet<>();
    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        keysASet.add(prefixKeyA + i);
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
      } else {
        addKeysToOM(volumeNameA, hadoopBucket, prefixKeyB + i, i);
      }
    }
    addKeysToOM(volumeNameA, ozoneTestBucket, prefixKeyC + 0, 0);

    TreeSet<String> keysBVolumeBSet = new TreeSet<>();
    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        addKeysToOM(volumeNameB, ozoneBucket, prefixKeyA + i, i);
      } else {
        keysBVolumeBSet.add(
            prefixKeyB + i);
        addKeysToOM(volumeNameB, hadoopBucket, prefixKeyB + i, i);
      }
    }


    // List all keys which have prefix "key-a"
    List<OmKeyInfo> omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 100).getKeys();

    assertEquals(omKeyInfoList.size(),  50);

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
    }


    String startKey = prefixKeyA + 10;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100).getKeys();

    assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    startKey = prefixKeyA + 38;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100).getKeys();

    assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
      assertNotEquals(prefixKeyA + 38, omKeyInfo.getBucketName());
    }



    omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
        null, prefixKeyB, 100).getKeys();

    assertEquals(omKeyInfoList.size(),  50);

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyB));
    }

    // Try to get keys by count 10, like that get all keys in the
    // volumeB/ozoneBucket with "key-a".
    startKey = null;
    TreeSet<String> expectedKeys = new TreeSet<>();
    for (int i = 0; i < 5; i++) {

      omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
          startKey, prefixKeyB, 10).getKeys();

      assertEquals(10, omKeyInfoList.size());

      for (OmKeyInfo omKeyInfo : omKeyInfoList) {
        expectedKeys.add(omKeyInfo.getKeyName());
        assertTrue(omKeyInfo.getKeyName().startsWith(
            prefixKeyB));
        startKey =  omKeyInfo.getKeyName();
      }
    }

    assertEquals(expectedKeys, keysBVolumeBSet);


    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
        startKey, prefixKeyB, 10).getKeys();

    assertEquals(omKeyInfoList.size(), 0);

    // List all keys with empty prefix
    omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
        null, null, 100).getKeys();
    assertEquals(50, omKeyInfoList.size());
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
    }
  }

  @Test
  public void testListKeysWithFewDeleteEntriesInCache() throws Exception {
    String volumeNameA = "volumeA";
    String ozoneBucket = "ozoneBucket";

    // Create volumes and bucket.
    OMRequestTestUtils.addVolumeToDB(volumeNameA, omMetadataManager);

    addBucketsToCache(volumeNameA, ozoneBucket);

    String prefixKeyA = "key-a";
    TreeSet<String> keysASet = new TreeSet<>();
    TreeSet<String> deleteKeySet = new TreeSet<>();


    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        keysASet.add(
            prefixKeyA + i);
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
      } else {
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
        String key = omMetadataManager.getOzoneKey(volumeNameA,
            ozoneBucket, prefixKeyA + i);
        // Mark as deleted in cache.
        omMetadataManager.getKeyTable(getDefaultBucketLayout()).addCacheEntry(
            new CacheKey<>(key),
            CacheValue.get(100L));
        deleteKeySet.add(key);
      }
    }

    // Now list keys which match with prefixKeyA.
    List<OmKeyInfo> omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 100).getKeys();

    // As in total 100, 50 are marked for delete. It should list only 50 keys.
    assertEquals(50, omKeyInfoList.size());
    assertEquals(50, deleteKeySet.size());

    TreeSet<String> expectedKeys = new TreeSet<>();

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      expectedKeys.add(omKeyInfo.getKeyName());
      assertTrue(omKeyInfo.getKeyName().startsWith(prefixKeyA));
    }

    assertEquals(expectedKeys, keysASet);


    // Now get key count by 10.
    String startKey = null;
    expectedKeys = new TreeSet<>();
    for (int i = 0; i < 5; i++) {

      omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
          startKey, prefixKeyA, 10).getKeys();

      System.out.println(i);
      assertEquals(10, omKeyInfoList.size());

      for (OmKeyInfo omKeyInfo : omKeyInfoList) {
        expectedKeys.add(omKeyInfo.getKeyName());
        assertTrue(omKeyInfo.getKeyName().startsWith(
            prefixKeyA));
        startKey =  omKeyInfo.getKeyName();
      }
    }

    assertEquals(keysASet, expectedKeys);


    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
        startKey, prefixKeyA, 10).getKeys();

    assertEquals(omKeyInfoList.size(), 0);



  }

  @Test
  public void testListKeysWithEntriesInCacheAndDB() throws Exception {
    String volumeNameA = "volumeA";
    String ozoneBucket = "ozoneBucket";

    // Create volumes and bucket.
    OMRequestTestUtils.addVolumeToDB(volumeNameA, omMetadataManager);

    addBucketsToCache(volumeNameA, ozoneBucket);

    String prefixKeyA = "key-a";
    TreeMap<String, OmKeyInfo> keyAMap = new TreeMap<>();

    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        // Add to DB
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);

        String key = omMetadataManager.getOzoneKey(volumeNameA,
            ozoneBucket, prefixKeyA + i);
        // Key is overwritten in cache (with higher updateID),
        // but the cache has not been flushed to the DB
        OmKeyInfo overwriteKey = OMRequestTestUtils.createOmKeyInfo(volumeNameA, ozoneBucket, prefixKeyA + i,
            RatisReplicationConfig.getInstance(ONE)).setUpdateID(100L).build();
        omMetadataManager.getKeyTable(getDefaultBucketLayout()).addCacheEntry(
            new CacheKey<>(key),
            CacheValue.get(100L, overwriteKey));
        keyAMap.put(prefixKeyA + i, overwriteKey);
      } else {
        // Add to cache
        OmKeyInfo omKeyInfo = addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
        keyAMap.put(prefixKeyA + i, omKeyInfo);
      }
    }

    // Now list keys which match with prefixKeyA.
    List<OmKeyInfo> omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 1000).getKeys();

    assertEquals(100, omKeyInfoList.size());

    TreeMap<String, OmKeyInfo> currentKeys = new TreeMap<>();

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      currentKeys.put(omKeyInfo.getKeyName(), omKeyInfo);
      assertTrue(omKeyInfo.getKeyName().startsWith(prefixKeyA));
    }

    assertEquals(keyAMap, currentKeys);

    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 100).getKeys();
    assertEquals(100, omKeyInfoList.size());

    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 98).getKeys();
    assertEquals(98, omKeyInfoList.size());

    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 1).getKeys();
    assertEquals(1, omKeyInfoList.size());
  }

  /**
   * Tests inner impl of listOpenFiles with different bucket types with and
   * without pagination. NOTE: This UT does NOT test hsync here since the hsync
   * status check is done purely on the client side.
   * @param bucketLayout BucketLayout
   */
  @ParameterizedTest
  @EnumSource
  public void testListOpenFiles(BucketLayout bucketLayout) throws Exception {
    final long clientID = 1000L;

    String volumeName = "volume-lof";
    String bucketName = "bucket-" + bucketLayout.name().toLowerCase();
    String keyPrefix = "key";

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, bucketLayout);

    long volumeId = -1L, bucketId = -1L;
    if (bucketLayout.isFileSystemOptimized()) {
      volumeId = omMetadataManager.getVolumeId(volumeName);
      bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    }

    int numOpenKeys = 3;
    for (int i = 0; i < numOpenKeys; i++) {
      OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyPrefix + i,
          RatisReplicationConfig.getInstance(ONE));
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();

      final String dbOpenKeyName;
      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addFileToKeyTable(true, false,
            keyInfo.getFileName(), keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenFileName(volumeId, bucketId,
            keyInfo.getParentObjectID(), keyInfo.getFileName(), clientID);
      } else {
        OMRequestTestUtils.addKeyToTable(true, false,
            keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenKey(volumeName, bucketName,
            keyInfo.getKeyName(), clientID);
      }
    }

    String dbPrefix;
    if (bucketLayout.isFileSystemOptimized()) {
      dbPrefix = omMetadataManager.getOzoneKeyFSO(volumeName, bucketName, "");
    } else {
      dbPrefix = omMetadataManager.getOzoneKey(volumeName, bucketName, "");
    }

    // Without pagination
    ListOpenFilesResult res = omMetadataManager.listOpenFiles(
        bucketLayout, 100, dbPrefix, false, dbPrefix);

    assertEquals(numOpenKeys, res.getTotalOpenKeyCount());
    assertFalse(res.hasMore());
    List<OpenKeySession> keySessionList = res.getOpenKeys();
    assertEquals(numOpenKeys, keySessionList.size());
    // Verify that every single open key shows up in the result, and in order
    for (int i = 0; i < numOpenKeys; i++) {
      OpenKeySession keySession = keySessionList.get(i);
      assertEquals(keyPrefix + i, keySession.getKeyInfo().getKeyName());
      assertEquals(clientID, keySession.getId());
      assertEquals(0, keySession.getOpenVersion());
    }

    // With pagination
    int pageSize = 2;
    int numExpectedKeys = pageSize;
    res = omMetadataManager.listOpenFiles(
        bucketLayout, pageSize, dbPrefix, false, dbPrefix);
    // total open key count should still be 3
    assertEquals(numOpenKeys, res.getTotalOpenKeyCount());
    // hasMore should have been set
    assertTrue(res.hasMore());
    keySessionList = res.getOpenKeys();
    assertEquals(numExpectedKeys, keySessionList.size());
    for (int i = 0; i < numExpectedKeys; i++) {
      OpenKeySession keySession = keySessionList.get(i);
      assertEquals(keyPrefix + i, keySession.getKeyInfo().getKeyName());
      assertEquals(clientID, keySession.getId());
      assertEquals(0, keySession.getOpenVersion());
    }

    // Get the second page
    res = omMetadataManager.listOpenFiles(
        bucketLayout, pageSize, dbPrefix, true, res.getContinuationToken());
    numExpectedKeys = numOpenKeys - pageSize;
    // total open key count should still be 3
    assertEquals(numOpenKeys, res.getTotalOpenKeyCount());
    assertFalse(res.hasMore());
    keySessionList = res.getOpenKeys();
    assertEquals(numExpectedKeys, keySessionList.size());
    for (int i = 0; i < numExpectedKeys; i++) {
      OpenKeySession keySession = keySessionList.get(i);
      assertEquals(keyPrefix + (pageSize + i),
          keySession.getKeyInfo().getKeyName());
      assertEquals(clientID, keySession.getId());
      assertEquals(0, keySession.getOpenVersion());
    }
  }

  private static BucketLayout getDefaultBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @ParameterizedTest
  @EnumSource
  public void testGetExpiredOpenKeys(BucketLayout bucketLayout)
      throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    // Add volume, bucket, key entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    final int numExpiredOpenKeys = 4;
    final int numUnexpiredOpenKeys = 1;
    final long clientID = 1000L;
    // To create expired keys, they will be assigned a creation time as
    // old as the minimum expiration time.
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);

    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);

    final long expiredOpenKeyCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    // Add expired keys to open key table.
    // The method under test does not check for expired open keys in the
    // cache, since they will be picked up once the cache is flushed.
    Set<String> expiredKeys = new HashSet<>();
    for (int i = 0; i < numExpiredOpenKeys + numUnexpiredOpenKeys; i++) {
      final long creationTime = i < numExpiredOpenKeys ?
          expiredOpenKeyCreationTime : Time.now();
      final OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(
              volumeName, bucketName, "expired" + i, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(creationTime);
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();

      final String dbOpenKeyName;
      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addFileToKeyTable(true, false,
            keyInfo.getFileName(), keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenFileName(volumeId, bucketId,
            keyInfo.getParentObjectID(), keyInfo.getFileName(), clientID);
      } else {
        OMRequestTestUtils.addKeyToTable(true, false,
            keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenKey(volumeName, bucketName,
            keyInfo.getKeyName(), clientID);
      }
      expiredKeys.add(dbOpenKeyName);
    }

    // Test retrieving fewer expired keys than actually exist.
    final Collection<OpenKeyBucket.Builder> someExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys - 1, bucketLayout, expireThreshold).getOpenKeyBuckets();
    List<String> names = getOpenKeyNames(someExpiredKeys);
    assertEquals(numExpiredOpenKeys - 1, names.size());
    assertThat(expiredKeys).containsAll(names);

    // Test attempting to retrieving more expired keys than actually exist.
    Collection<OpenKeyBucket.Builder> allExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys + 1, bucketLayout, expireThreshold).getOpenKeyBuckets();
    names = getOpenKeyNames(allExpiredKeys);
    assertEquals(numExpiredOpenKeys, names.size());
    assertThat(expiredKeys).containsAll(names);

    // Test retrieving exact amount of expired keys that exist.
    allExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys, bucketLayout, expireThreshold).getOpenKeyBuckets();
    names = getOpenKeyNames(allExpiredKeys);
    assertEquals(numExpiredOpenKeys, names.size());
    assertThat(expiredKeys).containsAll(names);
  }

  @ParameterizedTest
  @EnumSource
  public void testGetExpiredOpenKeysExcludeMPUKeys(
      BucketLayout bucketLayout) throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    // Add volume, bucket, key entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    final int numExpiredMPUOpenKeys = 4;
    final long clientID = 1000L;
    // To create expired keys, they will be assigned a creation time as
    // old as the minimum expiration time.
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);

    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);

    final long expiredOpenKeyCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    // Ensure that "expired" MPU-related open keys are not fetched.
    // MPU-related open keys, identified by isMultipartKey = false
    for (int i = 0; i < numExpiredMPUOpenKeys; i++) {
      final OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, "expired" + i,
              RatisReplicationConfig.getInstance(ONE), new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
          .setCreationTime(expiredOpenKeyCreationTime);
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();
      assertThat(keyInfo.getModificationTime()).isPositive();

      final String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      final OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils.
          createOmMultipartKeyInfo(uploadId, expiredOpenKeyCreationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addMultipartKeyToOpenFileTable(false,
            keyInfo.getFileName(), keyInfo, uploadId, 0L, omMetadataManager);
      } else {
        OMRequestTestUtils.addMultipartKeyToOpenKeyTable(false,
            keyInfo, uploadId, 0L, omMetadataManager);
      }
      OMRequestTestUtils.addMultipartInfoToTable(false, keyInfo,
          multipartKeyInfo, 0L, omMetadataManager);
    }

    // Return empty since only MPU-related open keys exist.
    assertTrue(omMetadataManager.getExpiredOpenKeys(expireThreshold,
        numExpiredMPUOpenKeys, bucketLayout, expireThreshold).getOpenKeyBuckets().isEmpty());


    // This is for MPU-related open keys prior to isMultipartKey fix in
    // HDDS-9017. Although these open keys are MPU-related,
    // the isMultipartKey flags are set to false
    for (int i = numExpiredMPUOpenKeys; i < 2 * numExpiredMPUOpenKeys; i++) {
      final OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(
              volumeName, bucketName, "expired" + i, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(expiredOpenKeyCreationTime);
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();

      final String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      final OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils.
          createOmMultipartKeyInfo(uploadId, expiredOpenKeyCreationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addMultipartKeyToOpenFileTable(false,
            keyInfo.getFileName(), keyInfo, uploadId, 0L, omMetadataManager);
      } else {
        OMRequestTestUtils.addMultipartKeyToOpenKeyTable(false,
            keyInfo, uploadId, 0L, omMetadataManager);
      }
      OMRequestTestUtils.addMultipartInfoToTable(false, keyInfo,
          multipartKeyInfo, 0L, omMetadataManager);
    }

    // MPU-related open keys should not be fetched regardless of isMultipartKey
    // flag if has the multipart upload characteristics
    assertTrue(omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredMPUOpenKeys, bucketLayout, expireThreshold).getOpenKeyBuckets()
        .isEmpty());
  }

  @Test
  public void testGetExpiredMPUs() throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    final int numExpiredMPUs = 4;
    final int numUnexpiredMPUs = 1;
    final int numPartsPerMPU = 5;
    // To create expired keys, they will be assigned a creation time as
    // old as the minimum expiration time.
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_MPU_EXPIRE_THRESHOLD,
        OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);

    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);

    final long expiredMPUCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    // Add expired MPUs to multipartInfoTable.
    // The method under test does not check for expired open keys in the
    // cache, since they will be picked up once the cache is flushed.
    Set<String> expiredMPUs = new HashSet<>();
    for (int i = 0; i < numExpiredMPUs + numUnexpiredMPUs; i++) {
      final long creationTime = i < numExpiredMPUs ?
          expiredMPUCreationTime : Time.now();

      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      final OmMultipartKeyInfo mpuKeyInfo = OMRequestTestUtils
          .createOmMultipartKeyInfo(uploadId, creationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      String keyName = "expired" + i;
      // Key info to construct the MPU DB key
      final OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
              bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(creationTime)
          .build();


      for (int j = 1; j <= numPartsPerMPU; j++) {
        PartKeyInfo partKeyInfo = OMRequestTestUtils
            .createPartKeyInfo(volumeName, bucketName, keyName, uploadId, j);
        OMRequestTestUtils.addPart(partKeyInfo, mpuKeyInfo);
      }

      final String mpuDbKey = OMRequestTestUtils.addMultipartInfoToTable(
          false, keyInfo, mpuKeyInfo, 0L, omMetadataManager);

      expiredMPUs.add(mpuDbKey);
    }

    // Test retrieving fewer expire MPU parts than actually exist (exact).
    List<ExpiredMultipartUploadsBucket>
        someExpiredMPUs = omMetadataManager.getExpiredMultipartUploads(
        expireThreshold,
        (numExpiredMPUs * numPartsPerMPU) - (numPartsPerMPU));
    List<String> names = getMultipartKeyNames(someExpiredMPUs);
    assertEquals(numExpiredMPUs - 1, names.size());
    assertThat(expiredMPUs).containsAll(names);

    // Test retrieving fewer expire MPU parts than actually exist (round up).
    someExpiredMPUs = omMetadataManager.getExpiredMultipartUploads(
        expireThreshold,
        (numExpiredMPUs * numPartsPerMPU) - (numPartsPerMPU + 1));
    names = getMultipartKeyNames(someExpiredMPUs);
    assertEquals(numExpiredMPUs - 1, names.size());
    assertThat(expiredMPUs).containsAll(names);

    // Test attempting to retrieving more expire MPU parts than actually exist.
    List<ExpiredMultipartUploadsBucket> allExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold,
            (numExpiredMPUs * numPartsPerMPU) + numPartsPerMPU);
    names = getMultipartKeyNames(allExpiredMPUs);
    assertEquals(numExpiredMPUs, names.size());
    assertThat(expiredMPUs).containsAll(names);

    // Test retrieving exact amount of MPU parts than actually exist.
    allExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold,
            (numExpiredMPUs * numPartsPerMPU));
    names = getMultipartKeyNames(allExpiredMPUs);
    assertEquals(numExpiredMPUs, names.size());
    assertThat(expiredMPUs).containsAll(names);
  }

  private List<String> getOpenKeyNames(
      Collection<OpenKeyBucket.Builder> openKeyBuckets) {
    return openKeyBuckets.stream()
        .map(OpenKeyBucket.Builder::getKeysList)
        .flatMap(List::stream)
        .map(OpenKey::getName)
        .collect(Collectors.toList());
  }

  private List<String> getMultipartKeyNames(
      List<ExpiredMultipartUploadsBucket> expiredMultipartUploadsBuckets) {
    return expiredMultipartUploadsBuckets.stream()
        .map(ExpiredMultipartUploadsBucket::getMultipartUploadsList)
        .flatMap(List::stream)
        .map(ExpiredMultipartUploadInfo::getName)
        .collect(Collectors.toList());
  }

  private OmKeyInfo addKeysToOM(String volumeName, String bucketName,
      String keyName, int i) throws Exception {

    if (i % 2 == 0) {
      return OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
          1000L, RatisReplicationConfig.getInstance(ONE), omMetadataManager);
    } else {
      return OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName, keyName,
          RatisReplicationConfig.getInstance(ONE),
          omMetadataManager);
    }
  }

  @Test
  public void testAllTablesAreProperInOMMetadataManagerImpl() {
    final Set<String> expected = new HashSet<>(Arrays.asList(ALL_TABLES));
    final Set<String> tablesByDefinition = new HashSet<>(OMDBDefinition.get().getColumnFamilyNames());
    assertEquals(expected, tablesByDefinition);

    final Set<String> tablesInManager = omMetadataManager.listTableNames();
    assertEquals(tablesByDefinition, tablesInManager);
  }

  @Test
  public void testListSnapshot() throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";

    OMRequestTestUtils.addVolumeToDB(vol1, omMetadataManager);
    addBucketsToCache(vol1, bucket1);
    String prefixA = "snapshotA";
    String prefixB = "snapshotB";
    TreeMap<String, SnapshotInfo> snapshotsASnapshotIDMap = new TreeMap<>();

    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        snapshotsASnapshotIDMap.put(prefixA + i,
            OMRequestTestUtils.addSnapshotToTable(vol1, bucket1, prefixA + i, omMetadataManager));
        if (i % 4 == 0) {
          snapshotsASnapshotIDMap.put(prefixA + i,
              OMRequestTestUtils.addSnapshotToTableCache(vol1, bucket1, prefixA + i, omMetadataManager));
        }
      } else {
        OMRequestTestUtils.addSnapshotToTableCache(vol1, bucket1, prefixB + i, omMetadataManager);
      }
    }

    //Test listing all snapshots.
    List<SnapshotInfo> snapshotInfos = omMetadataManager.listSnapshot(vol1, bucket1, null, null, 100)
        .getSnapshotInfos();
    assertEquals(100, snapshotInfos.size());

    snapshotInfos = omMetadataManager.listSnapshot(vol1, bucket1, prefixA, null, 50)
        .getSnapshotInfos();
    assertEquals(50, snapshotInfos.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos) {
      assertTrue(snapshotInfo.getName().startsWith(prefixA));
    }

    String startSnapshot = prefixA + 38;
    snapshotInfos = omMetadataManager.listSnapshot(vol1, bucket1, prefixA, startSnapshot, 50)
        .getSnapshotInfos();
    System.out.println(snapshotInfos.stream().map(SnapshotInfo::getName).collect(Collectors.joining(",")));
    assertEquals(snapshotsASnapshotIDMap.tailMap(startSnapshot).size() - 1, snapshotInfos.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos) {
      assertTrue(snapshotInfo.getName().startsWith(prefixA));
      assertEquals(snapshotInfo, snapshotsASnapshotIDMap.get(snapshotInfo.getName()));
      assertThat(snapshotInfo.getName().compareTo(startSnapshot)).isGreaterThanOrEqualTo(0);
    }

    String lastSnapshot = null;
    TreeSet<String> expectedSnapshot = new TreeSet<>();
    for (int i = 1; i <= 5; i++) {
      ListSnapshotResponse listSnapshotResponse =
          omMetadataManager.listSnapshot(vol1, bucket1, prefixA, lastSnapshot, 10);
      snapshotInfos = listSnapshotResponse.getSnapshotInfos();
      lastSnapshot = listSnapshotResponse.getLastSnapshot();
      assertEquals(10, snapshotInfos.size());

      for (SnapshotInfo snapshotInfo : snapshotInfos) {
        expectedSnapshot.add(snapshotInfo.getName());
        assertEquals(snapshotInfo, snapshotsASnapshotIDMap.get(snapshotInfo.getName()));
        assertTrue(snapshotInfo.getName().startsWith(prefixA));
      }
    }
    assertEquals(snapshotsASnapshotIDMap.keySet(), expectedSnapshot);
    assertNull(lastSnapshot);
  }

  @ParameterizedTest
  @MethodSource("listSnapshotWithInvalidPathCases")
  public void testListSnapshotWithInvalidPath(String volume,
                                              String bucket,
                                              ResultCodes expectedResultCode)
      throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";

    OMRequestTestUtils.addVolumeToDB(vol1, omMetadataManager);
    addBucketsToCache(vol1, bucket1);

    OMException oe = assertThrows(OMException.class,
        () -> omMetadataManager.listSnapshot(volume, bucket, null, null, 100));
    assertEquals(expectedResultCode, oe.getResult());
  }

  private static Stream<Arguments> listSnapshotWithInvalidPathCases() {
    return Stream.of(
        arguments(null, null, VOLUME_NOT_FOUND),
        arguments("vol1", null, BUCKET_NOT_FOUND),
        arguments("vol1", "nonexistentBucket", BUCKET_NOT_FOUND)
    );
  }

  @Test
  public void testListSnapshotDoesNotListOtherBucketSnapshots()
          throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";
    String bucket2 = "bucket2";

    OMRequestTestUtils.addVolumeToDB(vol1, omMetadataManager);
    addBucketsToCache(vol1, bucket1);
    addBucketsToCache(vol1, bucket2);
    String snapshotName1 = "snapshot1-";
    String snapshotName2 = "snapshot2-";

    for (int i = 1; i <= 2; i++) {
      OMRequestTestUtils.addSnapshotToTable(vol1, bucket1,
              snapshotName1 + i, omMetadataManager);
    }

    for (int i = 1; i <= 5; i++) {
      OMRequestTestUtils.addSnapshotToTable(vol1, bucket2,
              snapshotName2 + i, omMetadataManager);
    }

    //Test listing snapshots only lists snapshots of specified bucket
    List<SnapshotInfo> snapshotInfos1 = omMetadataManager.listSnapshot(vol1, bucket1, null, null, Integer.MAX_VALUE)
        .getSnapshotInfos();
    assertEquals(2, snapshotInfos1.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos1) {
      assertTrue(snapshotInfo.getName().startsWith(snapshotName1));
    }

    List<SnapshotInfo> snapshotInfos2 = omMetadataManager.listSnapshot(vol1, bucket2, null, null, Integer.MAX_VALUE)
        .getSnapshotInfos();
    assertEquals(5, snapshotInfos2.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos2) {
      assertTrue(snapshotInfo.getName().startsWith(snapshotName2));
    }
  }

  @Test
  public void testGetMultipartUploadKeys() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix = "dir/";
    int maxUploads = 10;

    // Create volume and bucket
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    addBucketsToCache(volumeName, bucketName);

    List<String> expectedKeys = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      String key = prefix + "key" + i;
      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();

      // Create multipart key info
      OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
          volumeName, bucketName, key,
          RatisReplicationConfig.getInstance(ONE))
          .build();

      OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils
          .createOmMultipartKeyInfo(uploadId, Time.now(),
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      if (i % 2 == 0) {
        OMRequestTestUtils.addMultipartInfoToTable(false, keyInfo,
            multipartKeyInfo, 0L, omMetadataManager);
      } else {
        OMRequestTestUtils.addMultipartInfoToTableCache(keyInfo,
            multipartKeyInfo, 0L, omMetadataManager);
      }

      expectedKeys.add(key);
    }
    Collections.sort(expectedKeys);

    // List first page without markers
    List<OmMultipartUpload> result = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix, null, null, maxUploads, false);

    assertEquals(maxUploads + 1, result.size());

    // List next page using markers from first page
    List<OmMultipartUpload> nextPage = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix,
        result.get(result.size() - 1).getKeyName(),
        result.get(result.size() - 1).getUploadId(),
        maxUploads, false);

    assertEquals(maxUploads + 1, nextPage.size());

    // List with different prefix
    List<OmMultipartUpload> differentPrefix = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, "different/", null, null, maxUploads, false);

    assertEquals(0, differentPrefix.size());

    // List all entries with large maxUploads
    List<OmMultipartUpload> allEntries = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix, null, null, 100, false);

    assertEquals(25, allEntries.size());

    // Verify all keys are present
    List<String> actualKeys = new ArrayList<>();
    for (OmMultipartUpload mpu : allEntries) {
      actualKeys.add(mpu.getKeyName());
    }
    Collections.sort(actualKeys);
    assertEquals(expectedKeys, actualKeys);

    // Test with no pagination
    List<OmMultipartUpload> noPagination = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix, null, null, 10, true);

    assertEquals(25, noPagination.size());
  }
}

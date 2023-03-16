/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;
import com.google.common.base.Optional;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests OzoneManager MetadataManager.
 */
public class TestOmMetadataManager {

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration ozoneConfiguration;
  @TempDir
  private File folder;


  @BeforeEach
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_DB_DIRS,
        folder.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
  }

  @Test
  public void testTransactionTable() throws Exception {
    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new TransactionInfo.Builder().setCurrentTerm(1)
            .setTransactionIndex(100).build());

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new TransactionInfo.Builder().setCurrentTerm(2)
            .setTransactionIndex(200).build());

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new TransactionInfo.Builder().setCurrentTerm(3)
            .setTransactionIndex(250).build());

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
    TreeSet<String> volumeABucketsPrefixWithHadoopOwner = new TreeSet<>();

    // Add exact name in prefixBucketNameWithOzoneOwner without postfix.
    volumeABucketsPrefixWithOzoneOwner.add(prefixBucketNameWithOzoneOwner);
    addBucketsToCache(volumeName1, prefixBucketNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 buckets.
        volumeABucketsPrefixWithOzoneOwner.add(
            prefixBucketNameWithOzoneOwner + i);
        addBucketsToCache(volumeName1, prefixBucketNameWithOzoneOwner + i);
      } else {
        volumeABucketsPrefixWithHadoopOwner.add(
            prefixBucketNameWithHadoopOwner + i);
        addBucketsToCache(volumeName1, prefixBucketNameWithHadoopOwner + i);
      }
    }

    String volumeName2 = "volumeB";
    TreeSet<String> volumeBBucketsPrefixWithOzoneOwner = new TreeSet<>();
    TreeSet<String> volumeBBucketsPrefixWithHadoopOwner = new TreeSet<>();
    OMRequestTestUtils.addVolumeToDB(volumeName2, omMetadataManager);

    // Add exact name in prefixBucketNameWithOzoneOwner without postfix.
    volumeBBucketsPrefixWithOzoneOwner.add(prefixBucketNameWithOzoneOwner);
    addBucketsToCache(volumeName2, prefixBucketNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 buckets.
        volumeBBucketsPrefixWithOzoneOwner.add(
            prefixBucketNameWithOzoneOwner + i);
        addBucketsToCache(volumeName2, prefixBucketNameWithOzoneOwner + i);
      } else {
        volumeBBucketsPrefixWithHadoopOwner.add(
            prefixBucketNameWithHadoopOwner + i);
        addBucketsToCache(volumeName2, prefixBucketNameWithHadoopOwner + i);
      }
    }

    // List all buckets which have prefix ozoneBucket
    List<OmBucketInfo> omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            null, prefixBucketNameWithOzoneOwner, 100);

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
            100);

    assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    startBucket = prefixBucketNameWithOzoneOwner + 38;
    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            startBucket, prefixBucketNameWithOzoneOwner,
            100);

    assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
      assertFalse(omBucketInfo.getBucketName().equals(
          prefixBucketNameWithOzoneOwner + 10));
    }



    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        null, prefixBucketNameWithHadoopOwner, 100);

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
          startBucket, prefixBucketNameWithHadoopOwner, 10);

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
        startBucket, prefixBucketNameWithHadoopOwner, 10);

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
        new CacheValue<>(Optional.of(omBucketInfo), 1));
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
    TreeSet<String> keysBSet = new TreeSet<>();
    TreeSet<String> keysCSet = new TreeSet<>();
    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        keysASet.add(prefixKeyA + i);
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
      } else {
        keysBSet.add(prefixKeyB + i);
        addKeysToOM(volumeNameA, hadoopBucket, prefixKeyB + i, i);
      }
    }
    keysCSet.add(prefixKeyC + 1);
    addKeysToOM(volumeNameA, ozoneTestBucket, prefixKeyC + 0, 0);

    TreeSet<String> keysAVolumeBSet = new TreeSet<>();
    TreeSet<String> keysBVolumeBSet = new TreeSet<>();
    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        keysAVolumeBSet.add(
            prefixKeyA + i);
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
            null, prefixKeyA, 100);

    assertEquals(omKeyInfoList.size(),  50);

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
    }


    String startKey = prefixKeyA + 10;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100);

    assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    startKey = prefixKeyA + 38;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100);

    assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
      assertFalse(omKeyInfo.getBucketName().equals(
          prefixKeyA + 38));
    }



    omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
        null, prefixKeyB, 100);

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
          startKey, prefixKeyB, 10);

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
        startKey, prefixKeyB, 10);

    assertEquals(omKeyInfoList.size(), 0);

    // List all keys with empty prefix
    omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
        null, null, 100);
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
            new CacheValue<>(Optional.absent(), 100L));
        deleteKeySet.add(key);
      }
    }

    // Now list keys which match with prefixKeyA.
    List<OmKeyInfo> omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 100);

    // As in total 100, 50 are marked for delete. It should list only 50 keys.
    assertEquals(50, omKeyInfoList.size());

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
          startKey, prefixKeyA, 10);

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
        startKey, prefixKeyA, 10);

    assertEquals(omKeyInfoList.size(), 0);



  }

  private static BucketLayout getDefaultBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @Test
  public void testGetExpiredOpenKeys() throws Exception {
    testGetExpiredOpenKeys(BucketLayout.DEFAULT);
  }

  @Test
  public void testGetExpiredOpenKeysFSO() throws Exception {
    testGetExpiredOpenKeys(BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  private void testGetExpiredOpenKeys(BucketLayout bucketLayout)
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
      final OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
          bucketName, "expired" + i, HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, 0L, creationTime);

      final String dbOpenKeyName;
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfo.setParentObjectID(i);
        keyInfo.setFileName(OzoneFSUtils.getFileName(keyInfo.getKeyName()));
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
            numExpiredOpenKeys - 1, bucketLayout).getOpenKeyBuckets();
    List<String> names = getOpenKeyNames(someExpiredKeys);
    assertEquals(numExpiredOpenKeys - 1, names.size());
    assertTrue(expiredKeys.containsAll(names));

    // Test attempting to retrieving more expired keys than actually exist.
    Collection<OpenKeyBucket.Builder> allExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys + 1, bucketLayout).getOpenKeyBuckets();
    names = getOpenKeyNames(allExpiredKeys);
    assertEquals(numExpiredOpenKeys, names.size());
    assertTrue(expiredKeys.containsAll(names));

    // Test retrieving exact amount of expired keys that exist.
    allExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys, bucketLayout).getOpenKeyBuckets();
    names = getOpenKeyNames(allExpiredKeys);
    assertEquals(numExpiredOpenKeys, names.size());
    assertTrue(expiredKeys.containsAll(names));
  }

  private List<String> getOpenKeyNames(
      Collection<OpenKeyBucket.Builder> openKeyBuckets) {
    return openKeyBuckets.stream()
        .map(OpenKeyBucket.Builder::getKeysList)
        .flatMap(List::stream)
        .map(OpenKey::getName)
        .collect(Collectors.toList());
  }

  private void addKeysToOM(String volumeName, String bucketName,
      String keyName, int i) throws Exception {

    if (i % 2 == 0) {
      OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
          1000L, HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    } else {
      OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName, keyName,
          HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE,
          omMetadataManager);
    }
  }

  @Test
  public void testAllTablesAreProperInOMMetadataManagerImpl() {
    Set<String> tablesByDefinition =
        new HashSet<>(Arrays.asList(OmMetadataManagerImpl.ALL_TABLES));
    Set<String> tablesInManager = omMetadataManager.listTableNames();

    assertEquals(tablesByDefinition, tablesInManager);
  }

  @Test
  public void testListSnapshot() throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";

    OMRequestTestUtils.addVolumeToDB(vol1, omMetadataManager);
    addBucketsToCache(vol1, bucket1);
    String snapshotName = "snapshot";

    for (int i = 1; i <= 10; i++) {
      if (i % 2 == 0) {
        OMRequestTestUtils.addSnapshotToTable(vol1, bucket1,
            snapshotName + i, omMetadataManager);
      } else {
        OMRequestTestUtils.addSnapshotToTableCache(vol1, bucket1,
            snapshotName + i, omMetadataManager);
      }
    }

    //Test listing all snapshots.
    List<SnapshotInfo> snapshotInfos = omMetadataManager.listSnapshot(vol1,
        bucket1);
    assertEquals(10, snapshotInfos.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos) {
      assertTrue(snapshotInfo.getName().startsWith(snapshotName));
    }

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
        () -> omMetadataManager.listSnapshot(volume, bucket));
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
    List<SnapshotInfo> snapshotInfos1 = omMetadataManager.listSnapshot(vol1,
            bucket1);
    assertEquals(2, snapshotInfos1.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos1) {
      assertTrue(snapshotInfo.getName().startsWith(snapshotName1));
    }

    List<SnapshotInfo> snapshotInfos2 = omMetadataManager.listSnapshot(vol1,
            bucket2);
    assertEquals(5, snapshotInfos2.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos2) {
      assertTrue(snapshotInfo.getName().startsWith(snapshotName2));
    }
  }
}

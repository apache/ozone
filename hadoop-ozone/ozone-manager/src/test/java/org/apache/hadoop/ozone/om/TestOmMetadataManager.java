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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.OMTransactionInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Tests OzoneManager MetadataManager.
 */
public class TestOmMetadataManager {

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration ozoneConfiguration;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  @Before
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_DB_DIRS,
        folder.getRoot().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
  }

  @Test
  public void testTransactionTable() throws Exception {
    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new OMTransactionInfo.Builder().setCurrentTerm(1)
            .setTransactionIndex(100).build());

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new OMTransactionInfo.Builder().setCurrentTerm(2)
            .setTransactionIndex(200).build());

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new OMTransactionInfo.Builder().setCurrentTerm(3)
            .setTransactionIndex(250).build());

    OMTransactionInfo omTransactionInfo =
        omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);

    Assert.assertEquals(3, omTransactionInfo.getTerm());
    Assert.assertEquals(250, omTransactionInfo.getTransactionIndex());


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

      TestOMRequestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      TestOMRequestUtils.addUserToDB(volName, ownerName, omMetadataManager);
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
    Assert.assertEquals(volumeList.size(), totalVol - startOrder - 1);
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
      TestOMRequestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      TestOMRequestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }
    for (int i = 0; i < 50; i++) {
      ownerName = "owner" + i;
      volName = "volb" + i;
      OmVolumeArgs omVolumeArgs = argsBuilder.
          setOwnerName(ownerName).setVolume(volName).build();
      TestOMRequestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      TestOMRequestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }

    String prefix = "";
    String startKey = "";

    // Test list all volumes
    List<OmVolumeArgs> volListA = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    Assert.assertEquals(volListA.size(), 100);

    // Test list all volumes with prefix
    prefix = "volb";
    List<OmVolumeArgs> volListB = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    Assert.assertEquals(volListB.size(), 50);

    // Test list all volumes with setting startVolume
    // that was not part of result.
    prefix = "";
    int totalVol = volListB.size();
    int startOrder = 0;
    startKey = "volb" + startOrder;
    List<OmVolumeArgs> volListC = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    Assert.assertEquals(volListC.size(), totalVol - startOrder - 1);
  }

  @Test
  public void testListBuckets() throws Exception {

    String volumeName1 = "volumeA";
    String prefixBucketNameWithOzoneOwner = "ozoneBucket";
    String prefixBucketNameWithHadoopOwner = "hadoopBucket";

    TestOMRequestUtils.addVolumeToDB(volumeName1, omMetadataManager);


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
    TestOMRequestUtils.addVolumeToDB(volumeName2, omMetadataManager);

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
    Assert.assertEquals(omBucketInfoList.size(), 50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
    }


    String startBucket = prefixBucketNameWithOzoneOwner + 10;
    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            startBucket, prefixBucketNameWithOzoneOwner,
            100);

    Assert.assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    startBucket = prefixBucketNameWithOzoneOwner + 38;
    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            startBucket, prefixBucketNameWithOzoneOwner,
            100);

    Assert.assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
      Assert.assertFalse(omBucketInfo.getBucketName().equals(
          prefixBucketNameWithOzoneOwner + 10));
    }



    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        null, prefixBucketNameWithHadoopOwner, 100);

    // Cause adding a exact name in prefixBucketNameWithOzoneOwner
    // and another 49 buckets, so if we list buckets with --prefix
    // prefixBucketNameWithOzoneOwner, we should get 50 buckets.
    Assert.assertEquals(omBucketInfoList.size(), 50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithHadoopOwner));
    }

    // Try to get buckets by count 10, like that get all buckets in the
    // volumeB with prefixBucketNameWithHadoopOwner.
    startBucket = null;
    TreeSet<String> expectedBuckets = new TreeSet<>();
    for (int i=0; i<5; i++) {

      omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
          startBucket, prefixBucketNameWithHadoopOwner, 10);

      Assert.assertEquals(omBucketInfoList.size(), 10);

      for (OmBucketInfo omBucketInfo : omBucketInfoList) {
        expectedBuckets.add(omBucketInfo.getBucketName());
        Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
            prefixBucketNameWithHadoopOwner));
        startBucket =  omBucketInfo.getBucketName();
      }
    }


    Assert.assertEquals(volumeBBucketsPrefixWithHadoopOwner, expectedBuckets);
    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        startBucket, prefixBucketNameWithHadoopOwner, 10);

    Assert.assertEquals(omBucketInfoList.size(), 0);

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
    TestOMRequestUtils.addVolumeToDB(volumeNameA, omMetadataManager);
    TestOMRequestUtils.addVolumeToDB(volumeNameB, omMetadataManager);
    addBucketsToCache(volumeNameA, ozoneBucket);
    addBucketsToCache(volumeNameB, hadoopBucket);
    addBucketsToCache(volumeNameA, ozoneTestBucket);

    String prefixKeyA = "key-a";
    String prefixKeyB = "key-b";
    String prefixKeyC = "key-c";
    TreeSet<String> keysASet = new TreeSet<>();
    TreeSet<String> keysBSet = new TreeSet<>();
    TreeSet<String> keysCSet = new TreeSet<>();
    for (int i=1; i<= 100; i++) {
      if (i % 2 == 0) {
        keysASet.add(
            prefixKeyA + i);
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
      } else {
        keysBSet.add(
            prefixKeyB + i);
        addKeysToOM(volumeNameA, hadoopBucket, prefixKeyB + i, i);
      }
    }
    keysCSet.add(prefixKeyC + 1);
    addKeysToOM(volumeNameA, ozoneTestBucket, prefixKeyC + 0, 0);

    TreeSet<String> keysAVolumeBSet = new TreeSet<>();
    TreeSet<String> keysBVolumeBSet = new TreeSet<>();
    for (int i=1; i<= 100; i++) {
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

    Assert.assertEquals(omKeyInfoList.size(),  50);

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      Assert.assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
    }


    String startKey = prefixKeyA + 10;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100);

    Assert.assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    startKey = prefixKeyA + 38;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100);

    Assert.assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      Assert.assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
      Assert.assertFalse(omKeyInfo.getBucketName().equals(
          prefixKeyA + 38));
    }



    omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
        null, prefixKeyB, 100);

    Assert.assertEquals(omKeyInfoList.size(),  50);

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      Assert.assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyB));
    }

    // Try to get keys by count 10, like that get all keys in the
    // volumeB/ozoneBucket with "key-a".
    startKey = null;
    TreeSet<String> expectedKeys = new TreeSet<>();
    for (int i=0; i<5; i++) {

      omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
          startKey, prefixKeyB, 10);

      Assert.assertEquals(10, omKeyInfoList.size());

      for (OmKeyInfo omKeyInfo : omKeyInfoList) {
        expectedKeys.add(omKeyInfo.getKeyName());
        Assert.assertTrue(omKeyInfo.getKeyName().startsWith(
            prefixKeyB));
        startKey =  omKeyInfo.getKeyName();
      }
    }

    Assert.assertEquals(expectedKeys, keysBVolumeBSet);


    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
        startKey, prefixKeyB, 10);

    Assert.assertEquals(omKeyInfoList.size(), 0);

    // List all keys with empty prefix
    omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
        null, null, 100);
    Assert.assertEquals(50, omKeyInfoList.size());
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      Assert.assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
    }
  }

  @Test
  public void testListKeysWithFewDeleteEntriesInCache() throws Exception {
    String volumeNameA = "volumeA";
    String ozoneBucket = "ozoneBucket";

    // Create volumes and bucket.
    TestOMRequestUtils.addVolumeToDB(volumeNameA, omMetadataManager);

    addBucketsToCache(volumeNameA, ozoneBucket);

    String prefixKeyA = "key-a";
    TreeSet<String> keysASet = new TreeSet<>();
    TreeSet<String> deleteKeySet = new TreeSet<>();


    for (int i=1; i<= 100; i++) {
      if (i % 2 == 0) {
        keysASet.add(
            prefixKeyA + i);
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
      } else {
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
        String key = omMetadataManager.getOzoneKey(volumeNameA,
            ozoneBucket, prefixKeyA + i);
        // Mark as deleted in cache.
        omMetadataManager.getKeyTable().addCacheEntry(
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
    Assert.assertEquals(50, omKeyInfoList.size());

    TreeSet<String> expectedKeys = new TreeSet<>();

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      expectedKeys.add(omKeyInfo.getKeyName());
      Assert.assertTrue(omKeyInfo.getKeyName().startsWith(prefixKeyA));
    }

    Assert.assertEquals(expectedKeys, keysASet);


    // Now get key count by 10.
    String startKey = null;
    expectedKeys = new TreeSet<>();
    for (int i=0; i<5; i++) {

      omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
          startKey, prefixKeyA, 10);

      System.out.println(i);
      Assert.assertEquals(10, omKeyInfoList.size());

      for (OmKeyInfo omKeyInfo : omKeyInfoList) {
        expectedKeys.add(omKeyInfo.getKeyName());
        Assert.assertTrue(omKeyInfo.getKeyName().startsWith(
            prefixKeyA));
        startKey =  omKeyInfo.getKeyName();
      }
    }

    Assert.assertEquals(keysASet, expectedKeys);


    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
        startKey, prefixKeyA, 10);

    Assert.assertEquals(omKeyInfoList.size(), 0);



  }

  @Test
  public void testGetExpiredOpenKeys() throws Exception {
    final String bucketName = "bucket";
    final String volumeName = "volume";
    final int numExpiredOpenKeys = 4;
    final int numUnexpiredOpenKeys = 1;
    final long clientID = 1000L;
    // To create expired keys, they will be assigned a creation time twice as
    // old as the minimum expiration time.
    final long minExpiredTimeSeconds = ozoneConfiguration.getInt(
            OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS,
            OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT);
    final long expiredAgeMillis =
            Instant.now().minus(minExpiredTimeSeconds * 2,
                    ChronoUnit.SECONDS).toEpochMilli();

    // Add expired keys to open key table.
    // The method under test does not check for expired open keys in the
    // cache, since they will be picked up once the cache is flushed.
    Set<String> expiredKeys = new HashSet<>();
    for (int i = 0; i < numExpiredOpenKeys; i++) {
      OmKeyInfo keyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
              bucketName, "expired" + i, HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L, expiredAgeMillis);

      TestOMRequestUtils.addKeyToTable(true, false,
              keyInfo, clientID, 0L, omMetadataManager);

      String groupID = omMetadataManager.getOpenKey(volumeName, bucketName,
              keyInfo.getKeyName(), clientID);
      expiredKeys.add(groupID);
    }

    // Add unexpired keys to open key table.
    for (int i = 0; i < numUnexpiredOpenKeys; i++) {
      OmKeyInfo keyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
              bucketName, "unexpired" + i, HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE);

      TestOMRequestUtils.addKeyToTable(true, false,
              keyInfo, clientID, 0L, omMetadataManager);
    }

    // Test retrieving fewer expired keys than actually exist.
    List<String> someExpiredKeys =
            omMetadataManager.getExpiredOpenKeys(numExpiredOpenKeys - 1);

    Assert.assertEquals(numExpiredOpenKeys - 1, someExpiredKeys.size());
    for (String key: someExpiredKeys) {
      Assert.assertTrue(expiredKeys.contains(key));
    }

    // Test attempting to retrieving more expired keys than actually exist.
    List<String> allExpiredKeys =
            omMetadataManager.getExpiredOpenKeys(numExpiredOpenKeys + 1);

    Assert.assertEquals(numExpiredOpenKeys, allExpiredKeys.size());
    for (String key: allExpiredKeys) {
      Assert.assertTrue(expiredKeys.contains(key));
    }

    // Test retrieving exact amount of expired keys that exist.
    allExpiredKeys =
            omMetadataManager.getExpiredOpenKeys(numExpiredOpenKeys);

    Assert.assertEquals(numExpiredOpenKeys, allExpiredKeys.size());
    for (String key: allExpiredKeys) {
      Assert.assertTrue(expiredKeys.contains(key));
    }
  }

  private void addKeysToOM(String volumeName, String bucketName,
      String keyName, int i) throws Exception {

    if (i%2== 0) {
      TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
          1000L, HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    } else {
      TestOMRequestUtils.addKeyToTableCache(volumeName, bucketName, keyName,
          HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE,
          omMetadataManager);
    }
  }

}
/*
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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteOpenKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests OMOpenKeysDeleteRequest.
 */
@RunWith(Parameterized.class)
public class TestOMOpenKeysDeleteRequest extends TestOMKeyRequest {

  private final BucketLayout bucketLayout;

  public TestOMOpenKeysDeleteRequest(BucketLayout bucketLayout) {
    this.bucketLayout = bucketLayout;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  @Parameters
  public static Collection<BucketLayout> bucketLayouts() {
    return Arrays.asList(
        BucketLayout.DEFAULT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED
    );
  }

  /**
   * Tests removing keys from the open key table cache that never existed there.
   * The operation should complete without errors.
   * <p>
   * This simulates a run of the open key cleanup service where a set of
   * expired open keys are identified and passed to the request, but before
   * the request can process them, those keys are committed and removed from
   * the open key table.
   * @throws Exception
   */
  @Test
  public void testDeleteOpenKeysNotInTable() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    List<Pair<Long, OmKeyInfo>> openKeys =
        makeOpenKeys(volumeName, bucketName, 5);
    deleteOpenKeysFromCache(openKeys);
    assertNotInOpenKeyTable(openKeys);
  }

  /**
   * Tests adding multiple keys to the open key table, and updating the table
   * cache to only remove some of them.
   * Keys not removed should still be present in the open key table.
   * Mixes which keys will be kept and deleted among different volumes and
   * buckets.
   * @throws Exception
   */
  @Test
  public void testDeleteSubsetOfOpenKeys() throws Exception {
    final String volume1 = UUID.randomUUID().toString();
    final String volume2 = UUID.randomUUID().toString();
    final String bucket1 = UUID.randomUUID().toString();
    final String bucket2 = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volume1, bucket1,
            omMetadataManager, getBucketLayout());
    OMRequestTestUtils.addVolumeAndBucketToDB(volume1, bucket2,
            omMetadataManager, getBucketLayout());
    OMRequestTestUtils.addVolumeAndBucketToDB(volume2, bucket2,
            omMetadataManager, getBucketLayout());

    List<Pair<Long, OmKeyInfo>> v1b1KeysToDelete =
        makeOpenKeys(volume1, bucket1, 3);
    List<Pair<Long, OmKeyInfo>> v1b1KeysToKeep =
        makeOpenKeys(volume1, bucket1, 3);

    List<Pair<Long, OmKeyInfo>> v1b2KeysToDelete =
        makeOpenKeys(volume1, bucket2, 3);
    List<Pair<Long, OmKeyInfo>> v1b2KeysToKeep =
        makeOpenKeys(volume1, bucket2, 2);

    List<Pair<Long, OmKeyInfo>> v2b2KeysToDelete =
        makeOpenKeys(volume2, bucket2, 2);
    List<Pair<Long, OmKeyInfo>> v2b2KeysToKeep =
        makeOpenKeys(volume2, bucket2, 3);

    addToOpenKeyTableDB(
        v1b1KeysToKeep,
        v1b2KeysToKeep,
        v2b2KeysToKeep,
        v1b1KeysToDelete,
        v1b2KeysToDelete,
        v2b2KeysToDelete
    );

    deleteOpenKeysFromCache(
        v1b1KeysToDelete,
        v1b2KeysToDelete,
        v2b2KeysToDelete
    );

    assertNotInOpenKeyTable(
        v1b1KeysToDelete,
        v1b2KeysToDelete,
        v2b2KeysToDelete
    );

    assertInOpenKeyTable(
        v1b1KeysToKeep,
        v1b2KeysToKeep,
        v2b2KeysToKeep
    );
  }

  /**
   * Tests removing keys from the open key table cache that have the same
   * name, but different client IDs.
   * @throws Exception
   */
  @Test
  public void testDeleteSameKeyNameDifferentClient() throws Exception {
    final String volume = UUID.randomUUID().toString();
    final String bucket = UUID.randomUUID().toString();
    final String key = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volume, bucket,
            omMetadataManager, getBucketLayout());

    List<Pair<Long, OmKeyInfo>> keysToKeep =
        makeOpenKeys(volume, bucket, key, 3);
    List<Pair<Long, OmKeyInfo>> keysToDelete =
        makeOpenKeys(volume, bucket, key, 3);

    addToOpenKeyTableDB(keysToKeep, keysToDelete);
    deleteOpenKeysFromCache(keysToDelete);

    assertNotInOpenKeyTable(keysToDelete);
    assertInOpenKeyTable(keysToKeep);
  }

  /**
   * Tests removing keys from the open key table cache that have higher
   * updateID than the transactionID. Those keys should be ignored.
   * It is OK if updateID equals to or less than transactionID.
   * See {@link WithObjectID#setUpdateID(long, boolean)}.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteKeyWithHigherUpdateID() throws Exception {
    final String volume = UUID.randomUUID().toString();
    final String bucket = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volume, bucket,
        omMetadataManager, getBucketLayout());

    final long updateId = 200L;
    final long transactionId = 100L;

    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setUpdateID(updateId)
        .setReplicationConfig(ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS, ReplicationFactor.THREE));

    if (getBucketLayout().isFileSystemOptimized()) {
      builder.setParentObjectID(random.nextLong());
    }

    List<Pair<Long, OmKeyInfo>> keysWithHigherUpdateID = new ArrayList<>(1);
    keysWithHigherUpdateID.add(Pair.of(clientID,
        builder.setKeyName("key")
            .setFileName("key")
            .setUpdateID(updateId)
            .build()));

    List<Pair<Long, OmKeyInfo>> keysWithSameUpdateID = new ArrayList<>(1);
    keysWithSameUpdateID.add(Pair.of(clientID,
        builder.setKeyName("key2")
            .setFileName("key2")
            .setUpdateID(transactionId)
            .build()));

    List<Pair<Long, OmKeyInfo>> allKeys = new ArrayList<>(2);
    allKeys.addAll(keysWithHigherUpdateID);
    allKeys.addAll(keysWithSameUpdateID);

    addToOpenKeyTableDB(allKeys);

    OMRequest omRequest = doPreExecute(createDeleteOpenKeyRequest(allKeys));
    OMOpenKeysDeleteRequest openKeyDeleteRequest =
        new OMOpenKeysDeleteRequest(omRequest, getBucketLayout());

    OMClientResponse omClientResponse =
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            transactionId, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(Status.OK,
        omClientResponse.getOMResponse().getStatus());

    assertInOpenKeyTable(keysWithHigherUpdateID);
    assertNotInOpenKeyTable(keysWithSameUpdateID);
  }

  /**
   * Tests metrics set by {@link OMOpenKeysDeleteRequest}.
   * Submits a set of keys for deletion where only some of the keys actually
   * exist in the open key table, and asserts that the metrics count keys
   * that were submitted for deletion versus those that were actually deleted.
   * @throws Exception
   */
  @Test
  public void testMetrics() throws Exception {
    final String volume = UUID.randomUUID().toString();
    final String bucket = UUID.randomUUID().toString();
    final String key = UUID.randomUUID().toString();
    final int numExistentKeys = 3;
    final int numNonExistentKeys = 5;

    OMRequestTestUtils.addVolumeAndBucketToDB(volume, bucket,
            omMetadataManager, getBucketLayout());

    OMMetrics metrics = ozoneManager.getMetrics();
    Assert.assertEquals(metrics.getNumOpenKeyDeleteRequests(), 0);
    Assert.assertEquals(metrics.getNumOpenKeyDeleteRequestFails(), 0);
    Assert.assertEquals(metrics.getNumOpenKeysSubmittedForDeletion(), 0);
    Assert.assertEquals(metrics.getNumOpenKeysDeleted(), 0);

    List<Pair<Long, OmKeyInfo>> existentKeys =
        makeOpenKeys(volume, bucket, key, numExistentKeys);
    List<Pair<Long, OmKeyInfo>> nonExistentKeys =
        makeOpenKeys(volume, bucket, key, numNonExistentKeys);

    addToOpenKeyTableDB(existentKeys);
    deleteOpenKeysFromCache(existentKeys, nonExistentKeys);

    assertNotInOpenKeyTable(existentKeys);
    assertNotInOpenKeyTable(nonExistentKeys);

    Assert.assertEquals(1, metrics.getNumOpenKeyDeleteRequests());
    Assert.assertEquals(0, metrics.getNumOpenKeyDeleteRequestFails());
    Assert.assertEquals(numExistentKeys + numNonExistentKeys,
        metrics.getNumOpenKeysSubmittedForDeletion());
    Assert.assertEquals(numExistentKeys, metrics.getNumOpenKeysDeleted());
  }

  /**
   * Runs the validate and update cache step of
   * {@link OMOpenKeysDeleteRequest} to mark the keys in {@code openKeys}
   * as deleted in the open key table cache.
   * Asserts that the call's response status is {@link Status#OK}.
   * @throws Exception
   */
  private void  deleteOpenKeysFromCache(List<Pair<Long, OmKeyInfo>>... allKeys)
      throws Exception {

    deleteOpenKeysFromCache(Arrays.stream(allKeys)
        .flatMap(List::stream)
        .collect(Collectors.toList()));
  }

  private void deleteOpenKeysFromCache(List<Pair<Long, OmKeyInfo>> openKeys)
      throws Exception {

    OMRequest omRequest =
        doPreExecute(createDeleteOpenKeyRequest(openKeys));

    OMOpenKeysDeleteRequest openKeyDeleteRequest =
        new OMOpenKeysDeleteRequest(omRequest, getBucketLayout());

    OMClientResponse omClientResponse =
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  /**
   * Adds {@code openKeys} to the open key table DB only, and asserts that they
   * are present after the addition.
   * @throws Exception
   */
  private void addToOpenKeyTableDB(List<Pair<Long, OmKeyInfo>>... allKeys)
      throws Exception {

    addToOpenKeyTableDB(0, Arrays.stream(allKeys)
        .flatMap(List::stream)
        .collect(Collectors.toList()));
  }

  private void addToOpenKeyTableDB(long keySize,
      List<Pair<Long, OmKeyInfo>> openKeys) throws Exception {

    for (Pair<Long, OmKeyInfo> openKey : openKeys) {
      final long clientID = openKey.getLeft();
      final OmKeyInfo omKeyInfo = openKey.getRight();
      if (keySize > 0) {
        OMRequestTestUtils.addKeyLocationInfo(omKeyInfo, 0, keySize);
      }
      if (getBucketLayout().isFileSystemOptimized()) {
        OMRequestTestUtils.addFileToKeyTable(
            true, false, omKeyInfo.getFileName(),
            omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);
      } else {
        OMRequestTestUtils.addKeyToTable(
            true, false,
            omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);
      }
    }
    assertInOpenKeyTable(openKeys);
  }

  /*
   * Make open keys with randomized key name and client ID
   */
  private List<Pair<Long, OmKeyInfo>> makeOpenKeys(
      String volume, String bucket, int count) {
    return makeOpenKeys(volume, bucket, null, count);
  }

  /*
   * Make open keys with same key name and randomized client ID
   */
  private List<Pair<Long, OmKeyInfo>> makeOpenKeys(
      String volume, String bucket, String key, int count) {

    List<Pair<Long, OmKeyInfo>> keys = new ArrayList<>(count);

    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setReplicationConfig(ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS, ReplicationFactor.THREE));

    if (getBucketLayout().isFileSystemOptimized()) {
      builder.setParentObjectID(random.nextLong());
    }

    for (int i = 0; i < count; i++) {
      final String name = key != null ? key : UUID.randomUUID().toString();
      builder.setKeyName(name);
      if (getBucketLayout().isFileSystemOptimized()) {
        builder.setFileName(OzoneFSUtils.getFileName(name));
      }
      long clientID = random.nextLong();
      keys.add(Pair.of(clientID, builder.build()));
    }
    return keys;
  }

  private void assertInOpenKeyTable(List<Pair<Long, OmKeyInfo>>... allKeys)
      throws Exception {

    assertInOpenKeyTable(Arrays.stream(allKeys)
        .flatMap(List::stream)
        .collect(Collectors.toList()));
  }

  private void assertInOpenKeyTable(List<Pair<Long, OmKeyInfo>> openKeys)
      throws Exception {

    for (String keyName : getDBKeyNames(openKeys)) {
      Assert.assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(keyName));
    }
  }

  private void assertNotInOpenKeyTable(List<Pair<Long, OmKeyInfo>>... allKeys)
      throws Exception {

    assertNotInOpenKeyTable(Arrays.stream(allKeys)
        .flatMap(List::stream)
        .collect(Collectors.toList()));
  }

  private void assertNotInOpenKeyTable(List<Pair<Long, OmKeyInfo>> openKeys)
      throws Exception {

    for (String keyName : getDBKeyNames(openKeys)) {
      Assert.assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(keyName));
    }
  }

  private List<String> getDBKeyNames(List<Pair<Long, OmKeyInfo>> openKeys)
          throws IOException {

    final List<String> result = new ArrayList<>();
    for (Pair<Long, OmKeyInfo> entry : openKeys) {
      final OmKeyInfo ki = entry.getRight();
      if (getBucketLayout().isFileSystemOptimized()) {
        result.add(omMetadataManager.getOpenFileName(
                omMetadataManager.getVolumeId(ki.getVolumeName()),
                omMetadataManager.getBucketId(ki.getVolumeName(),
                        ki.getBucketName()),
                ki.getParentObjectID(),
                ki.getFileName(),
                entry.getLeft()));
      } else {
        result.add(omMetadataManager.getOpenKey(
                entry.getRight().getVolumeName(),
                entry.getRight().getBucketName(),
                entry.getRight().getKeyName(),
                entry.getLeft()));
      }
    }
    return result;
  }

  /**
   * Constructs a new {@link OMOpenKeysDeleteRequest} objects, and calls its
   * {@link OMOpenKeysDeleteRequest#preExecute} method with {@code
   * originalOMRequest}. It verifies that {@code originalOMRequest} is modified
   * after the call, and returns it.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMOpenKeysDeleteRequest omOpenKeysDeleteRequest =
        new OMOpenKeysDeleteRequest(originalOmRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omOpenKeysDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Creates an {@code OpenKeyDeleteRequest} to delete the keys represented by
   * {@code keysToDelete}. Returns an {@code OMRequest} which encapsulates this
   * {@code OpenKeyDeleteRequest}.
   */
  private OMRequest createDeleteOpenKeyRequest(
      List<Pair<Long, OmKeyInfo>> keysToDelete) throws IOException {

    List<String> names = getDBKeyNames(keysToDelete);

    // TODO: HDDS-6563, volume and bucket in OpenKeyBucket doesn't matter
    List<OpenKeyBucket> openKeyBuckets = names.stream()
        .map(name -> OpenKeyBucket.newBuilder()
            .setVolumeName("")
            .setBucketName("")
            .addKeys(OpenKey.newBuilder().setName(name).build())
            .build())
        .collect(Collectors.toList());

    DeleteOpenKeysRequest deleteOpenKeysRequest =
        DeleteOpenKeysRequest.newBuilder()
            .addAllOpenKeysPerBucket(openKeyBuckets)
            .build();

    return OMRequest.newBuilder()
        .setDeleteOpenKeysRequest(deleteOpenKeysRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .setClientId(UUID.randomUUID().toString()).build();
  }

}

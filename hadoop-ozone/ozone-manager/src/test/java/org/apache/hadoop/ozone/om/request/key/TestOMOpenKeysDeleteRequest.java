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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteOpenKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This class tests the OM Open Keys Delete Request.
 */
public class TestOMOpenKeysDeleteRequest extends OMKeyRequestTests {

  /** Transaction index of the hsync that lands before the open key cleanup is applied at 100. */
  private static final long HSYNC_TRXN_INDEX = 50L;

  private BucketLayout bucketLayout;

  @Override
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

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
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testDeleteOpenKeysNotInTable(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
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
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testDeleteSubsetOfOpenKeys(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
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
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testDeleteSameKeyNameDifferentClient(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
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
   * See {@link #setUpdateID(long)}.
   *
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testDeleteKeyWithHigherUpdateID(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
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
            .setUpdateID(updateId)
            .build()));

    List<Pair<Long, OmKeyInfo>> keysWithSameUpdateID = new ArrayList<>(1);
    keysWithSameUpdateID.add(Pair.of(clientID,
        builder.setKeyName("key2")
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
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager, transactionId);

    assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
    String expectedCleanupTable =
        getBucketLayout().isFileSystemOptimized() ? OPEN_FILE_TABLE : OPEN_KEY_TABLE;
    assertEquals(Collections.singleton(expectedCleanupTable),
        omClientResponse.removeCleanupTables());

    assertInOpenKeyTable(keysWithHigherUpdateID);
    assertNotInOpenKeyTable(keysWithSameUpdateID);
  }

  /**
   * Tests that an open key hsync'ed after it was selected for cleanup is kept
   * as long as the committed key still refers to it.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testDeleteHsyncedKeys(BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volume = UUID.randomUUID().toString();
    final String bucket = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volume, bucket,
        omMetadataManager, getBucketLayout());

    final List<Pair<Long, OmKeyInfo>> openKeys = new ArrayList<>();
    for (Pair<Long, OmKeyInfo> openKey : makeOpenKeys(volume, bucket, 3)) {
      final OmKeyInfo keyInfo = openKey.getRight().toBuilder()
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0L, new ArrayList<>(), false)))
          .build();
      openKeys.add(Pair.of(openKey.getLeft(), keyInfo));
    }
    // a non-zero key size so that the keys have blocks that can be queued for deletion.
    // The DB copy carries no HSYNC_CLIENT_ID, which is why the cleanup scan selected these keys.
    addToOpenKeyTableDB(100L, openKeys);

    // the first hsync of each key is only in the open key table cache when the delete is applied
    final List<String> dbKeyNames = getDBKeyNames(openKeys);
    final List<Pair<Long, OmKeyInfo>> hsyncedKeys = new ArrayList<>(openKeys.size());
    for (int i = 0; i < openKeys.size(); i++) {
      final long clientId = openKeys.get(i).getLeft();
      final OmKeyInfo hsyncedKeyInfo = hsync(openKeys.get(i).getRight(), clientId).toBuilder()
          .setUpdateID(HSYNC_TRXN_INDEX)
          .build();
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(dbKeyNames.get(i)), CacheValue.get(HSYNC_TRXN_INDEX, hsyncedKeyInfo));
      hsyncedKeys.add(Pair.of(clientId, hsyncedKeyInfo));
    }
    final List<Pair<Long, OmKeyInfo>> keysToKeep = hsyncedKeys.subList(0, 1);
    final List<Pair<Long, OmKeyInfo>> keysToDelete = hsyncedKeys.subList(1, 3);

    // the committed key of the first open key still refers to it
    addToKeyTableDB(hsyncedKeys.get(0).getRight());
    // the second was overwritten, so its committed key carries another client's ID
    addToKeyTableDB(hsync(hsyncedKeys.get(1).getRight(), hsyncedKeys.get(1).getLeft() + 1));
    // the third has no committed key at all

    final OMRequest omRequest = doPreExecute(createDeleteOpenKeyRequest(hsyncedKeys));
    final OMClientResponse omClientResponse = new OMOpenKeysDeleteRequest(omRequest, getBucketLayout())
        .validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());

    assertInOpenKeyTable(keysToKeep);
    assertNotInOpenKeyTable(keysToDelete);

    // the kept key's blocks must not be queued for deletion
    try (BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation()) {
      omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
    assertNotInDeletedTable(keysToKeep);
    assertInDeletedTable(keysToDelete);
  }

  private void assertInDeletedTable(List<Pair<Long, OmKeyInfo>> openKeys) throws Exception {
    for (String deletePathKey : getDeletePathKeys(openKeys)) {
      assertTrue(omMetadataManager.getDeletedTable().isExist(deletePathKey));
    }
  }

  private void assertNotInDeletedTable(List<Pair<Long, OmKeyInfo>> openKeys) throws Exception {
    for (String deletePathKey : getDeletePathKeys(openKeys)) {
      assertFalse(omMetadataManager.getDeletedTable().isExist(deletePathKey));
    }
  }

  private List<String> getDeletePathKeys(List<Pair<Long, OmKeyInfo>> openKeys) throws IOException {
    final List<String> dbKeyNames = getDBKeyNames(openKeys);
    final List<String> result = new ArrayList<>(openKeys.size());
    for (int i = 0; i < openKeys.size(); i++) {
      result.add(omMetadataManager.getOzoneDeletePathKey(
          openKeys.get(i).getRight().getObjectID(), dbKeyNames.get(i)));
    }
    return result;
  }

  private OmKeyInfo hsync(OmKeyInfo keyInfo, long clientId) {
    return keyInfo.withMetadataMutations(
        metadata -> metadata.put(OzoneConsts.HSYNC_CLIENT_ID, String.valueOf(clientId)));
  }

  private void addToKeyTableDB(OmKeyInfo keyInfo) throws Exception {
    if (getBucketLayout().isFileSystemOptimized()) {
      OMRequestTestUtils.addFileToKeyTable(false, false, keyInfo.getFileName(),
          keyInfo, 0L, 0L, omMetadataManager);
    } else {
      OMRequestTestUtils.addKeyToTable(false, false, keyInfo, 0L, 0L, omMetadataManager);
    }
  }

  /**
   * Tests metrics set by {@link OMOpenKeysDeleteRequest}.
   * Submits a set of keys for deletion where only some of the keys actually
   * exist in the open key table, and asserts that the metrics count keys
   * that were submitted for deletion versus those that were actually deleted.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testMetrics(BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volume = UUID.randomUUID().toString();
    final String bucket = UUID.randomUUID().toString();
    final String key = UUID.randomUUID().toString();
    final int numExistentKeys = 3;
    final int numNonExistentKeys = 5;

    OMRequestTestUtils.addVolumeAndBucketToDB(volume, bucket,
            omMetadataManager, getBucketLayout());

    OMMetrics metrics = ozoneManager.getMetrics();
    assertEquals(metrics.getNumOpenKeyDeleteRequests(), 0);
    assertEquals(metrics.getNumOpenKeyDeleteRequestFails(), 0);
    assertEquals(metrics.getNumOpenKeysSubmittedForDeletion(), 0);
    assertEquals(metrics.getNumOpenKeysDeleted(), 0);

    List<Pair<Long, OmKeyInfo>> existentKeys =
        makeOpenKeys(volume, bucket, key, numExistentKeys);
    List<Pair<Long, OmKeyInfo>> nonExistentKeys =
        makeOpenKeys(volume, bucket, key, numNonExistentKeys);

    addToOpenKeyTableDB(existentKeys);
    deleteOpenKeysFromCache(existentKeys, nonExistentKeys);

    assertNotInOpenKeyTable(existentKeys);
    assertNotInOpenKeyTable(nonExistentKeys);

    assertEquals(1, metrics.getNumOpenKeyDeleteRequests());
    assertEquals(0, metrics.getNumOpenKeyDeleteRequestFails());
    assertEquals(numExistentKeys + numNonExistentKeys,
        metrics.getNumOpenKeysSubmittedForDeletion());
    assertEquals(numExistentKeys, metrics.getNumOpenKeysDeleted());
  }

  /**
   * Test OPEN_KEY_CLEANUP audit logging for both success and failure cases when open keys are deleted.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testOpenKeyCleanupAuditLogging(BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volume = UUID.randomUUID().toString();
    final String bucket = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volume, bucket,
        omMetadataManager, getBucketLayout());

    List<Pair<Long, OmKeyInfo>> openKeys = makeOpenKeys(volume, bucket, 3);
    addToOpenKeyTableDB(openKeys);

    OMRequest omRequest = doPreExecute(createDeleteOpenKeyRequest(openKeys));
    OMOpenKeysDeleteRequest openKeyDeleteRequest = spy(new OMOpenKeysDeleteRequest(omRequest, getBucketLayout()));

    OMClientResponse omClientResponse =
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());

    verify(ozoneManager, times(1))
        .buildAuditMessageForSuccess(eq(OMSystemAction.OPEN_KEY_CLEANUP),
            argThat(params -> {
              assertEquals("3", params.get("numOpenKeysDeleted"));
              assertTrue(params.containsKey("openKeysDeleted"));
              return true;
            }));

    assertNotInOpenKeyTable(openKeys);

    // Simulate failure by mocking updateOpenKeyTableCache to throw an IOException, and verify the failure audit log.
    doThrow(new IOException())
        .when(openKeyDeleteRequest)
        .updateOpenKeyTableCache(
            any(OzoneManager.class),
            anyLong(),
            any(OpenKeyBucket.class),
            anyMap());

    omClientResponse = openKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(Status.INTERNAL_ERROR, omClientResponse.getOMResponse().getStatus());

    verify(ozoneManager, times(1))
        .buildAuditMessageForFailure(eq(OMSystemAction.OPEN_KEY_CLEANUP),
            argThat(Map::isEmpty),
            any(Throwable.class));
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
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(Status.OK,
        omClientResponse.getOMResponse().getStatus());
    for (OmKeyInfo openKey : openKeys.stream().map(Pair::getRight).collect(Collectors.toList())) {
      assertEquals(0, omMetadataManager.getBucketTable().get(
          omMetadataManager.getBucketKey(openKey.getVolumeName(), openKey.getBucketName())).getSnapshotUsedBytes());
      assertEquals(0, omMetadataManager.getBucketTable().get(
          omMetadataManager.getBucketKey(openKey.getVolumeName(), openKey.getBucketName())).getSnapshotUsedNamespace());
    }
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
      assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(keyName));
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
      assertFalse(omMetadataManager.getOpenKeyTable(
          getBucketLayout()).isExist(keyName));
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
    assertNotEquals(originalOmRequest, modifiedOmRequest);

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

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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addBucketToDB;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the OM Response when open keys are deleted.
 */
public class TestOMOpenKeysDeleteResponse extends TestOMKeyResponse {
  private static final long KEY_LENGTH = 100;
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
   * Tests deleting a subset of keys from the open key table DB when the keys
   * have no associated block data.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAddToDBBatchWithEmptyBlocks(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    Map<String, Pair<Long, OmKeyInfo>> keysToDelete = addOpenKeysToDB(volumeName, 3);
    Map<String, Pair<Long, OmKeyInfo>> keysToKeep = addOpenKeysToDB(volumeName, 3);

    createAndCommitResponse(keysToDelete, Status.OK);

    for (String key: keysToDelete.keySet()) {
      // open keys with no associated block data should have been removed
      // from the open key table, but not added to the deleted table.
      assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(key));
      assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }

    for (String key: keysToKeep.keySet()) {
      // These keys should not have been removed from the open key table.
      assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(key));
      assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }

  /**
   * Tests deleting a subset of keys from the open key table DB when the keys
   * have associated block data.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAddToDBBatchWithNonEmptyBlocks(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    Map<String, Pair<Long, OmKeyInfo>> keysToDelete = addOpenKeysToDB(volumeName, 3,
        KEY_LENGTH);
    Map<String, Pair<Long, OmKeyInfo>> keysToKeep = addOpenKeysToDB(volumeName, 3,
        KEY_LENGTH);

    createAndCommitResponse(keysToDelete, Status.OK);

    for (Map.Entry<String, Pair<Long, OmKeyInfo>> entry: keysToDelete.entrySet()) {
      // These keys should have been moved from the open key table to the
      // delete table.
      assertFalse(
          omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(
              entry.getKey()));
      String deleteKey = omMetadataManager.getOzoneDeletePathKey(
          entry.getValue().getValue().getObjectID(), entry.getKey());
      assertTrue(omMetadataManager.getDeletedTable().isExist(deleteKey));
      assertFalse(omMetadataManager.getDeletedTable().get(deleteKey).getOmKeyInfoList().get(0).isDeletedKeyCommitted());
    }

    for (Map.Entry<String, Pair<Long, OmKeyInfo>> entry: keysToKeep.entrySet()) {
      // These keys should not have been moved out of the open key table.
      assertTrue(
          omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(
              entry.getKey()));
      String deleteKey = omMetadataManager.getOzoneDeletePathKey(
          entry.getValue().getValue().getObjectID(), entry.getKey());
      assertFalse(omMetadataManager.getDeletedTable().isExist(deleteKey));
    }
  }

  /**
   * Tests attempting deleting keys from the open key table DB when the
   * submitted response has an error status. In this case, no changes to the
   * DB should be made.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAddToDBBatchWithErrorResponse(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    Map<String, Pair<Long, OmKeyInfo>> keysToDelete = addOpenKeysToDB(volumeName, 3);

    createAndCommitResponse(keysToDelete, Status.INTERNAL_ERROR);

    for (String key: keysToDelete.keySet()) {
      // If an error occurs in the response, the batch operation moving keys
      // from the open key table to the delete table should not be committed.
      assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(key));
      assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }

  /**
   * Constructs an {@link OMOpenKeysDeleteResponse} to delete the keys in
   * {@code keysToDelete}, with the completion status set to {@code status}.
   * If {@code status} is {@link Status#OK}, the keys to delete will be added
   * to a batch operation and committed to the database.
   * @throws Exception
   */
  private void createAndCommitResponse(Map<String, Pair<Long, OmKeyInfo>> keysToDelete,
      Status status) throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setStatus(status)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .build();

    OMOpenKeysDeleteResponse response = new OMOpenKeysDeleteResponse(omResponse,
        keysToDelete, getBucketLayout());

    // Operations are only added to the batch by this method when status is OK.
    response.checkAndUpdateDB(omMetadataManager, batchOperation);

    // If status is not OK, this will do nothing.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  /**
   * Creates {@code numKeys} open keys with random names, maps each one to a
   * new {@link OmKeyInfo} object, adds them to the open key table cache, and
   * returns them. These keys will have no associated block data.
   */
  private Map<String, Pair<Long, OmKeyInfo>> addOpenKeysToDB(String volume, int numKeys)
      throws Exception {
    return addOpenKeysToDB(volume, numKeys, 0);
  }

  /**
   * Creates {@code numKeys} open keys with random names, maps each one to a
   * new {@link OmKeyInfo} object, adds them to the open key table cache, and
   * returns them.
   * If {@code keyLength} is greater than 0, adds one block with that many
   * bytes of data for each key.
   * @throws Exception
   */
  private Map<String, Pair<Long, OmKeyInfo>> addOpenKeysToDB(String volume, int numKeys,
      long keyLength) throws Exception {

    Map<String, Pair<Long, OmKeyInfo>> newOpenKeys = new HashMap<>();

    for (int i = 0; i < numKeys; i++) {
      String bucket = UUID.randomUUID().toString();
      String key = UUID.randomUUID().toString();
      OmBucketInfo bucketInfo = addBucketToDB(volume, bucket, omMetadataManager, getBucketLayout());
      long clientID = random.nextLong();
      long parentID = random.nextLong();

      OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volume, bucket, key, replicationConfig);
      if (getBucketLayout().isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(parentID);
      }
      OmKeyInfo omKeyInfo = keyInfoBuilder.build();

      if (keyLength > 0) {
        OMRequestTestUtils.addKeyLocationInfo(omKeyInfo, 0, keyLength);
      }

      final String openKey;

      // Add to the open key table DB, not cache.
      // In a real execution, the open key would have been removed from the
      // cache by the request, and it would only remain in the DB.
      if (getBucketLayout().isFileSystemOptimized()) {
        String file = OzoneFSUtils.getFileName(key);
        final long volumeId = omMetadataManager.getVolumeId(volume);
        final long bucketId = omMetadataManager.getBucketId(volume, bucket);
        OMRequestTestUtils.addFileToKeyTable(true, false, file, omKeyInfo,
            clientID, 0L, omMetadataManager);
        openKey = omMetadataManager.getOpenFileName(
                volumeId, bucketId, parentID, file, clientID);
      } else {
        OMRequestTestUtils.addKeyToTable(true, false, omKeyInfo,
            clientID, 0L, omMetadataManager);
        openKey = omMetadataManager.getOpenKey(volume, bucket, key, clientID);
      }
      assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(openKey));

      newOpenKeys.put(openKey, Pair.of(bucketInfo.getObjectID(), omKeyInfo));
    }

    return newOpenKeys;
  }
}

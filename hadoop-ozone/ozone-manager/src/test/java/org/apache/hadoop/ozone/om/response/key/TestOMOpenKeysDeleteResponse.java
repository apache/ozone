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

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Tests OMOpenKeysDeleteResponse.
 */
public class TestOMOpenKeysDeleteResponse extends TestOMKeyResponse {
  private static final long KEY_LENGTH = 100;

  @Test
  /**
   * Tests deleting a subset of keys from the open key table DB when the keys
   * have no associated block data.
   */
  public void testAddToDBBatchWithEmptyBlocks() throws Exception {
    Map<String, OmKeyInfo> keysToDelete = addOpenKeysToDB(volumeName, 3);
    Map<String, OmKeyInfo> keysToKeep = addOpenKeysToDB(volumeName, 3);
    createAndCommitResponse(keysToDelete, Status.OK);

    for (String key: keysToDelete.keySet()) {
      // open keys with no associated block data should have been removed
      // from the open key table, but not added to the deleted table.
      Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }

    for (String key: keysToKeep.keySet()) {
      // These keys should not have been removed from the open key table.
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }

  /**
   * Tests deleting a subset of keys from the open key table DB when the keys
   * have associated block data.
   */
  @Test
  public void testAddToDBBatchWithNonEmptyBlocks() throws Exception {
    Map<String, OmKeyInfo> keysToDelete = addOpenKeysToDB(volumeName, 3,
        KEY_LENGTH);
    Map<String, OmKeyInfo> keysToKeep = addOpenKeysToDB(volumeName, 3,
        KEY_LENGTH);

    createAndCommitResponse(keysToDelete, Status.OK);

    for (String key: keysToDelete.keySet()) {
      // These keys should have been moved from the open key table to the
      // delete table.
      Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(key));
    }

    for (String key: keysToKeep.keySet()) {
      // These keys should not have been moved out of the open key table.
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }

  /**
   * Tests attempting deleting keys from the open key table DB when the
   * submitted response has an error status. In this case, no changes to the
   * DB should be made.
   */
  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    Map<String, OmKeyInfo> keysToDelete = addOpenKeysToDB(volumeName, 3);
    createAndCommitResponse(keysToDelete, Status.INTERNAL_ERROR);

    for (String key: keysToDelete.keySet()) {
      // If an error occurs in the response, the batch operation moving keys
      // from the open key table to the delete table should not be committed.
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }

  /**
   * Tests that the byte usage information for a volume is updated in the
   * DB when open keys with block data are submitted for deletion from the DB.
   */
  @Test
  public void testAddToDBBatchWithVolumeModifications() throws Exception {
    // Put the volume in the cache and DB, and return a reference to the
    // cached value.
    OmVolumeArgs volumeArgs = createVolume(volumeName);
    // Updates the volume args in the cache only.
    volumeArgs.getUsedBytes().add(KEY_LENGTH);
    // Make sure updates were not persisted to DB to simulate the request
    // already running and doing the cache update only.
    Assert.assertEquals(0, getVolumeFromDB(volumeName).getUsedBytes().sum());
    Assert.assertEquals(KEY_LENGTH,
        getVolumeFromCache(volumeName).getUsedBytes().sum());

    Map<String, OmKeyInfo> keysToDelete = addOpenKeysToDB(volumeName, 3,
        KEY_LENGTH);

    createAndCommitResponse(keysToDelete, Arrays.asList(volumeArgs), Status.OK);

    for (String key: keysToDelete.keySet()) {
      // These keys should have been moved from the open key table to the
      // delete table.
      Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(key));
    }

    // Check that response persisted volume changes to DB.
    Assert.assertEquals(KEY_LENGTH,
        getVolumeFromDB(volumeName).getUsedBytes().sum());
  }

  /**
   * Constructs an {@link OMOpenKeysDeleteResponse} to delete the keys in
   * {@code keysToDelete}, with the completion status set to {@code status}.
   * If {@code status} is {@link Status#OK}, the keys to delete will be added
   * to a batch operation and committed to the database.
   * @throws Exception
   */
  private void createAndCommitResponse(Map<String, OmKeyInfo> keysToDelete,
      Status status) throws Exception {

    createAndCommitResponse(keysToDelete, new ArrayList<>(), status);
  }

  /**
   * Constructs an {@link OMOpenKeysDeleteResponse} to delete the keys in
   * {@code keysToDelete}, with the completion status set to {@code status}
   * and list of modified volume information set to {@code modifiedVolumes}.
   * If {@code status} is {@link Status#OK}, the keys to delete will be added
   * to a batch operation and committed to the database.
   * @throws Exception
   */
  private void createAndCommitResponse(Map<String, OmKeyInfo> keysToDelete,
      Collection<OmVolumeArgs> modifiedVolumes, Status status)
      throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setStatus(status)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .build();

    OMOpenKeysDeleteResponse response = new OMOpenKeysDeleteResponse(omResponse,
        keysToDelete, true, modifiedVolumes);

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
  private Map<String, OmKeyInfo> addOpenKeysToDB(String volume, int numKeys)
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
  private Map<String, OmKeyInfo> addOpenKeysToDB(String volume, int numKeys,
      long keyLength) throws Exception {

    Map<String, OmKeyInfo> newOpenKeys = new HashMap<>();

    for (int i = 0; i < numKeys; i++) {
      String bucket = UUID.randomUUID().toString();
      String key = UUID.randomUUID().toString();
      long clientID = new Random().nextLong();

      OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volume,
          bucket, key, replicationType, replicationFactor);

      if (keyLength > 0) {
        TestOMRequestUtils.addKeyLocationInfo(omKeyInfo, 0, keyLength);
      }

      String openKey = omMetadataManager.getOpenKey(volume, bucket,
          key, clientID);

      // Add to the open key table DB, not cache.
      // In a real execution, the open key would have been removed from the
      // cache by the request, and it would only remain in the DB.
      TestOMRequestUtils.addKeyToTable(true, false, omKeyInfo,
          clientID, 0L, omMetadataManager);
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(openKey));

      newOpenKeys.put(openKey, omKeyInfo);
    }

    return newOpenKeys;
  }

  private OmVolumeArgs getVolumeFromDB(String volume) throws Exception {
    String volumeKey = omMetadataManager.getVolumeKey(volume);
    return omMetadataManager.getVolumeTable().getSkipCache(volumeKey);
  }

  private OmVolumeArgs getVolumeFromCache(String volume) {
    String volumeKey = omMetadataManager.getVolumeKey(volume);
    return omMetadataManager.getVolumeTable()
            .getCacheValue(new CacheKey<>(volumeKey))
            .getCacheValue();
  }

  /**
   * Creates a volume with name {@code volume}, adds it to the volume table
   * cache and DB, and returns a reference to the cached version.
   */
  private OmVolumeArgs createVolume(String volume) throws Exception {
    // Adds the volume to the cache only.
    TestOMRequestUtils.addVolumeToDB(volume, omMetadataManager);

    // Get the volume from the cache and add it to the DB as well.
    String volumeKey = omMetadataManager.getVolumeKey(volume);
    OmVolumeArgs volumeArgs  =
        omMetadataManager.getVolumeTable()
            .getCacheValue(new CacheKey<>(volumeKey))
            .getCacheValue();

    omMetadataManager.getVolumeTable().put(volumeKey, volumeArgs);
    return volumeArgs;
  }
}

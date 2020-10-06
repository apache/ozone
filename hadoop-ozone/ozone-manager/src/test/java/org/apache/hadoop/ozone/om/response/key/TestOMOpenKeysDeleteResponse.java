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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Tests OMOpenKeysDeleteResponse.
 */
public class TestOMOpenKeysDeleteResponse extends TestOMKeyResponse {
  private static final long KEY_LENGTH = 100;

  /**
   * Tests deleting a subset of keys from the open key table DB when the keys
   * have no associated block data.
   */
  @Test
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
   * Constructs an {@link OMOpenKeysDeleteResponse} to delete the keys in
   * {@code keysToDelete}, with the completion status set to {@code status}.
   * If {@code status} is {@link Status#OK}, the keys to delete will be added
   * to a batch operation and committed to the database.
   * @throws Exception
   */
  private void createAndCommitResponse(Map<String, OmKeyInfo> keysToDelete,
      Status status) throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setStatus(status)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .build();

    OMOpenKeysDeleteResponse response = new OMOpenKeysDeleteResponse(omResponse,
        keysToDelete, true);

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
}

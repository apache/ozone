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

package org.apache.hadoop.ozone.om.request.key;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.response.key.OMOpenKeyDeleteRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Tests OmOpenKeyDelete request.
 */
public class TestOMOpenKeyDeleteRequest extends TestOMKeyRequest {

  /**
   * Specifies how a set of keys in a bucket should be generated.
   */
  private enum KeyCreateMethod {
    // All keys made in the bucket will have the same key name but different
    // client IDs.
    SAME_KEY_NAME,
    // All keys made in the bucket will have the same clientID but different
    // key names.
    SAME_CLIENT_ID,
    // All keys made in the bucket will have random names and client IDs.
    RANDOM
  }

  @Test
  public void testDeleteSubsetOfOpenKeys() throws Exception {

  }

  @Test
  public void testDeleteSameKeyName() throws Exception {

  }

  @Test
  public void testDeleteOpenKeysNotInTable() throws Exception {

  }

  private void deleteOpenKeys(List<DeletedKeys> openKeys) throws Exception {
    OMRequest omRequest =
        doPreExecute(createDeleteOpenKeyRequest(openKeys));

    OMOpenKeyDeleteRequest openKeyDeleteRequest =
        new OMOpenKeyDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());

    for (DeletedKeys keysPerBucket: openKeys) {
      String volume = keysPerBucket.getVolumeName();
      String bucket = keysPerBucket.getBucketName();

      // TODO: Key name currently has the client ID attached to it.
      //  When the new proto is created to separate them, remove this.
      for (String keyName: keysPerBucket.getKeysList()) {
        String openKey =
            omMetadataManager.getOzoneKey(volume, bucket, keyName);
        // Key should have been moved from open key table to deleted table.
        // Since we did not run the key deleting service, the key should remain
        // in the deleted table indefinitely.
        OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
        Assert.assertNull(omKeyInfo);
      }
    }
  }

  private DeletedKeys makeDeletedKeys(int numKeys,
    KeyCreateMethod createMethod, boolean addToTable) throws Exception {

    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    String key = UUID.randomUUID().toString();
    long client = new Random().nextLong();

    DeletedKeys.Builder keyBuilder = DeletedKeys.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket);

    for (int i = 0; i < numKeys; i++) {
      switch (createMethod) {
        case SAME_KEY_NAME:
          // Use the pre-created key name for all keys, generate new client IDs.
          client = new Random().nextLong();
          break;
        case SAME_CLIENT_ID:
          // Use the pre-created client ID for all keys, generate new key names.
          key = UUID.randomUUID().toString();
          break;
        case RANDOM:
          key = UUID.randomUUID().toString();
          client = Time.now();
          break;
      }

      if (addToTable) {
        TestOMRequestUtils.addKeyToTable(true, volume, bucket, key,
            client, replicationType, replicationFactor, omMetadataManager);

        // Check that key was added to open key table.
        String openKey = omMetadataManager.getOpenKey(volume, bucket, key,
            client);
        OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
        Assert.assertNotNull(omKeyInfo);
      }

      // TODO: Create new bucket level proto where key and clinet ID are
      //  stored separately.
      keyBuilder.addKeys(key + OzoneConsts.OM_KEY_PREFIX + client);
    }

    return keyBuilder.build();
  }

  /**
   * Tests executing an open key delete request on a key no longer present in
   * the open key table. The request should finish without error.
   * @throws Exception
   */
  @Test
  public void testValidateAndUpdateCacheWithKeyRemoved() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMOpenKeyDeleteRequest openKeyDeleteRequest =
        new OMOpenKeyDeleteRequest(modifiedOmRequest);

    // Add only volume and bucket entry to DB.
    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMClientResponse omClientResponse =
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omKeyDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates DeleteKeyRequest.
   * @return OMRequest
   */
  private OMRequest createDeleteOpenKeyRequest(List<DeletedKeys> deletedKeys) {
    DeleteOpenKeysRequest deleteOpenKeysRequest =
        DeleteOpenKeysRequest.newBuilder().setExpiredOpenKeys(deletedKeys).build();

    return OMRequest.newBuilder().setOpenKeyDeleteRequest(deleteOpenKeysRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}

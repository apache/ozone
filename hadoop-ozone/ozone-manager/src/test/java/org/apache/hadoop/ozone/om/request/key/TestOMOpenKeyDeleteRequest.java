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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.Random;

import org.apache.hadoop.ozone.om.response.key.OMOpenKeyDeleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteOpenKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OpenKeysPerBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Tests OmOpenKeyDelete request.
 */
public class TestOMOpenKeyDeleteRequest extends TestOMKeyRequest {
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
    final String volume1 = "volume1";
    final String volume2 = "bucket1";
    final String bucket1 = "volume2";
    final String bucket2 = "bucket2";

    OpenKeysPerBucket v1b1KeysToDelete = makeOpenKeys(volume1, bucket1, 3);
    OpenKeysPerBucket v1b1KeysToKeep = makeOpenKeys(volume1, bucket1, 3);

    OpenKeysPerBucket v1b2KeysToDelete = makeOpenKeys(volume1, bucket2, 3);
    OpenKeysPerBucket v1b2KeysToKeep = makeOpenKeys(volume1, bucket2, 3);

    OpenKeysPerBucket v2b2KeysToDelete = makeOpenKeys(volume2, bucket2, 3);
    OpenKeysPerBucket v2b2KeysToKeep = makeOpenKeys(volume2, bucket2, 3);

    addToOpenKeyTable(
        v1b1KeysToKeep,
        v1b2KeysToKeep,
        v2b2KeysToKeep,
        v1b1KeysToDelete,
        v1b2KeysToDelete,
        v2b2KeysToDelete
    );

    deleteOpenKeys(
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
  public void testDeleteSameKeyName() throws Exception {
    OpenKeysPerBucket keysToKeep =
        makeOpenKeys(volumeName, bucketName, keyName, 3);
    OpenKeysPerBucket keysToDelete =
        makeOpenKeys(volumeName, bucketName, keyName,3);

    addToOpenKeyTable(keysToKeep, keysToDelete);
    deleteOpenKeys(keysToDelete);

    assertNotInOpenKeyTable(keysToDelete);
    assertInOpenKeyTable(keysToKeep);
  }

  /**
   * Tests removing keys from the open key table cache that never existed there.
   * The operation should complete without errors.
   * @throws Exception
   */
  @Test
  public void testDeleteOpenKeysNotInTable() throws Exception {
    OpenKeysPerBucket openKeys = makeOpenKeys(volumeName, bucketName, 5);
    deleteOpenKeys(openKeys);
    assertNotInOpenKeyTable(openKeys);
  }

  /**
   * Runs the validate and update cache step of
   * {@link OMOpenKeyDeleteRequest} to mark the keys in {@code openKeys}
   * as deleted in the open key table.
   * Asserts that the call's response status is {@link Status#OK}.
   * @throws Exception
   */
  private void deleteOpenKeys(OpenKeysPerBucket... openKeys) throws Exception {
    OMRequest omRequest =
        doPreExecute(createDeleteOpenKeyRequest(openKeys));

    OMOpenKeyDeleteRequest openKeyDeleteRequest =
        new OMOpenKeyDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
  }

  /**
   * Adds {@code openKeys} to the open key table, and asserts that they are
   * present after the addition.
   * @throws Exception
   */
  private void addToOpenKeyTable(OpenKeysPerBucket... openKeys)
      throws Exception {

    for (OpenKeysPerBucket openKeysPerBucket: openKeys) {
      String volume  = openKeysPerBucket.getVolumeName();
      String bucket  = openKeysPerBucket.getBucketName();

      for (OpenKey openKey: openKeysPerBucket.getKeysList()) {
        TestOMRequestUtils.addKeyToTable(true,
            volume, bucket, openKey.getName(), openKey.getId(),
            replicationType, replicationFactor, omMetadataManager);
      }
    }

    assertInOpenKeyTable(openKeys);
  }

  /**
   * Constructs a list of {@link OpenKey} objects of size {@code numKeys}.
   * The keys created will all have the same volume and bucket, but
   * randomized key names and client IDs. These keys are not added to the
   * open key table.
   *
   * @param volume The volume all open keys created will have.
   * @param bucket The bucket all open keys created will have.
   * @param numKeys The number of keys with randomized key names and client
   * IDs to create.
   * @return A list of new open keys with size {@code numKeys}.
   */
  private OpenKeysPerBucket makeOpenKeys(String volume, String bucket,
      int numKeys) {

    OpenKeysPerBucket.Builder keysPerBucketBuilder =
        OpenKeysPerBucket.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket);

    for (int i = 0; i < numKeys; i++) {
      String keyName = UUID.randomUUID().toString();
      long clientID = new Random().nextLong();

      OpenKey openKey = OpenKey.newBuilder()
          .setName(keyName)
          .setId(clientID)
          .build();
      keysPerBucketBuilder.addKeys(openKey);
    }

    return keysPerBucketBuilder.build();
  }

  /**
   * Constructs a list of {@link OpenKey} objects of size {@code numKeys}.
   * The keys created will all have the same volume and bucket, and
   * key names, but randomized client IDs. These keys are not added to the
   * open key table.
   *
   * @param volume The volume all open keys created will have.
   * @param bucket The bucket all open keys created will have.
   * @param key The key name all open keys created will have.
   * @param numKeys The number of keys with randomized key names and client
   * IDs to create.
   * @return A list of new open keys with size {@code numKeys}.
   */
  private OpenKeysPerBucket makeOpenKeys(String volume, String bucket,
      String key, int numKeys) {

    OpenKeysPerBucket.Builder keysPerBucketBuilder =
        OpenKeysPerBucket.newBuilder()
            .setVolumeName(volume)
            .setBucketName(bucket);

    for (int i = 0; i < numKeys; i++) {
      long clientID = new Random().nextLong();

      OpenKey openKey = OpenKey.newBuilder()
          .setName(key)
          .setId(clientID)
          .build();
      keysPerBucketBuilder.addKeys(openKey);
    }

    return keysPerBucketBuilder.build();
  }

  private void assertInOpenKeyTable(OpenKeysPerBucket... openKeys)
      throws Exception {

    for (String keyName: getFullKeyNames(openKeys)) {
      OmKeyInfo omKeyInfo =
          omMetadataManager.getOpenKeyTable().get(keyName);
      Assert.assertNotNull(omKeyInfo);
    }
  }

  private void assertNotInOpenKeyTable(OpenKeysPerBucket... openKeys)
      throws Exception {

    for (String keyName: getFullKeyNames(openKeys)) {
      OmKeyInfo omKeyInfo =
          omMetadataManager.getOpenKeyTable().get(keyName);
      Assert.assertNull(omKeyInfo);
    }
  }

  private List<String> getFullKeyNames(OpenKeysPerBucket... openKeysPerBuckets) {
    List<String> fullKeyNames = new ArrayList<>();

    for(OpenKeysPerBucket keysPerBucket: openKeysPerBuckets) {
      String volume = keysPerBucket.getVolumeName();
      String bucket = keysPerBucket.getBucketName();

      for (OpenKey openKey: keysPerBucket.getKeysList()) {
        String fullName = omMetadataManager.getOpenKey(volume, bucket,
            openKey.getName(), openKey.getId());
        fullKeyNames.add(fullName);
      }
    }

    return fullKeyNames;
  }

  /**
   * Constructs a new {@link OMOpenKeyDeleteRequest} objects, and calls its
   * {@link OMOpenKeyDeleteRequest#preExecute} method with {@code
   * originalOMRequest}. It verifies that {@code originalOMRequest} is modified
   * after the call, and returns it.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMOpenKeyDeleteRequest omOpenKeyDeleteRequest =
        new OMOpenKeyDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest =
        omOpenKeyDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates OpenKeyDeleteRequest.
   * @return OMRequest
   */
  private OMRequest createDeleteOpenKeyRequest(OpenKeysPerBucket... keysToDelete) {
    DeleteOpenKeysRequest deleteOpenKeysRequest =
        DeleteOpenKeysRequest.newBuilder()
            .addAllOpenKeysToDelete(Arrays.asList(keysToDelete))
            .build();

    return OMRequest.newBuilder()
        .setDeleteOpenKeysRequest(deleteOpenKeysRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}

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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Random;
import java.util.HashMap;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.response.key.OMOpenKeyDeleteRequest;
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

    List<OpenKey> keysToDelete =
        new ArrayList<>(makeOpenKeys(volume1, bucket1, 3));
    List<OpenKey> keysToKeep =
        new ArrayList<>(makeOpenKeys(volume1, bucket1, 3));

    keysToDelete.addAll(
        makeOpenKeys(volume1, bucket2, 3)
    );
    keysToKeep.addAll(
        makeOpenKeys(volume1, bucket2, 3)
    );

    keysToDelete.addAll(
        makeOpenKeys(volume2, bucket2, 3)
    );
    keysToKeep.addAll(
        makeOpenKeys(volume2, bucket2, 3)
    );

    addToOpenKeyTable(ListUtils.union(keysToKeep, keysToDelete));
    deleteOpenKeys(keysToDelete);

    assertNotInOpenKeyTable(keysToDelete);
    assertInOpenKeyTable(keysToKeep);
  }

  /**
   * Tests removing keys from the open key table cache that have the same
   * name, but different client IDs.
   * @throws Exception
   */
  @Test
  public void testDeleteSameKeyName() throws Exception {
    List<OpenKey> keysToKeep =
        makeOpenKeys(volumeName, bucketName, keyName, 3);
    List<OpenKey> keysToDelete =
        makeOpenKeys(volumeName, bucketName, keyName,3);

    addToOpenKeyTable(ListUtils.union(keysToKeep, keysToDelete));
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
    List<OpenKey> openKeys = makeOpenKeys(volumeName, bucketName, 5);
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
  private void deleteOpenKeys(List<OpenKey> openKeys) throws Exception {
    List<DeletedKeys> openKeysPerBucket = gatherByBucket(openKeys);

    OMRequest omRequest =
        doPreExecute(createDeleteOpenKeyRequest(openKeysPerBucket));

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
  private void addToOpenKeyTable(List<OpenKey> openKeys) throws Exception {
    for (OpenKey openKey: openKeys) {
      TestOMRequestUtils.addKeyToTable(false,
          openKey.volume, openKey.bucket, openKey.key, openKey.clientID,
          replicationType, replicationFactor, omMetadataManager);
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
  private List<OpenKey> makeOpenKeys(String volume, String bucket,
      int numKeys) {

    List<OpenKey> openKeys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      String key = UUID.randomUUID().toString();
      long clientID = new Random().nextLong();

      openKeys.add(new OpenKey(volume, bucket, key, clientID));
    }

    return openKeys;
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
  private List<OpenKey> makeOpenKeys(String volume, String bucket,
      String key, int numKeys) {

    List<OpenKey> openKeys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      long clientID = new Random().nextLong();
      openKeys.add(new OpenKey(volume, bucket, key, clientID));
    }

    return openKeys;
  }

  private void assertInOpenKeyTable(List<OpenKey> openKeys) throws Exception {
    for (OpenKey openKey: openKeys) {
      OmKeyInfo omKeyInfo =
          omMetadataManager.getOpenKeyTable().get(openKey.toString());
      Assert.assertNotNull(omKeyInfo);
    }
  }

  private void assertNotInOpenKeyTable(List<OpenKey> openKeys) throws Exception {
    for (OpenKey openKey: openKeys) {
      OmKeyInfo omKeyInfo =
          omMetadataManager.getOpenKeyTable().get(openKey.toString());
      Assert.assertNull(omKeyInfo);
    }
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
   * Gathers a list of {@link OpenKey} objects into a list of
   * {@link DeletedKeys} objects based on common volumes and buckets.
   */
  private List<DeletedKeys> gatherByBucket(List<OpenKey> openKeys) {
    // Maps volume to bucket to DeletedKeys object for that bucket.
    // Will be filled and used to construct the list of DeletedKeys.
    Map<String, Map<String, DeletedKeys>> deletedKeys = new HashMap<>();
    List<DeletedKeys> result = new ArrayList<>();

    for (OpenKey key: openKeys) {
      Map<String, DeletedKeys> volumeKeys = deletedKeys.get(key.volume);

      if (volumeKeys == null) {
        volumeKeys = new HashMap<>();
        DeletedKeys gatheredKeys = DeletedKeys.newBuilder().build();

        // Map the new DeletedKey object to its volume and bucket.
        volumeKeys.put(key.bucket, gatheredKeys);
        deletedKeys.put(key.volume, volumeKeys);

        // Add the new DeletedKeys object to the return result.
        // Any future updates made to it will persist to this reference as well.
        result.add(gatheredKeys);
      }

      // TODO: Create new proto tracking client ID.
      // Add this key to its existing DeletedKeys object.
      volumeKeys.get(key.bucket).getKeysList().add(key.key);
    }

    return result;
  }

  /**
   * Create OMRequest which encapsulates OpenKeyDeleteRequest.
   * @return OMRequest
   */
  private OMRequest createDeleteOpenKeyRequest(List<DeletedKeys> deletedKeys) {
    DeleteOpenKeysRequest deleteOpenKeysRequest =
        DeleteOpenKeysRequest.newBuilder().setExpiredOpenKeys(deletedKeys).build();

    return OMRequest.newBuilder().setOpenKeyDeleteRequest(deleteOpenKeysRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  /**
   * Class used to represent a single open key, not grouped with any others
   * by volume or bucket.
   */
  class OpenKey {
    public String volume;
    public String bucket;
    public String key;
    public long clientID;

    public OpenKey(String volume, String bucket, String key, long clientID) {
      this.volume = volume;
      this.bucket = bucket;
      this.key = key;
      this.clientID = clientID;
    }

    @Override
    public String toString() {
      return TestOMOpenKeyDeleteRequest.this.omMetadataManager
          .getOpenKey(volume, bucket, key, clientID);
    }
  }
}

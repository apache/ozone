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

import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.response.key.OMOpenKeyDeleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
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

/**
 * Tests OMOpenKeyDeleteRequest.
 * This class updates the open key table cache only. It does not write to any
 * underlying database, and it does not update the delete table cache.
 */
public class TestOMOpenKeyDeleteRequest extends TestOMKeyRequest {
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
    OpenKeyBucket openKeys = makeOpenKeys(volumeName, bucketName, 5);
    deleteOpenKeys(openKeys);
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
    final String volume1 = "volume1";
    final String volume2 = "bucket1";
    final String bucket1 = "volume2";
    final String bucket2 = "bucket2";

    OpenKeyBucket v1b1KeysToDelete = makeOpenKeys(volume1, bucket1, 3);
    OpenKeyBucket v1b1KeysToKeep = makeOpenKeys(volume1, bucket1, 3);

    OpenKeyBucket v1b2KeysToDelete = makeOpenKeys(volume1, bucket2, 3);
    OpenKeyBucket v1b2KeysToKeep = makeOpenKeys(volume1, bucket2, 3);

    OpenKeyBucket v2b2KeysToDelete = makeOpenKeys(volume2, bucket2, 3);
    OpenKeyBucket v2b2KeysToKeep = makeOpenKeys(volume2, bucket2, 3);

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
    OpenKeyBucket keysToKeep =
        makeOpenKeys(volumeName, bucketName, keyName, 3);
    OpenKeyBucket keysToDelete =
        makeOpenKeys(volumeName, bucketName, keyName, 3);

    addToOpenKeyTable(keysToKeep, keysToDelete);
    deleteOpenKeys(keysToDelete);

    assertNotInOpenKeyTable(keysToDelete);
    assertInOpenKeyTable(keysToKeep);
  }

  /**
   * Tests metrics set by {@link OMOpenKeyDeleteRequest}.
   * Submits a set of keys for deletion where only some of the keys actually
   * exist in the open key table, and asserts that the metrics count keys
   * that were submitted for deletion versus those that were actually deleted.
   * @throws Exception
   */
  @Test
  public void testMetrics() throws Exception {
    final int numExistentKeys = 3;
    final int numNonExistentKeys = 5;

    OMMetrics metrics = ozoneManager.getMetrics();
    Assert.assertEquals(metrics.getNumOpenKeyDeleteRequests(), 0);
    Assert.assertEquals(metrics.getNumOpenKeyDeleteRequestFails(), 0);
    Assert.assertEquals(metrics.getNumOpenKeysSubmittedForDeletion(), 0);
    Assert.assertEquals(metrics.getNumOpenKeysDeleted(), 0);

    OpenKeyBucket existentKeys =
        makeOpenKeys(volumeName, bucketName, keyName, numExistentKeys);
    OpenKeyBucket nonExistentKeys =
        makeOpenKeys(volumeName, bucketName, keyName, numNonExistentKeys);

    addToOpenKeyTable(existentKeys);
    deleteOpenKeys(existentKeys, nonExistentKeys);

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
   * {@link OMOpenKeyDeleteRequest} to mark the keys in {@code openKeys}
   * as deleted in the open key table.
   * Asserts that the call's response status is {@link Status#OK}.
   * @throws Exception
   */
  private void deleteOpenKeys(OpenKeyBucket... openKeys) throws Exception {
    OMRequest omRequest =
        doPreExecute(createDeleteOpenKeyRequest(openKeys));

    OMOpenKeyDeleteRequest openKeyDeleteRequest =
        new OMOpenKeyDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        openKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  /**
   * Adds {@code openKeys} to the open key table, and asserts that they are
   * present after the addition.
   * @throws Exception
   */
  private void addToOpenKeyTable(OpenKeyBucket... openKeys)
      throws Exception {

    for (OpenKeyBucket openKeyBucket: openKeys) {
      String volume  = openKeyBucket.getVolumeName();
      String bucket  = openKeyBucket.getBucketName();

      for (OpenKey openKey: openKeyBucket.getKeysList()) {
        TestOMRequestUtils.addKeyToTable(true,
            volume, bucket, openKey.getName(), openKey.getClientID(),
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
  private OpenKeyBucket makeOpenKeys(String volume, String bucket,
      int numKeys) {

    OpenKeyBucket.Builder keysPerBucketBuilder =
        OpenKeyBucket.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket);

    for (int i = 0; i < numKeys; i++) {
      String keyName = UUID.randomUUID().toString();
      long clientID = new Random().nextLong();

      OpenKey openKey = OpenKey.newBuilder()
          .setName(keyName)
          .setClientID(clientID)
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
  private OpenKeyBucket makeOpenKeys(String volume, String bucket,
      String key, int numKeys) {

    OpenKeyBucket.Builder keysPerBucketBuilder =
        OpenKeyBucket.newBuilder()
            .setVolumeName(volume)
            .setBucketName(bucket);

    for (int i = 0; i < numKeys; i++) {
      long clientID = new Random().nextLong();

      OpenKey openKey = OpenKey.newBuilder()
          .setName(key)
          .setClientID(clientID)
          .build();
      keysPerBucketBuilder.addKeys(openKey);
    }

    return keysPerBucketBuilder.build();
  }

  private void assertInOpenKeyTable(OpenKeyBucket... openKeys)
      throws Exception {

    for (String keyName: getFullKeyNames(openKeys)) {
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(keyName));
    }
  }

  private void assertNotInOpenKeyTable(OpenKeyBucket... openKeys)
      throws Exception {

    for (String keyName: getFullKeyNames(openKeys)) {
      Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(keyName));
    }
  }

  private List<String> getFullKeyNames(OpenKeyBucket... openKeyBuckets) {
    List<String> fullKeyNames = new ArrayList<>();

    for(OpenKeyBucket keysPerBucket: openKeyBuckets) {
      String volume = keysPerBucket.getVolumeName();
      String bucket = keysPerBucket.getBucketName();

      for (OpenKey openKey: keysPerBucket.getKeysList()) {
        String fullName = omMetadataManager.getOpenKey(volume, bucket,
            openKey.getName(), openKey.getClientID());
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
  private OMRequest createDeleteOpenKeyRequest(OpenKeyBucket... keysToDelete) {
    DeleteOpenKeysRequest deleteOpenKeysRequest =
        DeleteOpenKeysRequest.newBuilder()
            .addAllOpenKeysPerBucket(Arrays.asList(keysToDelete))
            .build();

    return OMRequest.newBuilder()
        .setDeleteOpenKeysRequest(deleteOpenKeysRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}

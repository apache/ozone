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
   * Mixes keys to keep and delete among different volumes and buckets.
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
  }

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
    assertNotInOpenKeyTable(openKeys);
  }

  private void addToOpenKeyTable(List<OpenKey> openKeys) throws Exception {
    for (OpenKey openKey: openKeys) {
      TestOMRequestUtils.addKeyToTable(false,
          openKey.volume, openKey.bucket, openKey.key, openKey.clientID,
          replicationType, replicationFactor, omMetadataManager);
    }

    assertInOpenKeyTable(openKeys);
  }

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

  private List<DeletedKeys> gatherByBucket(List<OpenKey> openKeys) {
//    Map<String, Map<String, DeletedKeys>> deletedKeys = new HashMap<>();
//
//    for (OpenKey key: openKeys) {
//      Map<String, DeletedKeys>
//      if (deletedKeys.containsKey(key.volume)) {
//
//      }
//    }
    return null;
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
      return TestOMOpenKeyDeleteRequest.this.omMetadataManager.getOpenKey(volume,
          bucket, key, clientID);
    }
  }
}

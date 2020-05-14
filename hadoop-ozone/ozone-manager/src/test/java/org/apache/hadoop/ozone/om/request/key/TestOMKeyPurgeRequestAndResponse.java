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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

/**
 * Tests {@link OMKeyPurgeRequest} and {@link OMKeyPurgeResponse}.
 */
public class TestOMKeyPurgeRequestAndResponse extends TestOMKeyRequest {

  private int numKeys = 10;

  /**
   * Creates volume, bucket and key entries and adds to OM DB and then
   * deletes these keys to move them to deletedKeys table.
   */
  private List<String> createAndDeleteKeys(Integer trxnIndex, String bucket)
      throws Exception {
    if (bucket == null) {
      bucket = bucketName;
    }
    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucket,
        omMetadataManager);

    List<String> ozoneKeyNames = new ArrayList<>(numKeys);
    for (int i = 1; i <= numKeys; i++) {
      String key = keyName + "-" + i;
      TestOMRequestUtils.addKeyToTable(false, false, volumeName, bucket,
          key, clientID, replicationType, replication, trxnIndex++,
          omMetadataManager);
      ozoneKeyNames.add(omMetadataManager.getOzoneKey(
          volumeName, bucket, key));
    }

    List<String> deletedKeyNames = new ArrayList<>(numKeys);
    for (String ozoneKey : ozoneKeyNames) {
      String deletedKeyName = TestOMRequestUtils.deleteKey(
          ozoneKey, omMetadataManager, trxnIndex++);
      deletedKeyNames.add(deletedKeyName);
    }

    return deletedKeyNames;
  }

  /**
   * Create OMRequest which encapsulates DeleteKeyRequest.
   * @return OMRequest
   */
  private OMRequest createPurgeKeysRequest(List<String> deletedKeys) {
    DeletedKeys deletedKeysInBucket = DeletedKeys.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllKeys(deletedKeys)
        .build();
    PurgeKeysRequest purgeKeysRequest = PurgeKeysRequest.newBuilder()
        .addDeletedKeys(deletedKeysInBucket)
        .build();

    return OMRequest.newBuilder()
        .setPurgeKeysRequest(purgeKeysRequest)
        .setCmdType(Type.PurgeKeys)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  private OMRequest preExecute(OMRequest originalOmRequest) throws IOException {
    OMKeyPurgeRequest omKeyPurgeRequest =
        new OMKeyPurgeRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omKeyPurgeRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Create and Delete keys. The keys should be moved to DeletedKeys table
    List<String> deletedKeyNames = createAndDeleteKeys(1, null);

    // The keys should be present in the DeletedKeys table before purging
    for (String deletedKey : deletedKeyNames) {
      Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(
          deletedKey));
    }

    // Create PurgeKeysRequest to purge the deleted keys
    OMRequest omRequest = createPurgeKeysRequest(deletedKeyNames);

    OMRequest preExecutedRequest = preExecute(omRequest);
    OMKeyPurgeRequest omKeyPurgeRequest =
        new OMKeyPurgeRequest(preExecutedRequest);

    omKeyPurgeRequest.validateAndUpdateCache(ozoneManager, 100L,
        ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = OMResponse.newBuilder()
        .setPurgeKeysResponse(PurgeKeysResponse.getDefaultInstance())
        .setCmdType(Type.PurgeKeys)
        .setStatus(Status.OK)
        .build();

    BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation();

    OMKeyPurgeResponse omKeyPurgeResponse = new OMKeyPurgeResponse(
        omResponse, deletedKeyNames);
    omKeyPurgeResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // The keys should not exist in the DeletedKeys table
    for (String deletedKey : deletedKeyNames) {
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(
          deletedKey));
    }
  }

  @Test
  public void testPurgeKeysAcrossBuckets() throws Exception {
    String bucket1 = bucketName;
    String bucket2 = UUID.randomUUID().toString();

    // bucket1 is created during setup. Create bucket2 manually.
    TestOMRequestUtils.addBucketToDB(volumeName, bucket2, omMetadataManager);

    // Create and Delete keys in Bucket1 and Bucket2.
    List<String> deletedKeyInBucket1 = createAndDeleteKeys(1, bucket1);
    List<String> deletedKeyInBucket2 = createAndDeleteKeys(1, bucket2);
    List<String> deletedKeyNames = new ArrayList<>();
    deletedKeyNames.addAll(deletedKeyInBucket1);
    deletedKeyNames.addAll(deletedKeyInBucket2);

    // The keys should be present in the DeletedKeys table before purging
    for (String deletedKey : deletedKeyNames) {
      Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(
          deletedKey));
    }

    // Create PurgeKeysRequest to purge the deleted keys
    DeletedKeys deletedKeysInBucket1 = DeletedKeys.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucket1)
        .addAllKeys(deletedKeyInBucket1)
        .build();
    DeletedKeys deletedKeysInBucket2 = DeletedKeys.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucket2)
        .addAllKeys(deletedKeyInBucket1)
        .build();
    PurgeKeysRequest purgeKeysRequest = PurgeKeysRequest.newBuilder()
        .addDeletedKeys(deletedKeysInBucket1)
        .addDeletedKeys(deletedKeysInBucket2)
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setPurgeKeysRequest(purgeKeysRequest)
        .setCmdType(Type.PurgeKeys)
        .setClientId(UUID.randomUUID().toString())
        .build();

    OMRequest preExecutedRequest = preExecute(omRequest);
    OMKeyPurgeRequest omKeyPurgeRequest =
        new OMKeyPurgeRequest(preExecutedRequest);

    omKeyPurgeRequest.validateAndUpdateCache(ozoneManager, 100L,
        ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = OMResponse.newBuilder()
        .setPurgeKeysResponse(PurgeKeysResponse.getDefaultInstance())
        .setCmdType(Type.PurgeKeys)
        .setStatus(Status.OK)
        .build();

    BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation();

    OMKeyPurgeResponse omKeyPurgeResponse = new OMKeyPurgeResponse(
        omResponse, deletedKeyNames);
    omKeyPurgeResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // The keys should not exist in the DeletedKeys table
    for (String deletedKey : deletedKeyNames) {
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(
          deletedKey));
    }
  }

  @Test
  public void testReplayRequest() throws Exception {

    // Create and Delete keys. The keys should be moved to DeletedKeys table
    Integer trxnLogIndex = new Integer(1);
    List<String> deletedKeyNames = createAndDeleteKeys(trxnLogIndex, null);
    int purgeRequestTrxnLogIndex = ++trxnLogIndex;

    // The keys should be present in the DeletedKeys table before purging
    for (String deletedKey : deletedKeyNames) {
      Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(
          deletedKey));
    }

    // Execute PurgeKeys request to purge the keys from Deleted table.
    // Create PurgeKeysRequest to replay the purge request
    OMRequest omRequest = createPurgeKeysRequest(deletedKeyNames);
    OMRequest preExecutedRequest = preExecute(omRequest);
    OMKeyPurgeRequest omKeyPurgeRequest =
        new OMKeyPurgeRequest(preExecutedRequest);
    OMClientResponse omClientResponse = omKeyPurgeRequest
        .validateAndUpdateCache(ozoneManager, purgeRequestTrxnLogIndex,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus().equals(
        Status.OK));

    // Create and delete the same keys again
    createAndDeleteKeys(++trxnLogIndex, null);

    // Replay the PurgeKeys request. It should not purge the keys deleted
    // after the original request was played.
    OMClientResponse replayResponse = omKeyPurgeRequest
        .validateAndUpdateCache(ozoneManager, purgeRequestTrxnLogIndex,
            ozoneManagerDoubleBufferHelper);

    // Verify that the new deletedKeys exist in the DeletedKeys table
    for (String deletedKey : deletedKeyNames) {
      Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(
          deletedKey));
    }
  }
}

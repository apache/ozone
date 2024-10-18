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

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket,
        omMetadataManager);

    List<String> ozoneKeyNames = new ArrayList<>(numKeys);
    for (int i = 1; i <= numKeys; i++) {
      String key = keyName + "-" + i;
      OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucket,
          key, clientID, replicationConfig, trxnIndex++,
          omMetadataManager);
      ozoneKeyNames.add(omMetadataManager.getOzoneKey(
          volumeName, bucket, key));
    }

    List<String> deletedKeyNames = new ArrayList<>(numKeys);
    for (String ozoneKey : ozoneKeyNames) {
      String deletedKeyName = OMRequestTestUtils.deleteKey(
          ozoneKey, omMetadataManager, trxnIndex++);
      deletedKeyNames.add(deletedKeyName);
    }

    return deletedKeyNames;
  }

  /**
   * Create OMRequest which encapsulates DeleteKeyRequest.
   * @return OMRequest
   */
  private OMRequest createPurgeKeysRequest(List<String> deletedKeys,
       String snapshotDbKey) {
    DeletedKeys deletedKeysInBucket = DeletedKeys.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllKeys(deletedKeys)
        .build();
    PurgeKeysRequest.Builder purgeKeysRequest = PurgeKeysRequest.newBuilder()
        .addDeletedKeys(deletedKeysInBucket);

    if (snapshotDbKey != null) {
      purgeKeysRequest.setSnapshotTableKey(snapshotDbKey);
    }
    purgeKeysRequest.build();

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
    assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Create and Delete keys. The keys should be moved to DeletedKeys table
    List<String> deletedKeyNames = createAndDeleteKeys(1, null);

    // The keys should be present in the DeletedKeys table before purging
    for (String deletedKey : deletedKeyNames) {
      assertTrue(omMetadataManager.getDeletedTable().isExist(
          deletedKey));
    }

    // Create PurgeKeysRequest to purge the deleted keys
    OMRequest omRequest = createPurgeKeysRequest(deletedKeyNames, null);

    OMRequest preExecutedRequest = preExecute(omRequest);
    OMKeyPurgeRequest omKeyPurgeRequest =
        new OMKeyPurgeRequest(preExecutedRequest);

    omKeyPurgeRequest.validateAndUpdateCache(ozoneManager, 100L);

    OMResponse omResponse = OMResponse.newBuilder()
        .setPurgeKeysResponse(PurgeKeysResponse.getDefaultInstance())
        .setCmdType(Type.PurgeKeys)
        .setStatus(Status.OK)
        .build();

    try (BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation()) {

      OMKeyPurgeResponse omKeyPurgeResponse = new OMKeyPurgeResponse(
          omResponse, deletedKeyNames, null, null);
      omKeyPurgeResponse.addToDBBatch(omMetadataManager, batchOperation);

      // Do manual commit and see whether addToBatch is successful or not.
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }

    // The keys should not exist in the DeletedKeys table
    for (String deletedKey : deletedKeyNames) {
      assertFalse(omMetadataManager.getDeletedTable().isExist(deletedKey));
    }
  }

  @Test
  public void testKeyPurgeInSnapshot() throws Exception {
    // Create and Delete keys. The keys should be moved to DeletedKeys table
    List<String> deletedKeyNames = createAndDeleteKeys(1, null);

    SnapshotInfo snapInfo = createSnapshot("snap1");
    assertEquals(snapInfo.getLastTransactionInfo(),
        TransactionInfo.valueOf(TransactionInfo.getTermIndex(1L)).toByteString());
    // The keys should be not present in the active Db's deletedTable
    for (String deletedKey : deletedKeyNames) {
      assertFalse(omMetadataManager.getDeletedTable().isExist(deletedKey));
    }

    ReferenceCounted<OmSnapshot> rcOmSnapshot = ozoneManager.getOmSnapshotManager()
        .getSnapshot(snapInfo.getVolumeName(), snapInfo.getBucketName(), snapInfo.getName());
    OmSnapshot omSnapshot = rcOmSnapshot.get();

    // The keys should be present in the snapshot's deletedTable
    for (String deletedKey : deletedKeyNames) {
      assertTrue(omSnapshot.getMetadataManager()
          .getDeletedTable().isExist(deletedKey));
    }

    // Create PurgeKeysRequest to purge the deleted keys
    OMRequest omRequest = createPurgeKeysRequest(deletedKeyNames,
        snapInfo.getTableKey());

    OMRequest preExecutedRequest = preExecute(omRequest);
    OMKeyPurgeRequest omKeyPurgeRequest =
        new OMKeyPurgeRequest(preExecutedRequest);

    omKeyPurgeRequest.validateAndUpdateCache(ozoneManager, 100L);

    SnapshotInfo snapshotInfoOnDisk = omMetadataManager.getSnapshotInfoTable().getSkipCache(snapInfo.getTableKey());
    SnapshotInfo updatedSnapshotInfo = omMetadataManager.getSnapshotInfoTable().get(snapInfo.getTableKey());
    assertEquals(snapshotInfoOnDisk, snapInfo);
    snapInfo.setLastTransactionInfo(TransactionInfo.valueOf(TransactionInfo.getTermIndex(100L))
        .toByteString());
    assertEquals(snapInfo, updatedSnapshotInfo);
    OMResponse omResponse = OMResponse.newBuilder()
        .setPurgeKeysResponse(PurgeKeysResponse.getDefaultInstance())
        .setCmdType(Type.PurgeKeys)
        .setStatus(Status.OK)
        .build();

    try (BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation()) {

      OMKeyPurgeResponse omKeyPurgeResponse = new OMKeyPurgeResponse(omResponse, deletedKeyNames, snapInfo, null);
      omKeyPurgeResponse.addToDBBatch(omMetadataManager, batchOperation);

      // Do manual commit and see whether addToBatch is successful or not.
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
    snapshotInfoOnDisk = omMetadataManager.getSnapshotInfoTable().getSkipCache(snapInfo.getTableKey());
    assertEquals(snapshotInfoOnDisk, snapInfo);
    // The keys should not exist in the DeletedKeys table
    for (String deletedKey : deletedKeyNames) {
      assertFalse(omSnapshot.getMetadataManager()
          .getDeletedTable().isExist(deletedKey));
    }

    omSnapshot = null;
    rcOmSnapshot.close();
  }
}

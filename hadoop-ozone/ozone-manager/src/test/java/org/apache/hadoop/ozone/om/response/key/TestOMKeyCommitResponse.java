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

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Tests OMKeyCommitResponse.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMKeyCommitResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setCreationTime(Time.now()).build();

    OmKeyInfo omKeyInfo = getOmKeyInfo();

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCommitKeyResponse(
            OzoneManagerProtocolProtos.CommitKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
            .build();

    // As during commit Key, entry will be already there in openKeyTable.
    // Adding it here.
    addKeyToOpenKeyTable();

    String openKey = getOpenKeyName();
    Assert.assertTrue(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

    String ozoneKey = getOzoneKey();
    OMKeyCommitResponse omKeyCommitResponse = getOmKeyCommitResponse(
            omKeyInfo, omResponse, openKey, ozoneKey, keysToDelete);

    omKeyCommitResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // When key commit key is deleted from openKey table and added to keyTable.
    Assert.assertFalse(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
    Assert.assertTrue(omMetadataManager.getKeyTable().isExist(ozoneKey));
  }

  @Test
  public void testAddToDBBatchNoOp() throws Exception {

    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor);
    omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setCreationTime(Time.now()).build();

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCommitKeyResponse(
            OzoneManagerProtocolProtos.CommitKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
            .build();

    String openKey = getOpenKeyName();
    String ozoneKey = getOzoneKey();

    OMKeyCommitResponse omKeyCommitResponse = getOmKeyCommitResponse(
            omKeyInfo, omResponse, openKey, ozoneKey, null);

    // As during commit Key, entry will be already there in openKeyTable.
    // Adding it here.
    addKeyToOpenKeyTable();

    Assert.assertTrue(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

    omKeyCommitResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op. So, entry should still be in
    // openKey table.
    Assert.assertTrue(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
    Assert.assertFalse(omMetadataManager.getKeyTable().isExist(ozoneKey));
  }

  @Test
  public void testAddToDBBatchOnOverwrite() throws Exception {
    omBucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .setCreationTime(Time.now()).build();
    OmKeyInfo omKeyInfo = getOmKeyInfo();
    keysToDelete =
            OmUtils.prepareKeyForDeleteWithoutUpdateID(omKeyInfo, null);
    Assert.assertNotNull(keysToDelete);
    testAddToDBBatch();

    RepeatedOmKeyInfo keysInDeleteTable =
            omMetadataManager.getDeletedTable().get(getOzoneKey());
    Assert.assertNotNull(keysInDeleteTable);
    Assert.assertEquals(1, keysInDeleteTable.getOmKeyInfoList().size());

  }

  @NotNull
  protected void addKeyToOpenKeyTable() throws Exception {
    TestOMRequestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
            clientID, replicationType, replicationFactor, omMetadataManager);
  }

  @NotNull
  protected String getOzoneKey() {
    Assert.assertNotNull(omBucketInfo);
    return omMetadataManager.getOzoneKey(volumeName,
            omBucketInfo.getBucketName(), keyName);
  }

  @NotNull
  protected OMKeyCommitResponse getOmKeyCommitResponse(OmKeyInfo omKeyInfo,
          OzoneManagerProtocolProtos.OMResponse omResponse, String openKey,
          String ozoneKey, RepeatedOmKeyInfo deleteKeys) {
    Assert.assertNotNull(omBucketInfo);
    return new OMKeyCommitResponse(omResponse, omKeyInfo, ozoneKey, openKey,
            omBucketInfo, deleteKeys);
  }
}

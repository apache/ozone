/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests OMKeyCommitResponse.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMKeyCommitResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

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
    assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

    String ozoneKey = getOzoneKey();
    OMKeyCommitResponse omKeyCommitResponse = getOmKeyCommitResponse(
            omKeyInfo, omResponse, openKey, ozoneKey, keysToDelete, false, null);

    omKeyCommitResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // When key commit key is deleted from openKey table and added to keyTable.
    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
    assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
  }

  @Test
  public void testAddToDBBatchNoOp() throws Exception {

    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationConfig).build();

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCommitKeyResponse(
            OzoneManagerProtocolProtos.CommitKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
            .build();

    String openKey = getOpenKeyName();
    String ozoneKey = getOzoneKey();

    OMKeyCommitResponse omKeyCommitResponse = getOmKeyCommitResponse(
            omKeyInfo, omResponse, openKey, ozoneKey, null, false, null);

    // As during commit Key, entry will be already there in openKeyTable.
    // Adding it here.
    addKeyToOpenKeyTable();

    assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

    omKeyCommitResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);


    // As omResponse is error it is a no-op. So, entry should still be in
    // openKey table.
    assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
    assertFalse(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
  }

  @Test
  public void testAddToDBBatchOnOverwrite() throws Exception {
    OmKeyInfo omKeyInfo = getOmKeyInfo();
    keysToDelete =
            OmUtils.prepareKeyForDelete(omBucketInfo.getObjectID(), omKeyInfo, 100);
    assertNotNull(keysToDelete);
    testAddToDBBatch();

    String deletedKey = omMetadataManager.getOzoneKey(volumeName,
        omBucketInfo.getBucketName(), keyName);
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(
        null, 100, deletedKey);
    assertThat(rangeKVs.size()).isGreaterThan(0);
    assertEquals(1, rangeKVs.get(0).getValue().getOmKeyInfoList().size());

  }

  @Nonnull
  protected void addKeyToOpenKeyTable() throws Exception {
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
        clientID, replicationConfig, omMetadataManager);
  }

  @Nonnull
  protected String getOzoneKey() throws IOException {
    assertNotNull(omBucketInfo);
    return omMetadataManager.getOzoneKey(volumeName,
            omBucketInfo.getBucketName(), keyName);
  }

  @Nonnull
  protected OMKeyCommitResponse getOmKeyCommitResponse(OmKeyInfo omKeyInfo,
          OzoneManagerProtocolProtos.OMResponse omResponse, String openKey,
          String ozoneKey, RepeatedOmKeyInfo deleteKeys, Boolean isHSync, OmKeyInfo newOpenKeyInfo)
          throws IOException {
    assertNotNull(omBucketInfo);
    Map<String, RepeatedOmKeyInfo> deleteKeyMap = new HashMap<>();
    if (null != deleteKeys) {
      deleteKeys.getOmKeyInfoList().stream().forEach(e -> deleteKeyMap.put(
          omMetadataManager.getOzoneDeletePathKey(e.getObjectID(), ozoneKey),
          new RepeatedOmKeyInfo(e, omBucketInfo.getObjectID())));
    }
    return new OMKeyCommitResponse(omResponse, omKeyInfo, ozoneKey, openKey,
        omBucketInfo, deleteKeyMap, isHSync, newOpenKeyInfo, null, null);
  }
}

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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests OMKeyDeleteResponse - prefix layout.
 */
public class TestOMKeyDeleteResponseWithFSO extends TestOMKeyDeleteResponse {

  @Override
  protected OMKeyDeleteResponse getOmKeyDeleteResponse(OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse) throws Exception {
    return new OMKeyDeleteResponseWithFSO(omResponse, omKeyInfo.getKeyName(),
        omKeyInfo, getOmBucketInfo(), false,
        omMetadataManager.getVolumeId(volumeName), null);
  }

  /**
   * HDDS-16768: the DELETED_HSYNC_KEY marker has to be persisted under the openFileTable key the request flagged
   * in the table cache, not under the openKeyTable key format.
   */
  @Test
  public void testAddToDBBatchMarksHsyncOpenKey() throws Exception {
    String ozoneKey = addKeyToTable();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    String hsyncClientId = String.valueOf(clientID);

    // the hsync writer's open key, stored next to the committed file under the openFileTable key
    OmKeyInfo openKeyInfo = omKeyInfo.toBuilder().addMetadata(OzoneConsts.HSYNC_CLIENT_ID, hsyncClientId).build();
    String dbOpenKey = OMRequestTestUtils.addFileToKeyTable(true, false, keyName, openKeyInfo, clientID, 50L,
        omMetadataManager);
    OmKeyInfo deletedOpenKeyInfo = openKeyInfo.withMetadataMutations(
        metadata -> metadata.put(OzoneConsts.DELETED_HSYNC_KEY, "true"));

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteKeyResponse(
            OzoneManagerProtocolProtos.DeleteKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
            .build();
    OMKeyDeleteResponse omKeyDeleteResponse = new OMKeyDeleteResponseWithFSO(omResponse, keyName, omKeyInfo,
        getOmBucketInfo(), false, omMetadataManager.getVolumeId(volumeName), deletedOpenKeyInfo);
    omKeyDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertFalse(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
    // the marker lands on the row the request flagged in the cache ...
    OmKeyInfo markedOpenKey = omMetadataManager.getOpenKeyTable(getBucketLayout()).getSkipCache(dbOpenKey);
    assertNotNull(markedOpenKey);
    assertEquals("true", markedOpenKey.getMetadata().get(OzoneConsts.DELETED_HSYNC_KEY));
    // ... and no stray row is written under the openKeyTable key format
    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(
        omMetadataManager.getOpenKey(volumeName, bucketName, keyName, hsyncClientId)));
  }

  @Override
  protected String addKeyToTable() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
            bucketName, "", omMetadataManager);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1)
            .setParentObjectID(parentId)
            .setUpdateID(100L)
            .build();
    OMRequestTestUtils.addFileToKeyTable(false, false,
            keyName, omKeyInfo, -1, 50, omMetadataManager);
    return omMetadataManager.getOzonePathKey(
            omMetadataManager.getVolumeId(volumeName),
            omMetadataManager.getBucketId(volumeName, bucketName),
           omKeyInfo.getParentObjectID(), keyName);
  }

  @Override
  protected OmKeyInfo getOmKeyInfo() {
    assertNotNull(getOmBucketInfo());
    return OMRequestTestUtils.createOmKeyInfo(volumeName, omBucketInfo.getBucketName(), keyName,
            replicationConfig)
        .setObjectID(getOmBucketInfo().getObjectID() + 1)
        .setParentObjectID(getOmBucketInfo().getObjectID())
        .setUpdateID(100L)
        .build();
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}

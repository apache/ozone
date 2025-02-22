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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Tests OMKeyRenameResponse.
 */
public class TestOMKeysRenameResponse extends TestOMKeyResponse {
  private OmRenameKeys omRenameKeys;
  private int count = 10;
  private String parentDir = "/test";

  @Test
  public void testKeysRenameResponse() throws Exception {

    createPreRequisities();

    OMResponse omResponse = OMResponse.newBuilder()
        .setRenameKeysResponse(RenameKeysResponse.getDefaultInstance())
        .setStatus(Status.OK).setCmdType(Type.RenameKeys).build();

    OMKeysRenameResponse omKeysRenameResponse = new OMKeysRenameResponse(
        omResponse, omRenameKeys);

    omKeysRenameResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    for (int i = 0; i < count; i++) {
      String key = parentDir.concat("/key" + i);
      String toKey = parentDir.concat("/newKey" + i);
      key = omMetadataManager.getOzoneKey(volumeName, bucketName, key);
      toKey = omMetadataManager.getOzoneKey(volumeName, bucketName, toKey);
      assertFalse(omMetadataManager.getKeyTable(getBucketLayout()).isExist(key));
      assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).isExist(toKey));
    }
  }

  @Test
  public void testKeysRenameResponseFail() throws Exception {

    createPreRequisities();

    OMResponse omResponse = OMResponse.newBuilder().setRenameKeysResponse(
        RenameKeysResponse.getDefaultInstance())
        .setStatus(Status.KEY_NOT_FOUND)
        .setCmdType(Type.RenameKeys)
        .build();

    OMKeysRenameResponse omKeyRenameResponse = new OMKeysRenameResponse(
        omResponse, omRenameKeys);

    omKeyRenameResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    for (int i = 0; i < count; i++) {
      String key = parentDir.concat("/key" + i);
      String toKey = parentDir.concat("/newKey" + i);
      key = omMetadataManager.getOzoneKey(volumeName, bucketName, key);
      toKey = omMetadataManager.getOzoneKey(volumeName, bucketName, toKey);
      // As omResponse has error, it is a no-op. So, no changes should happen.
      assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).isExist(key));
      assertFalse(omMetadataManager.getKeyTable(getBucketLayout()).isExist(toKey));
    }

  }

  private void createPreRequisities() throws Exception {

    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    Map<String, OmKeyInfo> formAndToKeyInfo = new HashMap<>();

    for (int i = 0; i < count; i++) {
      String key = parentDir.concat("/key" + i);
      String toKey = parentDir.concat("/newKey" + i);
      OMRequestTestUtils.addKeyToTable(false, volumeName,
          bucketName, parentDir.concat("/key" + i), 0L,
          RatisReplicationConfig.getInstance(THREE),
          omMetadataManager);

      OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName, key));
      omKeyInfo.setKeyName(toKey);
      formAndToKeyInfo.put(key, omKeyInfo);
    }
    omRenameKeys =
        new OmRenameKeys(volumeName, bucketName, null, formAndToKeyInfo);

  }
}

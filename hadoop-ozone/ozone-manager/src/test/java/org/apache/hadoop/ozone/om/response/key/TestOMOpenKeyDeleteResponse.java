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

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Tests OMOpenKeyDeleteResponse.
 */
public class TestOMOpenKeyDeleteResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatchWithEmptyBlocks() throws Exception {
    Map<String, OmKeyInfo> keysToDelete = createOpenKeys(3, false);
    Map<String, OmKeyInfo> keysToKeep = createOpenKeys(3, false);
    createAndCommitResponse(keysToDelete, Status.OK);

    for (String key: keysToDelete.keySet()) {
      Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(key));

      // As default key entry does not have any blocks, it should not be in
      // deletedKeyTable.
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }

    for (String key: keysToKeep.keySet()) {
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }

  @Test
  public void testAddToDBBatchWithNonEmptyBlocks() throws Exception {
    Map<String, OmKeyInfo> keysToDelete = createOpenKeys(3, true);
    Map<String, OmKeyInfo> keysToKeep = createOpenKeys(3, true);
    createAndCommitResponse(keysToDelete, Status.OK);

    for (String key: keysToDelete.keySet()) {
      Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(key));
    }

    for (String key: keysToKeep.keySet()) {
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }


  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    Map<String, OmKeyInfo> keysToDelete = createOpenKeys(3, true);
    createAndCommitResponse(keysToDelete, Status.INTERNAL_ERROR);

    for (String key: keysToDelete.keySet()) {
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(key));
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(key));
    }
  }

  private void createAndCommitResponse(Map<String, OmKeyInfo> keysToDelete,
      Status status) throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setStatus(status)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .build();

    OMOpenKeyDeleteResponse response = new OMOpenKeyDeleteResponse(omResponse,
        keysToDelete, true);

    // Operations are only added to the batch by this method when status is OK.
    response.checkAndUpdateDB(omMetadataManager, batchOperation);

    // If status is not OK, this will do nothing.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  private Map<String, OmKeyInfo> createOpenKeys(int numKeys, boolean addBlocks)
      throws Exception {

    Map<String, OmKeyInfo> newOpenKeys = new HashMap<>();

    for (int i = 0; i < numKeys; i++) {
      String volume = UUID.randomUUID().toString();
      String bucket = UUID.randomUUID().toString();
      String key = UUID.randomUUID().toString();
      long clientID = new Random().nextLong();

      OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volume,
          bucket, key, replicationType, replicationFactor);
      String openKey = omMetadataManager.getOpenKey(volume, bucket,
          key, clientID);

      if (addBlocks) {
        addBlocksTo(omKeyInfo);
      }

      TestOMRequestUtils.addKeyToTable(true, volume, bucket, key,
          clientID, replicationType, replicationFactor, omMetadataManager);
      Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(openKey));

      newOpenKeys.put(openKey, omKeyInfo);
    }

    return newOpenKeys;
  }

  private void addBlocksTo(OmKeyInfo keyInfo) throws Exception {
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setType(replicationType)
        .setFactor(replicationFactor)
        .setNodes(new ArrayList<>())
        .build();

    OmKeyLocationInfo omKeyLocationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(100L, 1000L))
        .setOffset(0)
        .setLength(100L)
        .setPipeline(pipeline)
        .build();

    omKeyLocationInfoList.add(omKeyLocationInfo);

    keyInfo.appendNewBlocks(omKeyLocationInfoList, false);
  }
}

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests OMKeyDeleteResponse.
 */
public class TestOMKeyDeleteResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {
    String ozoneKey = addKeyToTable();
    OmKeyInfo omKeyInfo = omMetadataManager
            .getKeyTable(getBucketLayout()).get(ozoneKey);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteKeyResponse(
            OzoneManagerProtocolProtos.DeleteKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
            .build();

    OMKeyDeleteResponse omKeyDeleteResponse = getOmKeyDeleteResponse(omKeyInfo,
            omResponse);

    assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
    omKeyDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertFalse(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));

    String deletedKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    // As default key entry does not have any blocks, it should not be in
    // deletedKeyTable.
    assertFalse(omMetadataManager.getDeletedTable().isExist(deletedKey));
  }

  @Test
  public void testAddToDBBatchWithNonEmptyBlocks() throws Exception {
    final String ozoneKey = addKeyToTable();
    final OmKeyInfo omKeyInfo = omMetadataManager
            .getKeyTable(getBucketLayout())
            .get(ozoneKey);

    // Add block to key.
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(replicationConfig)
        .setNodes(new ArrayList<>())
        .build();

    OmKeyLocationInfo omKeyLocationInfo =
        new OmKeyLocationInfo.Builder().setBlockID(
            new BlockID(100L, 1000L))
            .setOffset(0).setLength(100L).setPipeline(pipeline).build();


    omKeyLocationInfoList.add(omKeyLocationInfo);

    omKeyInfo.appendNewBlocks(omKeyLocationInfoList, false);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteKeyResponse(
            OzoneManagerProtocolProtos.DeleteKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
            .build();

    OMKeyDeleteResponse omKeyDeleteResponse = getOmKeyDeleteResponse(omKeyInfo,
            omResponse);

    assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
    omKeyDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    assertFalse(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
    
    String deletedKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(
        null, 100, deletedKey);

    // Key has blocks, it should not be in deletedKeyTable.
    assertThat(rangeKVs.size()).isGreaterThan(0);
    for (Table.KeyValue<String, RepeatedOmKeyInfo> kv : rangeKVs) {
      assertTrue(kv.getValue().getOmKeyInfoList().get(0).isDeletedKeyCommitted());
    }
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OmKeyInfo omKeyInfo = getOmKeyInfo();

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteKeyResponse(
            OzoneManagerProtocolProtos.DeleteKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
            .build();

    OMKeyDeleteResponse omKeyDeleteResponse = getOmKeyDeleteResponse(omKeyInfo,
            omResponse);

    String ozoneKey = addKeyToTable();

    assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));

    omKeyDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op. So, entry should be still in the
    // keyTable.
    assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));

  }

  protected String addKeyToTable() throws Exception {
    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName);

    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationConfig, omMetadataManager);
    return ozoneKey;
  }

  protected OMKeyDeleteResponse getOmKeyDeleteResponse(OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse) throws Exception {
    return new OMKeyDeleteResponse(omResponse, omKeyInfo, omBucketInfo, null);
  }

  protected OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }
}

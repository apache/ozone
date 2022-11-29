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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

import org.apache.hadoop.ozone.om.DeleteTablePrefix;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests OMKeyDeleteResponse.
 */
public class TestOMKeyDeleteResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {
    String ozoneKey = addKeyToTable();
    OmKeyInfo omKeyInfo = omMetadataManager
            .getKeyTable(getBucketLayout()).get(ozoneKey);

    long trxnLogIndex = 0x360L;

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteKeyResponse(
            OzoneManagerProtocolProtos.DeleteKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
            .build();

    DeleteTablePrefix prefix = new DeleteTablePrefix(trxnLogIndex, true);

    OMKeyDeleteResponse omKeyDeleteResponse = getOmKeyDeleteResponse(omKeyInfo,
            omResponse, prefix);

    Assert.assertTrue(
        omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
    omKeyDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertFalse(
        omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));

    // As default key entry does not have any blocks, it should not be in
    // deletedKeyTable.
    String deleteKey = prefix.buildKey(omKeyInfo);
    Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(deleteKey));
  }

  @Test
  public void testAddToDBBatchWithNonEmptyBlocks() throws Exception {

    final String ozoneKey = addKeyToTable();
    final OmKeyInfo omKeyInfo = omMetadataManager
            .getKeyTable(getBucketLayout())
            .get(ozoneKey);

    long trxnLogIndex = 0x360L;

    // Add block to key.
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(RatisReplicationConfig
            .getInstance(replicationFactor))
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

    DeleteTablePrefix prefix = new DeleteTablePrefix(trxnLogIndex, true);
    OMKeyDeleteResponse omKeyDeleteResponse = getOmKeyDeleteResponse(omKeyInfo,
            omResponse, prefix);

    Assert.assertTrue(
        omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
    omKeyDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertFalse(
        omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));

    // Key has blocks, it should be in deletedKeyTable.
    String deleteKey = prefix.buildKey(omKeyInfo);
    Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(deleteKey));
  }


  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {

    long trxnLogIndex = 0x360L;
    omBucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .build();

    OmKeyInfo omKeyInfo = getOmKeyInfo();

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteKeyResponse(
            OzoneManagerProtocolProtos.DeleteKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
            .build();

    DeleteTablePrefix prefix = new DeleteTablePrefix(trxnLogIndex, true);
    OMKeyDeleteResponse omKeyDeleteResponse = getOmKeyDeleteResponse(omKeyInfo,
            omResponse, prefix);

    String ozoneKey = addKeyToTable();

    Assert.assertTrue(
        omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));

    omKeyDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op. So, entry should be still in the
    // keyTable.
    Assert.assertTrue(
        omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozoneKey));
    String deleteKey = prefix.buildKey(omKeyInfo);
    Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(deleteKey));

  }

  protected String addKeyToTable() throws Exception {
    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName);

    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
            clientID, replicationType, replicationFactor, omMetadataManager);
    return ozoneKey;
  }

  protected OMKeyDeleteResponse getOmKeyDeleteResponse(OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse,
      DeleteTablePrefix deleteTablePrefix) throws Exception {
    return new OMKeyDeleteResponse(omResponse, deleteTablePrefix,
        Arrays.asList(omKeyInfo), omBucketInfo);
  }

  protected OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }
}

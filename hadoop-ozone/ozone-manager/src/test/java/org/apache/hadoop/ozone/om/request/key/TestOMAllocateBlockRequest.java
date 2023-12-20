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


import java.util.List;
import java.util.UUID;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Tests OMAllocateBlockRequest class.
 */
public class TestOMAllocateBlockRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {

    doPreExecute(createAllocateBlockRequest());

  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Add volume, bucket, key entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    addKeyToOpenKeyTable(volumeName, bucketName);

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);

    // Check before calling validateAndUpdateCache. As adding DB entry has
    // not added any blocks, so size should be zero.

    OmKeyInfo omKeyInfo = verifyPathInOpenKeyTable(keyName, clientID,
            true);

    List<OmKeyLocationInfo> omKeyLocationInfo =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    Assertions.assertEquals(0, omKeyLocationInfo.size());

    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omAllocateBlockResponse.getOMResponse().getStatus());

    // Check open table whether new block is added or not.

    omKeyInfo = verifyPathInOpenKeyTable(keyName, clientID,
            true);

    // Check modification time
    Assertions.assertEquals(modifiedOmRequest.getAllocateBlockRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    // creationTime was assigned at OMRequestTestUtils.addKeyToTable
    // modificationTime was assigned at
    // doPreExecute(createAllocateBlockRequest())
    Assertions.assertTrue(
        omKeyInfo.getCreationTime() <= omKeyInfo.getModificationTime());

    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getAllocateBlockRequest().getKeyLocation();

    omKeyLocationInfo =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    Assertions.assertEquals(1, omKeyLocationInfo.size());

    Assertions.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.get(0).getContainerID());

    Assertions.assertEquals(keyLocation.getBlockID().getContainerBlockID()
            .getLocalID(), omKeyLocationInfo.get(0).getLocalID());

  }

  @NotNull
  protected OMAllocateBlockRequest getOmAllocateBlockRequest(
          OMRequest modifiedOmRequest) {
    return new OMAllocateBlockRequest(modifiedOmRequest, BucketLayout.DEFAULT);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);


    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omAllocateBlockResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);


    // Added only volume to DB.
    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);

    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omAllocateBlockResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateBlockRequest());

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(modifiedOmRequest);

    // Add volume, bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omAllocateBlockRequest.getBucketLayout());


    OMClientResponse omAllocateBlockResponse =
        omAllocateBlockRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omAllocateBlockResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND);

  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  protected OMRequest doPreExecute(OMRequest originalOMRequest)
      throws Exception {

    OMAllocateBlockRequest omAllocateBlockRequest =
            getOmAllocateBlockRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        omAllocateBlockRequest.preExecute(ozoneManager);


    Assertions.assertEquals(originalOMRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    Assertions.assertEquals(originalOMRequest.getClientId(),
        modifiedOmRequest.getClientId());

    Assertions.assertTrue(modifiedOmRequest.hasAllocateBlockRequest());
    AllocateBlockRequest allocateBlockRequest =
        modifiedOmRequest.getAllocateBlockRequest();
    // Time should be set
    Assertions.assertTrue(allocateBlockRequest.getKeyArgs()
        .getModificationTime() > 0);

    // KeyLocation should be set.
    Assertions.assertTrue(allocateBlockRequest.hasKeyLocation());
    Assertions.assertEquals(CONTAINER_ID,
        allocateBlockRequest.getKeyLocation().getBlockID()
            .getContainerBlockID().getContainerID());
    Assertions.assertEquals(LOCAL_ID,
        allocateBlockRequest.getKeyLocation().getBlockID()
            .getContainerBlockID().getLocalID());
    Assertions.assertTrue(allocateBlockRequest.getKeyLocation().hasPipeline());

    Assertions.assertEquals(allocateBlockRequest.getClientID(),
        allocateBlockRequest.getClientID());

    return modifiedOmRequest;
  }


  protected OMRequest createAllocateBlockRequest() {

    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName)
        .setFactor(replicationFactor).setType(replicationType)
        .build();

    AllocateBlockRequest allocateBlockRequest =
        AllocateBlockRequest.newBuilder().setClientID(clientID)
            .setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .setClientId(UUID.randomUUID().toString())
        .setAllocateBlockRequest(allocateBlockRequest).build();

  }

  protected String addKeyToOpenKeyTable(String volumeName, String bucketName)
          throws Exception {
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName,
            keyName, clientID, replicationType, replicationFactor,
            omMetadataManager);
    return "";
  }
}

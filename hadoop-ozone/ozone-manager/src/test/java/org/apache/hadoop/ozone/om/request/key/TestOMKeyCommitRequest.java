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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Class tests OMKeyCommitRequest class.
 */
public class TestOMKeyCommitRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createCommitKeyRequest());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
        new OMKeyCommitRequest(modifiedOmRequest);

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
        clientID, replicationType, replication, omMetadataManager);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
        100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    omKeyInfo = omMetadataManager.getOpenKeyTable().get(ozoneKey);
    Assert.assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNotNull(omKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assert.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    Assert.assertEquals(locationInfoListFromCommitKeyRequest,
        omKeyInfo.getLatestVersionLocations().getLocationList());

  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
        new OMKeyCommitRequest(modifiedOmRequest);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
        new OMKeyCommitRequest(modifiedOmRequest);


    TestOMRequestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);
    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
        new OMKeyCommitRequest(modifiedOmRequest);


    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testReplayRequest() throws Exception {

    // Manually add Volume, Bucket to DB
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    // Manually add Key to OpenKey table in DB
    TestOMRequestUtils.addKeyToTable(true, false, volumeName, bucketName,
        keyName, clientID, replicationType, replication, 1L,
        omMetadataManager);

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest = new OMKeyCommitRequest(
        modifiedOmRequest);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);
    Assert.assertNull(omKeyInfo);

    // Execute original KeyCommit request
    omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 10L,
        ozoneManagerDoubleBufferHelper);

    // Replay the transaction - Execute the createKey request again
    OMClientResponse replayResponse = omKeyCommitRequest.validateAndUpdateCache(
        ozoneManager, 10L, ozoneManagerDoubleBufferHelper);

    // Replay should result in Replay response
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.REPLAY,
        replayResponse.getOMResponse().getStatus());
  }

  @Test
  public void testReplayRequestDeletesOpenKeyEntry() throws Exception {

    // Manually add Volume, Bucket to DB
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    // Manually add Key to OpenKey table in DB
    TestOMRequestUtils.addKeyToTable(true, false, volumeName, bucketName,
        keyName, clientID, replicationType, replication, 1L,
        omMetadataManager);

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest());
    OMKeyCommitRequest omKeyCommitRequest = new OMKeyCommitRequest(
        modifiedOmRequest);

    // Execute original KeyCommit request
    omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 10L,
        ozoneManagerDoubleBufferHelper);

    // Replay the Key Create request - add Key to OpenKey table manually again
    TestOMRequestUtils.addKeyToTable(true, true, volumeName, bucketName,
        keyName, clientID, replicationType, replication, 1L,
        omMetadataManager);

    // Key should be present in OpenKey table
    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, clientID);
    OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
    Assert.assertNotNull(openKeyInfo);

    // Replay the transaction - Execute the createKey request again
    OMClientResponse replayResponse = omKeyCommitRequest.validateAndUpdateCache(
        ozoneManager, 10L, ozoneManagerDoubleBufferHelper);

    // Replay should result in DELETE_OPEN_KEY_ONLY response and delete the
    // key from OpenKey table
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        replayResponse.getOMResponse().getStatus());
    openKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
    Assert.assertNull(openKeyInfo);
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMKeyCommitRequest omKeyCommitRequest =
        new OMKeyCommitRequest(originalOMRequest);

    OMRequest modifiedOmRequest = omKeyCommitRequest.preExecute(ozoneManager);

    Assert.assertTrue(modifiedOmRequest.hasCommitKeyRequest());
    KeyArgs originalKeyArgs =
        originalOMRequest.getCommitKeyRequest().getKeyArgs();
    KeyArgs modifiedKeyArgs =
        modifiedOmRequest.getCommitKeyRequest().getKeyArgs();
    verifyKeyArgs(originalKeyArgs, modifiedKeyArgs);
    return modifiedOmRequest;
  }

  /**
   * Verify KeyArgs.
   * @param originalKeyArgs
   * @param modifiedKeyArgs
   */
  private void verifyKeyArgs(KeyArgs originalKeyArgs, KeyArgs modifiedKeyArgs) {

    // Check modification time is set or not.
    Assert.assertTrue(modifiedKeyArgs.getModificationTime() > 0);
    Assert.assertTrue(originalKeyArgs.getModificationTime() == 0);

    Assert.assertEquals(originalKeyArgs.getVolumeName(),
        modifiedKeyArgs.getVolumeName());
    Assert.assertEquals(originalKeyArgs.getBucketName(),
        modifiedKeyArgs.getBucketName());
    Assert.assertEquals(originalKeyArgs.getKeyName(),
        modifiedKeyArgs.getKeyName());
    Assert.assertEquals(originalKeyArgs.getDataSize(),
        modifiedKeyArgs.getDataSize());
    Assert.assertEquals(originalKeyArgs.getKeyLocationsList(),
        modifiedKeyArgs.getKeyLocationsList());
    Assert.assertEquals(originalKeyArgs.getType(),
        modifiedKeyArgs.getType());
    Assert.assertEquals(originalKeyArgs.getFactor(),
        modifiedKeyArgs.getFactor());
  }

  /**
   * Create OMRequest which encapsulates CommitKeyRequest.
   */
  private OMRequest createCommitKeyRequest() {
    KeyArgs keyArgs =
        KeyArgs.newBuilder().setDataSize(dataSize).setVolumeName(volumeName)
            .setKeyName(keyName).setBucketName(bucketName)
            .setType(replicationType).setReplication(replication)
            .addAllKeyLocations(getKeyLocation()).build();

    CommitKeyRequest commitKeyRequest =
        CommitKeyRequest.newBuilder().setKeyArgs(keyArgs)
            .setClientID(clientID).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
        .setCommitKeyRequest(commitKeyRequest)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  /**
   * Create KeyLocation list.
   */
  private List<KeyLocation> getKeyLocation() {
    List<KeyLocation> keyLocations = new ArrayList<>();

    for (int i=0; i < 5; i++) {
      KeyLocation keyLocation =
          KeyLocation.newBuilder()
              .setBlockID(HddsProtos.BlockID.newBuilder()
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(i+1000).setLocalID(i+100).build()))
              .setOffset(0).setLength(200).build();
      keyLocations.add(keyLocation);
    }
    return keyLocations;
  }

}

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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Class tests OMKeyCommitRequestV1 class.
 */
public class TestOMKeyCommitRequestV1 extends TestOMKeyRequestV1 {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createCommitKeyRequest());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequestV1 omKeyCommitRequestV1 =
        new OMKeyCommitRequestV1(modifiedOmRequest);

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    long bucketID = getbucketID();
    long objectId = 100;

    OmKeyInfo omKeyInfoV1 =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, bucketID,
                    Time.now());

    String fileName = OzoneFSUtils.getFileName(keyName);
    TestOMRequestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoV1, clientID,  txnLogId, omMetadataManager);

    String ozoneFileKey = omMetadataManager.getOzonePathKey(bucketID, fileName);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo dbOMKeyInfo = omMetadataManager.getKeyTable().get(ozoneFileKey);

    Assert.assertNull(dbOMKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequestV1.validateAndUpdateCache(ozoneManager, txnLogId,
                ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    dbOMKeyInfo = omMetadataManager.getOpenKeyTable().get(ozoneFileKey);
    Assert.assertNull(dbOMKeyInfo);

    // Now entry should be created in key Table.
    dbOMKeyInfo = omMetadataManager.getKeyTable().get(ozoneFileKey);

    Assert.assertNotNull(dbOMKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assert.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
            dbOMKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    Assert.assertEquals(locationInfoListFromCommitKeyRequest,
        dbOMKeyInfo.getLatestVersionLocations().getLocationList());

  }

  private long getbucketID() throws java.io.IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    return omBucketInfo.getObjectID();
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
            .setType(replicationType).setFactor(replicationFactor)
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

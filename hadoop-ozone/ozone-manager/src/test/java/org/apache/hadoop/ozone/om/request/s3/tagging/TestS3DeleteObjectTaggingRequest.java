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

package org.apache.hadoop.ozone.om.request.s3.tagging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteObjectTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Test delete object tagging request.
 */
public class TestS3DeleteObjectTaggingRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(volumeName, bucketName, keyName);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    Map<String, String> tags = getTags(5);
    String ozoneKey = addKeyToTable(tags);

    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(omKeyInfo);
    assertEquals(tags.size(), omKeyInfo.getTags().size());

    OMRequest originalRequest = createDeleteObjectTaggingRequest(volumeName, bucketName, keyName);

    S3DeleteObjectTaggingRequest request = getDeleteObjectTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);

    request = getDeleteObjectTaggingRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getDeleteObjectTaggingResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    assertEquals(Type.DeleteObjectTagging, omResponse.getCmdType());

    OmKeyInfo updatedKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(updatedKeyInfo);
    assertEquals(omKeyInfo.getVolumeName(), updatedKeyInfo.getVolumeName());
    assertEquals(omKeyInfo.getBucketName(), updatedKeyInfo.getBucketName());
    assertEquals(omKeyInfo.getKeyName(), updatedKeyInfo.getKeyName());
    assertEquals(0, updatedKeyInfo.getTags().size());
  }

  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, keyName);

    S3DeleteObjectTaggingRequest request = getDeleteObjectTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE, omMetadataManager);

    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, keyName);

    S3DeleteObjectTaggingRequest request = getDeleteObjectTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheKeyNotFound() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());

    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, keyName);

    S3DeleteObjectTaggingRequest request = getDeleteObjectTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  protected OMRequest doPreExecute(String volumeName, String bucketName,
                                   String keyName) throws Exception {
    OMRequest originalRequest = createDeleteObjectTaggingRequest(
        volumeName, bucketName, keyName);

    S3DeleteObjectTaggingRequest request = getDeleteObjectTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);

    return modifiedRequest;
  }

  public OMRequest createDeleteObjectTaggingRequest(String volumeName,
                                                    String bucketName,
                                                    String keyName) {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName);


    DeleteObjectTaggingRequest deleteObjectTaggingRequest =
        DeleteObjectTaggingRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .build();

    return OMRequest.newBuilder()
        .setDeleteObjectTaggingRequest(deleteObjectTaggingRequest)
        .setCmdType(Type.DeleteObjectTagging)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  private void verifyRequest(OMRequest modifiedRequest, OMRequest originalRequest) {

    KeyArgs original = originalRequest.getDeleteObjectTaggingRequest().getKeyArgs();

    KeyArgs updated = modifiedRequest.getDeleteObjectTaggingRequest().getKeyArgs();

    assertEquals(original.getVolumeName(), updated.getVolumeName());
    assertEquals(original.getBucketName(), updated.getBucketName());
    assertEquals(original.getKeyName(), updated.getKeyName());
    assertEquals(original.getTagsList(), updated.getTagsList());
    // Modification time will not be set for object tagging request
    assertFalse(updated.hasModificationTime());
  }

  protected String addKeyToTable(Map<String, String> tags) throws Exception {
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .addAllTags(tags)
        .build();
    OMRequestTestUtils.addKeyToTable(false, false, omKeyInfo,
        clientID, 1L, omMetadataManager);
    return omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  protected S3DeleteObjectTaggingRequest getDeleteObjectTaggingRequest(OMRequest originalRequest) {
    return new S3DeleteObjectTaggingRequest(originalRequest, getBucketLayout());
  }

  protected Map<String, String> getTags(int size) {
    Map<String, String> tags = new HashMap<>();
    for (int i = 0; i < size; i++) {
      tags.put("tag-key-" + UUID.randomUUID(), "tag-value-" + UUID.randomUUID());
    }
    return tags;
  }
}

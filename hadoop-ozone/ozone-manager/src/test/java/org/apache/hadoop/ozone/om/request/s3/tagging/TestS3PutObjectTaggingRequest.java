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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutObjectTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Test put object tagging request.
 */
public class TestS3PutObjectTaggingRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {
    Map<String, String> tags = new HashMap<>();
    getTags(2);
    doPreExecute(volumeName, bucketName, keyName, tags);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    assertNotNull(omKeyInfo);
    assertTrue(omKeyInfo.getTags().isEmpty());

    Map<String, String> tags = getTags(5);

    OMRequest originalRequest = createPutObjectTaggingRequest(volumeName, bucketName, keyName, tags);

    S3PutObjectTaggingRequest request = getPutObjectTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);

    request = getPutObjectTaggingRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getPutObjectTaggingResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    assertEquals(Type.PutObjectTagging, omResponse.getCmdType());

    OmKeyInfo updatedKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(updatedKeyInfo);
    assertEquals(omKeyInfo.getVolumeName(), updatedKeyInfo.getVolumeName());
    assertEquals(omKeyInfo.getBucketName(), updatedKeyInfo.getBucketName());
    assertEquals(omKeyInfo.getKeyName(), updatedKeyInfo.getKeyName());
    assertEquals(tags.size(), updatedKeyInfo.getTags().size());
    for (Map.Entry<String, String> tag: tags.entrySet()) {
      String value = updatedKeyInfo.getTags().get(tag.getKey());
      assertNotNull(value);
      assertEquals(tag.getValue(), value);
    }
  }

  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, keyName, getTags(2));

    S3PutObjectTaggingRequest request = getPutObjectTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE, omMetadataManager);

    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, keyName, getTags(2));

    S3PutObjectTaggingRequest request = getPutObjectTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheKeyNotFound() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());

    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, keyName, getTags(2));

    S3PutObjectTaggingRequest request = getPutObjectTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheEmptyTagSet() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(omKeyInfo);
    assertTrue(omKeyInfo.getTags().isEmpty());

    Map<String, String> tags = getTags(0);

    OMRequest originalRequest = createPutObjectTaggingRequest(volumeName, bucketName, keyName, tags);

    S3PutObjectTaggingRequest request = getPutObjectTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);

    request = getPutObjectTaggingRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 1L);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getPutObjectTaggingResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    assertEquals(Type.PutObjectTagging, omResponse.getCmdType());

    OmKeyInfo updatedKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(omKeyInfo.getVolumeName(), updatedKeyInfo.getVolumeName());
    assertEquals(omKeyInfo.getBucketName(), updatedKeyInfo.getBucketName());
    assertEquals(omKeyInfo.getKeyName(), updatedKeyInfo.getKeyName());
    assertTrue(omKeyInfo.getTags().isEmpty());
    assertEquals(tags.size(), updatedKeyInfo.getTags().size());
  }

  protected OMRequest doPreExecute(String volumeName,
                                   String bucketName,
                                   String keyName,
                                   Map<String, String> tags) throws Exception {
    OMRequest originalRequest = createPutObjectTaggingRequest(
        volumeName, bucketName, keyName, tags);

    S3PutObjectTaggingRequest request = getPutObjectTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);

    return modifiedRequest;
  }

  private OMRequest createPutObjectTaggingRequest(String volumeName,
                                                  String bucketName,
                                                  String keyName,
                                                  Map<String, String> tags) {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName);

    if (tags != null && !tags.isEmpty()) {
      keyArgs.addAllTags(KeyValueUtil.toProtobuf(tags));
    }

    PutObjectTaggingRequest putObjectTaggingRequest =
        PutObjectTaggingRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .build();

    return OMRequest.newBuilder()
        .setPutObjectTaggingRequest(putObjectTaggingRequest)
        .setCmdType(Type.PutObjectTagging)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  private void verifyRequest(OMRequest modifiedRequest, OMRequest originalRequest) {

    KeyArgs original = originalRequest.getPutObjectTaggingRequest().getKeyArgs();

    KeyArgs updated = modifiedRequest.getPutObjectTaggingRequest().getKeyArgs();

    assertEquals(original.getVolumeName(), updated.getVolumeName());
    assertEquals(original.getBucketName(), updated.getBucketName());
    assertEquals(original.getKeyName(), updated.getKeyName());
    assertEquals(original.getTagsList(), updated.getTagsList());
    // Modification time will not be set for object tagging request
    assertFalse(updated.hasModificationTime());
  }

  protected String addKeyToTable() throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), 1L,
        omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
  }

  protected S3PutObjectTaggingRequest getPutObjectTaggingRequest(OMRequest originalRequest) {
    return new S3PutObjectTaggingRequest(originalRequest, getBucketLayout());
  }

  protected Map<String, String> getTags(int size) {
    Map<String, String> tags = new HashMap<>();
    for (int i = 0; i < size; i++) {
      tags.put("tag-key-" + UUID.randomUUID(), "tag-value-" + UUID.randomUUID());
    }
    return tags;
  }
}

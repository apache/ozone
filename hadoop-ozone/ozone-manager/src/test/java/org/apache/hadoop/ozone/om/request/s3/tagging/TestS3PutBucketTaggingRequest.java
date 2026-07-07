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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.BucketManagerImpl;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.bucket.TestBucketRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutBucketTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3PutBucketTaggingRequest}.
 */
public class TestS3PutBucketTaggingRequest extends TestBucketRequest {

  private String volumeName;
  private String bucketName;

  @BeforeEach
  public void setupPutBucketTagging() throws Exception {
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();

    OMPerformanceMetrics perfMetrics = OMPerformanceMetrics.register();
    when(ozoneManager.getPerfMetrics()).thenReturn(perfMetrics);
    when(ozoneManager.getAclsEnabled()).thenReturn(false);

    doAnswer(invocation -> new ResolvedBucket(
        invocation.getArgument(0), invocation.getArgument(0),
        "", BucketLayout.DEFAULT))
        .when(ozoneManager)
        .resolveBucketLink(any(Pair.class), any(OMClientRequest.class));

    BucketManager bucketManager =
        new BucketManagerImpl(ozoneManager, omMetadataManager);
    when(ozoneManager.getBucketManager()).thenReturn(bucketManager);
  }

  @AfterEach
  public void teardown() {
    OMPerformanceMetrics.unregister();
  }

  @Test
  public void testPreExecute() throws Exception {
    Map<String, String> tags = getTags(2);
    doPreExecute(volumeName, bucketName, tags);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OmBucketInfo bucketInfo = getBucketFromDb(volumeName, bucketName);
    assertNotNull(bucketInfo);
    assertTrue(bucketInfo.getTags().isEmpty());

    Map<String, String> tags = getTags(5);

    OMRequest originalRequest =
        createPutBucketTaggingRequest(volumeName, bucketName, tags);
    S3PutBucketTaggingRequest request =
        getPutBucketTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);
    request = getPutBucketTaggingRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getPutBucketTaggingResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    assertEquals(Type.PutBucketTagging, omResponse.getCmdType());

    OmBucketInfo updatedBucketInfo = getBucketFromDb(volumeName, bucketName);
    assertNotNull(updatedBucketInfo);
    assertEquals(bucketInfo.getVolumeName(), updatedBucketInfo.getVolumeName());
    assertEquals(bucketInfo.getBucketName(), updatedBucketInfo.getBucketName());
    assertEquals(tags.size(), updatedBucketInfo.getTags().size());
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      String value = updatedBucketInfo.getTags().get(tag.getKey());
      assertNotNull(value);
      assertEquals(tag.getValue(), value);
    }
    assertThat(updatedBucketInfo.getModificationTime())
        .isGreaterThan(bucketInfo.getModificationTime());
    assertEquals(2L, updatedBucketInfo.getUpdateID());
  }

  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, getTags(2));

    S3PutBucketTaggingRequest request =
        getPutBucketTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);

    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, getTags(2));

    S3PutBucketTaggingRequest request =
        getPutBucketTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheEmptyTagSet() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OmBucketInfo bucketInfo = getBucketFromDb(volumeName, bucketName);
    assertNotNull(bucketInfo);
    assertTrue(bucketInfo.getTags().isEmpty());

    Map<String, String> tags = getTags(0);

    OMRequest originalRequest =
        createPutBucketTaggingRequest(volumeName, bucketName, tags);
    S3PutBucketTaggingRequest request =
        getPutBucketTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);
    request = getPutBucketTaggingRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 1L);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getPutBucketTaggingResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    assertEquals(Type.PutBucketTagging, omResponse.getCmdType());

    OmBucketInfo updatedBucketInfo = getBucketFromDb(volumeName, bucketName);
    assertEquals(bucketInfo.getVolumeName(), updatedBucketInfo.getVolumeName());
    assertEquals(bucketInfo.getBucketName(), updatedBucketInfo.getBucketName());
    assertTrue(updatedBucketInfo.getTags().isEmpty());
    assertEquals(tags.size(), updatedBucketInfo.getTags().size());
  }

  protected OMRequest doPreExecute(String vol, String buck,
      Map<String, String> tags) throws Exception {
    OMRequest originalRequest =
        createPutBucketTaggingRequest(vol, buck, tags);

    S3PutBucketTaggingRequest request =
        getPutBucketTaggingRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);

    return modifiedRequest;
  }

  private OMRequest createPutBucketTaggingRequest(String vol, String buck,
      Map<String, String> tags) {
    BucketArgs.Builder bucketArgs = BucketArgs.newBuilder()
        .setVolumeName(vol)
        .setBucketName(buck);

    if (tags != null && !tags.isEmpty()) {
      bucketArgs.addAllTags(KeyValueUtil.toProtobuf(tags));
    }

    PutBucketTaggingRequest putBucketTaggingRequest =
        PutBucketTaggingRequest.newBuilder()
            .setBucketArgs(bucketArgs)
            .setModificationTime(0)
            .build();

    return OMRequest.newBuilder()
        .setPutBucketTaggingRequest(putBucketTaggingRequest)
        .setCmdType(Type.PutBucketTagging)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  private void verifyRequest(OMRequest modifiedRequest, OMRequest originalRequest) {
    BucketArgs original =
        originalRequest.getPutBucketTaggingRequest().getBucketArgs();
    BucketArgs updated =
        modifiedRequest.getPutBucketTaggingRequest().getBucketArgs();

    assertEquals(original.getVolumeName(), updated.getVolumeName());
    assertEquals(original.getBucketName(), updated.getBucketName());
    assertEquals(original.getTagsList(), updated.getTagsList());

    long originModTime =
        originalRequest.getPutBucketTaggingRequest().getModificationTime();
    long newModTime =
        modifiedRequest.getPutBucketTaggingRequest().getModificationTime();
    assertThat(newModTime).isGreaterThan(originModTime);
  }

  protected S3PutBucketTaggingRequest getPutBucketTaggingRequest(
      OMRequest originalRequest) {
    return new S3PutBucketTaggingRequest(originalRequest);
  }

  private OmBucketInfo getBucketFromDb(String volume, String bucket)
      throws Exception {
    return omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volume, bucket));
  }

  protected Map<String, String> getTags(int size) {
    Map<String, String> tags = new HashMap<>();
    for (int i = 0; i < size; i++) {
      tags.put("tag-key-" + UUID.randomUUID(), "tag-value-" + UUID.randomUUID());
    }
    return tags;
  }
}

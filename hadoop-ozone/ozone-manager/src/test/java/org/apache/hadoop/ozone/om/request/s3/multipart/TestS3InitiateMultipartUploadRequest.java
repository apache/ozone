/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.util.UUID;

import com.google.common.base.Optional;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.mockito.Mockito;

import static org.apache.hadoop.crypto.CipherSuite.AES_CTR_NOPADDING;
import static org.apache.hadoop.crypto.CryptoProtocolVersion.ENCRYPTION_ZONES;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_SUPPORTED_OPERATION;
import static org.mockito.Mockito.when;

/**
 * Tests S3 Initiate Multipart Upload request.
 */
public class TestS3InitiateMultipartUploadRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecuteInitiateMPU(UUID.randomUUID().toString(),
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Add volume and bucket to DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        new S3InitiateMultipartUploadRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    Assert.assertNotNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNotNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));

    Assert.assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID(),
        omMetadataManager.getMultipartInfoTable().get(multipartKey)
            .getUploadID());

    Assert.assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getModificationTime(),
        omMetadataManager.getOpenKeyTable().get(multipartKey)
        .getModificationTime());
    Assert.assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getModificationTime(),
        omMetadataManager.getOpenKeyTable().get(multipartKey)
            .getCreationTime());

  }


  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(
        volumeName, bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        new S3InitiateMultipartUploadRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));

  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();


    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName, bucketName,
        keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        new S3InitiateMultipartUploadRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));

  }

  @Test
  public void testMPUNotSupported() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    when(ozoneManager.getKmsProvider())
        .thenReturn(Mockito.mock(KeyProviderCryptoExtension.class));

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);

    // Set encryption info and create bucket
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now())
            .setBucketEncryptionKey(new BucketEncryptionKeyInfo.Builder()
                .setKeyName("dummy").setSuite(AES_CTR_NOPADDING)
                .setVersion(ENCRYPTION_ZONES).build())
            .build();

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    omMetadataManager.getBucketTable().put(bucketKey, omBucketInfo);

    omMetadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 100L));

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName, bucketName,
        keyName);

    OMClientRequest omClientRequest =
        new S3InitiateMultipartUploadRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        omClientRequest.validateAndUpdateCache(ozoneManager, 1L,
        ozoneManagerDoubleBufferHelper);

    Assert.assertNotNull(omClientResponse.getOMResponse());
    Assert.assertEquals(NOT_SUPPORTED_OPERATION,
        omClientResponse.getOMResponse().getStatus());

  }
}

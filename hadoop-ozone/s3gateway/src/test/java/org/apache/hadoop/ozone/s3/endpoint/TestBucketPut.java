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

package org.apache.hadoop.ozone.s3.endpoint;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BUCKET_ALREADY_OWNED_BY_YOU;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class test Create Bucket functionality.
 */
public class TestBucketPut {

  private static final String BUCKET_OWNER = "my-s3-owner";
  private static final String OTHER_BUCKET_OWNER = "other-s3-owner";

  private String bucketName = OzoneConsts.BUCKET;
  private BucketEndpoint bucketEndpoint;
  private OzoneClient clientStub;

  @BeforeEach
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();

    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
  }

  @Test
  public void testCreateBucketAndFailOnDuplicateWithSameOwner() throws Exception {
    BucketEndpoint endpoint = newBucketEndpointWithRequestOwner(BUCKET_OWNER);
    clientStub.getObjectStore().createVolume(OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
    clientStub.getObjectStore().getS3Volume().createBucket(bucketName,
        BucketArgs.newBuilder().setOwner(BUCKET_OWNER).build());

    assertErrorResponse(BUCKET_ALREADY_OWNED_BY_YOU,
        () -> endpoint.put(bucketName, null));
  }

  @Test
  public void testCreateBucketAndFailOnDuplicateWithUnknownRequestOwner() throws Exception {
    clientStub.getObjectStore().createVolume(OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
    clientStub.getObjectStore().getS3Volume().createBucket(bucketName);

    assertErrorResponse(BUCKET_ALREADY_EXISTS,
        () -> bucketEndpoint.put(bucketName, null));
  }

  @Test
  public void testCreateBucketAndFailOnDuplicateWithDifferentOwner() throws Exception {
    BucketEndpoint endpoint = newBucketEndpointWithRequestOwner(BUCKET_OWNER);
    clientStub.getObjectStore().createVolume(OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
    clientStub.getObjectStore().getS3Volume().createBucket(bucketName,
        BucketArgs.newBuilder().setOwner(OTHER_BUCKET_OWNER).build());

    assertErrorResponse(BUCKET_ALREADY_EXISTS,
        () -> endpoint.put(bucketName, null));
  }

  @Test
  public void testCreateBucketSuccess() throws Exception {
    Response response = bucketEndpoint.put(bucketName, null);
    assertEquals(HTTP_OK, response.getStatus());
    assertNotNull(response.getLocation());
  }

  private BucketEndpoint newBucketEndpointWithRequestOwner(String requestOwner) {
    SignatureInfo signatureInfo = mock(SignatureInfo.class);
    when(signatureInfo.isSignPayload()).thenReturn(true);
    when(signatureInfo.getAwsAccessId()).thenReturn(requestOwner);
    when(signatureInfo.getStringToSign()).thenReturn("");
    when(signatureInfo.getSignature()).thenReturn("");

    return EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .setSignatureInfo(signatureInfo)
        .build();
  }
}

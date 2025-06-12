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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;

/**
 * This class tests Initiate Multipart Upload request.
 */
public class TestInitiateMultipartUpload {

  @Test
  public void testInitiateMultipartUpload() throws Exception {

    String bucket = OzoneConsts.S3_BUCKET;
    String key = OzoneConsts.KEY;
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    ObjectEndpoint rest = getObjectEndpoint(client, headers);

    Response response = rest.initializeMultipartUpload(bucket, key);

    assertEquals(200, response.getStatus());
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    // Calling again should return different uploadID.
    response = rest.initializeMultipartUpload(bucket, key);
    assertEquals(200, response.getStatus());
    multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    assertNotEquals(multipartUploadInitiateResponse.getUploadID(), uploadID);
  }

  @Test
  public void testInitiateMultipartUploadWithECKey() throws Exception {
    String bucket = OzoneConsts.S3_BUCKET;
    String key = OzoneConsts.KEY;
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);
    HttpHeaders headers = mock(HttpHeaders.class);
    ObjectEndpoint rest = getObjectEndpoint(client, headers);
    client.getObjectStore().getS3Bucket(bucket)
        .setReplicationConfig(new ECReplicationConfig("rs-3-2-1024K"));
    Response response = rest.initializeMultipartUpload(bucket, key);

    assertEquals(200, response.getStatus());
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
  }

  @Test
  public void testBucketOwnerCondition() throws Exception {
    HttpHeaders headers = mock(HttpHeaders.class);
    String bucket = OzoneConsts.S3_BUCKET;
    String key = OzoneConsts.KEY;
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);
    ObjectEndpoint rest = getObjectEndpoint(client, headers);
    client.getObjectStore().getS3Bucket(bucket)
        .setReplicationConfig(new ECReplicationConfig("rs-3-2-1024K"));

    // use wrong bucket owner header to test access denied
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("wrongOwner");

    OS3Exception exception =
        assertThrows(OS3Exception.class, () -> rest.initializeMultipartUpload(bucket, key));

    assertEquals(ACCESS_DENIED.getMessage(), exception.getMessage());

    // use correct bucket owner header to pass bucket owner condition verification
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("defaultOwner");

    Response response = rest.initializeMultipartUpload(bucket, key);

    assertEquals(200, response.getStatus());
  }

  @Nonnull
  private ObjectEndpoint getObjectEndpoint(OzoneClient client,
      HttpHeaders headers) {
    ObjectEndpoint rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();
    return rest;
  }
}

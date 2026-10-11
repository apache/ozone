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

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

    String uploadID = initiateMultipartUpload(rest, bucket, key);

    // Calling again should return different uploadID.
    String nextID = initiateMultipartUpload(rest, bucket, key);
    assertNotEquals(uploadID, nextID);
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
    initiateMultipartUpload(rest, bucket, key);
  }

  @ParameterizedTest
  @CsvSource({"false, wrong-owner, NO_SUCH_BUCKET", "true, wrong-owner, BUCKET_OWNER_MISMATCH",
      "true, bucket-owner, INVALID_STORAGE_CLASS"})
  public void testLookupAndOwnerChecksPrecedeStorageClass(boolean bucketExists, String expectedOwner,
      S3ErrorTable expectedError) throws Exception {
    try (OzoneClient client = new OzoneClientStub()) {
      String bucket = OzoneConsts.S3_BUCKET;
      client.getObjectStore().createVolume(OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
      if (bucketExists) {
        client.getObjectStore().getS3Volume().createBucket(bucket,
            BucketArgs.newBuilder().setOwner("bucket-owner").build());
      }
      HttpHeaders headers = mock(HttpHeaders.class);
      when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
      when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("invalid-storage-class");
      ObjectEndpoint endpoint = getObjectEndpoint(client, headers);
      assertErrorResponse(expectedError, () -> endpoint.initializeMultipartUpload(bucket, OzoneConsts.KEY));
    }
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

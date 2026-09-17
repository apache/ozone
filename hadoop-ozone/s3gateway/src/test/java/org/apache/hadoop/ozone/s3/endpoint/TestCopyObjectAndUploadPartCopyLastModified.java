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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.util.HashMap;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies CopyObject and UploadPartCopy return LastModified from the commit
 * response rather than the source key time or a synthetic gateway timestamp.
 */
public class TestCopyObjectAndUploadPartCopyLastModified {

  private static final String SOURCE_BUCKET = "source-bucket";
  private static final String DEST_BUCKET = "dest-bucket";
  private static final String SOURCE_KEY = "source-key";
  private static final String DEST_KEY = "dest-key";
  private static final String MPU_KEY = "mpu-key";
  private static final String CONTENT = "copy-last-modified-test-content";
  private static final long SOURCE_AGE_MS = 100L;

  private ObjectEndpoint endpoint;
  private HttpHeaders headers;
  private OzoneBucket sourceBucket;
  private OzoneBucket destBucket;

  @BeforeEach
  void setUp() throws Exception {
    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("UNSIGNED-PAYLOAD");
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    final OzoneClient client = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .build()
        .getClient();
    client.getObjectStore().createS3Bucket(SOURCE_BUCKET);
    client.getObjectStore().createS3Bucket(DEST_BUCKET);
    sourceBucket = client.getObjectStore().getS3Bucket(SOURCE_BUCKET);
    destBucket = client.getObjectStore().getS3Bucket(DEST_BUCKET);

    createSourceKey();

    endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();
  }

  @Test
  void testCopyObjectLastModifiedReflectsDestCommitTime() throws Exception {
    final long sourceModificationTime = sourceBucket.getKey(SOURCE_KEY)
        .getModificationTime().toEpochMilli();
    Thread.sleep(SOURCE_AGE_MS);

    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        SOURCE_BUCKET + "/" + urlEncode(SOURCE_KEY));

    try (Response response = put(endpoint, DEST_BUCKET, DEST_KEY, CONTENT)) {
      assertEquals(200, response.getStatus());

      final CopyObjectResponse copyObjectResponse = (CopyObjectResponse) response.getEntity();
      assertNotNull(copyObjectResponse.getLastModified());
      assertNotNull(copyObjectResponse.getETag());

      final long responseModificationTime = copyObjectResponse.getLastModified().toEpochMilli();
      final long destModificationTime = destBucket.getKey(DEST_KEY).getModificationTime().toEpochMilli();

      assertEquals(destModificationTime, responseModificationTime);
      assertNotEquals(sourceModificationTime, responseModificationTime);
    }
  }

  @Test
  void testUploadPartCopyLastModifiedReflectsPartCommitTime() throws Exception {
    final long sourceModificationTime = sourceBucket.getKey(SOURCE_KEY)
        .getModificationTime().toEpochMilli();
    Thread.sleep(SOURCE_AGE_MS);

    final String uploadId = initiateMultipartUpload(endpoint, DEST_BUCKET, MPU_KEY);

    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(SOURCE_BUCKET + "/" + urlEncode(SOURCE_KEY));

    try (Response response = put(endpoint, DEST_BUCKET, MPU_KEY, 1, uploadId, "")) {
      assertEquals(200, response.getStatus());

      final CopyPartResult copyPartResult = (CopyPartResult) response.getEntity();
      assertNotNull(copyPartResult.getETag());
      assertNotNull(copyPartResult.getLastModified());

      final long responseModificationTime = copyPartResult.getLastModified().toEpochMilli();

      // Retrieve the actual modification time of the newly uploaded part from the bucket
      final OzoneMultipartUploadPartListParts parts = destBucket.listParts(
          MPU_KEY, uploadId, 0, 100);
      final long destPartModificationTime = parts.getPartInfoList().get(0).getModificationTime();

      // Assert that the response precisely matches the part's actual commit time
      assertEquals(destPartModificationTime, responseModificationTime);
      assertNotEquals(sourceModificationTime, responseModificationTime);
    }
  }

  private void createSourceKey() throws Exception {
    try (OutputStream stream = sourceBucket.createKey(
        SOURCE_KEY, CONTENT.length(), ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS, ReplicationFactor.THREE), new HashMap<>())) {
      stream.write(CONTENT.getBytes(UTF_8));
    }
    Thread.sleep(SOURCE_AGE_MS);
  }
}

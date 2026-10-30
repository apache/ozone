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

import static org.apache.hadoop.ozone.client.OzoneClientTestUtils.assertKeyContent;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.get;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.getObjectAttributes;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.listParts;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.uploadPart;
import static org.apache.hadoop.ozone.s3.endpoint.ServerSideEncryption.AES256;
import static org.apache.hadoop.ozone.s3.endpoint.ServerSideEncryption.AWS_KMS;
import static org.apache.hadoop.ozone.s3.endpoint.ServerSideEncryption.AWS_KMS_DSSE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_SSE_CUSTOMER_ALGORITHM_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_SSE_CUSTOMER_KEY_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_SSE_CUSTOMER_KEY_MD5_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CONTEXT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_ALGORITHM_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_KEY_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_KEY_MD5_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_KMS_KEY_ID_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests the server-side encryption headers of object requests. */
public class TestServerSideEncryption {

  private static final String PLAIN = "plain";
  private static final String ENCRYPTED = "encrypted";
  private static final String BUCKET_KEY = "bucket-key";
  private static final String KEY_NAME = "key1";
  private static final String CONTENT = "content";

  private OzoneBucket plainBucket;
  private OzoneBucket encryptedBucket;
  private HttpHeaders headers;
  private ObjectEndpoint objectEndpoint;

  @BeforeEach
  public void setup() throws IOException {
    final OzoneClient clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(PLAIN);
    plainBucket = clientStub.getObjectStore().getS3Bucket(PLAIN);
    clientStub.getObjectStore().getVolume(plainBucket.getVolumeName())
        .createBucket(ENCRYPTED, BucketArgs.newBuilder().setBucketEncryptionKey(BUCKET_KEY).build());
    encryptedBucket = clientStub.getObjectStore().getS3Bucket(ENCRYPTED);

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("UNSIGNED-PAYLOAD");
    objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setHeaders(headers)
        .build();
  }

  private static Stream<String> customerKeyHeaders() {
    return Stream.of(SSE_CUSTOMER_ALGORITHM_HEADER, SSE_CUSTOMER_KEY_HEADER, SSE_CUSTOMER_KEY_MD5_HEADER,
        COPY_SOURCE_SSE_CUSTOMER_ALGORITHM_HEADER, COPY_SOURCE_SSE_CUSTOMER_KEY_HEADER,
        COPY_SOURCE_SSE_CUSTOMER_KEY_MD5_HEADER);
  }

  private void setHeader(String name, String value) {
    when(headers.getHeaderString(name)).thenReturn(value);
  }

  private void assertKeyAbsent(OzoneBucket bucket) throws IOException {
    assertFalse(bucket.listKeys("").hasNext(), "A key was written");
  }

  @Test
  public void putWithoutEncryptionSucceeds() throws IOException {
    try (Response response = put(objectEndpoint, PLAIN, KEY_NAME, CONTENT)) {
      assertEquals(200, response.getStatus());
      assertNull(response.getHeaderString(SSE_HEADER));
    }
    assertKeyContent(plainBucket, KEY_NAME, CONTENT);
  }

  @ParameterizedTest
  @ValueSource(strings = {AES256, AWS_KMS})
  public void putToPlainBucketIsNotImplemented(String algorithm) throws IOException {
    setHeader(SSE_HEADER, algorithm);

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, PLAIN, KEY_NAME, CONTENT));
    assertKeyAbsent(plainBucket);
  }

  @Test
  public void putToEncryptedBucketWithAes256Succeeds() throws IOException {
    setHeader(SSE_HEADER, AES256);

    try (Response response = put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT)) {
      assertEquals(200, response.getStatus());
      assertEquals(AES256, response.getHeaderString(SSE_HEADER));
      assertNull(response.getHeaderString(SSE_KMS_KEY_ID_HEADER));
    }
    assertKeyContent(encryptedBucket, KEY_NAME, CONTENT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void putToEncryptedBucketWithKmsSucceeds(boolean withBucketKeyId) throws IOException {
    setHeader(SSE_HEADER, AWS_KMS);
    if (withBucketKeyId) {
      setHeader(SSE_KMS_KEY_ID_HEADER, BUCKET_KEY);
    }

    try (Response response = put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT)) {
      assertEquals(200, response.getStatus());
      assertEquals(AWS_KMS, response.getHeaderString(SSE_HEADER));
      assertEquals(BUCKET_KEY, response.getHeaderString(SSE_KMS_KEY_ID_HEADER));
    }
    assertKeyContent(encryptedBucket, KEY_NAME, CONTENT);
  }

  @Test
  public void putToEncryptedBucketWithOtherKmsKeyIsNotImplemented() throws IOException {
    setHeader(SSE_HEADER, AWS_KMS);
    setHeader(SSE_KMS_KEY_ID_HEADER, "other-key");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT));
    assertKeyAbsent(encryptedBucket);
  }

  @Test
  public void putWithEncryptionContextIsNotImplemented() throws IOException {
    setHeader(SSE_HEADER, AWS_KMS);
    setHeader(SSE_CONTEXT_HEADER, "eyJhIjoiYiJ9");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT));
    assertKeyAbsent(encryptedBucket);
  }

  @Test
  public void putWithDualLayerKmsIsNotImplemented() throws IOException {
    setHeader(SSE_HEADER, AWS_KMS_DSSE);

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT));
    assertKeyAbsent(encryptedBucket);
  }

  @Test
  public void putWithUnknownAlgorithmIsInvalid() throws IOException {
    setHeader(SSE_HEADER, "AES128");

    assertErrorResponse(S3ErrorTable.INVALID_ARGUMENT, () -> put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT));
    assertKeyAbsent(encryptedBucket);
  }

  @Test
  public void putWithKmsKeyWithoutKmsAlgorithmIsInvalid() throws IOException {
    setHeader(SSE_KMS_KEY_ID_HEADER, BUCKET_KEY);
    assertErrorResponse(S3ErrorTable.INVALID_ARGUMENT, () -> put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT));

    setHeader(SSE_HEADER, AES256);
    assertErrorResponse(S3ErrorTable.INVALID_ARGUMENT, () -> put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT));
    assertKeyAbsent(encryptedBucket);
  }

  @ParameterizedTest
  @MethodSource("customerKeyHeaders")
  public void putWithCustomerKeyIsNotImplementedInEveryBucket(String header) throws IOException {
    setHeader(header, "value");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, PLAIN, KEY_NAME, CONTENT));
    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT));
    assertKeyAbsent(plainBucket);
    assertKeyAbsent(encryptedBucket);
  }

  @ParameterizedTest
  @MethodSource("customerKeyHeaders")
  public void readWithCustomerKeyIsNotImplemented(String header) throws IOException {
    put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT).close();
    setHeader(header, "value");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> get(objectEndpoint, ENCRYPTED, KEY_NAME));
    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> objectEndpoint.head(ENCRYPTED, KEY_NAME));
    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED,
        () -> getObjectAttributes(objectEndpoint, ENCRYPTED, KEY_NAME, "ETag"));
  }

  @ParameterizedTest
  @MethodSource("customerKeyHeaders")
  public void completeMultipartUploadWithCustomerKeyIsNotImplemented(String header) throws IOException {
    final String uploadId = initiateMultipartUpload(objectEndpoint, PLAIN, KEY_NAME);
    CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest();
    request.setPartList(Collections.singletonList(uploadPart(objectEndpoint, PLAIN, KEY_NAME, 1, uploadId, CONTENT)));
    setHeader(header, "value");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED,
        () -> objectEndpoint.completeMultipartUpload(PLAIN, KEY_NAME, request));
    assertKeyAbsent(plainBucket);
  }

  @ParameterizedTest
  @MethodSource("customerKeyHeaders")
  public void listPartsWithCustomerKeyIsNotImplemented(String header) throws IOException {
    final String uploadId = initiateMultipartUpload(objectEndpoint, PLAIN, KEY_NAME);
    uploadPart(objectEndpoint, PLAIN, KEY_NAME, 1, uploadId, CONTENT);
    setHeader(header, "value");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED,
        () -> listParts(objectEndpoint, PLAIN, KEY_NAME, uploadId, 10, 0));
  }

  @Test
  public void copyToPlainBucketWithEncryptionIsNotImplemented() throws IOException {
    put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT).close();
    setHeader(COPY_SOURCE_HEADER, ENCRYPTED + "/" + KEY_NAME);
    setHeader(SSE_HEADER, AES256);

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, PLAIN, "copy", CONTENT));
    assertKeyAbsent(plainBucket);
  }

  @Test
  public void copyToEncryptedBucketWithEncryptionSucceeds() throws IOException {
    put(objectEndpoint, PLAIN, KEY_NAME, CONTENT).close();
    setHeader(COPY_SOURCE_HEADER, PLAIN + "/" + KEY_NAME);
    setHeader(SSE_HEADER, AES256);

    try (Response response = put(objectEndpoint, ENCRYPTED, "copy", CONTENT)) {
      assertEquals(200, response.getStatus());
      assertEquals(AES256, response.getHeaderString(SSE_HEADER));
    }
    assertKeyContent(encryptedBucket, "copy", CONTENT);
  }

  @Test
  public void multipartUploadToPlainBucketWithEncryptionIsNotImplemented() throws IOException {
    final String uploadId = initiateMultipartUpload(objectEndpoint, PLAIN, KEY_NAME);
    setHeader(SSE_HEADER, AES256);

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> objectEndpoint.initializeMultipartUpload(PLAIN, "other"));
    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, PLAIN, KEY_NAME, 1, uploadId, CONTENT));
  }

  @Test
  public void multipartUploadToEncryptedBucketWithEncryptionSucceeds() throws IOException {
    setHeader(SSE_HEADER, AWS_KMS);

    try (Response response = objectEndpoint.initializeMultipartUpload(ENCRYPTED, KEY_NAME)) {
      assertEquals(200, response.getStatus());
      assertEquals(AWS_KMS, response.getHeaderString(SSE_HEADER));
      assertEquals(BUCKET_KEY, response.getHeaderString(SSE_KMS_KEY_ID_HEADER));
    }
  }
}

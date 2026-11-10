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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.completeMultipartUpload;
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
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_COPY_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RESERVED_USER_METADATA_KEY_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CONTEXT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_ALGORITHM_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_KEY_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_KEY_MD5_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_KMS_KEY_ID_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneBucketStub;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

  /** Asserts the SSE headers that GetObject and HeadObject return for {@code key}. */
  private void assertReadReports(String bucket, String key, String algorithm, String kmsKeyId) throws IOException {
    // the read requests carry no SSE headers
    setHeader(SSE_HEADER, null);
    setHeader(SSE_KMS_KEY_ID_HEADER, null);
    setHeader(COPY_SOURCE_HEADER, null);
    setHeader(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER, null);
    try (Response getResponse = get(objectEndpoint, bucket, key);
         Response headResponse = objectEndpoint.head(bucket, key)) {
      for (Response response : new Response[] {getResponse, headResponse}) {
        assertEquals(200, response.getStatus());
        assertEquals(algorithm, response.getHeaderString(SSE_HEADER));
        assertEquals(kmsKeyId, response.getHeaderString(SSE_KMS_KEY_ID_HEADER));
        // the stored algorithm is not returned as user metadata
        assertThat(response.getStringHeaders().keySet())
            .noneMatch(name -> name.contains(RESERVED_USER_METADATA_KEY_PREFIX));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", AES256, AWS_KMS})
  public void readOfEncryptedObjectReportsItsEncryption(String writeAlgorithm) throws IOException {
    if (!writeAlgorithm.isEmpty()) {
      setHeader(SSE_HEADER, writeAlgorithm);
    }
    put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT).close();

    // the algorithm of the write, aws:kms with the bucket key if the write did not ask for encryption
    if (AES256.equals(writeAlgorithm)) {
      assertReadReports(ENCRYPTED, KEY_NAME, AES256, null);
    } else {
      assertReadReports(ENCRYPTED, KEY_NAME, AWS_KMS, BUCKET_KEY);
    }
  }

  @Test
  public void readOfPlainObjectReportsNoEncryption() throws IOException {
    put(objectEndpoint, PLAIN, KEY_NAME, CONTENT).close();

    assertReadReports(PLAIN, KEY_NAME, null, null);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", AES256})
  public void multipartObjectReportsEncryptionOfItsUpload(String writeAlgorithm) throws IOException {
    if (!writeAlgorithm.isEmpty()) {
      setHeader(SSE_HEADER, writeAlgorithm);
    }
    final String uploadId = initiateMultipartUpload(objectEndpoint, ENCRYPTED, KEY_NAME);
    setHeader(SSE_HEADER, null);
    completeMultipartUpload(objectEndpoint, ENCRYPTED, KEY_NAME, uploadId,
        Collections.singletonList(uploadPart(objectEndpoint, ENCRYPTED, KEY_NAME, 1, uploadId, CONTENT)));
    objectEndpoint.queryParamsForTest().unset(QueryParams.UPLOAD_ID);
    objectEndpoint.queryParamsForTest().unset(QueryParams.PART_NUMBER);

    final String algorithm = writeAlgorithm.isEmpty() ? AWS_KMS : writeAlgorithm;
    final String kmsKeyId = writeAlgorithm.isEmpty() ? BUCKET_KEY : null;
    assertReadReports(ENCRYPTED, KEY_NAME, algorithm, kmsKeyId);
    objectEndpoint.queryParamsForTest().setInt(QueryParams.PART_NUMBER, 1);
    assertReadReports(ENCRYPTED, KEY_NAME, algorithm, kmsKeyId);
  }

  /** A copy reports the algorithm of the copy request, not the one its source was written with. */
  @ParameterizedTest
  @CsvSource({"AES256,,COPY", "AES256,aws:kms,COPY", "AES256,,REPLACE", "aws:kms,AES256,COPY",
      "aws:kms,AES256,REPLACE"})
  public void copyReportsEncryptionOfTheCopyRequest(String sourceAlgorithm, String copyAlgorithm, String directive)
      throws IOException {
    setHeader(SSE_HEADER, sourceAlgorithm);
    put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT).close();
    setHeader(SSE_HEADER, copyAlgorithm);
    setHeader(COPY_SOURCE_HEADER, ENCRYPTED + "/" + KEY_NAME);
    setHeader(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER, directive);
    put(objectEndpoint, ENCRYPTED, "copy", CONTENT).close();

    if (AES256.equals(copyAlgorithm)) {
      assertReadReports(ENCRYPTED, "copy", AES256, null);
    } else {
      assertReadReports(ENCRYPTED, "copy", AWS_KMS, BUCKET_KEY);
    }
    // the source is unchanged
    if (AES256.equals(sourceAlgorithm)) {
      assertReadReports(ENCRYPTED, KEY_NAME, AES256, null);
    } else {
      assertReadReports(ENCRYPTED, KEY_NAME, AWS_KMS, BUCKET_KEY);
    }
  }

  /**
   * User metadata {@code x-amz-server-side-encryption} written before the algorithm was stored is still returned as
   * user metadata, and not read as the algorithm.
   */
  @Test
  public void userMetadataNamedLikeTheHeaderWrittenBeforeIsKept() throws IOException {
    put(objectEndpoint, ENCRYPTED, KEY_NAME, CONTENT).close();
    ((OzoneBucketStub) encryptedBucket).putKeyMetadataForTest(KEY_NAME, SSE_HEADER, AES256);

    assertReadReports(ENCRYPTED, KEY_NAME, AWS_KMS, BUCKET_KEY);
    try (Response response = get(objectEndpoint, ENCRYPTED, KEY_NAME)) {
      assertEquals(AES256, response.getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + SSE_HEADER));
    }
  }

  /** User metadata with the name of the SSE header is kept apart from the stored algorithm, in any bucket. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void userMetadataNamedLikeTheAlgorithmIsKept(boolean encrypted) throws IOException {
    final MultivaluedMap<String, String> requestHeaders = new MultivaluedHashMap<>();
    requestHeaders.putSingle(CUSTOM_METADATA_HEADER_PREFIX + SSE_HEADER, "user-value");
    when(headers.getRequestHeaders()).thenReturn(requestHeaders);
    final String bucket = encrypted ? ENCRYPTED : PLAIN;
    if (encrypted) {
      setHeader(SSE_HEADER, AES256);
    }
    put(objectEndpoint, bucket, KEY_NAME, CONTENT).close();

    assertReadReports(bucket, KEY_NAME, encrypted ? AES256 : null, null);
    try (Response response = get(objectEndpoint, bucket, KEY_NAME)) {
      assertEquals("user-value", response.getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + SSE_HEADER));
    }
  }
}

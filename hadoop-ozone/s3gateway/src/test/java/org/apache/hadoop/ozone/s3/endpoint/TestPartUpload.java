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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/**
 * This class tests Upload part request.
 */
@ParameterizedClass
@ValueSource(booleans = {false, true})
public class TestPartUpload {

  private ObjectEndpoint rest;
  private OzoneClient client;

  private HttpHeaders headers;

  @Parameter
  private boolean enableDataStream;

  @BeforeEach
  public void setUp() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, enableDataStream);

    rest = spy(EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .setConfig(conf)
        .build());
    assertEquals(enableDataStream, rest.isDatastreamEnabled());
  }

  @Test
  public void testPartUpload() throws Exception {
    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    String content = "Multipart Upload";
    String eTag;
    try (Response response = put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, content)) {
      eTag = response.getHeaderString(OzoneConsts.ETAG);
      assertNotNull(eTag);
    }
    assertContentLength(uploadID, OzoneConsts.KEY, content.length());

    // Upload part again with same part Number, the ETag should be changed.
    String newContent = "Multipart Upload Changed";
    try (Response response = put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, newContent)) {
      String newETag = response.getHeaderString(OzoneConsts.ETAG);
      assertNotNull(newETag);
      assertNotEquals(eTag, newETag);
    }
  }

  @Test
  public void testPartUploadWithIncorrectUploadID() {
    assertErrorResponse(S3ErrorTable.NO_SUCH_UPLOAD,
        () -> put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, "random", "any"));
  }

  @Test
  public void testPartUploadStreamContentLength()
      throws IOException, OS3Exception {
    String keyName = UUID.randomUUID().toString();

    int contentLength = 15;
    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
    when(headers.getHeaderString(DECODED_CONTENT_LENGTH_HEADER))
        .thenReturn(String.valueOf(contentLength));

    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, keyName);

    assertSucceeds(() -> put(rest, OzoneConsts.S3_BUCKET, keyName, 1, uploadID, chunkedContent));
    assertContentLength(uploadID, keyName, contentLength);
  }

  @Test
  public void testPartUploadMessageDigestResetDuringException() throws IOException, OS3Exception {
    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    MessageDigest messageDigest = mock(MessageDigest.class);
    when(messageDigest.getAlgorithm()).thenReturn("MD5");
    MessageDigest sha256Digest = mock(MessageDigest.class);
    when(sha256Digest.getAlgorithm()).thenReturn("SHA-256");
    try (MockedStatic<IOUtils> ioutils = mockStatic(IOUtils.class);
        MockedStatic<ObjectEndpointStreaming> streaming = mockStatic(ObjectEndpointStreaming.class)) {
      // Add the mocked methods only during part upload
      when(rest.getMessageDigestInstance()).thenReturn(messageDigest);
      when(rest.getSha256DigestInstance()).thenReturn(sha256Digest);
      if (enableDataStream) {
        streaming.when(() -> ObjectEndpointStreaming.createMultipartKey(any(), any(), anyLong(), anyInt(), any(),
                anyInt(), any(), any(), any()))
            .thenThrow(IOException.class);
      } else {
        ioutils.when(() -> IOUtils.copyLarge(any(InputStream.class), any(OutputStream.class), anyLong(),
                anyLong(), any(byte[].class)))
            .thenThrow(IOException.class);
      }

      String content = "Multipart Upload";
      try (Response ignored = put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, content)) {
        fail("Should throw IOException");
      } catch (IOException ignored) {
        // Verify that the message digest is reset so that the instance can be reused for the
        // next request in the same thread
        verify(messageDigest, times(1)).reset();
        verify(sha256Digest, times(1)).reset();
      }
    }
  }

  @Test
  public void testPartUploadWithContentMD5() throws Exception {
    String content = "Multipart Upload Part";
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
    byte[] md5Bytes = MessageDigest.getInstance("MD5").digest(contentBytes);
    String md5Base64 = Base64.getEncoder().encodeToString(md5Bytes);

    HttpHeaders headersWithMD5 = mock(HttpHeaders.class);
    when(headersWithMD5.getHeaderString("Content-MD5")).thenReturn(md5Base64);
    when(headersWithMD5.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersWithMD5.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    ObjectEndpoint endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headersWithMD5)
        .setClient(client)
        .build();

    String uploadID = initiateMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    try (Response response = put(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, content)) {
      assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
      assertEquals(200, response.getStatus());
    }

    assertContentLength(uploadID, OzoneConsts.KEY, content.length());
  }

  public static Stream<Arguments> wrongContentMD5Provider() throws NoSuchAlgorithmException {
    byte[] wrongContentBytes = "wrong".getBytes(StandardCharsets.UTF_8);
    byte[] wrongMd5Bytes = MessageDigest.getInstance("MD5").digest(wrongContentBytes);
    String wrongMd5Base64 = Base64.getEncoder().encodeToString(wrongMd5Bytes);
    return Stream.of(
        Arguments.arguments(wrongMd5Base64),
        Arguments.arguments("invalid-base64")
    );
  }

  @ParameterizedTest
  @MethodSource("wrongContentMD5Provider")
  public void testPartUploadWithWrongContentMD5(String wrongContentMD5) throws Exception {
    String content = "Multipart Upload Part";

    HttpHeaders headersWithWrongMD5 = mock(HttpHeaders.class);
    when(headersWithWrongMD5.getHeaderString("Content-MD5")).thenReturn(wrongContentMD5);
    when(headersWithWrongMD5.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersWithWrongMD5.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    ObjectEndpoint endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headersWithWrongMD5)
        .setClient(client)
        .build();

    String uploadID = initiateMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    assertErrorResponse(S3ErrorTable.BAD_DIGEST,
        () -> put(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, content));
  }

  private void assertContentLength(String uploadID, String key,
      long contentLength) throws IOException {
    OzoneMultipartUploadPartListParts parts =
        client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET)
            .listParts(key, uploadID, 0, 100);
    assertEquals(1, parts.getPartInfoList().size());
    assertEquals(contentLength,
        parts.getPartInfoList().get(0).getSize());
  }
}

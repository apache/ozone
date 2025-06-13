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

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.UUID;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * This class tests Upload part request.
 */
public class TestPartUpload {

  private ObjectEndpoint rest;
  private OzoneClient client;

  @BeforeEach
  public void setUp() throws Exception {

    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);


    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();
  }

  @Test
  public void testPartUpload() throws Exception {

    Response response = rest.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = rest.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, null, null, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

  }

  @Test
  public void testPartUploadWithOverride() throws Exception {

    Response response = rest.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = rest.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, null, null, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

    String eTag = response.getHeaderString(OzoneConsts.ETAG);

    // Upload part again with same part Number, the ETag should be changed.
    content = "Multipart Upload Changed";
    response = rest.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, null, null, body);
    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
    assertNotEquals(eTag, response.getHeaderString(OzoneConsts.ETAG));

  }

  @Test
  public void testPartUploadWithIncorrectUploadID() throws Exception {
    OS3Exception ex = assertThrows(OS3Exception.class, () -> {
      String content = "Multipart Upload With Incorrect uploadID";
      ByteArrayInputStream body =
          new ByteArrayInputStream(content.getBytes(UTF_8));
      rest.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY, content.length(), 1,
          "random", null, null, body);
    });
    assertEquals("NoSuchUpload", ex.getCode());
    assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
  }

  @Test
  public void testPartUploadStreamContentLength()
      throws IOException, OS3Exception {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");
    ObjectEndpoint objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();
    String keyName = UUID.randomUUID().toString();

    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";
    when(headers.getHeaderString("x-amz-content-sha256"))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
    when(headers.getHeaderString(DECODED_CONTENT_LENGTH_HEADER))
        .thenReturn("15");

    Response response = objectEndpoint.initializeMultipartUpload(
        OzoneConsts.S3_BUCKET, keyName);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();
    long contentLength = chunkedContent.length();

    objectEndpoint.put(OzoneConsts.S3_BUCKET, keyName, contentLength, 1,
        uploadID, null, null, new ByteArrayInputStream(chunkedContent.getBytes(UTF_8)));
    assertContentLength(uploadID, keyName, 15);
  }

  @Test
  public void testPartUploadContentLength() throws IOException, OS3Exception {
    // The contentLength specified when creating the Key should be the same as
    // the Content-Length, the key Commit will compare the Content-Length with
    // the actual length of the data written.

    String keyName = UUID.randomUUID().toString();
    Response response = rest.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        keyName);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();
    String content = "Multipart Upload";
    long contentLength = content.length();

    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    rest.put(OzoneConsts.S3_BUCKET, keyName,
        contentLength, 1, uploadID, null, null, body);
    assertContentLength(uploadID, keyName, content.length());
  }

  @Test
  public void testPartUploadMessageDigestResetDuringException() throws IOException, OS3Exception {
    OzoneClient clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);


    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    ObjectEndpoint objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(clientStub)
        .build();

    objectEndpoint = spy(objectEndpoint);

    Response response = objectEndpoint.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    MessageDigest messageDigest = mock(MessageDigest.class);
    try (MockedStatic<IOUtils> mocked = mockStatic(IOUtils.class)) {
      // Add the mocked methods only during the copy request
      when(objectEndpoint.getMessageDigestInstance()).thenReturn(messageDigest);
      mocked.when(() -> IOUtils.copyLarge(any(InputStream.class), any(OutputStream.class), anyLong(),
              anyLong(), any(byte[].class)))
          .thenThrow(IOException.class);

      String content = "Multipart Upload";
      ByteArrayInputStream body =
          new ByteArrayInputStream(content.getBytes(UTF_8));
      try {
        objectEndpoint.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
            content.length(), 1, uploadID, null, null, body);
        fail("Should throw IOException");
      } catch (IOException ignored) {
        // Verify that the message digest is reset so that the instance can be reused for the
        // next request in the same thread
        verify(messageDigest, times(1)).reset();
      }
    }
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

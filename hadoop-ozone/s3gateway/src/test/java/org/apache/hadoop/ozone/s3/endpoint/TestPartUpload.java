/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * This class tests Upload part request.
 */
public class TestPartUpload {

  private static final ObjectEndpoint REST = new ObjectEndpoint();
  private static OzoneClient client;

  @BeforeClass
  public static void setUp() throws Exception {

    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);


    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(client);
    REST.setOzoneConfiguration(new OzoneConfiguration());
  }


  @Test
  public void testPartUpload() throws Exception {

    Response response = REST.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = REST.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

  }

  @Test
  public void testPartUploadWithOverride() throws Exception {

    Response response = REST.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = REST.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

    String eTag = response.getHeaderString("ETag");

    // Upload part again with same part Number, the ETag should be changed.
    content = "Multipart Upload Changed";
    response = REST.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, body);
    assertNotNull(response.getHeaderString("ETag"));
    assertNotEquals(eTag, response.getHeaderString("ETag"));

  }


  @Test
  public void testPartUploadWithIncorrectUploadID() throws Exception {
    try {
      String content = "Multipart Upload With Incorrect uploadID";
      ByteArrayInputStream body =
          new ByteArrayInputStream(content.getBytes(UTF_8));
      REST.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY, content.length(), 1,
          "random", body);
      fail("testPartUploadWithIncorrectUploadID failed");
    } catch (OS3Exception ex) {
      assertEquals("NoSuchUpload", ex.getCode());
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
    }
  }

  @Test
  public void testPartUploadStreamContentLength()
      throws IOException, OS3Exception {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setHeaders(headers);
    objectEndpoint.setClient(client);
    objectEndpoint.setOzoneConfiguration(new OzoneConfiguration());
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
        uploadID, new ByteArrayInputStream(chunkedContent.getBytes(UTF_8)));
    assertContentLength(uploadID, keyName, 15);
  }

  @Test
  public void testPartUploadContentLength() throws IOException, OS3Exception {
    // The contentLength specified when creating the Key should be the same as
    // the Content-Length, the key Commit will compare the Content-Length with
    // the actual length of the data written.

    String keyName = UUID.randomUUID().toString();
    Response response = REST.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        keyName);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();
    String content = "Multipart Upload";
    long contentLength = content.length();

    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    REST.put(OzoneConsts.S3_BUCKET, keyName,
        contentLength, 1, uploadID, body);
    assertContentLength(uploadID, keyName, content.length());
  }

  private void assertContentLength(String uploadID, String key,
      long contentLength) throws IOException {
    OzoneMultipartUploadPartListParts parts =
        client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET)
            .listParts(key, uploadID, 0, 100);
    Assert.assertEquals(1, parts.getPartInfoList().size());
    Assert.assertEquals(contentLength,
        parts.getPartInfoList().get(0).getSize());
  }
}

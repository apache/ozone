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
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class tests Upload part request.
 */
public class TestPartUploadWithStream {

  private static final ObjectEndpoint REST = new ObjectEndpoint();

  private static final String S3BUCKET = "streampartb1";
  private static final String S3KEY = "testkey";

  @BeforeAll
  public static void setUp() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(S3BUCKET);


    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    REST.setHeaders(headers);
    REST.setClient(client);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        true);
    REST.setOzoneConfiguration(conf);
    REST.init();
  }

  @Test
  public void testEnableStream() {
    assertTrue(REST.isDatastreamEnabled());
  }

  @Test
  public void testPartUpload() throws Exception {

    Response response = REST.initializeMultipartUpload(S3BUCKET, S3KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = REST.put(S3BUCKET, S3KEY,
        content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

  }

  @Test
  public void testPartUploadWithOverride() throws Exception {

    Response response = REST.initializeMultipartUpload(S3BUCKET, S3KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = REST.put(S3BUCKET, S3KEY,
        content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

    String eTag = response.getHeaderString(OzoneConsts.ETAG);

    // Upload part again with same part Number, the ETag should be changed.
    content = "Multipart Upload Changed";
    response = REST.put(S3BUCKET, S3KEY,
        content.length(), 1, uploadID, body);
    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
    assertNotEquals(eTag, response.getHeaderString(OzoneConsts.ETAG));

  }

  @Test
  public void testPartUploadWithIncorrectUploadID() throws Exception {
    OS3Exception ex = assertThrows(OS3Exception.class, () -> {
      String content = "Multipart Upload With Incorrect uploadID";
      ByteArrayInputStream body =
          new ByteArrayInputStream(content.getBytes(UTF_8));
      REST.put(S3BUCKET, S3KEY, content.length(), 1,
          "random", body);
    });
    assertEquals("NoSuchUpload", ex.getCode());
    assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
  }
}

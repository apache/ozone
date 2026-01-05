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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests Upload part request.
 */
public class TestPartUploadWithStream {

  private ObjectEndpoint rest;

  private static final String S3BUCKET = "streampartb1";
  private static final String S3KEY = "testkey";

  @BeforeEach
  public void setUp() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(S3BUCKET);


    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");


    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        true);

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .setConfig(conf)
        .build();
  }

  @Test
  public void testEnableStream() {
    assertTrue(rest.isDatastreamEnabled());
  }

  @Test
  public void testPartUpload() throws Exception {
    String uploadID = initiateMultipartUpload(rest, S3BUCKET, S3KEY);

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    Response response = rest.put(S3BUCKET, S3KEY,
        content.length(), 1, uploadID, null, null, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

  }

  @Test
  public void testPartUploadWithOverride() throws Exception {
    String uploadID = initiateMultipartUpload(rest, S3BUCKET, S3KEY);

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    Response response = rest.put(S3BUCKET, S3KEY,
        content.length(), 1, uploadID, null, null, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

    String eTag = response.getHeaderString(OzoneConsts.ETAG);

    // Upload part again with same part Number, the ETag should be changed.
    content = "Multipart Upload Changed";
    response = rest.put(S3BUCKET, S3KEY,
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
      rest.put(S3BUCKET, S3KEY, content.length(), 1,
          "random", null, null, body);
    });
    assertEquals("NoSuchUpload", ex.getCode());
    assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
  }
}

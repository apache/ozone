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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.endpoint.TestPartUpload.initiateUpload;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
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
    String uploadID = initiateUpload(rest, S3BUCKET, S3KEY);

    String content = "Multipart Upload";
    Response response = put(rest, S3BUCKET, S3KEY, 1, uploadID, content);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
  }

  @Test
  public void testPartUploadWithOverride() throws Exception {
    String uploadID = initiateUpload(rest, S3BUCKET, S3KEY);

    String content = "Multipart Upload";
    Response response = put(rest, S3BUCKET, S3KEY, 1, uploadID, content);

    String eTag = response.getHeaderString(OzoneConsts.ETAG);
    assertNotNull(eTag);

    // Upload part again with same part Number, the ETag should be changed.
    content = "Multipart Upload Changed";
    response = put(rest, S3BUCKET, S3KEY, 1, uploadID, content);
    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
    assertNotEquals(eTag, response.getHeaderString(OzoneConsts.ETAG));

  }

  @Test
  public void testPartUploadWithIncorrectUploadID() {
    String content = "Multipart Upload With Incorrect uploadID";
    assertErrorResponse(S3ErrorTable.NO_SUCH_UPLOAD, () -> put(rest, S3BUCKET, S3KEY, 1, "random", content));
  }
}

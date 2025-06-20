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
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class test list parts request.
 */
public class TestListParts {

  private ObjectEndpoint rest;
  private String uploadID;

  @BeforeEach
  public void setUp() throws Exception {

    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();

    Response response = rest.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = rest.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, null, null, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

    response = rest.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 2, uploadID, null, null, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

    response = rest.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 3, uploadID, null, null, body);

    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
  }

  @Test
  public void testListParts() throws Exception {
    Response response = rest.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 0,
        uploadID, 3, "0", null);

    ListPartsResponse listPartsResponse =
        (ListPartsResponse) response.getEntity();

    assertFalse(listPartsResponse.getTruncated());
    assertEquals(3, listPartsResponse.getPartList().size());

  }

  @Test
  public void testListPartsContinuation() throws Exception {
    Response response = rest.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 0,
        uploadID, 2, "0", null);
    ListPartsResponse listPartsResponse =
        (ListPartsResponse) response.getEntity();

    assertTrue(listPartsResponse.getTruncated());
    assertEquals(2, listPartsResponse.getPartList().size());

    // Continue
    response = rest.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 0, uploadID, 2,
        Integer.toString(listPartsResponse.getNextPartNumberMarker()), null);
    listPartsResponse = (ListPartsResponse) response.getEntity();

    assertFalse(listPartsResponse.getTruncated());
    assertEquals(1, listPartsResponse.getPartList().size());

  }

  @Test
  public void testListPartsWithUnknownUploadID() throws Exception {
    try {
      rest.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 0,
          uploadID, 2, "0", null);
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getErrorMessage(),
          ex.getErrorMessage());
    }
  }
}

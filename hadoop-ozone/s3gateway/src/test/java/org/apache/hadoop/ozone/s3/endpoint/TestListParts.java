/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * This class test list parts request.
 */
public class TestListParts {


  private static final ObjectEndpoint REST = new ObjectEndpoint();
  private static String uploadID;

  @BeforeClass
  public static void setUp() throws Exception {

    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(client);

    Response response = REST.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    String content = "Multipart Upload";
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    response = REST.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

    response = REST.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 2, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

    response = REST.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        content.length(), 3, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));
  }

  @Test
  public void testListParts() throws Exception {
    Response response = REST.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        uploadID, 3, "0", null);

    ListPartsResponse listPartsResponse =
        (ListPartsResponse) response.getEntity();

    Assert.assertFalse(listPartsResponse.getTruncated());
    assertEquals(3, listPartsResponse.getPartList().size());

  }

  @Test
  public void testListPartsContinuation() throws Exception {
    Response response = REST.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        uploadID, 2, "0", null);
    ListPartsResponse listPartsResponse =
        (ListPartsResponse) response.getEntity();

    Assert.assertTrue(listPartsResponse.getTruncated());
    assertEquals(2, listPartsResponse.getPartList().size());

    // Continue
    response = REST.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY, uploadID, 2,
        Integer.toString(listPartsResponse.getNextPartNumberMarker()), null);
    listPartsResponse = (ListPartsResponse) response.getEntity();

    Assert.assertFalse(listPartsResponse.getTruncated());
    assertEquals(1, listPartsResponse.getPartList().size());

  }

  @Test
  public void testListPartsWithUnknownUploadID() throws Exception {
    try {
      REST.get(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
          uploadID, 2, "0", null);
    } catch (OS3Exception ex) {
      Assert.assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getErrorMessage(),
          ex.getErrorMessage());
    }
  }


}

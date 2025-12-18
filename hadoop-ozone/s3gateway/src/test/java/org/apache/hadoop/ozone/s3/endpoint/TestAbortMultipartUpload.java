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

import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;

/**
 * This class tests abort multipart upload request.
 */
public class TestAbortMultipartUpload {

  @Test
  public void testAbortMultipartUpload() throws Exception {

    String bucket = OzoneConsts.S3_BUCKET;
    String key = OzoneConsts.KEY;
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    ObjectEndpoint rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();


    Response response = rest.initializeMultipartUpload(bucket, key);

    assertEquals(200, response.getStatus());
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    // Abort multipart upload
    rest.queryParamsForTest().set(S3Consts.QueryParams.UPLOAD_ID, uploadID);
    response = rest.delete(bucket, key);

    assertEquals(204, response.getStatus());

    // test with unknown upload Id.
    try {
      rest.queryParamsForTest().set(S3Consts.QueryParams.UPLOAD_ID, "random");
      rest.delete(bucket, key);
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getErrorMessage(),
          ex.getErrorMessage());
    }

  }
}

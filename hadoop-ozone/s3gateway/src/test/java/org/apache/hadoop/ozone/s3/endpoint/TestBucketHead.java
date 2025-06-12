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
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class test HeadBucket functionality.
 */
public class TestBucketHead {

  private String bucketName = OzoneConsts.BUCKET;
  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;
  private HttpHeaders httpHeaders;

  @BeforeEach
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(bucketName);
    httpHeaders = mock(HttpHeaders.class);

    // Create HeadBucket and setClient to OzoneClientStub
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
  }

  @Test
  public void testHeadBucket() throws Exception {

    Response response = bucketEndpoint.head(bucketName, httpHeaders);
    assertEquals(200, response.getStatus());

  }

  @Test
  public void testHeadFail() throws Exception {
    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.head("unknownbucket", httpHeaders));
    assertEquals(HTTP_NOT_FOUND, e.getHttpCode());
    assertEquals("NoSuchBucket", e.getCode());
  }

  @Test
  public void testBucketOwnerCondition() throws Exception {
    // Use wrong bucket owner header to fail bucket owner condition verification
    when(httpHeaders.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("wrongOwner");

    OS3Exception exception =
        assertThrows(OS3Exception.class, () -> bucketEndpoint.head(bucketName, httpHeaders));

    assertEquals(ACCESS_DENIED.getMessage(), exception.getMessage());

    // Use correct bucket owner header to pass bucket owner condition verification
    when(httpHeaders.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("defaultOwner");

    Response response = bucketEndpoint.head(bucketName, httpHeaders);

    assertEquals(200, response.getStatus());
  }
}

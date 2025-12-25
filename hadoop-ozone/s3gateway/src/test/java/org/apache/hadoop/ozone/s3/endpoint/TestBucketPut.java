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

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class test Create Bucket functionality.
 */
public class TestBucketPut {

  private String bucketName = OzoneConsts.BUCKET;
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws Exception {

    //Create client stub and object store stub.
    OzoneClient clientStub = new OzoneClientStub();

    // Create HeadBucket and setClient to OzoneClientStub
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
  }

  @Test
  public void testBucketFailWithAuthHeaderMissing() throws Exception {
    try {
      bucketEndpoint.put(bucketName, null);
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(MALFORMED_HEADER.getCode(), ex.getCode());
    }
  }

  @Test
  public void testBucketPut() throws Exception {
    Response response = bucketEndpoint.put(bucketName, null);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getLocation());

    // Create-bucket on an existing bucket fails
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.put(
        bucketName, null));
    assertEquals(HTTP_CONFLICT, e.getHttpCode());
    assertEquals(BUCKET_ALREADY_EXISTS.getCode(), e.getCode());
  }

  @Test
  public void testBucketFailWithInvalidHeader() throws Exception {
    try {
      bucketEndpoint.put(bucketName, null);
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(MALFORMED_HEADER.getCode(), ex.getCode());
    }
  }
}

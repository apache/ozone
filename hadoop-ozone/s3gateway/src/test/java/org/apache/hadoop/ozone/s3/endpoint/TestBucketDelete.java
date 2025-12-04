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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests delete bucket functionality.
 */
public class TestBucketDelete {

  private String bucketName = OzoneConsts.BUCKET;
  private ObjectStore objectStoreStub;
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws Exception {

    //Create client stub and object store stub.
    OzoneClient clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();

    clientStub.getObjectStore().createS3Bucket(bucketName);

    // Create HeadBucket and setClient to OzoneClientStub
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
  }

  @Test
  public void testBucketEndpoint() throws Exception {
    Response response = bucketEndpoint.delete(bucketName);
    assertEquals(HttpStatus.SC_NO_CONTENT, response.getStatus());

  }

  @Test
  public void testDeleteWithNoSuchBucket() throws Exception {
    try {
      bucketEndpoint.delete("unknownbucket");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
      return;
    }
    fail("testDeleteWithNoSuchBucket failed");
  }

  @Test
  public void testDeleteWithBucketNotEmpty() throws Exception {
    try {
      ObjectStoreStub stub = (ObjectStoreStub) objectStoreStub;
      stub.setBucketEmptyStatus(bucketName, false);
      bucketEndpoint.delete(bucketName);
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.BUCKET_NOT_EMPTY.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.BUCKET_NOT_EMPTY.getErrorMessage(),
          ex.getErrorMessage());
      return;
    }
    fail("testDeleteWithBucketNotEmpty failed");
  }
}

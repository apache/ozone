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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for unsupported bucket subresource operations. */
public class TestUnsupportedBucketSubresource {

  private static final String BUCKET_NAME = OzoneConsts.BUCKET;
  private ObjectStore objectStore;
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws IOException {
    OzoneClient client = new OzoneClientStub();
    objectStore = client.getObjectStore();
    objectStore.createS3Bucket(BUCKET_NAME);

    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();
    bucketEndpoint.queryParamsForTest().set(QueryParams.PUBLIC_ACCESS_BLOCK, "");
  }

  @Test
  public void getPublicAccessBlockIsNotImplemented() {
    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED,
        () -> bucketEndpoint.get(BUCKET_NAME));
    assertBucketExists();
  }

  @Test
  public void putPublicAccessBlockIsNotImplemented() {
    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED,
        () -> bucketEndpoint.put(BUCKET_NAME, null));
    assertBucketExists();
  }

  @Test
  public void deletePublicAccessBlockIsNotImplemented() {
    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED,
        () -> bucketEndpoint.delete(BUCKET_NAME));
    assertBucketExists();
  }

  private void assertBucketExists() {
    assertDoesNotThrow(() -> objectStore.getS3Bucket(BUCKET_NAME));
  }
}

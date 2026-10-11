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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for bucket subresource operations that are not implemented. */
public class TestBucketNotImplemented {

  private static final String BUCKET_NAME = "b1";
  private ObjectStore objectStore;
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws IOException {
    final OzoneClient clientStub = new OzoneClientStub();
    objectStore = clientStub.getObjectStore();
    objectStore.createS3Bucket(BUCKET_NAME);

    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
  }

  /** Bucket subresources that are not implemented for any HTTP method. */
  private static Stream<String> subresources() {
    return Stream.of(QueryParams.ABAC, QueryParams.ACCELERATE, QueryParams.ANALYTICS, QueryParams.CORS,
        QueryParams.DELETE, QueryParams.ENCRYPTION, QueryParams.INTELLIGENT_TIERING, QueryParams.INVENTORY,
        QueryParams.LOCATION, QueryParams.LOGGING, QueryParams.METADATA_ANNOTATION_TABLE,
        QueryParams.METADATA_CONFIGURATION, QueryParams.METADATA_INVENTORY_TABLE, QueryParams.METADATA_JOURNAL_TABLE,
        QueryParams.METADATA_TABLE, QueryParams.METRICS, QueryParams.NOTIFICATION, QueryParams.OBJECT_LOCK,
        QueryParams.OWNERSHIP_CONTROLS, QueryParams.POLICY, QueryParams.POLICY_STATUS, QueryParams.PUBLIC_ACCESS_BLOCK,
        QueryParams.REPLICATION, QueryParams.REQUEST_PAYMENT, QueryParams.SESSION, QueryParams.VERSIONING,
        QueryParams.VERSIONS, QueryParams.WEBSITE);
  }

  /** Same as {@link #subresources()}, plus uploads, for which only GET (ListMultipartUploads) is implemented. */
  private static Stream<String> putSubresources() {
    return Stream.concat(subresources(), Stream.of(QueryParams.UPLOADS));
  }

  /** Same as {@link #putSubresources()}, plus acl, for which only GET and PUT are implemented. */
  private static Stream<String> deleteSubresources() {
    return Stream.concat(putSubresources(), Stream.of(QueryParams.ACL));
  }

  @ParameterizedTest
  @MethodSource("subresources")
  public void getIsNotImplemented(String subresource) {
    bucketEndpoint.queryParamsForTest().set(subresource, "");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> bucketEndpoint.get(BUCKET_NAME));
  }

  @ParameterizedTest
  @MethodSource("putSubresources")
  public void putIsNotImplementedAndDoesNotCreateBucket(String subresource) {
    final String newBucketName = "b2";
    bucketEndpoint.queryParamsForTest().set(subresource, "");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> bucketEndpoint.put(newBucketName, null));
    assertThrows(OMException.class, () -> objectStore.getS3Bucket(newBucketName));
  }

  @ParameterizedTest
  @MethodSource("deleteSubresources")
  public void deleteIsNotImplementedAndDoesNotDeleteBucket(String subresource) throws IOException {
    bucketEndpoint.queryParamsForTest().set(subresource, "");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> bucketEndpoint.delete(BUCKET_NAME));
    assertNotNull(objectStore.getS3Bucket(BUCKET_NAME));
  }
}

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

import static org.apache.hadoop.ozone.client.OzoneClientTestUtils.assertKeyContent;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.delete;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.get;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for object subresource operations that are not implemented. */
public class TestObjectNotImplemented {

  private static final String BUCKET_NAME = "b1";
  private static final String KEY_NAME = "key1";
  private static final String CONTENT = "content";
  private OzoneBucket bucket;
  private ObjectEndpoint objectEndpoint;

  @BeforeEach
  public void setup() throws IOException {
    final OzoneClient clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);
    bucket = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME);

    final HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("UNSIGNED-PAYLOAD");

    objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setHeaders(headers)
        .build();
    assertSucceeds(() -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, CONTENT));
  }

  /** Object subresources that are not implemented for PUT and DELETE. */
  private static Stream<String> subresources() {
    return Stream.of(QueryParams.ACL, QueryParams.ANNOTATION, QueryParams.ATTRIBUTES, QueryParams.ENCRYPTION,
        QueryParams.LEGAL_HOLD, QueryParams.RENAME_OBJECT, QueryParams.RETENTION, QueryParams.TORRENT,
        QueryParams.UPLOADS);
  }

  /** Same as {@link #subresources()}, except GetObjectAttributes, which is implemented. */
  private static Stream<String> getSubresources() {
    return subresources().filter(subresource -> !QueryParams.ATTRIBUTES.equals(subresource));
  }

  @ParameterizedTest
  @MethodSource("getSubresources")
  public void getIsNotImplemented(String subresource) {
    objectEndpoint.queryParamsForTest().set(subresource, "");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> get(objectEndpoint, BUCKET_NAME, KEY_NAME));
  }

  @ParameterizedTest
  @MethodSource("subresources")
  public void putIsNotImplementedAndDoesNotOverwriteObject(String subresource) throws IOException {
    objectEndpoint.queryParamsForTest().set(subresource, "");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, "other"));
    assertKeyContent(bucket, KEY_NAME, CONTENT);
  }

  @ParameterizedTest
  @MethodSource("subresources")
  public void deleteIsNotImplementedAndDoesNotDeleteObject(String subresource) throws IOException {
    objectEndpoint.queryParamsForTest().set(subresource, "");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> delete(objectEndpoint, BUCKET_NAME, KEY_NAME));
    assertKeyContent(bucket, KEY_NAME, CONTENT);
  }

  @Test
  public void deleteWithVersionIdIsNotImplementedAndDoesNotDeleteObject() throws IOException {
    objectEndpoint.queryParamsForTest().set(QueryParams.VERSION_ID, "nonexistent");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> delete(objectEndpoint, BUCKET_NAME, KEY_NAME));
    assertKeyContent(bucket, KEY_NAME, CONTENT);
  }
}

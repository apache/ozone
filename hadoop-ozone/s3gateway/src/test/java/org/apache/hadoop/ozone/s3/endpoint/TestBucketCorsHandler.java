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

import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests S3 bucket CORS subresource operations.
 */
public class TestBucketCorsHandler {
  private static final String BUCKET_NAME = OzoneConsts.S3_BUCKET;
  private OzoneClient client;
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(Mockito.mock(HttpHeaders.class))
        .build();
    bucketEndpoint.queryParamsForTest().set(QueryParams.CORS, "");
  }

  @AfterEach
  public void clean() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void putGetAndDeleteCorsConfiguration() throws Exception {
    String xml = "<CORSConfiguration>"
        + "<CORSRule>"
        + "<ID>read-rule</ID>"
        + "<AllowedOrigin>https://example.com</AllowedOrigin>"
        + "<AllowedMethod>GET</AllowedMethod>"
        + "<AllowedMethod>HEAD</AllowedMethod>"
        + "<AllowedHeader>Authorization</AllowedHeader>"
        + "<ExposeHeader>ETag</ExposeHeader>"
        + "<MaxAgeSeconds>3000</MaxAgeSeconds>"
        + "</CORSRule>"
        + "</CORSConfiguration>";

    Response putResponse = bucketEndpoint.put(BUCKET_NAME,
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    assertEquals(HTTP_OK, putResponse.getStatus());

    Response getResponse = bucketEndpoint.get(BUCKET_NAME);
    assertEquals(HTTP_OK, getResponse.getStatus());
    assertThat(getResponse.getEntity()).isInstanceOf(S3BucketCors.class);
    assertThat(getResponse.getEntity().toString()).contains("read-rule");

    Response deleteResponse = bucketEndpoint.delete(BUCKET_NAME);
    assertEquals(HTTP_NO_CONTENT, deleteResponse.getStatus());

    OS3Exception noCors = assertThrows(OS3Exception.class,
        () -> bucketEndpoint.get(BUCKET_NAME));
    assertEquals("NoSuchCORSConfiguration", noCors.getCode());
  }

  @Test
  public void deleteCorsWithoutConfigurationIsIdempotent() throws Exception {
    Response firstDeleteResponse = bucketEndpoint.delete(BUCKET_NAME);
    assertEquals(HTTP_NO_CONTENT, firstDeleteResponse.getStatus());

    Response secondDeleteResponse = bucketEndpoint.delete(BUCKET_NAME);
    assertEquals(HTTP_NO_CONTENT, secondDeleteResponse.getStatus());

    OS3Exception noCors = assertThrows(OS3Exception.class,
        () -> bucketEndpoint.get(BUCKET_NAME));
    assertEquals("NoSuchCORSConfiguration", noCors.getCode());
  }

  @Test
  public void putCorsFailsWhenExpectedBucketOwnerDoesNotMatch()
      throws Exception {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("wrong-owner");

    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();
    bucketEndpoint.queryParamsForTest().set(QueryParams.CORS, "");

    String xml = "<CORSConfiguration>"
        + "<CORSRule>"
        + "<AllowedOrigin>https://example.com</AllowedOrigin>"
        + "<AllowedMethod>GET</AllowedMethod>"
        + "</CORSRule>"
        + "</CORSConfiguration>";

    OS3Exception ex = assertThrows(OS3Exception.class,
        () -> bucketEndpoint.put(BUCKET_NAME,
            new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
    assertEquals(S3ErrorTable.BUCKET_OWNER_MISMATCH.getCode(),
        ex.getCode());
    assertEquals(S3ErrorTable.BUCKET_OWNER_MISMATCH.getErrorMessage(),
        ex.getErrorMessage());
  }
}

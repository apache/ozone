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
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertStatus;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PRECOND_FAILED;
import static org.apache.hadoop.ozone.s3.util.S3Consts.IF_MATCH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.IF_NONE_MATCH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.IF_UNMODIFIED_SINCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.RFC1123Util;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test head object.
 */
public class TestObjectHead {
  private String bucketName = "b1";
  private ObjectEndpoint keyEndpoint;
  private OzoneBucket bucket;
  private HttpHeaders headers;

  @BeforeEach
  public void setup() throws IOException {
    OzoneClient clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(bucketName);
    bucket = clientStub.getObjectStore().getS3Bucket(bucketName);
    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("UNSIGNED-PAYLOAD");

    keyEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setHeaders(headers)
        .build();
  }

  @Test
  public void testHeadObject() throws Exception {
    //GIVEN
    byte[] bytes = createKey("key1");

    //WHEN
    Response response = keyEndpoint.head(bucketName, "key1");

    //THEN
    assertEquals(HttpStatus.SC_OK, response.getStatus());
    assertEquals(bytes.length,
        Long.parseLong(response.getHeaderString("Content-Length")));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));
  }

  @Test
  public void testHeadFailByBadName() throws Exception {
    assertStatus(HttpStatus.SC_NOT_FOUND, () -> keyEndpoint.head(bucketName, "badKeyName"));
  }

  @Test
  public void testHeadIfMatch() throws Exception {
    assertSucceeds(() -> put(keyEndpoint, bucketName, "etag-key", "head-content"));

    Response response = keyEndpoint.head(bucketName, "etag-key");
    String eTag = response.getHeaderString(HttpHeaders.ETAG);
    assertNotNull(eTag);

    when(headers.getHeaderString(IF_MATCH_HEADER)).thenReturn(eTag);

    response = keyEndpoint.head(bucketName, "etag-key");
    assertEquals(HttpStatus.SC_OK, response.getStatus());
  }

  @Test
  public void testHeadIfNoneMatchReturnsNotModified() throws Exception {
    assertSucceeds(() -> put(keyEndpoint, bucketName, "etag-key", "head-content"));

    Response response = keyEndpoint.head(bucketName, "etag-key");
    String eTag = response.getHeaderString(HttpHeaders.ETAG);
    assertNotNull(eTag);

    when(headers.getHeaderString(IF_NONE_MATCH_HEADER)).thenReturn(eTag);

    response = keyEndpoint.head(bucketName, "etag-key");
    assertEquals(Response.Status.NOT_MODIFIED.getStatusCode(),
        response.getStatus());
  }

  @Test
  public void testHeadIfUnmodifiedSinceFailure() throws Exception {
    assertSucceeds(() -> put(keyEndpoint, bucketName, "etag-key", "head-content"));

    when(headers.getHeaderString(IF_UNMODIFIED_SINCE_HEADER))
        .thenReturn(formatHttpDate(bucket.getKey("etag-key")
            .getModificationTime().minusSeconds(60)));

    OS3Exception ex = assertErrorResponse(PRECOND_FAILED,
        () -> keyEndpoint.head(bucketName, "etag-key"));
    assertNotNull(ex);
  }

  @Test
  public void testHeadWhenKeyIsAFileAndKeyPathDoesNotEndWithASlash()
      throws IOException, OS3Exception {
    // GIVEN
    final String keyPath = "keyDir";
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    keyEndpoint.setOzoneConfiguration(config);
    createKey(keyPath);

    assertSucceeds(() -> keyEndpoint.head(bucketName, keyPath));
  }

  @Test
  public void testHeadWhenKeyIsDirectoryAndKeyPathDoesNotEndWithASlash()
      throws IOException, OS3Exception {
    // GIVEN
    final String keyPath = "keyDir";
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    keyEndpoint.setOzoneConfiguration(config);
    bucket.createDirectory(keyPath);

    assertStatus(HttpStatus.SC_NOT_FOUND, () -> keyEndpoint.head(bucketName, keyPath));
  }

  @Test
  public void testHeadWhenKeyIsDirectoryAndKeyPathEndsWithASlash()
      throws IOException, OS3Exception {
    // GIVEN
    final String keyPath = "keyDir/";
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    keyEndpoint.setOzoneConfiguration(config);
    bucket.createDirectory(keyPath);

    // THEN
    assertSucceeds(() -> keyEndpoint.head(bucketName, keyPath));
  }

  @Test
  public void testHeadWhenKeyIsAFileAndKeyPathEndsWithASlash()
      throws IOException, OS3Exception {
    final String keyPath = "keyFile";
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    keyEndpoint.setOzoneConfiguration(config);
    createKey(keyPath);

    assertStatus(HttpStatus.SC_NOT_FOUND, () -> keyEndpoint.head(bucketName, keyPath + "/"));
  }

  private byte[] createKey(String keyPath) throws IOException {
    byte[] bytes = RandomStringUtils.secure().nextAlphanumeric(32).getBytes(UTF_8);
    try (OutputStream out = bucket.createKey(keyPath, bytes.length)) {
      out.write(bytes);
    }
    return bytes;
  }

  private static String formatHttpDate(Instant instant) {
    return RFC1123Util.FORMAT.format(
        instant.atZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE)));
  }
}

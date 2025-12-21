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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
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
  private OzoneClient client;

  @BeforeEach
  public void setup() throws IOException {
    //Create client stub and object store stub.
    client = new OzoneClientStub();

    // Create volume and bucket
    client.getObjectStore().createS3Bucket(bucketName);

    bucket = client.getObjectStore().getS3Bucket(bucketName);

    // Create HeadBucket and setClient to OzoneClientStub
    keyEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .build();
  }

  @Test
  public void testHeadObject() throws Exception {
    //GIVEN
    String value = RandomStringUtils.secure().nextAlphanumeric(32);
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.ONE), new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    //WHEN
    Response response = keyEndpoint.head(bucketName, "key1");

    //THEN
    assertEquals(200, response.getStatus());
    assertEquals(value.getBytes(UTF_8).length,
        Long.parseLong(response.getHeaderString("Content-Length")));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));

  }

  @Test
  public void testHeadFailByBadName() throws Exception {
    //Head an object that doesn't exist.
    try {
      Response response =  keyEndpoint.head(bucketName, "badKeyName");
      assertEquals(404, response.getStatus());
    } catch (OS3Exception ex) {
      assertThat(ex.getCode()).contains("NoSuchObject");
      assertThat(ex.getErrorMessage()).contains("object does not exist");
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
    }
  }

  @Test
  public void testHeadWhenKeyIsAFileAndKeyPathDoesNotEndWithASlash()
      throws IOException, OS3Exception {
    // GIVEN
    final String keyPath = "keyDir";
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    keyEndpoint.setOzoneConfiguration(config);
    String keyContent = "content";
    OzoneOutputStream out = bucket.createKey(keyPath,
        keyContent.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
            ReplicationFactor.ONE), new HashMap<>());
    out.write(keyContent.getBytes(UTF_8));
    out.close();

    // WHEN
    final Response response = keyEndpoint.head(bucketName, keyPath);

    // THEN
    assertEquals(HttpStatus.SC_OK, response.getStatus());
    bucket.deleteKey(keyPath);
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

    // WHEN
    final Response response = keyEndpoint.head(bucketName, keyPath);

    // THEN
    assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
    bucket.deleteKey(keyPath);
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

    // WHEN
    final Response response = keyEndpoint.head(bucketName, keyPath);

    // THEN
    assertEquals(HttpStatus.SC_OK, response.getStatus());
    bucket.deleteKey(keyPath);
  }

  @Test
  public void testHeadWhenKeyIsAFileAndKeyPathEndsWithASlash()
      throws IOException, OS3Exception {
    // GIVEN
    final String keyPath = "keyFile";
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    keyEndpoint.setOzoneConfiguration(config);
    String keyContent = "content";
    OzoneOutputStream out = bucket.createKey(keyPath,
        keyContent.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
            ReplicationFactor.ONE), new HashMap<>());
    out.write(keyContent.getBytes(UTF_8));
    out.close();

    // WHEN
    final Response response = keyEndpoint.head(bucketName, keyPath + "/");

    // THEN
    assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
    bucket.deleteKey(keyPath);
  }

  @Test
  public void testHeadWithRangeHeader() throws Exception {
    //GIVEN
    String value = "0123456789";
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.ONE), new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-0");
    keyEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    //WHEN
    Response response = keyEndpoint.head(bucketName, "key1");

    //THEN
    assertEquals(206, response.getStatus());
    assertEquals("1", response.getHeaderString("Content-Length"));
    assertEquals(String.format("bytes 0-0/%d", value.length()),
        response.getHeaderString("Content-Range"));
    assertEquals("bytes", response.getHeaderString("Accept-Ranges"));

    // Test range from start to end
    when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-");
    response = keyEndpoint.head(bucketName, "key1");
    assertEquals(206, response.getStatus());
    assertEquals(String.valueOf(value.length()),
        response.getHeaderString("Content-Length"));
    assertEquals(String.format("bytes 0-%d/%d", value.length() - 1, value.length()),
        response.getHeaderString("Content-Range"));

    bucket.deleteKey("key1");
  }

  @Test
  public void testHeadWithInvalidRangeHeader() throws Exception {
    //GIVEN
    String value = "0123456789"; // length = 10
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.ONE), new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    HttpHeaders headers = mock(HttpHeaders.class);
    // Invalid range: both start and end are beyond file length
    // According to RangeHeaderParserUtil, bytes=11-10 with length=10 will trigger isInValidRange()
    when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=11-10");
    keyEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    //WHEN/THEN
    OS3Exception ex = assertThrows(OS3Exception.class,
        () -> keyEndpoint.head(bucketName, "key1"));
    assertEquals("InvalidRange", ex.getCode());
    assertEquals(416, ex.getHttpCode());

    bucket.deleteKey("key1");
  }

  @Test
  public void testHeadWithoutRangeHeader() throws Exception {
    //GIVEN
    String value = "0123456789";
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.ONE), new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    //WHEN
    Response response = keyEndpoint.head(bucketName, "key1");

    //THEN
    assertEquals(200, response.getStatus());
    assertEquals(String.valueOf(value.length()),
        response.getHeaderString("Content-Length"));
    assertEquals("bytes", response.getHeaderString("Accept-Ranges"));
    assertNull(response.getHeaderString("Content-Range"));

    bucket.deleteKey("key1");
  }
}

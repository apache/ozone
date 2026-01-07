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
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_COUNT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test get object.
 */
public class TestObjectGet {

  private static final String CONTENT = "0123456789";
  private static final String BUCKET_NAME = "b1";
  private static final String KEY_NAME = "key1";
  private static final String KEY_WITH_TAG = "keyWithTag";
  public static final String CONTENT_TYPE1 = "video/mp4";
  public static final String CONTENT_TYPE2 = "text/html; charset=UTF-8";
  public static final String CONTENT_LANGUAGE1 = "en-CA";
  public static final String CONTENT_LANGUAGE2 = "de-DE, en-CA";
  public static final String EXPIRES1 = "Wed, 21 Oct 2015 07:29:00 GMT";
  public static final String EXPIRES2 = "Wed, 21 Oct 2015 07:28:00 GMT";
  public static final String CACHE_CONTROL1 = "no-cache";
  public static final String CACHE_CONTROL2 = "max-age=604800";
  public static final String CONTENT_DISPOSITION1 = "inline";
  public static final String CONTENT_DISPOSITION2 = "attachment; "
      + "filename=\"filename.jpg\"";
  public static final String CONTENT_ENCODING1 = "gzip";
  public static final String CONTENT_ENCODING2 = "compress";

  private HttpHeaders headers;
  private ObjectEndpoint rest;
  private OzoneClient client;

  @BeforeEach
  public void init() throws OS3Exception, IOException {
    //GIVEN
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    ByteArrayInputStream body = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    rest.put(BUCKET_NAME, KEY_NAME, CONTENT.length(),
        1, null, null, null, body);
    // Create a key with object tags
    when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");
    rest.put(BUCKET_NAME, KEY_WITH_TAG, CONTENT.length(),
        1, null, null, null, body);
  }

  @Test
  public void get() throws IOException, OS3Exception {
    //WHEN
    Response response = rest.get(BUCKET_NAME, KEY_NAME, 0, null, 0, null, null);

    //THEN
    OzoneInputStream ozoneInputStream =
        client.getObjectStore().getS3Bucket(BUCKET_NAME)
            .readKey(KEY_NAME);
    String keyContent =
        IOUtils.toString(ozoneInputStream, UTF_8);

    assertEquals(CONTENT, keyContent);
    assertEquals(String.valueOf(keyContent.length()),
        response.getHeaderString("Content-Length"));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));

    assertNull(response.getHeaderString(TAG_COUNT_HEADER));
  }

  @Test
  public void getKeyWithTag() throws IOException, OS3Exception {
    //WHEN
    Response response = rest.get(BUCKET_NAME, KEY_WITH_TAG, 0, null, 0, null, null);

    //THEN
    OzoneInputStream ozoneInputStream =
        client.getObjectStore().getS3Bucket(BUCKET_NAME)
            .readKey(KEY_NAME);
    String keyContent =
        IOUtils.toString(ozoneInputStream, UTF_8);

    assertEquals(CONTENT, keyContent);
    assertEquals(String.valueOf(keyContent.length()),
        response.getHeaderString("Content-Length"));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));
    assertEquals("2", response.getHeaderString(TAG_COUNT_HEADER));
  }

  @Test
  public void inheritRequestHeader() throws IOException, OS3Exception {
    setDefaultHeader();

    Response response = rest.get(BUCKET_NAME, KEY_NAME, 0, null, 0, null, null);

    assertEquals(CONTENT_TYPE1,
        response.getHeaderString("Content-Type"));
    assertEquals(CONTENT_LANGUAGE1,
        response.getHeaderString("Content-Language"));
    assertEquals(EXPIRES1,
        response.getHeaderString("Expires"));
    assertEquals(CACHE_CONTROL1,
        response.getHeaderString("Cache-Control"));
    assertEquals(CONTENT_DISPOSITION1,
        response.getHeaderString("Content-Disposition"));
    assertEquals(CONTENT_ENCODING1,
        response.getHeaderString("Content-Encoding"));
  }

  @Test
  public void overrideResponseHeader() throws IOException, OS3Exception {
    setDefaultHeader();

    MultivaluedMap<String, String> queryParameter = rest.getContext().getUriInfo().getQueryParameters();
    // overrider request header
    queryParameter.putSingle("response-content-type", CONTENT_TYPE2);
    queryParameter.putSingle("response-content-language", CONTENT_LANGUAGE2);
    queryParameter.putSingle("response-expires", EXPIRES2);
    queryParameter.putSingle("response-cache-control", CACHE_CONTROL2);
    queryParameter.putSingle("response-content-disposition",
        CONTENT_DISPOSITION2);
    queryParameter.putSingle("response-content-encoding", CONTENT_ENCODING2);

    Response response = rest.get(BUCKET_NAME, KEY_NAME, 0, null, 0, null, null);

    assertEquals(CONTENT_TYPE2,
        response.getHeaderString("Content-Type"));
    assertEquals(CONTENT_LANGUAGE2,
        response.getHeaderString("Content-Language"));
    assertEquals(EXPIRES2,
        response.getHeaderString("Expires"));
    assertEquals(CACHE_CONTROL2,
        response.getHeaderString("Cache-Control"));
    assertEquals(CONTENT_DISPOSITION2,
        response.getHeaderString("Content-Disposition"));
    assertEquals(CONTENT_ENCODING2,
        response.getHeaderString("Content-Encoding"));
  }

  @Test
  public void getRangeHeader() throws IOException, OS3Exception {
    Response response;
    when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-0");

    response = rest.get(BUCKET_NAME, KEY_NAME, 0, null, 0, null, null);
    assertEquals("1", response.getHeaderString("Content-Length"));
    assertEquals(String.format("bytes 0-0/%s", CONTENT.length()),
        response.getHeaderString("Content-Range"));

    when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-");
    response = rest.get(BUCKET_NAME, KEY_NAME, 0, null, 0, null, null);
    assertEquals(String.valueOf(CONTENT.length()),
        response.getHeaderString("Content-Length"));
    assertEquals(
        String.format("bytes 0-%s/%s", CONTENT.length() - 1, CONTENT.length()),
        response.getHeaderString("Content-Range"));

    assertNull(response.getHeaderString(TAG_COUNT_HEADER));
  }

  @Test
  public void getStatusCode() throws IOException, OS3Exception {
    Response response;
    response = rest.get(BUCKET_NAME, KEY_NAME, 0, null, 0, null, null);
    assertEquals(response.getStatus(),
        Response.Status.OK.getStatusCode());

    // https://www.rfc-editor.org/rfc/rfc7233#section-4.1
    // The 206 (Partial Content) status code indicates that the server is
    //   successfully fulfilling a range request for the target resource
    when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-1");
    response = rest.get(BUCKET_NAME, KEY_NAME, 0, null, 0, null, null);
    assertEquals(response.getStatus(),
        Response.Status.PARTIAL_CONTENT.getStatusCode());
    assertNull(response.getHeaderString(TAG_COUNT_HEADER));
  }

  private void setDefaultHeader() {
    doReturn(CONTENT_TYPE1)
        .when(headers).getHeaderString("Content-Type");
    doReturn(CONTENT_LANGUAGE1)
        .when(headers).getHeaderString("Content-Language");
    doReturn(EXPIRES1)
        .when(headers).getHeaderString("Expires");
    doReturn(CACHE_CONTROL1)
        .when(headers).getHeaderString("Cache-Control");
    doReturn(CONTENT_DISPOSITION1)
        .when(headers).getHeaderString("Content-Disposition");
    doReturn(CONTENT_ENCODING1)
        .when(headers).getHeaderString("Content-Encoding");
  }

  @Test
  public void testGetWhenKeyIsDirectoryAndDoesNotEndWithASlash()
      throws IOException {
    // GIVEN
    final String keyPath = "keyDir";
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, "true");
    rest.setOzoneConfiguration(config);
    OzoneBucket bucket = client.getObjectStore().getS3Bucket(BUCKET_NAME);
    bucket.createDirectory(keyPath);

    // WHEN
    final OS3Exception ex = assertThrows(OS3Exception.class,
            () -> rest.get(BUCKET_NAME, keyPath, 0, null, 0, null, null));

    // THEN
    assertEquals(NO_SUCH_KEY.getCode(), ex.getCode());
    bucket.deleteKey(keyPath);
  }
}

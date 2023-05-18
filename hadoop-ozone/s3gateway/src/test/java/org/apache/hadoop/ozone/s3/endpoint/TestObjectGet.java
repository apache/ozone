/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER;
import static org.mockito.Mockito.doReturn;

/**
 * Test get object.
 */
public class TestObjectGet {

  public static final String CONTENT = "0123456789";
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
  private ContainerRequestContext context;

  @Before
  public void init() throws IOException {
    //GIVEN
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("b1");
    OzoneBucket bucket = client.getObjectStore().getS3Bucket("b1");
    OzoneOutputStream keyStream =
        bucket.createKey("key1", CONTENT.getBytes(UTF_8).length);
    keyStream.write(CONTENT.getBytes(UTF_8));
    keyStream.close();

    rest = new ObjectEndpoint();
    rest.setClient(client);
    rest.setOzoneConfiguration(new OzoneConfiguration());
    headers = Mockito.mock(HttpHeaders.class);
    rest.setHeaders(headers);

    context = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(context.getUriInfo()).thenReturn(Mockito.mock(UriInfo.class));
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(new MultivaluedHashMap<>());
    rest.setContext(context);
  }

  @Test
  public void get() throws IOException, OS3Exception {
    //WHEN
    Response response = rest.get("b1", "key1", null, 0, null);

    //THEN
    OzoneInputStream ozoneInputStream =
        client.getObjectStore().getS3Bucket("b1")
            .readKey("key1");
    String keyContent =
        IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(CONTENT, keyContent);
    Assert.assertEquals("" + keyContent.length(),
        response.getHeaderString("Content-Length"));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));

  }

  @Test
  public void inheritRequestHeader() throws IOException, OS3Exception {
    setDefaultHeader();

    Response response = rest.get("b1", "key1", null, 0, null);

    Assert.assertEquals(CONTENT_TYPE1,
        response.getHeaderString("Content-Type"));
    Assert.assertEquals(CONTENT_LANGUAGE1,
        response.getHeaderString("Content-Language"));
    Assert.assertEquals(EXPIRES1,
        response.getHeaderString("Expires"));
    Assert.assertEquals(CACHE_CONTROL1,
        response.getHeaderString("Cache-Control"));
    Assert.assertEquals(CONTENT_DISPOSITION1,
        response.getHeaderString("Content-Disposition"));
    Assert.assertEquals(CONTENT_ENCODING1,
        response.getHeaderString("Content-Encoding"));
  }

  @Test
  public void overrideResponseHeader() throws IOException, OS3Exception {
    setDefaultHeader();

    MultivaluedHashMap<String, String> queryParameter =
        new MultivaluedHashMap<>();
    // overrider request header
    queryParameter.putSingle("response-content-type", CONTENT_TYPE2);
    queryParameter.putSingle("response-content-language", CONTENT_LANGUAGE2);
    queryParameter.putSingle("response-expires", EXPIRES2);
    queryParameter.putSingle("response-cache-control", CACHE_CONTROL2);
    queryParameter.putSingle("response-content-disposition",
        CONTENT_DISPOSITION2);
    queryParameter.putSingle("response-content-encoding", CONTENT_ENCODING2);

    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(queryParameter);
    Response response = rest.get("b1", "key1", null, 0, null);

    Assert.assertEquals(CONTENT_TYPE2,
        response.getHeaderString("Content-Type"));
    Assert.assertEquals(CONTENT_LANGUAGE2,
        response.getHeaderString("Content-Language"));
    Assert.assertEquals(EXPIRES2,
        response.getHeaderString("Expires"));
    Assert.assertEquals(CACHE_CONTROL2,
        response.getHeaderString("Cache-Control"));
    Assert.assertEquals(CONTENT_DISPOSITION2,
        response.getHeaderString("Content-Disposition"));
    Assert.assertEquals(CONTENT_ENCODING2,
        response.getHeaderString("Content-Encoding"));
  }

  @Test
  public void getRangeHeader() throws IOException, OS3Exception {
    Response response;
    Mockito.when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-0");

    response = rest.get("b1", "key1", null, 0, null);
    Assert.assertEquals("1", response.getHeaderString("Content-Length"));
    Assert.assertEquals(String.format("bytes 0-0/%s", CONTENT.length()),
        response.getHeaderString("Content-Range"));

    Mockito.when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-");
    response = rest.get("b1", "key1", null, 0, null);
    Assert.assertEquals(String.valueOf(CONTENT.length()),
        response.getHeaderString("Content-Length"));
    Assert.assertEquals(
        String.format("bytes 0-%s/%s", CONTENT.length() - 1, CONTENT.length()),
        response.getHeaderString("Content-Range"));
  }

  @Test
  public void getStatusCode() throws IOException, OS3Exception {
    Response response;
    response = rest.get("b1", "key1", null, 0, null);
    Assert.assertEquals(response.getStatus(),
        Response.Status.OK.getStatusCode());

    // https://www.rfc-editor.org/rfc/rfc7233#section-4.1
    // The 206 (Partial Content) status code indicates that the server is
    //   successfully fulfilling a range request for the target resource
    Mockito.when(headers.getHeaderString(RANGE_HEADER)).thenReturn("bytes=0-1");
    response = rest.get("b1", "key1", null, 0, null);
    Assert.assertEquals(response.getStatus(),
        Response.Status.PARTIAL_CONTENT.getStatusCode());
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
}

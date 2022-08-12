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
import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.doReturn;

public class TestOverrideResponseHeader {
  public static final String CONTENT = "0123456789";
  public static final String ContentType1 = "video/mp4";
  public static final String ContentType2 = "text/html; charset=UTF-8";
  public static final String ContentLanguage1 = "en-CA";
  public static final String ContentLanguage2 = "de-DE, en-CA";
  public static final String Expires1 = "Wed, 21 Oct 2015 07:29:00 GMT";
  public static final String Expires2 = "Wed, 21 Oct 2015 07:28:00 GMT";
  public static final String CacheControl1 = "no-cache";
  public static final String CacheControl2 = "max-age=604800";
  public static final String ContentDisposition1 = "inline";
  public static final String ContentDisposition2 = "attachment; "
      + "filename=\"filename.jpg\"";
  public static final String ContentEncoding1 = "gzip";
  public static final String ContentEncoding2 = "compress";
  HttpHeaders headers;
  ContainerRequestContext context;

  ObjectEndpoint rest;

  @Before
  public void init() throws IOException {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("b1");
    OzoneBucket bucket = client.getObjectStore().getS3Bucket("b1");
    OzoneOutputStream keyStream =
        bucket.createKey("key1", CONTENT.getBytes(UTF_8).length);
    keyStream.write(CONTENT.getBytes(UTF_8));
    keyStream.close();

    rest = new ObjectEndpoint();
    rest.setClient(client);

    headers = Mockito.mock(HttpHeaders.class);
    rest.setHeaders(headers);

    context = Mockito.mock(ContainerRequestContext.class);
    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    Mockito.when(context.getUriInfo()).thenReturn(uriInfo);
    MultivaluedHashMap<String, String> queryParameter = new MultivaluedHashMap<>();
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(queryParameter);
    rest.setContext(context);
  }

  private void setHeaders() {
    doReturn(ContentType1).when(headers).getHeaderString("Content-Type");
    doReturn(ContentLanguage1).when(headers).getHeaderString("Content-Language");
    doReturn(Expires1).when(headers).getHeaderString("Expires");
    doReturn(CacheControl1).when(headers).getHeaderString("Cache-Control");
    doReturn(ContentDisposition1).when(headers).getHeaderString("Content-Disposition");
    doReturn(ContentEncoding1).when(headers).getHeaderString("Content-Encoding");
  }

  @Test
  public void testInheritRequestHeader() throws IOException, OS3Exception {
    setHeaders();
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    Response response = rest.get("b1", "key1", null, 0, null, body);

    Assert.assertEquals(ContentType1, response.getHeaderString("Content-Type"));
    Assert.assertEquals(ContentLanguage1, response.getHeaderString("Content-Language"));
    Assert.assertEquals(Expires1, response.getHeaderString("Expires"));
    Assert.assertEquals(CacheControl1, response.getHeaderString("Cache-Control"));
    Assert.assertEquals(ContentDisposition1, response.getHeaderString("Content-Disposition"));
    Assert.assertEquals(ContentEncoding1, response.getHeaderString("Content-Encoding"));
  }

  @Test
  public void testOverrideResponseHeader() throws IOException, OS3Exception {

    MultivaluedHashMap<String, String> queryParameter = new MultivaluedHashMap<>();

    // overrider
    queryParameter.putSingle("response-content-type", ContentType2);
    queryParameter.putSingle("response-content-language", ContentLanguage2);
    queryParameter.putSingle("response-expires", Expires2);
    queryParameter.putSingle("response-cache-control", CacheControl2);
    queryParameter.putSingle("response-content-disposition", ContentDisposition2);
    queryParameter.putSingle("response-content-encoding", ContentEncoding2);

    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(queryParameter);

    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    //WHEN
    Response response = rest.get("b1", "key1", null, 0, null, body);

    Assert.assertEquals(ContentType2, response.getHeaderString("Content-Type"));
    Assert.assertEquals(ContentLanguage2, response.getHeaderString("Content-Language"));
    Assert.assertEquals(Expires2, response.getHeaderString("Expires"));
    Assert.assertEquals(CacheControl2, response.getHeaderString("Cache-Control"));
    Assert.assertEquals(ContentDisposition2, response.getHeaderString("Content-Disposition"));
    Assert.assertEquals(ContentEncoding2, response.getHeaderString("Content-Encoding"));

  }
}

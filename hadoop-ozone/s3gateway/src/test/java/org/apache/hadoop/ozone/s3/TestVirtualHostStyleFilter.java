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

package org.apache.hadoop.ozone.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

/**
 * This class test virtual host style mapping conversion to path style.
 */
public class TestVirtualHostStyleFilter {

  private OzoneConfiguration conf;
  private String s3HttpAddr;

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    s3HttpAddr = "localhost:9878";
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, s3HttpAddr);
    s3HttpAddr = s3HttpAddr.substring(0, s3HttpAddr.lastIndexOf(':'));
    conf.set(S3GatewayConfigKeys.OZONE_S3G_DOMAIN_NAME, s3HttpAddr);
  }

  private ContainerRequestContext createRequestContext(String host,
      String path) {
    return createRequestContext(host, path, new MultivaluedHashMap<>());
  }

  private ContainerRequestContext createRequestContext(String host, String path,
      MultivaluedMap<String, String> queryParams) {
    URI baseUri = URI.create("http://" + s3HttpAddr);
    UriBuilder requestUriBuilder = UriBuilder.fromUri(baseUri);
    if (path != null) {
      requestUriBuilder.path(path);
    }
    queryParams.forEach((key, values) ->
        requestUriBuilder.queryParam(key, values.toArray()));

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUri()).thenReturn(baseUri);
    when(uriInfo.getPath()).thenReturn(path == null ? "" : path.substring(1));
    when(uriInfo.getQueryParameters()).thenReturn(queryParams);
    when(uriInfo.getRequestUri()).thenReturn(requestUriBuilder.build());

    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    when(requestContext.getHeaderString(HttpHeaders.HOST)).thenReturn(host);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    return requestContext;
  }

  @Test
  public void testVirtualHostStyle() throws  Exception {
    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);


    ContainerRequestContext requestContext = createRequestContext(
        "mybucket.localhost:9878", "/myfile");
    virtualHostStyleFilter.filter(requestContext);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket/myfile");
    verify(requestContext).setRequestUri(new URI("http://" + s3HttpAddr), expected);
  }

  @Test
  public void testPathStyle() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequestContext requestContext = createRequestContext(s3HttpAddr,
        "/mybucket/myfile");
    virtualHostStyleFilter.filter(requestContext);
    verify(requestContext, never()).setRequestUri(any(URI.class), any(URI.class));

  }

  @Test
  public void testVirtualHostStyleWithCreateBucketRequest() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequestContext requestContext = createRequestContext(
        "mybucket.localhost:9878", null);
    virtualHostStyleFilter.filter(requestContext);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket");
    verify(requestContext).setRequestUri(new URI("http://" + s3HttpAddr), expected);

  }

  @Test
  public void testVirtualHostStyleWithCreateKeyRequest() throws Exception {
    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequestContext requestContext = createRequestContext(
        "mybucket.localhost:9878", "/key1");
    virtualHostStyleFilter.filter(requestContext);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket/key1");
    verify(requestContext).setRequestUri(new URI("http://" + s3HttpAddr), expected);
  }

  @Test
  public void testVirtualHostStyleWithQueryParams() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    URI baseUri = new URI("http://" + s3HttpAddr);
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.add("prefix", "bh");
    ContainerRequestContext requestContext = createRequestContext(
        "mybucket.localhost:9878", null, queryParams);
    virtualHostStyleFilter.filter(requestContext);
    verify(requestContext).setRequestUri(baseUri,
        new URI("http://" + s3HttpAddr + "/mybucket?prefix=bh"));

    queryParams.add("type", "dir");
    requestContext = createRequestContext(
        "mybucket.localhost:9878", null, queryParams);
    virtualHostStyleFilter.filter(requestContext);
    ArgumentCaptor<URI> requestUriCaptor = ArgumentCaptor.forClass(URI.class);
    verify(requestContext).setRequestUri(eq(baseUri), requestUriCaptor.capture());
    assertThat(requestUriCaptor.getValue().getPath()).isEqualTo("/mybucket");
    assertThat(requestUriCaptor.getValue().getQuery().split("&"))
        .containsExactlyInAnyOrder("prefix=bh", "type=dir");

  }

  @ParameterizedTest
  @CsvSource(value = {"mybucket.myhost:9999,No matching domain", "mybucketlocalhost:9878,invalid format"})
  public void testVirtualHostStyleWithInvalidInputs(String hostAddress,
                                                    String expectErrorMessage) throws Exception {
    VirtualHostStyleFilter virtualHostStyleFilter = new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    ContainerRequestContext requestContext = createRequestContext(hostAddress, null);
    InvalidRequestException exception = assertThrows(InvalidRequestException.class,
        () -> virtualHostStyleFilter.filter(requestContext));
    assertThat(exception).hasMessageContaining(expectErrorMessage);
  }
}

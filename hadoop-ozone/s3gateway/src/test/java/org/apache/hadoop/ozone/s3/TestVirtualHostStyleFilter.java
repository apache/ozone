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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.net.URI;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.glassfish.jersey.internal.PropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
    s3HttpAddr = s3HttpAddr.substring(0, s3HttpAddr.lastIndexOf(":"));
    conf.set(S3GatewayConfigKeys.OZONE_S3G_DOMAIN_NAME, s3HttpAddr);
  }

  /**
   * Create containerRequest object.
   * @return ContainerRequest
   * @throws Exception
   */
  public ContainerRequest createContainerRequest(String host, String path,
                                                 String queryParams,
                                                 boolean virtualHostStyle)
      throws Exception {
    URI baseUri = new URI("http://" + s3HttpAddr);
    URI virtualHostStyleUri;
    if (path == null && queryParams == null) {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr);
    } else if (path != null && queryParams == null) {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr + path);
    } else if (path != null && queryParams != null)  {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr + path +
          queryParams);
    } else {
      virtualHostStyleUri = new URI("http://" + s3HttpAddr  + queryParams);
    }
    URI pathStyleUri;
    if (queryParams == null) {
      pathStyleUri = new URI("http://" + s3HttpAddr + path);
    } else {
      pathStyleUri = new URI("http://" + s3HttpAddr + path + queryParams);
    }
    String httpMethod = "DELETE";
    SecurityContext securityContext = mock(SecurityContext.class);
    PropertiesDelegate propertiesDelegate = mock(PropertiesDelegate.class);
    ContainerRequest containerRequest;
    if (virtualHostStyle) {
      containerRequest = new ContainerRequest(baseUri, virtualHostStyleUri,
          httpMethod, securityContext, propertiesDelegate);
      containerRequest.header(HttpHeaders.HOST, host);
    } else {
      containerRequest = new ContainerRequest(baseUri, pathStyleUri,
          httpMethod, securityContext, propertiesDelegate);
      containerRequest.header(HttpHeaders.HOST, host);
    }
    return containerRequest;
  }

  @Test
  public void testVirtualHostStyle() throws  Exception {
    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);


    ContainerRequest containerRequest = createContainerRequest("mybucket" +
            ".localhost:9878", "/myfile", null, true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket/myfile");
    assertEquals(expected, containerRequest.getRequestUri());
  }

  @Test
  public void testPathStyle() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequest containerRequest = createContainerRequest(s3HttpAddr,
        "/mybucket/myfile", null, false);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr +
        "/mybucket/myfile");
    assertEquals(expected, containerRequest.getRequestUri());

  }

  @Test
  public void testVirtualHostStyleWithCreateBucketRequest() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".localhost:9878", null, null, true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket");
    assertEquals(expected, containerRequest.getRequestUri());

  }

  @Test
  public void testVirtualHostStyleWithCreateKeyRequest() throws Exception {
    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);

    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".localhost:9878", "/key1", null, true);
    virtualHostStyleFilter.filter(containerRequest);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket/key1");
    assertEquals(expected, containerRequest.getRequestUri());
  }

  @Test
  public void testVirtualHostStyleWithQueryParams() throws Exception {

    VirtualHostStyleFilter virtualHostStyleFilter =
        new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    URI expected = new URI("http://" + s3HttpAddr + "/mybucket?prefix=bh");
    ContainerRequest containerRequest = createContainerRequest("mybucket" +
        ".localhost:9878", null, "?prefix=bh", true);
    virtualHostStyleFilter.filter(containerRequest);
    assertThat(expected.toString())
        .contains(containerRequest.getRequestUri().toString());

    containerRequest = createContainerRequest("mybucket" +
        ".localhost:9878", null, "?prefix=bh&type=dir", true);
    virtualHostStyleFilter.filter(containerRequest);
    expected = new URI("http://" + s3HttpAddr +
        "/mybucket?prefix=bh&type=dir");
    assertThat(expected.toString()).contains(containerRequest.getRequestUri().toString());

  }

  @ParameterizedTest
  @CsvSource(value = {"mybucket.myhost:9999,No matching domain", "mybucketlocalhost:9878,invalid format"})
  public void testVirtualHostStyleWithInvalidInputs(String hostAddress,
                                                    String expectErrorMessage) throws Exception {
    VirtualHostStyleFilter virtualHostStyleFilter = new VirtualHostStyleFilter();
    virtualHostStyleFilter.setConfiguration(conf);
    ContainerRequest containerRequest = createContainerRequest(hostAddress, null, null, true);
    InvalidRequestException exception = assertThrows(InvalidRequestException.class,
        () -> virtualHostStyleFilter.filter(containerRequest));
    assertThat(exception).hasMessageContaining(expectErrorMessage);
  }
}

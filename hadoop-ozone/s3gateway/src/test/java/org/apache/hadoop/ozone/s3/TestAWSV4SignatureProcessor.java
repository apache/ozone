/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV4;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the Auth parser.
 */
public class TestAWSV4SignatureProcessor {

  @Test
  public void testInitialization() throws Exception {

    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.putSingle("Content-Length", "123");
    headers.putSingle("Host", "123");
    headers.putSingle("X-AMZ-Content-Sha256", "Content-SHA");
    headers.putSingle("X-AMZ-Date", "123");
    headers.putSingle("Content-Type", "ozone/mpu");
    headers.putSingle(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE, "streaming");

    String authHeader =
        "AWS4-HMAC-SHA256 Credential=AKIAJWFJK62WUTKNFJJA/20181009/us-east-1"
            + "/s3/aws4_request, "
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date;"
            + "content-type, "
            + "Signature"
            +
            "=db81b057718d7c1b3b8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";
    headers.putSingle("Authorization",
        authHeader);

    AuthorizationHeaderV4 parserAuthHeader =
        new AuthorizationHeaderV4(authHeader) {
          @Override
          public void validateDateRange() throws OS3Exception {
          }
        };

    MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();

    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    Mockito.when(uriInfo.getQueryParameters()).thenReturn(queryParameters);
    Mockito.when(uriInfo.getRequestUri())
        .thenReturn(new URI("http://localhost/buckets"));

    ContainerRequestContext mock = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(mock.getHeaders()).thenReturn(headers);
    Mockito.when(mock.getMethod()).thenReturn("GET");
    Mockito.when(mock.getUriInfo()).thenReturn(uriInfo);

    AWSV4SignatureProcessor parser = new AWSV4SignatureProcessor() {
      @Override
      void validateSignedHeader(String header, String headerValue)
          throws OS3Exception {
        super.validateSignedHeader(header, headerValue);
      }
    };
    parser.setV4Header(parserAuthHeader);
    parser.setContext(mock);
    parser.init();

    Assert.assertTrue(
        "the ozone/mpu header is not changed back before signature processing",
        parser.buildCanonicalRequest().contains("content-type:streaming"));

    Assert.assertEquals(
        "String to sign is invalid",
        "AWS4-HMAC-SHA256\n"
            + "123\n"
            + "20181009/us-east-1/s3/aws4_request\n"
            +
            "21478b9cf7d628717e9029a93c4ddc35bb38cf50669ada529b94673b23ff23e2",
        parser.getStringToSign());
  }
}
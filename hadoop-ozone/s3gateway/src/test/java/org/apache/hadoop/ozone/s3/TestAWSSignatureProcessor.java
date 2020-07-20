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
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV2;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV4;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the Auth parser.
 */
public class TestAWSSignatureProcessor {

  @Test
  public void testV4Initialization() throws Exception {

    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.putSingle("Content-Length", "123");
    headers.putSingle("Host", "0.0.0.0:9878");
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

    AWSSignatureProcessor parser = new AWSSignatureProcessor() {
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
            "f20d4de80af2271545385e8d4c7df608cae70a791c69b97aab1527ed93a0d665",
        parser.getStringToSign());
  }

  @Test
  public void testV2Initialization() throws Exception {

    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    String authHeader = "AWS root:ixWQAgWvJDuqLUqgDG9o4b2HF7c=";
    headers.putSingle("Authorization", authHeader);

    AuthorizationHeaderV2 parserAuthHeader =
        new AuthorizationHeaderV2(authHeader);

    MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();

    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    Mockito.when(uriInfo.getQueryParameters()).thenReturn(queryParameters);
    Mockito.when(uriInfo.getRequestUri())
        .thenReturn(new URI("http://localhost/buckets"));

    ContainerRequestContext mock = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(mock.getHeaders()).thenReturn(headers);
    Mockito.when(mock.getMethod()).thenReturn("GET");
    Mockito.when(mock.getUriInfo()).thenReturn(uriInfo);

    AWSSignatureProcessor parser = new AWSSignatureProcessor() {
      @Override
      void validateSignedHeader(String header, String headerValue)
          throws OS3Exception {
        super.validateSignedHeader(header, headerValue);
      }
    };
    parser.setV2Header(parserAuthHeader);
    parser.setContext(mock);
    parser.init();

    Assert.assertEquals("root", parser.getAwsAccessId());
    Assert.assertEquals("ixWQAgWvJDuqLUqgDG9o4b2HF7c=", parser.getSignature());
  }
}
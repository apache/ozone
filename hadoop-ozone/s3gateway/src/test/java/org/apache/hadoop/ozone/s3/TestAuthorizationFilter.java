/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.stream.Stream;

import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.signature.SignatureParser.AUTHORIZATION_HEADER;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.CONTENT_MD5;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.CONTENT_TYPE;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.HOST_HEADER;
import static org.apache.hadoop.ozone.s3.signature.StringToSignProducer.X_AMAZ_DATE;
import static org.apache.hadoop.ozone.s3.signature.StringToSignProducer.X_AMZ_CONTENT_SHA256;
import static org.junit.Assert.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * This class test string to sign generation.
 */
public class TestAuthorizationFilter {

  private AuthorizationFilter authorizationFilter = new AuthorizationFilter();

  private MultivaluedMap<String, String> headerMap;
  private MultivaluedMap<String, String> queryMap;
  private MultivaluedMap<String, String> pathParamsMap;

  private static Stream<Arguments>testAuthFilterFailuresInput() {
    return Stream.of(
        arguments(
            "AWS4-HMAC-SHA256 Credential=testuser1/20190221/us-west-1/s3" +
                "/aws4_request, SignedHeaders=content-md5;host;" +
                "x-amz-content-sha256;x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Zi68x2nPDDXv5qfDC+ZWTg==",
            "s3g:9878",
            "e2bd43f11c97cde3465e0e8d1aad77af7ec7aa2ed8e213cd0e24" +
                "1e28375860c6",
            "20190221T002037Z",
            "",
            "/"
        ),
        arguments(
            "AWS4-HMAC-SHA256 " +
                "Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request," +
                " SignedHeaders=content-type;host;x-amz-date, " +
                "Signature=" +
                "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400" +
                "e06b5924a6f2b5d7",
            "",
            "iam.amazonaws.com",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "20150830T123600Z",
            "application/x-www-form-urlencoded; charset=utf-8",
            ""
        ),
        arguments(null, null, null, null, null, null, null),
        arguments("", null, null, null, null, null, null),
        // AWS V2 signature
        arguments(
            "AWS AKIDEXAMPLE:St7bHPOdkmsX/GITGe98rOQiUCg=",
            "",
            "s3g:9878",
            "",
            "Wed, 22 Mar 2023 17:00:06 +0000",
            "application/octet-stream",
            "/"
        )
    );
  }

  @ParameterizedTest
  @MethodSource("testAuthFilterFailuresInput")
  public void testAuthFilterFailures(
      String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType,
      String path
  ) {
    headerMap = new MultivaluedHashMap<>();
    queryMap = new MultivaluedHashMap<>();
    pathParamsMap = new MultivaluedHashMap<>();
    try {
      System.err.println("Testing: " + authHeader);
      headerMap.putSingle(AUTHORIZATION_HEADER, authHeader);
      headerMap.putSingle(CONTENT_MD5, contentMd5);
      headerMap.putSingle(HOST_HEADER, host);
      headerMap.putSingle(X_AMZ_CONTENT_SHA256, amzContentSha256);
      headerMap.putSingle(X_AMAZ_DATE, date);
      headerMap.putSingle(CONTENT_TYPE, contentType);

      UriInfo uriInfo = Mockito.mock(UriInfo.class);
      ContainerRequestContext context = Mockito.mock(
          ContainerRequestContext.class);
      Mockito.when(uriInfo.getQueryParameters()).thenReturn(queryMap);
      Mockito.when(uriInfo.getRequestUri()).thenReturn(
          new URI("http://" + host + path));

      Mockito.when(context.getUriInfo()).thenReturn(uriInfo);
      Mockito.when(context.getHeaders()).thenReturn(headerMap);
      Mockito.when(context.getHeaderString(AUTHORIZATION_HEADER))
          .thenReturn(authHeader);
      Mockito.when(context.getUriInfo().getQueryParameters())
          .thenReturn(queryMap);
      Mockito.when(context.getUriInfo().getPathParameters())
          .thenReturn(pathParamsMap);

      AWSSignatureProcessor awsSignatureProcessor = new AWSSignatureProcessor();
      awsSignatureProcessor.setContext(context);

      SignatureInfo signatureInfo = new SignatureInfo();

      authorizationFilter.setSignatureParser(awsSignatureProcessor);
      authorizationFilter.setSignatureInfo(signatureInfo);

      authorizationFilter.filter(context);
      if ("".equals(authHeader)) {
        fail("Empty AuthHeader must fail");
      }
    } catch (WebApplicationException ex) {
      if (authHeader == null || authHeader.isEmpty() ||
              authHeader.startsWith("AWS ")) {
        // Empty auth header and unsupported AWS signature
        // should fail with Invalid Request.
        Assert.assertEquals(HTTP_FORBIDDEN, ex.getResponse().getStatus());
        Assert.assertEquals(S3_AUTHINFO_CREATION_ERROR.getErrorMessage(),
            ex.getMessage());
      } else {
        // Other requests have stale timestamp and
        // should fail with Malformed Authorization Header.
        Assert.assertEquals(HTTP_BAD_REQUEST, ex.getResponse().getStatus());
        Assert.assertEquals(MALFORMED_HEADER.getErrorMessage(),
            ex.getMessage());

      }

    } catch (Exception ex) {
      fail("Unexpected exception: " + ex);
    }
  }

  //testStrToSign generation

}

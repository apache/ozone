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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.apache.kerby.util.Hex;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;

import static org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.DATE_FORMATTER;
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

  private static final String DATETIME = StringToSignProducer.TIME_FORMATTER.
      format(LocalDateTime.now());

  private static final String CURDATE = DATE_FORMATTER.format(LocalDate.now());

  private static Stream<Arguments>testAuthFilterFailuresInput() {
    return Stream.of(
        arguments(
            "GET",
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
            "/",
            MALFORMED_HEADER.getErrorMessage()
        ),
        arguments(
            "GET",
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
            "",
            MALFORMED_HEADER.getErrorMessage()
        ),
        arguments(null, null, null, null, null, null, null, null,
            S3_AUTHINFO_CREATION_ERROR.getErrorMessage()),
        arguments(null, "", null, null, null, null, null, null,
            S3_AUTHINFO_CREATION_ERROR.getErrorMessage()),
        // AWS V2 signature
        arguments(
            "GET",
            "AWS AKIDEXAMPLE:St7bHPOdkmsX/GITGe98rOQiUCg=",
            "",
            "s3g:9878",
            "",
            "Wed, 22 Mar 2023 17:00:06 +0000",
            "application/octet-stream",
            "/",
            S3_AUTHINFO_CREATION_ERROR.getErrorMessage()
        )
    );
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  @ParameterizedTest
  @MethodSource("testAuthFilterFailuresInput")
  public void testAuthFilterFailures(
      String method, String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType,
      String path, String expectedErrorMsg
  ) {
    try {
      ContainerRequestContext context = setupContext(method, authHeader,
          contentMd5, host, amzContentSha256, date, contentType, path);

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
        Assert.assertEquals(expectedErrorMsg,
            ex.getMessage());
      } else {
        // Other requests have stale timestamp and
        // should fail with Malformed Authorization Header.
        Assert.assertEquals(HTTP_BAD_REQUEST, ex.getResponse().getStatus());
        Assert.assertEquals(expectedErrorMsg,
            ex.getMessage());

      }

    } catch (Exception ex) {
      fail("Unexpected exception: " + ex);
    }
  }

  private static Stream<Arguments>testAuthFilterInput() {
    return Stream.of(
        // Path style URI
        arguments(
            "GET",
            "AWS4-HMAC-SHA256 Credential=testuser1/" + CURDATE +
                "/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host;x-amz-content-sha256;" +
                "x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Content-SHA",
            "s3g:9878",
            "Content-SHA",
            DATETIME,
            "",
            "/bucket1/key1"
        ),
        // Virtual style URI
        arguments(
            "GET",
            "AWS4-HMAC-SHA256 Credential=testuser1/" + CURDATE +
                "/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host;x-amz-content-sha256;" +
                "x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Content-SHA",
            "bucket1.s3g.internal:9878",
            "Content-SHA",
            DATETIME,
            "",
            "/key1"
        ),
        // S3 secret generation endpoint
        arguments(
            "POST",
            null,
            null,
            "s3g:9878",
            null,
            null,
            "",
            "/secret/generate"
        ),
        // S3 secret generation endpoint
        arguments(
            "POST",
            null,
            null,
            "s3g:9878",
            null,
            null,
            "",
            "/secret/revoke"
        )
    );
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  @ParameterizedTest
  @MethodSource("testAuthFilterInput")
  public void testAuthFilter(
      String method, String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType,
      String path
  ) {
    try {
      ContainerRequestContext context = setupContext(method, authHeader,
          contentMd5, host, amzContentSha256, date, contentType, path);

      AWSSignatureProcessor awsSignatureProcessor = new AWSSignatureProcessor();
      awsSignatureProcessor.setContext(context);

      SignatureInfo signatureInfo = new SignatureInfo();

      authorizationFilter.setSignatureParser(awsSignatureProcessor);
      authorizationFilter.setSignatureInfo(signatureInfo);

      authorizationFilter.filter(context);

      if (path.startsWith("/secret")) {
        Assert.assertNull(
            authorizationFilter.getSignatureInfo().getUnfilteredURI());

        Assert.assertNull(
            authorizationFilter.getSignatureInfo().getStringToSign());
      } else {
        String canonicalRequest = method + "\n"
            + path + "\n"
            + "\n"
            + "host:" + host + "\nx-amz-content-sha256:" + amzContentSha256 +
            "\n"
            + "x-amz-date:" + DATETIME + "\n"
            + "\n"
            + "host;x-amz-content-sha256;x-amz-date\n"
            + amzContentSha256;

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(canonicalRequest.getBytes(StandardCharsets.UTF_8));

        String expectedStrToSign = "AWS4-HMAC-SHA256\n"
            + DATETIME + "\n"
            + CURDATE + "/us-east-1/s3/aws4_request\n"
            + Hex.encode(md.digest()).toLowerCase();

        Assert.assertEquals("Unfiltered URI is not preserved",
            path,
            authorizationFilter.getSignatureInfo().getUnfilteredURI());

        Assert.assertEquals("String to sign is invalid",
            expectedStrToSign,
            authorizationFilter.getSignatureInfo().getStringToSign());
      }
    } catch (Exception ex) {
      fail("Unexpected exception: " + ex);
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private ContainerRequestContext setupContext(
      String method, String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType,
      String path) throws URISyntaxException {
    headerMap = new MultivaluedHashMap<>();
    queryMap = new MultivaluedHashMap<>();
    pathParamsMap = new MultivaluedHashMap<>();

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

    Mockito.when(context.getMethod()).thenReturn(method);
    Mockito.when(context.getUriInfo()).thenReturn(uriInfo);
    Mockito.when(context.getHeaders()).thenReturn(headerMap);
    Mockito.when(context.getHeaderString(AUTHORIZATION_HEADER))
        .thenReturn(authHeader);
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(queryMap);
    Mockito.when(context.getUriInfo().getPathParameters())
        .thenReturn(pathParamsMap);

    return context;
  }

}

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

package org.apache.hadoop.ozone.s3.signature;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.LowerCaseKeyStringMap;
import org.apache.kerby.util.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test string2sign creation.
 */
public class TestStringToSignProducer {

  private static final String DATETIME = StringToSignProducer.TIME_FORMATTER.
          format(LocalDateTime.now());

  @Test
  public void test() throws Exception {

    LowerCaseKeyStringMap headers = new LowerCaseKeyStringMap();
    headers.put("Content-Length", "123");
    headers.put("Host", "0.0.0.0:9878");
    headers.put("X-AMZ-Content-Sha256", "Content-SHA");
    headers.put("X-AMZ-Date", DATETIME);
    headers.put("Content-Type", "ozone/mpu");
    headers.put(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE, "streaming");

    String canonicalRequest = "GET\n"
        + "/buckets\n"
        + "\n"
        + "host:0.0.0.0:9878\nx-amz-content-sha256:Content-SHA\n"
        + "x-amz-date:" + DATETIME + "\ncontent-type:streaming\n"
        + "\n"
        + "host;x-amz-content-sha256;x-amz-date;content-type\n"
        + "Content-SHA";

    String authHeader =
        "AWS4-HMAC-SHA256 Credential=AKIAJWFJK62WUTKNFJJA/20181009/us-east-1"
            + "/s3/aws4_request, "
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date;"
            + "content-type, "
            + "Signature"
            +
            "=db81b057718d7c1b3b8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";

    headers.put("Authorization",
        authHeader);

    Map<String, String> queryParameters = new HashMap<>();

    final SignatureInfo signatureInfo =
        new AuthorizationV4HeaderParser(authHeader, DATETIME) {
          @Override
          public void validateDateRange(Credential credentialObj) {
            //NOOP
          }
        }.parseSignature();
    signatureInfo.setPayloadHash("Content-SHA");
    signatureInfo.setUnfilteredURI("/buckets");

    headers.fixContentType();

    final String signatureBase =
        StringToSignProducer.createSignatureBase(
            signatureInfo,
            "http",
            "GET",
            headers,
            queryParameters);

    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(canonicalRequest.getBytes(StandardCharsets.UTF_8));

    assertEquals("AWS4-HMAC-SHA256\n"
            + DATETIME + "\n"
            + "20181009/us-east-1/s3/aws4_request\n"
            + Hex.encode(md.digest()).toLowerCase(),
        signatureBase, "String to sign is invalid");
  }

  private ContainerRequestContext setupContext(
      URI uri,
      String method,
      MultivaluedMap<String, String> headerMap,
      MultivaluedMap<String, String> queryMap) {
    ContainerRequestContext context =
        mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);

    when(uriInfo.getRequestUri()).thenReturn(uri);
    when(uriInfo.getQueryParameters()).thenReturn(queryMap);

    when(context.getUriInfo()).thenReturn(uriInfo);
    when(context.getMethod()).thenReturn(method);
    when(context.getHeaders()).thenReturn(headerMap);

    return context;
  }

  private static Stream<Arguments> testValidateRequestHeadersInput() {
    String authHeader = "AWS4-HMAC-SHA256 Credential=ozone/"
        + DATE_FORMATTER.format(LocalDate.now())
        + "/us-east-1/s3/aws4_request, "
        + "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date,"
        + " Signature=db81b057718d7c1b3b8"
        + "dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";

    // Well-formed request headers
    MultivaluedMap<String, String> headersMap1 =
        new MultivaluedHashMap<>();
    headersMap1.putSingle("Authorization", authHeader);
    headersMap1.putSingle("Content-Type", "application/octet-stream");
    headersMap1.putSingle("Host", "0.0.0.0:9878");
    headersMap1.putSingle("X-Amz-Content-Sha256", "Content-SHA");
    headersMap1.putSingle("X-Amz-Date", DATETIME);
    //Missing X-Amz-Date Header
    MultivaluedMap<String, String> headersMap2 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap2.remove("X-Amz-Date");
    // Invalid X-Amz-Date format
    MultivaluedMap<String, String> headersMap3 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap3.remove("X-Amz-Date");
    headersMap3.putSingle("X-Amz-Date", LocalDateTime.now().toString());
    // Expired X-Amz-Date
    MultivaluedMap<String, String> headersMap4 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap4.remove("X-Amz-Date");
    headersMap4.putSingle("X-Amz-Date", StringToSignProducer.TIME_FORMATTER.
        format(LocalDateTime.now().minusDays(8)));
    MultivaluedMap<String, String> headersMap5 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap5.remove("X-Amz-Date");
    headersMap5.putSingle("X-Amz-Date", StringToSignProducer.TIME_FORMATTER.
        format(LocalDateTime.now().plusDays(8)));
    // Missing X-Amz-Content-Sha256
    MultivaluedMap<String, String> headersMap6 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap6.remove("X-Amz-Content-Sha256");

    return Stream.of(
        arguments(headersMap1, "success"),
        arguments(headersMap2, S3_AUTHINFO_CREATION_ERROR.getCode()),
        arguments(headersMap3, S3_AUTHINFO_CREATION_ERROR.getCode()),
        arguments(headersMap4, S3_AUTHINFO_CREATION_ERROR.getCode()),
        arguments(headersMap5, S3_AUTHINFO_CREATION_ERROR.getCode()),
        arguments(headersMap6, S3_AUTHINFO_CREATION_ERROR.getCode())
    );
  }

  @ParameterizedTest
  @MethodSource("testValidateRequestHeadersInput")
  public void testValidateRequestHeaders(
      MultivaluedMap<String, String> headerMap,
      String expectedResult)
      throws Exception {
    String actualResult = "success";
    ContainerRequestContext context = setupContext(
        new URI("https://0.0.0.0:9878/"),
        "GET",
        headerMap,
        new MultivaluedHashMap<>());
    SignatureInfo signatureInfo = new AuthorizationV4HeaderParser(
        headerMap.getFirst("Authorization"),
        headerMap.getFirst("X-Amz-Date")).parseSignature();
    signatureInfo.setUnfilteredURI("/");
    try {
      StringToSignProducer.createSignatureBase(signatureInfo, context);
    } catch (OS3Exception e) {
      actualResult = e.getCode();
    }

    assertEquals(expectedResult, actualResult);
  }

  private static Stream<Arguments> testValidateCanonicalHeadersInput() {
    return Stream.of(
        // Well-formed signed headers
        arguments("content-type;host;x-amz-content-sha256;" +
                "x-amz-date;x-amz-security-token",
            "success"),
        // No host signed header
        arguments("content-type;x-amz-content-sha256;" +
                "x-amz-date;x-amz-security-token",
            S3_AUTHINFO_CREATION_ERROR.getCode()),
        // Missing x-amz-* i.e., x-amz-security-token signed header
        arguments("content-type;host;x-amz-content-sha256;x-amz-date",
            S3_AUTHINFO_CREATION_ERROR.getCode()),
        // signed header missing in request headers
        arguments("content-type;dummy;host;x-amz-content-sha256;" +
                "x-amz-date;x-amz-security-token",
            S3_AUTHINFO_CREATION_ERROR.getCode())
    );
  }

  @ParameterizedTest
  @MethodSource("testValidateCanonicalHeadersInput")
  public void testValidateCanonicalHeaders(
      String signedHeaders,
      String expectedResult) throws Exception {
    String actualResult = "success";
    String authHeader = "AWS4-HMAC-SHA256 Credential=ozone/"
        + DATE_FORMATTER.format(LocalDate.now())
        + "/us-east-1/s3/aws4_request, "
        + "SignedHeaders=" + signedHeaders + ", "
        + "Signature=db81b057718d7c1b3b" +
        "8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";
    MultivaluedMap<String, String> headerMap = new MultivaluedHashMap<>();
    headerMap.putSingle("Authorization", authHeader);
    headerMap.putSingle("Content-Length", "123");
    headerMap.putSingle("content-type", "application/octet-stream");
    headerMap.putSingle("host", "0.0.0.0:9878");
    headerMap.putSingle("x-amz-content-sha256", "Content-SHA");
    headerMap.putSingle("x-amz-date", DATETIME);
    headerMap.putSingle("x-amz-security-token", "dummy");
    ContainerRequestContext context = setupContext(
        new URI("https://0.0.0.0:9878/"),
        "GET",
        headerMap,
        new MultivaluedHashMap<>());
    SignatureInfo signatureInfo = new AuthorizationV4HeaderParser(
        headerMap.getFirst("Authorization"),
        headerMap.getFirst("x-amz-date")).parseSignature();
    signatureInfo.setUnfilteredURI("/");

    try {
      StringToSignProducer.createSignatureBase(signatureInfo, context);
    } catch (OS3Exception e) {
      actualResult = e.getCode();
    }

    assertEquals(expectedResult, actualResult);
  }
}

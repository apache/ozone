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

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_CREDENTIAL_DATE;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.REQUEST_TIME_TOO_SKEWED;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests Authorization header format v2.
 */

public class TestAuthorizationV4HeaderParser {

  private String curDate;
  private String sampleDate;

  @BeforeEach
  public void setup() {
    LocalDate now = LocalDate.now(ZoneOffset.UTC);
    curDate = DATE_FORMATTER.format(now);
    sampleDate = StringToSignProducer.TIME_FORMATTER.format(
        LocalDateTime.now(ZoneOffset.UTC));
  }

  @Test
  public void testV4HeaderWellFormed() throws Exception {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=ozone/" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, sampleDate);
    final SignatureInfo signatureInfo = v4.parseSignature();
    assertEquals("ozone", signatureInfo.getAwsAccessId());
    assertEquals(curDate, signatureInfo.getDate());
    assertEquals("host;range;x-amz-date", signatureInfo.getSignedHeaders());
    assertEquals("fe5f80f77d5fa3beca038a248ff027",
        signatureInfo.getSignature());
  }

  @Test
  public void testV4HeaderMissingParts() {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=ozone/" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date,";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, sampleDate);
    assertThrows(MalformedResourceException.class, () -> v4.parseSignature());
  }

  @Test
  public void testV4HeaderInvalidCredential() {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, sampleDate);
    assertThrows(MalformedResourceException.class, () -> v4.parseSignature());
  }

  @Test
  public void testV4HeaderWithoutSpace() throws MalformedResourceException {

    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";

    AuthorizationV4HeaderParser v4 = new AuthorizationV4HeaderParser(auth,
        sampleDate);
    SignatureInfo signature = v4.parseSignature();

    assertEquals("AWS4-HMAC-SHA256", signature.getAlgorithm());
    assertEquals("ozone", signature.getAwsAccessId());
    assertEquals(curDate, signature.getDate());
    assertEquals("host;x-amz-content-sha256;x-amz-date",
        signature.getSignedHeaders());
    assertEquals("fe5f80f77d5fa3beca038a248ff027", signature.getSignature());

  }

  @Test
  public void testV4HeaderDateValidationSuccess()
      throws MalformedResourceException {
    testRequestWithSpecificDate(curDate);

    String amzDatePlus14Min = StringToSignProducer.TIME_FORMATTER.format(
        LocalDateTime.now(ZoneOffset.UTC).plus(14, MINUTES));
    testRequestWithSpecificDate(curDate, amzDatePlus14Min);
  }

  @Test
  public void testV4HeaderCredentialDateMismatch() {
    LocalDate yesterday = LocalDate.now(ZoneOffset.UTC).minus(1, DAYS);
    String mismatchedCredentialDate = DATE_FORMATTER.format(yesterday);
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + mismatchedCredentialDate
            + "/us-east-1/s3/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, sampleDate);
    MalformedResourceException ex = assertThrows(MalformedResourceException.class,
        () -> v4.parseSignature());
    assertEquals(MALFORMED_CREDENTIAL_DATE, ex.getErrorCode());
  }

  @Test
  public void testV4HeaderDateSkewValidationFailure() {
    String staleDateTime = StringToSignProducer.TIME_FORMATTER.format(
        LocalDateTime.now(ZoneOffset.UTC).minusMinutes(16));
    String auth = "AWS4-HMAC-SHA256 Credential=ozone/" + curDate
        + "/us-east-1/s3/aws4_request, "
        + "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
        + "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, staleDateTime);
    MalformedResourceException ex = assertThrows(MalformedResourceException.class,
        () -> v4.parseSignature());
    assertEquals(REQUEST_TIME_TOO_SKEWED, ex.getErrorCode());
  }

  @Test
  public void testV4HeaderDateValidationFailure() {
    // Case 1: Empty date.
    String dateStr = "";
    assertThrows(MalformedResourceException.class,
        () -> testRequestWithSpecificDate(dateStr));

    // Case 2: Invalid date format
    String dateStr2 = LocalDate.now(ZoneOffset.UTC).toString();
    assertThrows(MalformedResourceException.class,
        () -> testRequestWithSpecificDate(dateStr2));
  }

  private void testRequestWithSpecificDate(String dateStr)
      throws MalformedResourceException {
    testRequestWithSpecificDate(dateStr, sampleDate);
  }

  private void testRequestWithSpecificDate(String dateStr, String amzDate)
      throws MalformedResourceException {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + dateStr + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, amzDate);
    SignatureInfo signature = v4.parseSignature();

    assertEquals("AWS4-HMAC-SHA256", signature.getAlgorithm());
    assertEquals("ozone", signature.getAwsAccessId());
    assertEquals(dateStr, signature.getDate());
    assertEquals(amzDate, signature.getDateTime());
    assertEquals("host;x-amz-content-sha256;x-amz-date",
        signature.getSignedHeaders());
    assertEquals("fe5f80f77d5fa3beca038a248ff027", signature.getSignature());
  }

  @Test
  public void testV4HeaderRegionValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate +
            "//s3/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027%";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth, sampleDate)
            .parseSignature());
    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "s3/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027%";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, sampleDate)
            .parseSignature());
  }

  @Test
  public void testV4HeaderServiceValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1" +
            "//aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth, sampleDate)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, sampleDate)
            .parseSignature());
  }

  @Test
  public void testV4HeaderRequestValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/   ,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth, sampleDate)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, sampleDate)
            .parseSignature());

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            ","
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth3, sampleDate)
            .parseSignature());

    String auth4 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/invalid_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
            () -> new AuthorizationV4HeaderParser(auth4, sampleDate)
                .parseSignature());
  }

  @Test
  public void testV4HeaderSignedHeaderValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=;;,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth, sampleDate)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, sampleDate)
            .parseSignature());

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "=x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth3, sampleDate)
            .parseSignature());

    String auth4 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "=,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth4, sampleDate)
            .parseSignature());
  }

  @Test
  public void testV4HeaderSignatureValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027%";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth, sampleDate)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, sampleDate)
            .parseSignature());

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "=";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth3, sampleDate)
            .parseSignature());
  }

  @Test
  public void testV4HeaderHashAlgoValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth, sampleDate)
            .parseSignature());

    String auth2 =
        "SHA-256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertNull(new AuthorizationV4HeaderParser(auth2, sampleDate)
        .parseSignature());

    String auth3 =
        " Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertNull(new AuthorizationV4HeaderParser(auth3, sampleDate)
        .parseSignature());

    // Invalid algorithm
    String auth4 = "AWS4-ZAVC-HJUA123 " +
        "Credential=" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth4, sampleDate)
            .parseSignature());
  }

  @Test
  public void testV4HeaderCredentialValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA Credential=/" + curDate + "//" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth, sampleDate)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA =/" + curDate + "//" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, sampleDate)
            .parseSignature());
  }

}

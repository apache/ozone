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
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests Authorization header format v2.
 */

public class TestAuthorizationV4HeaderParser {

  private static final String SAMPLE_DATE = "20210202T144559Z";

  private String curDate;

  @BeforeEach
  public void setup() {
    LocalDate now = LocalDate.now();
    curDate = DATE_FORMATTER.format(now);
  }

  @Test
  public void testV4HeaderWellFormed() throws Exception {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=ozone/" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, SAMPLE_DATE);
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
        new AuthorizationV4HeaderParser(auth, SAMPLE_DATE);
    assertThrows(MalformedResourceException.class, () -> v4.parseSignature());
  }

  @Test
  public void testV4HeaderInvalidCredential() {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, SAMPLE_DATE);
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
        SAMPLE_DATE);
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
    // Case 1: valid date within range.
    LocalDate now = LocalDate.now();
    String dateStr = DATE_FORMATTER.format(now);
    testRequestWithSpecificDate(dateStr);

    // Case 2: Valid date with in range.
    dateStr = DATE_FORMATTER.format(now.plus(1, DAYS));
    testRequestWithSpecificDate(dateStr);

    // Case 3: Valid date with in range.
    dateStr = DATE_FORMATTER.format(now.minus(1, DAYS));
    testRequestWithSpecificDate(dateStr);
  }

  @Test
  public void testV4HeaderDateValidationFailure() {
    // Case 1: Empty date.
    LocalDate now = LocalDate.now();
    String dateStr = "";
    assertThrows(MalformedResourceException.class,
        () -> testRequestWithSpecificDate(dateStr));

    // Case 2: Date after yesterday.
    String dateStr2 = DATE_FORMATTER.format(now.plus(2, DAYS));
    assertThrows(MalformedResourceException.class,
        () -> testRequestWithSpecificDate(dateStr2));

    // Case 3: Date before yesterday.
    String dateStr3 = DATE_FORMATTER.format(now.minus(2, DAYS));
    assertThrows(MalformedResourceException.class,
        () -> testRequestWithSpecificDate(dateStr3));

    // Case 4: Invalid date format
    String dateStr4 = now.toString();
    assertThrows(MalformedResourceException.class,
        () -> testRequestWithSpecificDate(dateStr4));
  }

  private void testRequestWithSpecificDate(String dateStr)
      throws MalformedResourceException {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + dateStr + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationV4HeaderParser v4 =
        new AuthorizationV4HeaderParser(auth, SAMPLE_DATE);
    SignatureInfo signature = v4.parseSignature();

    assertEquals("AWS4-HMAC-SHA256", signature.getAlgorithm());
    assertEquals("ozone", signature.getAwsAccessId());
    assertEquals(dateStr, signature.getDate());
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
        () -> new AuthorizationV4HeaderParser(auth, SAMPLE_DATE)
            .parseSignature());
    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "s3/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027%";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, SAMPLE_DATE)
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
        () -> new AuthorizationV4HeaderParser(auth, SAMPLE_DATE)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, SAMPLE_DATE)
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
        () -> new AuthorizationV4HeaderParser(auth, SAMPLE_DATE)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, SAMPLE_DATE)
            .parseSignature());

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            ","
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth3, SAMPLE_DATE)
            .parseSignature());

    String auth4 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/invalid_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
            () -> new AuthorizationV4HeaderParser(auth4, SAMPLE_DATE)
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
        () -> new AuthorizationV4HeaderParser(auth, SAMPLE_DATE)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, SAMPLE_DATE)
            .parseSignature());

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "=x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth3, SAMPLE_DATE)
            .parseSignature());

    String auth4 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "=,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth4, SAMPLE_DATE)
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
        () -> new AuthorizationV4HeaderParser(auth, SAMPLE_DATE)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, SAMPLE_DATE)
            .parseSignature());

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "=";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth3, SAMPLE_DATE)
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
        () -> new AuthorizationV4HeaderParser(auth, SAMPLE_DATE)
            .parseSignature());

    String auth2 =
        "SHA-256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertNull(new AuthorizationV4HeaderParser(auth2, SAMPLE_DATE)
        .parseSignature());

    String auth3 =
        " Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertNull(new AuthorizationV4HeaderParser(auth3, SAMPLE_DATE)
        .parseSignature());

    // Invalid algorithm
    String auth4 = "AWS4-ZAVC-HJUA123 " +
        "Credential=" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth4, SAMPLE_DATE)
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
        () -> new AuthorizationV4HeaderParser(auth, SAMPLE_DATE)
            .parseSignature());

    String auth2 =
        "AWS4-HMAC-SHA =/" + curDate + "//" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    assertThrows(MalformedResourceException.class,
        () -> new AuthorizationV4HeaderParser(auth2, SAMPLE_DATE)
            .parseSignature());
  }

}

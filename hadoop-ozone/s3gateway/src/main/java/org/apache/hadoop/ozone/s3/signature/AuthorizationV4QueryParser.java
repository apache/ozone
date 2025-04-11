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

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.AWS4_SIGNING_ALGORITHM;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlDecode;

import com.google.common.annotations.VisibleForTesting;
import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;

/**
 * Parser for getting auth info from query parameters.
 * <p>
 * See: https://docs.aws.amazon
 * .com/AmazonS3/latest/API/sigv4-query-string-auth.html
 */
public class AuthorizationV4QueryParser implements SignatureParser {

  private final Map<String, String> queryParameters;

  private static final Long X_AMZ_EXPIRES_MIN = 1L;
  private static final Long X_AMZ_EXPIRES_MAX = 604800L;
  private static final String AWS_REQUEST = "aws4_request";

  public AuthorizationV4QueryParser(
      Map<String, String> queryParameters
  ) {
    this.queryParameters = queryParameters;
  }

  @Override
  public SignatureInfo parseSignature() throws MalformedResourceException {

    if (!queryParameters.containsKey("X-Amz-Signature")) {
      return null;
    }

    validateAlgorithm();
    try {
      validateDateAndExpires();
    } catch (DateTimeParseException ex) {
      throw new MalformedResourceException(
          "Invalid X-Amz-Date format: " + queryParameters.get("X-Amz-Date"));
    }

    final String rawCredential = queryParameters.get("X-Amz-Credential");

    Credential credential =
        null;
    try {
      credential = new Credential(urlDecode(rawCredential));
    } catch (UnsupportedEncodingException e) {
      throw new MalformedResourceException(
          "X-Amz-Credential is not proper URL encoded rawCredential:"
              + rawCredential);
    }
    validateCredential(credential);
    validateSignedHeaders();
    validateSignature();

    return new SignatureInfo.Builder(Version.V4)
        .setDate(credential.getDate())
        .setDateTime(queryParameters.get("X-Amz-Date"))
        .setAwsAccessId(credential.getAccessKeyID())
        .setSignature(queryParameters.get("X-Amz-Signature"))
        .setSignedHeaders(queryParameters.get("X-Amz-SignedHeaders"))
        .setCredentialScope(credential.createScope())
        .setAlgorithm(queryParameters.get("X-Amz-Algorithm"))
        .setSignPayload(false)
        .build();
  }

  /**
   * Validate if algorithm is in expected format.
   */
  private void validateAlgorithm() throws MalformedResourceException {
    String algorithm = queryParameters.get("X-Amz-Algorithm");
    if (algorithm == null) {
      throw new MalformedResourceException("Unspecified signature algorithm.");
    }
    if (isEmpty(algorithm) || !algorithm.equals(AWS4_SIGNING_ALGORITHM)) {
      throw new MalformedResourceException("Unsupported signature algorithm: "
          + algorithm);
    }
  }

  /** Validates Date and Expires Query parameters.
   * According to AWS documentation:
   * https://docs.aws.amazon.com/AmazonS3/latest/
   * API/sigv4-query-string-auth.html
   * X-Amz-Date: The date and time format must follow the
   * ISO 8601 standard, and must be formatted with the
   * "yyyyMMddTHHmmssZ" format
   * X-Amz-Expires: The minimum value you can set is 1,
   * and the maximum is 604800
   */
  @VisibleForTesting
  protected void validateDateAndExpires()
      throws MalformedResourceException, DateTimeParseException {
    final String dateString = queryParameters.get("X-Amz-Date");
    final String expiresString = queryParameters.get("X-Amz-Expires");
    if (dateString == null ||
        expiresString == null ||
        dateString.isEmpty() ||
        expiresString.isEmpty()) {
      throw new MalformedResourceException(
          "dateString or expiresString are missing or empty.");
    }
    final Long expires = Long.parseLong(expiresString);
    if (expires >= X_AMZ_EXPIRES_MIN && expires <= X_AMZ_EXPIRES_MAX) {
      if (ZonedDateTime.parse(dateString, StringToSignProducer.TIME_FORMATTER)
          .plus(expires, SECONDS).isBefore(ZonedDateTime.now())) {
        throw new MalformedResourceException("Pre-signed S3 url is expired. "
            + "dateString:" + dateString
            + " expiresString:" + expiresString);
      }
    } else {
      throw new MalformedResourceException("Invalid expiry duration. "
          + "X-Amz-Expires should be between " + X_AMZ_EXPIRES_MIN
          + "and" + X_AMZ_EXPIRES_MAX + " expiresString:" + expiresString);
    }
  }

  private void validateCredential(Credential credential)
      throws MalformedResourceException {
    if (credential.getAccessKeyID().isEmpty()) {
      throw new MalformedResourceException(
          "AWS access id is empty. credential: " + credential);
    }
    if (credential.getAwsRegion().isEmpty()) {
      throw new MalformedResourceException(
          "AWS region is empty. credential: " + credential);
    }
    if (credential.getAwsRequest().isEmpty() ||
            !(credential.getAwsRequest().equals(AWS_REQUEST))) {
      throw new MalformedResourceException(
          "AWS request is empty or invalid. credential:" + credential);
    }
    if (credential.getAwsService().isEmpty()) {
      throw new MalformedResourceException(
          "AWS service is empty. credential:" + credential);
    }
    // Date should not be empty and should be properly formatted.
    if (!credential.getDate().isEmpty()) {
      try {
        LocalDate.parse(credential.getDate(), DATE_FORMATTER);
      } catch (DateTimeParseException ex) {
        throw new MalformedResourceException(
            "AWS date format is invalid. credential:" + credential);
      }
    } else {
      throw new MalformedResourceException(
          "AWS date is empty. credential:{}" + credential);
    }
  }

  /**
   * Validate Signed headers.
   */
  private void validateSignedHeaders() throws MalformedResourceException {
    String signedHeadersStr = queryParameters.get("X-Amz-SignedHeaders");
    if (signedHeadersStr == null || isEmpty(signedHeadersStr)
        || signedHeadersStr.split(";").length == 0) {
      throw new MalformedResourceException("No signed headers found.");
    }
  }

  /**
   * Validate signature.
   */
  private void validateSignature() throws MalformedResourceException {
    String signature = queryParameters.get("X-Amz-Signature");
    if (isEmpty(signature)) {
      throw new MalformedResourceException("Signature is empty.");
    }
    try {
      Hex.decodeHex(signature);
    } catch (DecoderException e) {
      throw new MalformedResourceException(
          "Signature:" + signature + " should be in hexa-decimal encoding.");
    }
  }
}

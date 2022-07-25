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
package org.apache.hadoop.ozone.s3.signature;

import java.time.LocalDate;
import java.util.Collection;

import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import static java.time.temporal.ChronoUnit.DAYS;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.AWS4_SIGNING_ALGORITHM;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to parse v4 auth information from header.
 */
public class AuthorizationV4HeaderParser implements SignatureParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(AuthorizationV4HeaderParser.class);

  private static final String CREDENTIAL = "Credential=";
  private static final String SIGNEDHEADERS = "SignedHeaders=";
  private static final String SIGNATURE = "Signature=";

  private String authHeader;

  private String dateHeader;

  public AuthorizationV4HeaderParser(String authHeader, String dateHeader) {
    this.authHeader = authHeader;
    this.dateHeader = dateHeader;
  }

  /**
   * This method parses authorization header.
   * <p>
   * Authorization Header sample:
   * AWS4-HMAC-SHA256 Credential=AKIAJWFJK62WUTKNFJJA/20181009/us-east-1/s3
   * /aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date,
   * Signature
   * =db81b057718d7c1b3b8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2
   */
  @SuppressWarnings("StringSplitter")
  @Override
  public SignatureInfo parseSignature() throws MalformedResourceException {
    if (authHeader == null || !authHeader.startsWith("AWS4")) {
      return null;
    }
    int firstSep = authHeader.indexOf(' ');
    if (firstSep < 0) {
      throw new MalformedResourceException(authHeader);
    }

    //split the value parts of the authorization header
    String[] split = authHeader.substring(firstSep + 1).trim().split(", *");

    if (split.length != 3) {
      throw new MalformedResourceException(authHeader);
    }

    String algorithm = parseAlgorithm(authHeader.substring(0, firstSep));
    Credential credentialObj = parseCredentials(split[0]);
    String signedHeaders = parseSignedHeaders(split[1]);
    String signature = parseSignature(split[2]);
    return new SignatureInfo(
        Version.V4,
        credentialObj.getDate(),
        dateHeader,
        credentialObj.getAccessKeyID(),
        signature,
        signedHeaders,
        credentialObj.createScope(),
        algorithm,
        true
    );
  }

  /**
   * Validate Signed headers.
   */
  private String parseSignedHeaders(String signedHeadersStr)
      throws MalformedResourceException {
    if (isNotEmpty(signedHeadersStr)
        && signedHeadersStr.startsWith(SIGNEDHEADERS)) {
      String parsedSignedHeaders =
          signedHeadersStr.substring(SIGNEDHEADERS.length());
      Collection<String> signedHeaders =
          StringUtils.getStringCollection(parsedSignedHeaders, ";");
      if (signedHeaders.size() == 0) {
        throw new MalformedResourceException("No signed headers found.",
            authHeader);
      }
      return parsedSignedHeaders;
    } else {
      throw new MalformedResourceException("No signed headers found.",
          authHeader);
    }
  }

  /**
   * Validate signature.
   */
  private String parseSignature(String signature)
      throws MalformedResourceException {
    if (signature.startsWith(SIGNATURE)) {
      String parsedSignature = signature.substring(SIGNATURE.length());
      if (isEmpty(parsedSignature)) {
        throw new MalformedResourceException(
            "Signature can't be empty: " + signature,
            authHeader);
      }
      try {
        Hex.decodeHex(parsedSignature);
      } catch (DecoderException e) {
        throw new MalformedResourceException(
            "Signature:" + signature + " should be in hexa-decimal encoding.",
            authHeader);
      }
      return parsedSignature;
    } else {
      throw new MalformedResourceException("No signature found: " + signature,
          authHeader);
    }
  }

  /**
   * Validate credentials.
   */
  private Credential parseCredentials(String credential)
      throws MalformedResourceException {
    Credential credentialObj = null;
    if (isNotEmpty(credential) && credential.startsWith(CREDENTIAL)) {
      credential = credential.substring(CREDENTIAL.length());
      // Parse credential. Other parts of header are not validated yet. When
      // security comes, it needs to be completed.
      credentialObj = new Credential(credential);
    } else {
      throw new MalformedResourceException(authHeader);
    }

    if (credentialObj.getAccessKeyID().isEmpty()) {
      throw new MalformedResourceException(
          "AWS access id shouldn't be empty. credential: " + credential,
          authHeader);
    }
    if (credentialObj.getAwsRegion().isEmpty()) {
      throw new MalformedResourceException(
          "AWS region shouldn't be empty. credential: " + credential,
          authHeader);
    }
    if (credentialObj.getAwsRequest().isEmpty()) {
      throw new MalformedResourceException(
          "AWS request shouldn't be empty. credential:" + credential,
          authHeader);
    }
    if (credentialObj.getAwsService().isEmpty()) {
      throw new MalformedResourceException(
          "AWS service shouldn't be empty. credential:" + credential,
          authHeader);
    }

    // Date should not be empty and within valid range.
    if (!credentialObj.getDate().isEmpty()) {
      validateDateRange(credentialObj);
    } else {
      throw new MalformedResourceException(
          "AWS date shouldn't be empty. credential:{}" + credential,
          authHeader);
    }
    return credentialObj;
  }

  @VisibleForTesting
  public void validateDateRange(Credential credentialObj)
      throws MalformedResourceException {
    LocalDate date = LocalDate.parse(credentialObj.getDate(), DATE_FORMATTER);
    LocalDate now = LocalDate.now();
    if (date.isBefore(now.minus(1, DAYS)) ||
        date.isAfter(now.plus(1, DAYS))) {
      throw new MalformedResourceException(
          "AWS date not in valid range. Date: " + date + " should not be older "
              + "than 1 day(i.e yesterday) and greater than 1 day(i.e " +
              "tomorrow).", authHeader);
    }
  }

  /**
   * Validate if algorithm is in expected format.
   */
  private String parseAlgorithm(String algorithm)
      throws MalformedResourceException {
    if (isEmpty(algorithm) || !algorithm.equals(AWS4_SIGNING_ALGORITHM)) {
      throw new MalformedResourceException("Unexpected hash algorithm. Algo:"
          + algorithm, authHeader);
    }
    return algorithm;
  }

}

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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kerby.util.Hex;

/**
 * Shared AWS Signature Version 4 helpers for tests.
 */
public final class SignatureTestUtils {

  private static final String EMPTY_STRING_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  private static final String DATE = "20260827";
  private static final String DATE_TIME = DATE + "T010203Z";
  private static final String CREDENTIAL_SCOPE = DATE + "/us-east-1/s3/aws4_request";
  private static final String SEED_SIGNATURE =
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  private static final byte[] SIGNING_KEY = signingKey("secret", DATE, "us-east-1", "s3");

  private SignatureTestUtils() {
  }

  /**
   * Derive the SigV4 signing key: {@code HMAC(HMAC(HMAC(HMAC("AWS4"+secret,
   * date), region), service), "aws4_request")}.
   */
  public static byte[] signingKey(String secretKey, String date, String region,
      String service) {
    byte[] key = hmac(("AWS4" + secretKey).getBytes(UTF_8), date);
    key = hmac(key, region);
    key = hmac(key, service);
    return hmac(key, "aws4_request");
  }

  public static byte[] signingKey() {
    return SIGNING_KEY.clone();
  }

  public static SignatureInfo signatureInfo() {
    return new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setDate(DATE)
        .setDateTime(DATE_TIME)
        .setCredentialScope(CREDENTIAL_SCOPE)
        .setSignature(SEED_SIGNATURE)
        .build();
  }

  /** @return {@code HMAC-SHA256(key, msg)}. */
  public static byte[] hmac(byte[] key, String msg) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(msg.getBytes(UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** @return hex SHA-256 of {@code data[off, off+len)}. */
  public static String sha256Hex(byte[] data, int off, int len) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(data, off, len);
      return Hex.encode(digest.digest()).toLowerCase(Locale.ROOT);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Compute one SigV4 streaming chunk signature. */
  public static String chunkSignature(byte[] signingKey, String dateTime, String credentialScope,
      String previousSignature, byte[] payload) {
    String stringToSign = String.join("\n", "AWS4-HMAC-SHA256-PAYLOAD", dateTime, credentialScope,
        previousSignature, EMPTY_STRING_SHA256, sha256Hex(payload, 0, payload.length));
    return Hex.encode(hmac(signingKey, stringToSign)).toLowerCase(Locale.ROOT);
  }

  /** Build a one-data-chunk SigV4 streaming body, including the terminating zero-byte chunk. */
  public static String signedChunkedBody(byte[] signingKey, String dateTime, String credentialScope,
      String seedSignature, String content) {
    byte[] payload = content.getBytes(UTF_8);
    String previousSignature = seedSignature;
    StringBuilder body = new StringBuilder();
    if (payload.length > 0) {
      previousSignature = chunkSignature(
          signingKey, dateTime, credentialScope, seedSignature, payload);
      body.append(Integer.toHexString(payload.length))
          .append(";chunk-signature=").append(previousSignature).append("\r\n")
          .append(content).append("\r\n");
    }
    String finalSignature = chunkSignature(
        signingKey, dateTime, credentialScope, previousSignature, new byte[0]);
    return body.append("0;chunk-signature=").append(finalSignature).append("\r\n\r\n").toString();
  }

  public static String signedChunkedBody(String content) {
    return signedChunkedBody(SIGNING_KEY, DATE_TIME, CREDENTIAL_SCOPE, SEED_SIGNATURE, content);
  }
}

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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.SIGNATURE_DOES_NOT_MATCH;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/**
 * Verifies the per-chunk signatures of a SigV4 chunked upload
 * ({@code STREAMING-AWS4-HMAC-SHA256-PAYLOAD}).
 * <p>
 * Each chunk signature is {@code hex(HMAC-SHA256(signingKey, stringToSign))},
 * where the string-to-sign is:
 * <pre>
 * AWS4-HMAC-SHA256-PAYLOAD\n
 * &lt;date-time&gt;\n
 * &lt;credential-scope&gt;\n
 * &lt;previous-signature&gt;\n
 * &lt;SHA-256("")&gt;\n
 * &lt;SHA-256(chunk-payload)&gt;
 * </pre>
 * The signatures are chained: the first chunk uses the request (seed) signature
 * as the previous signature, and each subsequent chunk uses the previous
 * chunk's computed signature. The signing key is the SigV4 signing key derived
 * from the caller's secret; it is provided by the caller so that the S3 Gateway
 * does not have to handle the secret directly (see HDDS-15140).
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html">
 *     Signature Calculation: Transfer Payload in Multiple Chunks</a>
 */
public class ChunksValidator {

  private static final String CHUNK_STRING_TO_SIGN_ALGORITHM =
      "AWS4-HMAC-SHA256-PAYLOAD";
  private static final String HMAC_SHA256 = "HmacSHA256";
  private static final String NEWLINE = "\n";

  /** SHA-256 hex of the empty string (the hashed empty headers slot). */
  private static final String EMPTY_STRING_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  private static final ThreadLocal<Mac> HMAC = ThreadLocal.withInitial(() -> {
    try {
      return Mac.getInstance(HMAC_SHA256);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(HMAC_SHA256 + " not available", e);
    }
  });

  /** The signing key is fixed for the request, so the key spec is built once instead of per chunk. */
  private final SecretKeySpec signingKey;
  private final String dateTime;
  private final String credentialScope;
  private final String resource;
  private String previousSignature;

  public ChunksValidator(byte[] signingKey, String dateTime,
      String credentialScope, String seedSignature, String resource) {
    this.signingKey = new SecretKeySpec(signingKey, HMAC_SHA256);
    this.dateTime = dateTime;
    this.credentialScope = credentialScope;
    this.previousSignature = seedSignature;
    this.resource = resource;
  }

  /**
   * Verify one chunk and advance the signature chain.
   *
   * @param chunkSignature the signature parsed from the chunk header line. The chunk header pattern in
   *        {@link org.apache.hadoop.ozone.s3.SignedChunksInputStream} guarantees 64 hex characters.
   * @param payloadSha256Hex hex SHA-256 of the chunk payload
   * @throws OS3Exception if the computed signature does not match
   */
  public void validateChunk(String chunkSignature, String payloadSha256Hex)
      throws OS3Exception {
    String stringToSign = String.join(NEWLINE,
        CHUNK_STRING_TO_SIGN_ALGORITHM, dateTime, credentialScope,
        previousSignature, EMPTY_STRING_SHA256, payloadSha256Hex);
    byte[] expected = hmacSha256(stringToSign);
    // Constant-time comparison to avoid leaking the signature via timing. Decoding the hex also
    // makes the comparison case-insensitive, as the signature may be sent in either case.
    if (!MessageDigest.isEqual(expected, DatatypeConverter.parseHexBinary(chunkSignature))) {
      throw newError(SIGNATURE_DOES_NOT_MATCH, resource);
    }
    // The chain feeds this chunk's signature, in hex, into the next chunk's string-to-sign.
    previousSignature = hex(expected);
  }

  private byte[] hmacSha256(String msg) {
    try {
      Mac mac = HMAC.get();
      mac.init(signingKey);
      return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
    } catch (InvalidKeyException e) {
      throw new IllegalStateException("Failed to compute " + HMAC_SHA256, e);
    }
  }

  private static String hex(byte[] bytes) {
    return DatatypeConverter.printHexBinary(bytes).toLowerCase(Locale.ROOT);
  }
}

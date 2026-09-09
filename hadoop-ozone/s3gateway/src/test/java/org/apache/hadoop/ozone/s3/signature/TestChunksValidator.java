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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Locale;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Verifies {@link ChunksValidator} against the canonical AWS SigV4 streaming
 * example (secret {@code wJalr...}, region us-east-1, service s3, date
 * 20130524, a 66560-byte payload of 'a' in chunks of 65536 + 1024 + 0).
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html">
 *     Signature Calculation: Transfer Payload in Multiple Chunks</a>
 */
class TestChunksValidator {

  private static final String SECRET_KEY =
      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  private static final String DATE_TIME = "20130524T000000Z";
  private static final String SCOPE = "20130524/us-east-1/s3/aws4_request";
  private static final String SEED_SIGNATURE =
      "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9";

  private static final String KEY_PATH = "key1";
  private static final String CHUNK1_SIGNATURE =
      "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648";
  private static final String CHUNK2_SIGNATURE =
      "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497";
  private static final String FINAL_CHUNK_SIGNATURE =
      "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9";
  private static final String TRAILER_SEED_SIGNATURE =
      "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e";
  private static final String TRAILER_CHUNK1_SIGNATURE =
      "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2";
  private static final String TRAILER_CHUNK2_SIGNATURE =
      "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7";
  private static final String TRAILER_FINAL_CHUNK_SIGNATURE =
      "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992";
  private static final String TRAILER_SIGNATURE =
      "d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435";

  /** A chunk that fails verification must surface as SignatureDoesNotMatch (HTTP 403), not any other error. */
  private static void assertSignatureMismatch(Executable call) {
    OS3Exception ex = assertThrows(OS3Exception.class, call);
    assertEquals(SIGNATURE_DOES_NOT_MATCH.getCode(), ex.getCode());
    assertEquals(SIGNATURE_DOES_NOT_MATCH.getHttpCode(), ex.getHttpCode());
  }

  private ChunksValidator newValidator() {
    return new ChunksValidator(
        SignatureTestUtils.signingKey(SECRET_KEY, "20130524", "us-east-1", "s3"),
        DATE_TIME, SCOPE, SEED_SIGNATURE, KEY_PATH);
  }

  private ChunksValidator newTrailerValidator() {
    return new ChunksValidator(
        SignatureTestUtils.signingKey(SECRET_KEY, "20130524", "us-east-1", "s3"),
        DATE_TIME, SCOPE, TRAILER_SEED_SIGNATURE, KEY_PATH);
  }

  @Test
  void acceptsMatchingChunkSignatures() {
    ChunksValidator validator = newValidator();

    byte[] chunk1 = repeat('a', 65536);
    byte[] chunk2 = repeat('a', 1024);
    byte[] finalChunk = new byte[0];

    assertDoesNotThrow(() -> validator.validateChunk(CHUNK1_SIGNATURE,
        SignatureTestUtils.sha256Hex(chunk1, 0, chunk1.length)));
    assertDoesNotThrow(() -> validator.validateChunk(CHUNK2_SIGNATURE,
        SignatureTestUtils.sha256Hex(chunk2, 0, chunk2.length)));
    assertDoesNotThrow(() -> validator.validateChunk(FINAL_CHUNK_SIGNATURE,
        SignatureTestUtils.sha256Hex(finalChunk, 0, finalChunk.length)));
  }

  @Test
  void acceptsUppercaseChunkSignature() {
    byte[] chunk = repeat('a', 65536);

    assertThatCode(() -> newValidator().validateChunk(CHUNK1_SIGNATURE.toUpperCase(Locale.ROOT),
        SignatureTestUtils.sha256Hex(chunk, 0, chunk.length))).doesNotThrowAnyException();
  }

  @Test
  void acceptsMatchingTrailerSignature() {
    ChunksValidator validator = newTrailerValidator();
    byte[] chunk1 = repeat('a', 65536);
    byte[] chunk2 = repeat('a', 1024);
    String trailer = "x-amz-checksum-crc32c:sOO8/Q==";

    assertDoesNotThrow(() -> validator.validateChunk(TRAILER_CHUNK1_SIGNATURE,
        SignatureTestUtils.sha256Hex(chunk1, 0, chunk1.length)));
    assertDoesNotThrow(() -> validator.validateChunk(TRAILER_CHUNK2_SIGNATURE,
        SignatureTestUtils.sha256Hex(chunk2, 0, chunk2.length)));
    assertDoesNotThrow(() -> validator.validateChunk(TRAILER_FINAL_CHUNK_SIGNATURE,
        SignatureTestUtils.sha256Hex(new byte[0], 0, 0)));
    assertDoesNotThrow(() -> validator.validateTrailer(TRAILER_SIGNATURE,
        SignatureTestUtils.sha256Hex((trailer + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8),
            0, trailer.length() + 1)));
  }

  @Test
  void rejectsTamperedTrailerSignature() {
    ChunksValidator validator = newTrailerValidator();
    assertSignatureMismatch(() -> validator.validateTrailer(
        TRAILER_SIGNATURE.substring(0, TRAILER_SIGNATURE.length() - 1) + "0",
        "invalid-trailer-hash"));
  }

  @Test
  void rejectsTamperedChunkSignature() {
    ChunksValidator validator = newValidator();
    byte[] chunk1 = repeat('a', 65536);

    // Wrong signature for the first chunk.
    assertSignatureMismatch(() -> validator.validateChunk(
        CHUNK2_SIGNATURE, SignatureTestUtils.sha256Hex(chunk1, 0, chunk1.length)));
  }

  @Test
  void rejectsTamperedChunkPayload() {
    ChunksValidator validator = newValidator();
    byte[] tampered = repeat('b', 65536);

    // Correct signature but the payload was modified.
    assertSignatureMismatch(() -> validator.validateChunk(
        CHUNK1_SIGNATURE,
        SignatureTestUtils.sha256Hex(tampered, 0, tampered.length)));
  }

  @Test
  void interleavedValidatorsWithDifferentKeysDoNotCrossContaminate() {
    // The Mac is a shared ThreadLocal re-init'd with each validator's key per call. Interleaving a
    // wrong-key and a correct-key validator on the same thread must not leak the key between them.
    ChunksValidator correct = newValidator();
    ChunksValidator wrongKey = new ChunksValidator(
        SignatureTestUtils.signingKey("wrong-secret", "20130524", "us-east-1", "s3"),
        DATE_TIME, SCOPE, SEED_SIGNATURE, KEY_PATH);
    String sha65536 = SignatureTestUtils.sha256Hex(repeat('a', 65536), 0, 65536);
    String sha1024 = SignatureTestUtils.sha256Hex(repeat('a', 1024), 0, 1024);

    assertSignatureMismatch(() -> wrongKey.validateChunk(CHUNK1_SIGNATURE, sha65536));
    // If the shared Mac were not re-keyed, this would still hold the wrong key and fail.
    assertDoesNotThrow(() -> correct.validateChunk(CHUNK1_SIGNATURE, sha65536));
    assertSignatureMismatch(() -> wrongKey.validateChunk(CHUNK1_SIGNATURE, sha65536));
    assertDoesNotThrow(() -> correct.validateChunk(CHUNK2_SIGNATURE, sha1024));
  }

  private static byte[] repeat(char c, int count) {
    byte[] bytes = new byte[count];
    Arrays.fill(bytes, (byte) c);
    return bytes;
  }
}

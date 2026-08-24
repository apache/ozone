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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;

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

  private static final String CHUNK1_SIGNATURE =
      "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648";
  private static final String CHUNK2_SIGNATURE =
      "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497";
  private static final String FINAL_CHUNK_SIGNATURE =
      "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9";

  private ChunksValidator newValidator() {
    return new ChunksValidator(
        SignatureTestUtils.signingKey(SECRET_KEY, "20130524", "us-east-1", "s3"),
        DATE_TIME, SCOPE, SEED_SIGNATURE);
  }

  @Test
  void acceptsMatchingChunkSignatures() {
    ChunksValidator validator = newValidator();

    byte[] chunk1 = repeat('a', 65536);
    byte[] chunk2 = repeat('a', 1024);
    byte[] finalChunk = new byte[0];

    assertDoesNotThrow(() -> validator.validateChunk(CHUNK1_SIGNATURE,
        ChunksValidator.sha256Hex(chunk1, 0, chunk1.length)));
    assertDoesNotThrow(() -> validator.validateChunk(CHUNK2_SIGNATURE,
        ChunksValidator.sha256Hex(chunk2, 0, chunk2.length)));
    assertDoesNotThrow(() -> validator.validateChunk(FINAL_CHUNK_SIGNATURE,
        ChunksValidator.sha256Hex(finalChunk, 0, finalChunk.length)));
  }

  @Test
  void rejectsTamperedChunkSignature() {
    ChunksValidator validator = newValidator();
    byte[] chunk1 = repeat('a', 65536);

    // Wrong signature for the first chunk.
    assertThrows(OS3Exception.class, () -> validator.validateChunk(
        CHUNK2_SIGNATURE, ChunksValidator.sha256Hex(chunk1, 0, chunk1.length)));
  }

  @Test
  void rejectsTamperedChunkPayload() {
    ChunksValidator validator = newValidator();
    byte[] tampered = repeat('b', 65536);

    // Correct signature but the payload was modified.
    assertThrows(OS3Exception.class, () -> validator.validateChunk(
        CHUNK1_SIGNATURE,
        ChunksValidator.sha256Hex(tampered, 0, tampered.length)));
  }

  private static byte[] repeat(char c, int count) {
    byte[] bytes = new byte[count];
    Arrays.fill(bytes, (byte) c);
    return bytes;
  }
}

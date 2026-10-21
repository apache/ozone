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

package org.apache.hadoop.ozone.s3.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests payload classification, including the existing fallback for unknown streaming algorithms. */
class TestS3ContentSha256Type {

  @ParameterizedTest
  @CsvSource({
      "UNSIGNED-PAYLOAD, UNSIGNED_PAYLOAD",
      "STREAMING-UNSIGNED-PAYLOAD-TRAILER, STREAMING_UNSIGNED_PAYLOAD_TRAILER",
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD, STREAMING_AWS4_HMAC_SHA256_PAYLOAD",
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER, STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER",
      "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD, STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD",
      "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER, STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER"
  })
  void parsesKnownHeaders(String header, S3ContentSha256Type expected) {
    assertThat(S3ContentSha256Type.fromHeaderValue(header)).isEqualTo(expected);
  }

  static Stream<Arguments> unrecognizedHeaders() {
    return Stream.of(
        Arguments.of("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", S3ContentSha256Type.OTHER),
        Arguments.of("", S3ContentSha256Type.OTHER),
        Arguments.of("not-a-hash", S3ContentSha256Type.OTHER),
        Arguments.of("unsigned-payload", S3ContentSha256Type.OTHER),
        Arguments.of(" UNSIGNED-PAYLOAD", S3ContentSha256Type.OTHER),
        Arguments.of("UNSIGNED-PAYLOAD ", S3ContentSha256Type.OTHER),
        Arguments.of("streaming-AWS4-HMAC-SHA256-PAYLOAD", S3ContentSha256Type.OTHER),
        Arguments.of(" STREAMING-AWS4-HMAC-SHA256-PAYLOAD", S3ContentSha256Type.OTHER),
        Arguments.of("STREAMING", S3ContentSha256Type.OTHER_STREAMING),
        Arguments.of("STREAMING-FUTURE-PAYLOAD", S3ContentSha256Type.OTHER_STREAMING),
        Arguments.of("STREAMINGanything", S3ContentSha256Type.OTHER_STREAMING),
        Arguments.of("STREAMING-AWS4-HMAC-SHA256-PAYLOAD ", S3ContentSha256Type.OTHER_STREAMING));
  }

  @ParameterizedTest
  @MethodSource("unrecognizedHeaders")
  void preservesUnrecognizedHeaders(String header, S3ContentSha256Type expected) {
    assertThat(S3ContentSha256Type.fromHeaderValue(header)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
      "UNSIGNED_PAYLOAD, true, false, false",
      "STREAMING_UNSIGNED_PAYLOAD_TRAILER, true, true, false",
      "STREAMING_AWS4_HMAC_SHA256_PAYLOAD, false, true, true",
      "STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER, false, true, true",
      "STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD, false, true, false",
      "STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER, false, true, false",
      "OTHER, false, false, false",
      "OTHER_STREAMING, false, true, false"
  })
  void classifiesPayload(S3ContentSha256Type type, boolean unsigned, boolean streaming, boolean hmacSignedStreaming) {
    assertThat(type.isUnsigned()).isEqualTo(unsigned);
    assertThat(type.isStreaming()).isEqualTo(streaming);
    assertThat(type.isHmacSignedStreaming()).isEqualTo(hmacSignedStreaming);
  }

  @Test
  void rejectsNull() {
    assertThrows(NullPointerException.class, () -> S3ContentSha256Type.fromHeaderValue(null));
  }
}

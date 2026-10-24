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

package org.apache.hadoop.ozone.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kerby.util.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests the shared HMAC primitive against independent expected signatures. */
class TestHmacSha256 {

  private static final String FIRST_SIGNATURE = "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
  private static final String SECOND_SIGNATURE = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";

  private static byte[] firstKey() {
    byte[] key = new byte[20];
    Arrays.fill(key, (byte) 0x0b);
    return key;
  }

  static Stream<Arguments> signatures() {
    // The first two cases are RFC 4231 test cases 1 and 2; the third checks UTF-8 encoding.
    return Stream.of(
        Arguments.of(firstKey(), "Hi There", FIRST_SIGNATURE),
        Arguments.of("Jefe".getBytes(UTF_8), "what do ya want for nothing?", SECOND_SIGNATURE),
        Arguments.of("key".getBytes(UTF_8), "你好 Ozone",
            "2dc40af6acb137a9137b62f00ec73e96430db1b91003d1d5f7e99f967636ac6f"));
  }

  @ParameterizedTest
  @MethodSource("signatures")
  void matchesKnownSignatures(byte[] key, String message, String expected) throws Exception {
    assertEquals(expected, Hex.encode(HmacSha256.sign(new SecretKeySpec(key, "HmacSHA256"), message)));
  }

  @Test
  void reinitializesForDifferentKeys() throws Exception {
    SecretKeySpec first = new SecretKeySpec(firstKey(), "HmacSHA256");
    SecretKeySpec second = new SecretKeySpec("Jefe".getBytes(UTF_8), "HmacSHA256");
    assertEquals(FIRST_SIGNATURE, Hex.encode(HmacSha256.sign(first, "Hi There")));
    assertEquals(SECOND_SIGNATURE, Hex.encode(HmacSha256.sign(second, "what do ya want for nothing?")));
    assertEquals(FIRST_SIGNATURE, Hex.encode(HmacSha256.sign(first, "Hi There")));
  }

  @Test
  void independentThreadsUseDifferentKeys() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier barrier = new CyclicBarrier(2);
    try {
      Future<?> first = executor.submit(() -> {
        assertRepeatedSigning(firstKey(), "Hi There", FIRST_SIGNATURE, barrier);
        return null;
      });
      Future<?> second = executor.submit(() -> {
        assertRepeatedSigning("Jefe".getBytes(UTF_8), "what do ya want for nothing?", SECOND_SIGNATURE, barrier);
        return null;
      });
      first.get(30, TimeUnit.SECONDS);
      second.get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertRepeatedSigning(byte[] key, String message, String expected, CyclicBarrier barrier)
      throws Exception {
    SecretKeySpec signingKey = new SecretKeySpec(key, "HmacSHA256");
    for (int i = 0; i < 100; i++) {
      barrier.await(10, TimeUnit.SECONDS);
      assertEquals(expected, Hex.encode(HmacSha256.sign(signingKey, message)));
    }
  }
}

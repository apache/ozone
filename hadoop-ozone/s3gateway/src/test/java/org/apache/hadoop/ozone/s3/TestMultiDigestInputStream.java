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

package org.apache.hadoop.ozone.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test {@link MultiDigestInputStream}.
 */
public class TestMultiDigestInputStream {

  private static final String TEST_DATA = "1234567890";

  static Stream<Arguments> algorithmAndDataTestCases() throws Exception {
    return Stream.of(
        // Empty stream
        Arguments.of("empty stream with MD5",
            Arrays.asList(MessageDigest.getInstance("MD5")), ""),
        Arguments.of("empty stream with multiple algorithms",
            Arrays.asList(MessageDigest.getInstance("MD5"),
                MessageDigest.getInstance("SHA-256")), ""),
        // Normal data
        Arguments.of("MD5",
            Arrays.asList(MessageDigest.getInstance("MD5")), TEST_DATA),
        Arguments.of("MD5 and SHA-256",
            Arrays.asList(MessageDigest.getInstance("MD5"),
                MessageDigest.getInstance("SHA-256")), TEST_DATA),
        Arguments.of("MD5, SHA-1 and SHA-256",
            Arrays.asList(MessageDigest.getInstance("MD5"),
                MessageDigest.getInstance("SHA-1"),
                MessageDigest.getInstance("SHA-256")), TEST_DATA)
    );
  }

  @ParameterizedTest
  @MethodSource("algorithmAndDataTestCases")
  void testRead(String testName, List<MessageDigest> digests, String data) throws Exception {
    byte[] dataBytes = data.getBytes(UTF_8);

    try (MultiDigestInputStream mdis = new MultiDigestInputStream(
        new ByteArrayInputStream(dataBytes), digests)) {
      String result = IOUtils.toString(mdis, UTF_8);
      assertEquals(data, result);

      for (MessageDigest digest : digests) {
        String algorithm = digest.getAlgorithm();
        byte[] expectedDigest = MessageDigest.getInstance(algorithm).digest(dataBytes);
        assertArrayEquals(expectedDigest, mdis.getMessageDigest(algorithm).digest());
      }
    }
  }

  @Test
  void testOnOffFunctionality() throws Exception {
    byte[] data = TEST_DATA.getBytes(UTF_8);

    try (MultiDigestInputStream mdis = new MultiDigestInputStream(new ByteArrayInputStream(data),
        Collections.singletonList(MessageDigest.getInstance("MD5")))) {

      mdis.on(false);

      String result = IOUtils.toString(mdis, UTF_8);
      assertEquals(TEST_DATA, result);

      // Digest should be empty since it was turned off
      MessageDigest md5 = mdis.getMessageDigest("MD5");
      assertNotNull(md5);
      byte[] emptyDigest = MessageDigest.getInstance("MD5").digest();
      assertArrayEquals(emptyDigest, md5.digest());
    }
  }

  @Test
  void testOnOffWithPartialRead() throws Exception {
    String firstPart = "12345";
    String secondPart = "67890";
    byte[] data = (firstPart + secondPart).getBytes(UTF_8);

    try (MultiDigestInputStream mdis = new MultiDigestInputStream(new ByteArrayInputStream(data),
        Collections.singletonList(MessageDigest.getInstance("MD5")))) {
      // Read first part with digest on
      byte[] buffer1 = new byte[firstPart.length()];
      int bytesRead1 = mdis.read(buffer1, 0, buffer1.length);
      assertEquals(firstPart.length(), bytesRead1);
      assertEquals(firstPart, new String(buffer1, UTF_8));

      mdis.on(false);
      byte[] buffer2 = new byte[secondPart.length()];
      int bytesRead2 = mdis.read(buffer2, 0, buffer2.length);
      assertEquals(secondPart.length(), bytesRead2);
      assertEquals(secondPart, new String(buffer2, UTF_8));

      // Digest should only contain first part
      MessageDigest md5 = mdis.getMessageDigest("MD5");
      byte[] expectedDigest = MessageDigest.getInstance("MD5").digest(firstPart.getBytes(UTF_8));
      assertArrayEquals(expectedDigest, md5.digest());
    }
  }

  @Test
  void testResetDigests() throws Exception {
    byte[] data = TEST_DATA.getBytes(UTF_8);

    try (MultiDigestInputStream mdis = new MultiDigestInputStream(new ByteArrayInputStream(data),
        Collections.singletonList(MessageDigest.getInstance("MD5")))) {

      int byte1 = mdis.read();
      int byte2 = mdis.read();
      assertTrue(byte1 != -1 && byte2 != -1);

      mdis.resetDigests();

      MessageDigest md5 = mdis.getMessageDigest("MD5");
      byte[] emptyDigest = MessageDigest.getInstance("MD5").digest();
      assertArrayEquals(emptyDigest, md5.digest());
    }
  }

  @Test
  void testDigestManagement() throws Exception {
    byte[] data = TEST_DATA.getBytes(UTF_8);

    try (MultiDigestInputStream mdis = new MultiDigestInputStream(new ByteArrayInputStream(data),
        Arrays.asList(MessageDigest.getInstance("MD5"), MessageDigest.getInstance("SHA-1")))) {

      // Test initial state - getAllDigests
      Map<String, MessageDigest> allDigests = mdis.getAllDigests();
      assertEquals(2, allDigests.size());
      assertTrue(allDigests.containsKey("MD5"));
      assertTrue(allDigests.containsKey("SHA-1"));

      // Test add
      mdis.addMessageDigest("SHA-256");
      assertNotNull(mdis.getMessageDigest("SHA-256"));
      assertEquals(3, mdis.getAllDigests().size());

      // Test set - replace with new instance
      MessageDigest newMd5 = MessageDigest.getInstance("MD5");
      mdis.setMessageDigest("MD5", newMd5);
      assertNotNull(mdis.getMessageDigest("MD5"));

      // Test remove
      MessageDigest removed = mdis.removeMessageDigest("SHA-1");
      assertNotNull(removed);
      assertNull(mdis.getMessageDigest("SHA-1"));
      assertEquals(2, mdis.getAllDigests().size());

      // Test get non-existent
      assertNull(mdis.getMessageDigest("SHA-512"));

      // Read data and verify remaining digests work correctly
      String result = IOUtils.toString(mdis, UTF_8);
      assertEquals(TEST_DATA, result);

      byte[] expectedMd5 = MessageDigest.getInstance("MD5").digest(data);
      assertArrayEquals(expectedMd5, mdis.getMessageDigest("MD5").digest());

      byte[] expectedSha256 = MessageDigest.getInstance("SHA-256").digest(data);
      assertArrayEquals(expectedSha256, mdis.getMessageDigest("SHA-256").digest());
    }
  }

}

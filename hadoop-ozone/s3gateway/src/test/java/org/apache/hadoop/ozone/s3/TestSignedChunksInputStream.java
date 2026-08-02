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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.ChunksValidator;
import org.junit.jupiter.api.Test;

/**
 * Test {@link SignedChunksInputStream}.
 */
public class TestSignedChunksInputStream {

  @Test
  void testEmptyFile() throws IOException {
    try (InputStream is = wrapContent("0;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n")) {
      assertEquals("", IOUtils.toString(is, UTF_8));
    }
  }

  @Test
  void testEmptyFileWithTrailer() throws IOException {
    try (InputStream is = wrapContent("0;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "x-amz-checksum-crc32c:sOO8/Q==\r\n"
        + "x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n")) {
      assertEquals("", IOUtils.toString(is, UTF_8));
    }
  }

  @Test
  void testEmptyFileWithoutEnd() throws IOException {
    try (InputStream is = wrapContent("0;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40")) {
      assertEquals("", IOUtils.toString(is, UTF_8));
    }
  }

  @Test
  void testSingleChunk() throws IOException {
    //test simple read()
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890\r\n")) {
      assertEquals("1234567890", IOUtils.toString(is, UTF_8));
    }

    //test read(byte[],int,int)
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890\r\n")) {
      byte[] bytes = new byte[10];
      IOUtils.read(is, bytes, 0, 10);
      assertEquals("1234567890", new String(bytes, UTF_8));
    }

    //test read(byte[],int,int) with length parameter larger than the payload
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890\r\n")) {
      byte[] bytes = new byte[10];
      int readLength = IOUtils.read(is, bytes, 0, 10);
      assertEquals(10, readLength);
      assertEquals("1234567890", new String(bytes, UTF_8));
    }
  }

  @Test
  void testSingleChunkWithTrailer() throws IOException {
    //test simple read()
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890\r\n"
        + "0;chunk-signature=signature\r\n"
        + "x-amz-checksum-crc32c:sOO8/Q==\r\n"
        + "x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n")) {
      assertEquals("1234567890", IOUtils.toString(is, UTF_8));
    }

    //test read(byte[],int,int)
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890\r\n"
        + "0;chunk-signature=signature\r\n"
        + "x-amz-checksum-crc32c:sOO8/Q==\r\n"
        + "x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n")) {
      byte[] bytes = new byte[10];
      IOUtils.read(is, bytes, 0, 10);
      assertEquals("1234567890", new String(bytes, UTF_8));
    }

    //test read(byte[],int,int) with length parameter larger than the payload
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890\r\n"
        + "0;chunk-signature=signature\r\n"
        + "x-amz-checksum-crc32c:sOO8/Q==\r\n"
        + "x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n")) {
      byte[] bytes = new byte[10];
      int readLength = IOUtils.read(is, bytes, 0, 10);
      assertEquals(10, readLength);
      assertEquals("1234567890", new String(bytes, UTF_8));
    }
  }

  @Test
  void testSingleChunkWithoutEnd() throws IOException {
    //test simple read()
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890")) {
      assertEquals("1234567890", IOUtils.toString(is, UTF_8));
    }
    //test read(byte[],int,int)
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890")) {
      byte[] bytes = new byte[10];
      IOUtils.read(is, bytes, 0, 10);
      assertEquals("1234567890", new String(bytes, UTF_8));
    }
    //test read(byte[],int,int) with length parameter larger than the payload
    try (InputStream is = wrapContent("0A;chunk-signature"
        + "=23abb2bd920ddeeaac78a63ed808bc59fa6e7d3ef0e356474b82cdc2f8c93c40\r\n"
        + "1234567890")) {
      byte[] bytes = new byte[15];
      int readLength = IOUtils.read(is, bytes, 0, 15);
      assertEquals(10, readLength);
      assertEquals("1234567890", new String(bytes, UTF_8).substring(0, 10));
    }
  }

  @Test
  void testMultiChunks() throws IOException {
    //test simple read()
    try (InputStream is = wrapContent("0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n"
        + "0;chunk-signature=signature\r\n")) {
      String result = IOUtils.toString(is, UTF_8);
      assertEquals("1234567890abcde", result);
    }

    //test read(byte[],int,int)
    try (InputStream is = wrapContent("0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n"
        + "0;chunk-signature=signature\r\n")) {
      byte[] bytes = new byte[15];
      IOUtils.read(is, bytes, 0, 15);
      assertEquals("1234567890abcde", new String(bytes, UTF_8));
    }

    //test read(byte[],int,int) with length parameter larger than the payload
    try (InputStream is = wrapContent("0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n"
        + "0;chunk-signature=signature\r\n")) {
      byte[] bytes = new byte[20];
      int readLength = IOUtils.read(is, bytes, 0, 20);
      assertEquals(15, readLength);
      assertEquals("1234567890abcde", new String(bytes, UTF_8).substring(0, 15));
    }
  }

  @Test
  void testMultiChunksWithTrailer() throws Exception {
    //test simple read()
    try (InputStream is = wrapContent("0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n"
        + "0;chunk-signature=signature\r\n"
        + "x-amz-checksum-crc32c:sOO8/Q==\r\n"
        + "x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n")) {
      String result = IOUtils.toString(is, UTF_8);
      assertEquals("1234567890abcde", result);
    }

    //test read(byte[],int,int)
    try (InputStream is = wrapContent("0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n"
        + "0;chunk-signature=signature\r\n"
        + "x-amz-checksum-crc32c:sOO8/Q==\r\n"
        + "x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n")) {
      byte[] bytes = new byte[15];
      IOUtils.read(is, bytes, 0, 15);
      assertEquals("1234567890abcde", new String(bytes, UTF_8));
    }

    //test read(byte[],int,int) with length parameter larger than the payload
    try (InputStream is = wrapContent("0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n"
        + "0;chunk-signature=signature\r\n"
        + "x-amz-checksum-crc32c:sOO8/Q==\r\n"
        + "x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n")) {
      byte[] bytes = new byte[20];
      int readLength = IOUtils.read(is, bytes, 0, 20);
      assertEquals(15, readLength);
      assertEquals("1234567890abcde", new String(bytes, UTF_8).substring(0, 15));
    }
  }

  // Canonical AWS SigV4 streaming example: 66560 bytes of 'a' in chunks of
  // 65536 + 1024 + 0, secret wJalr..., us-east-1/s3, date 20130524.
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

  @Test
  void verifiesRealChunkSignatures() throws IOException {
    String content = signedChunkedBody('a');
    try (InputStream is = new SignedChunksInputStream(
        new ByteArrayInputStream(content.getBytes(UTF_8)), newValidator())) {
      assertEquals(repeat('a', 66560), IOUtils.toString(is, UTF_8));
    }
  }

  @Test
  void rejectsTamperedChunkPayload() {
    // Same signatures, but the first chunk carries different bytes.
    String content = signedChunkedBody('b');
    InputStream is = new SignedChunksInputStream(
        new ByteArrayInputStream(content.getBytes(UTF_8)), newValidator());
    assertThrows(OS3Exception.class, () -> IOUtils.toString(is, UTF_8));
  }

  private static String signedChunkedBody(char payloadChar) {
    return "10000;chunk-signature=" + CHUNK1_SIGNATURE + "\r\n"
        + repeat(payloadChar, 65536) + "\r\n"
        + "400;chunk-signature=" + CHUNK2_SIGNATURE + "\r\n"
        + repeat(payloadChar, 1024) + "\r\n"
        + "0;chunk-signature=" + FINAL_CHUNK_SIGNATURE + "\r\n";
  }

  private static ChunksValidator newValidator() {
    return new ChunksValidator(signingKey("20130524", "us-east-1", "s3"),
        DATE_TIME, SCOPE, SEED_SIGNATURE);
  }

  private static String repeat(char c, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, c);
    return new String(chars);
  }

  private static byte[] signingKey(String date, String region, String service) {
    byte[] key = hmac(("AWS4" + SECRET_KEY).getBytes(UTF_8), date);
    key = hmac(key, region);
    key = hmac(key, service);
    return hmac(key, "aws4_request");
  }

  private static byte[] hmac(byte[] key, String msg) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(msg.getBytes(UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private InputStream wrapContent(String content) {
    return new SignedChunksInputStream(
        new ByteArrayInputStream(content.getBytes(UTF_8)));
  }
}

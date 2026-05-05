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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
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

  private InputStream wrapContent(String content) {
    return new SignedChunksInputStream(
        new ByteArrayInputStream(content.getBytes(UTF_8)));
  }
}

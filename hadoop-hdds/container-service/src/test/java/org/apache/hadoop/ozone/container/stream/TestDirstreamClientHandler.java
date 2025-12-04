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

package org.apache.hadoop.ozone.container.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test streaming client.
 */
public class TestDirstreamClientHandler {

  @TempDir
  private Path tmpDir;

  @Test
  public void oneFileStream() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd.txt\nxxxx0 END"));

    assertEquals("xxxx", getContent("asd.txt"));
    assertTrue(handler.isAtTheEnd());

  }

  @Test
  public void splitAtHeader() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd.txt\n"));
    handler.doRead(null, wrap("1234"));
    handler.doRead(null, wrap("3 bsd.txt\n"));
    handler.doRead(null, wrap("1230 "));
    handler.doRead(null, wrap("END"));

    assertEquals("1234", getContent("asd.txt"));
    assertTrue(handler.isAtTheEnd());
  }

  @Test
  public void splitInHeader() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd."));
    handler.doRead(null, wrap("txt\nxxxx0 END"));

    assertEquals("xxxx", getContent("asd.txt"));
    assertTrue(handler.isAtTheEnd());

  }

  @Test
  public void splitSecondHeader() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd.txt\nxxxx3"));
    handler.doRead(null, wrap(" bsd.txt\nyyy0 END"));

    assertEquals("xxxx", getContent("asd.txt"));
    assertEquals("yyy", getContent("bsd.txt"));
    assertTrue(handler.isAtTheEnd());
  }

  @Test
  public void splitContent() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd.txt\nxx"));
    handler.doRead(null, wrap("xx3 bsd.txt\nyyy\nEND"));

    assertEquals("xxxx", getContent("asd.txt"));
    assertEquals("yyy", getContent("bsd.txt"));
  }

  @ParameterizedTest(name = "Invalid format: {0}")
  @MethodSource("provideInvalidFormatTestCases")
  public void testInvalidFormat(String testCaseName, String invalidInput) {
    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(tmpDir));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      handler.doRead(null, wrap(invalidInput));
    });
    assertThat(exception)
        .hasMessageContaining("Invalid file name format");
  }

  private static Stream<Arguments> provideInvalidFormatTestCases() {
    return Stream.of(
        // Test case: Missing space between size and filename
        Arguments.of("Missing space", "123File.txt\n"),
        // Test case: Empty filename after space
        Arguments.of("Empty filename", "123 \n"),
        // Test case: Only size number, no filename
        Arguments.of("Only size", "12345\n"),
        // Test case: Size is not a number
        Arguments.of("Invalid size (non-numeric)", "oops filename.txt\n")
    );
  }

  @Nonnull
  private String getContent(String name) throws IOException {
    return new String(Files.readAllBytes(tmpDir.resolve(name)),
        StandardCharsets.UTF_8);
  }

  private ByteBuf wrap(String content) {
    return Unpooled.wrappedBuffer(content.getBytes(StandardCharsets.UTF_8));
  }
}

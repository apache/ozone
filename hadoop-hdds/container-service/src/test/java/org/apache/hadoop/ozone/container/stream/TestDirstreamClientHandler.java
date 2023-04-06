/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.FileUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Test streaming client.
 */
public class TestDirstreamClientHandler {

  private Path tmpDir;

  @BeforeEach
  public void init() {
    tmpDir = GenericTestUtils.getRandomizedTestDir().toPath();
  }

  @AfterEach
  public void destroy() throws IOException {
    FileUtils.deleteDirectory(tmpDir.toFile());
  }

  @Test
  public void oneFileStream() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd.txt\nxxxx0 END"));

    Assertions.assertEquals("xxxx", getContent("asd.txt"));
    Assertions.assertTrue(handler.isAtTheEnd());

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

    Assertions.assertEquals("1234", getContent("asd.txt"));
    Assertions.assertTrue(handler.isAtTheEnd());
  }

  @Test
  public void splitInHeader() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd."));
    handler.doRead(null, wrap("txt\nxxxx0 END"));

    Assertions.assertEquals("xxxx", getContent("asd.txt"));
    Assertions.assertTrue(handler.isAtTheEnd());

  }


  @Test
  public void splitSecondHeader() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd.txt\nxxxx3"));
    handler.doRead(null, wrap(" bsd.txt\nyyy0 END"));

    Assertions.assertEquals("xxxx", getContent("asd.txt"));
    Assertions.assertEquals("yyy", getContent("bsd.txt"));
    Assertions.assertTrue(handler.isAtTheEnd());
  }


  @Test
  public void splitContent() throws IOException {

    final DirstreamClientHandler handler = new DirstreamClientHandler(
        new DirectoryServerDestination(
            tmpDir));

    handler.doRead(null, wrap("4 asd.txt\nxx"));
    handler.doRead(null, wrap("xx3 bsd.txt\nyyy\nEND"));

    Assertions.assertEquals("xxxx", getContent("asd.txt"));
    Assertions.assertEquals("yyy", getContent("bsd.txt"));
  }

  @NotNull
  private String getContent(String name) throws IOException {
    return new String(Files.readAllBytes(tmpDir.resolve(name)),
        StandardCharsets.UTF_8);
  }

  private ByteBuf wrap(String content) {
    return Unpooled.wrappedBuffer(content.getBytes(StandardCharsets.UTF_8));
  }
}

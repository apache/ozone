/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.checknative;

import org.apache.hadoop.ozone.shell.checknative.CheckNative;
import org.apache.ozone.test.tag.Native;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests for {@link CheckNative}.
 */
public class TestCheckNative {

  private static PrintStream psBackup;
  private static ByteArrayOutputStream outputStream;

  private static final String DEFAULT_ENCODING = UTF_8.name();

  @BeforeAll
  public static void init() throws UnsupportedEncodingException {
    psBackup = System.out;
    outputStream = new ByteArrayOutputStream();
    PrintStream psOut = new PrintStream(outputStream, false, DEFAULT_ENCODING);
    System.setOut(psOut);
  }

  @Test
  public void testCheckNativeNotLoaded() throws UnsupportedEncodingException {
    outputStream.reset();
    new CheckNative()
        .run(new String[] {});
    // trims multiple spaces
    String stdOut = outputStream.toString(DEFAULT_ENCODING)
        .replaceAll(" +", " ");
    assertThat(stdOut).contains("Native library checking:");
    assertThat(stdOut).contains("hadoop: false");
    assertThat(stdOut).contains("ISA-L: false");
    assertThat(stdOut).contains("rocks-tools: false");
  }

  @Native(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)
  @Test
  public void testCheckNativeRocksToolsLoaded() throws UnsupportedEncodingException {
    outputStream.reset();
    new CheckNative()
        .run(new String[] {});
    // trims multiple spaces
    String stdOut = outputStream.toString(DEFAULT_ENCODING)
        .replaceAll(" +", " ");
    assertThat(stdOut).contains("Native library checking:");
    assertThat(stdOut).contains("hadoop: false");
    assertThat(stdOut).contains("ISA-L: false");
    assertThat(stdOut).contains("rocks-tools: true");
  }

  @AfterEach
  public void setUp() {
    outputStream.reset();
  }

  @AfterAll
  public static void tearDown() {
    System.setOut(psBackup);
  }
}

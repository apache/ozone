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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CheckNative}.
 */
public class TestCheckNative {

  private static PrintStream psBackup;
  private static ByteArrayOutputStream outputStream;

  private static final String DEFAULT_ENCODING = UTF_8.name();

  @BeforeClass
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
    assertTrue(stdOut.contains("Native library checking:"));
    assertTrue(stdOut.contains("hadoop: false"));
    assertTrue(stdOut.contains("ISA-L: false"));
  }

  @After
  public void setUp() {
    outputStream.reset();
  }

  @AfterClass
  public static void tearDown() {
    System.setOut(psBackup);
  }
}

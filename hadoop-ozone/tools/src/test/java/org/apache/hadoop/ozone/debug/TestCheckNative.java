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

package org.apache.hadoop.ozone.debug;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Tests for {@link CheckNative}.
 */
class TestCheckNative {

  private GenericTestUtils.PrintStreamCapturer out;

  @BeforeEach
  void init() {
    out = GenericTestUtils.captureOut();
  }

  @DisabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
  @Test
  void testCheckNativeNotLoaded() {
    executeCheckNative();
    assertOutput(false);
  }

  @EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
  @Test
  void testCheckNativeRocksToolsLoaded() {
    executeCheckNative();
    assertOutput(true);
  }

  private void assertOutput(boolean expectedRocksNative) {
    // trims multiple spaces
    String stdOut = out.get()
        .replaceAll("  +", " ");
    assertThat(stdOut)
        .contains("Native library checking:")
        .contains("hadoop: false")
        .contains("ISA-L: false")
        .contains("OpenSSL: false")
        .contains("rocks-tools: " + expectedRocksNative);
  }

  @AfterEach
  void setUp() {
    IOUtils.closeQuietly(out);
  }

  private static void executeCheckNative() {
    new OzoneDebug().getCmd().execute("checknative");
  }
}

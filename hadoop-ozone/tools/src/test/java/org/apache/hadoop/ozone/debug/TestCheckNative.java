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

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Native;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CheckNative}.
 */
class TestCheckNative {

  private GenericTestUtils.PrintStreamCapturer out;

  @BeforeEach
  void init() {
    out = GenericTestUtils.captureOut();
  }

  @Test
  void testCheckNativeNotLoaded() {
    executeCheckNative();
    assertOutput(false);
  }

  @Native(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)
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

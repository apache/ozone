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

package org.apache.hadoop.ozone.debug.kdiag;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OzoneKDiag}.
 */
class TestOzoneKDiag {

  private GenericTestUtils.PrintStreamCapturer out;

  @BeforeEach
  void init() {
    out = GenericTestUtils.captureOut();
  }

  @Test
  void testKDiagExecution() {
    executeKDiag();
    assertOutput();
  }

  private void assertOutput() {

    String stdOut = normalize(out.get());

    assertThat(stdOut)
        .contains("Ozone Kerberos Diagnostics")
        .contains("Host Information")
        .contains("Environment Variables")
        .contains("Kerberos Configuration")
        .contains("Kerberos Ticket")
        .contains("Ozone Service Principals")
        .contains("Keytab Validation")
        .contains("Security Configuration")
        .contains("Authorization Configuration")
        .contains("HTTP Kerberos Authentication")
        .contains("Diagnostic Summary");
  }

  @AfterEach
  void cleanup() {
    IOUtils.closeQuietly(out);
  }

  private static void executeKDiag() {
    new OzoneDebug().getCmd().execute("kdiag");
  }

  private static String normalize(String s) {
    return s.replaceAll("  +", " ");
  }
}

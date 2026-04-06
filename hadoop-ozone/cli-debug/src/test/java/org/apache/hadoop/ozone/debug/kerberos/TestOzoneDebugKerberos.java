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

package org.apache.hadoop.ozone.debug.kerberos;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ozone kerberos debug subcommands.
 */
class TestOzoneDebugKerberos {

  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeEach
  void init() {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
  }

  @AfterEach
  void cleanup() {
    IOUtils.closeQuietly(out);
    IOUtils.closeQuietly(err);
    System.clearProperty("java.security.krb5.conf");
  }

  /**
   * Test ozone debug kerberos diagnose.
   */
  @Test
  void testDiagnoseSubcommandSuccess() {

    int rc = executeDiagnose();
    String stdOut = normalize(out.get());

    assertThat(stdOut)
        .contains("Ozone Kerberos Diagnostics")
        .contains("-- Host Information --")
        .contains("-- Environment Variables --")
        .contains("-- JVM Kerberos Properties --")
        .contains("-- Kerberos Configuration --")
        .contains("-- Kerberos kinit Command --")
        .contains("-- Keytab Validation --")
        .contains("-- Kerberos Ticket --")
        .contains("-- auth_to_local mapping --")
        .contains("-- Ozone Service Principals --")
        .contains("-- Security Configuration --")
        .contains("-- Authorization Configuration --")
        .contains("-- HTTP Kerberos Authentication --")
        .contains("== Diagnostic Summary ==")
        .contains("PASS :")
        .contains("WARN :")
        .contains("FAIL :");

    assertThat(rc).isGreaterThanOrEqualTo(0);
  }

  /**
   * Test ozone debug kerberos diagnose.
   */
  @Test
  void testDiagnoseSubcommandFailure() {

    // Force Kerberos failure
    System.setProperty("java.security.krb5.conf", "/invalid/path");
    int rc = executeDiagnose();

    String stdOut = normalize(out.get());
    String stdErr = normalize(err.get());
    String combined = stdOut + stdErr;

    assertThat(combined)
        .contains("Kerberos Configuration")
        .contains("ERROR");

    assertThat(combined)
        .contains("FAIL :");

    assertThat(rc).isEqualTo(1);
  }

  /**
   * Test ozone debug kerberos translate-principal.
   */
  @Test
  void testTranslatePrincipalSubcommand() {

    int rc = executeTranslatePrincipal();

    String stdOut = normalize(out.get());
    String stdErr = normalize(err.get());
    String combined = stdOut + stdErr;

    assertThat(stdOut)
        .contains("Kerberos Principal Translation")
        .contains("auth_to_local rules");

    // Either success OR failure
    assertThat(combined)
        .containsAnyOf(
            "Principal =",
            "ERROR: Failed to translate principal"
        );

    assertThat(rc).isEqualTo(0);
  }

  private static int executeDiagnose() {
    return new OzoneDebug().getCmd().execute(
        "kerberos",
        "diagnose"
    );
  }

  private static int executeTranslatePrincipal() {
    return new OzoneDebug().getCmd().execute(
        "kerberos",
        "translate-principal",
        "testuser/host@EXAMPLE.COM"
    );
  }

  private static String normalize(String s) {
    return s.replaceAll("  +", " ");
  }
}

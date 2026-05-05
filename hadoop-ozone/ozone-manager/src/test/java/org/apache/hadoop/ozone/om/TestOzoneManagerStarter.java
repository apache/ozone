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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.cli.GenericCli.EXECUTION_ERROR_EXIT_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static picocli.CommandLine.ExitCode.OK;
import static picocli.CommandLine.ExitCode.USAGE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class is used to test the CLI provided by OzoneManagerStarter, which is
 * used to start and init the OzoneManager. The calls to the Ozone Manager are
 * mocked so the tests only validate the CLI calls the correct methods are
 * invoked.
 */
public class TestOzoneManagerStarter {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  private MockOMStarter mock;

  @BeforeEach
  public void setUpStreams() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
    mock = new MockOMStarter();
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCallsStartWhenServerStarted() throws Exception {
    assertEquals(OK, executeCommand());
    assertTrue(mock.startCalled);
  }

  @Test
  public void testExceptionThrownWhenStartFails() throws Exception {
    mock.throwOnStart = true;
    assertEquals(EXECUTION_ERROR_EXIT_CODE, executeCommand());
  }

  @Test
  public void testStartNotCalledWithInvalidParam() throws Exception {
    assertEquals(USAGE, executeCommand("--invalid"));
    assertFalse(mock.startCalled);
  }

  @Test
  public void testPassingInitSwitchCallsInit() {
    assertEquals(OK, executeCommand("--init"));
    assertTrue(mock.initCalled);
  }

  @Test
  public void testInitSwitchWithInvalidParamDoesNotRun() {
    assertEquals(USAGE, executeCommand("--init", "--invalid"));
    assertFalse(mock.initCalled);
  }

  @Test
  public void testUnSuccessfulInitThrowsException() {
    mock.throwOnInit = true;
    assertEquals(EXECUTION_ERROR_EXIT_CODE, executeCommand("--init"));
  }

  @Test
  public void testInitThatReturnsFalseThrowsException() {
    mock.initStatus = false;
    assertEquals(EXECUTION_ERROR_EXIT_CODE, executeCommand("--init"));
  }

  @Test
  public void testCallsStartAndCancelPrepareWithUpgradeFlag() {
    assertEquals(OK, executeCommand("--upgrade"));
    assertTrue(mock.startAndCancelPrepareCalled);
  }

  @Test
  public void testUnsuccessfulUpgradeThrowsException() {
    mock.throwOnStartAndCancelPrepare = true;
    assertEquals(EXECUTION_ERROR_EXIT_CODE, executeCommand("--upgrade"));
  }

  @Test
  public void testUsagePrintedOnInvalidInput()
      throws UnsupportedEncodingException {
    assertEquals(USAGE, executeCommand("--invalid"));
    Pattern p = Pattern.compile("^Unknown option:.*--invalid.*\nUsage");
    Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  private int executeCommand(String... args) {
    return new OzoneManagerStarter(mock).execute(args);
  }

  static class MockOMStarter implements OMStarterInterface {

    private boolean startCalled = false;
    private boolean initCalled = false;
    private boolean initStatus = true;
    private boolean throwOnStart = false;
    private boolean throwOnInit = false;
    private boolean startAndCancelPrepareCalled = false;
    private boolean throwOnStartAndCancelPrepare = false;

    @Override
    public void start(OzoneConfiguration conf) throws IOException,
        AuthenticationException {
      startCalled = true;
      if (throwOnStart) {
        throw new IOException("Simulated Exception");
      }
    }

    @Override
    public boolean init(OzoneConfiguration conf) throws IOException,
        AuthenticationException {
      initCalled = true;
      if (throwOnInit) {
        throw new IOException("Simulated Exception");
      }
      return initStatus;
    }

    @Override
    public void bootstrap(OzoneConfiguration conf, boolean force)
        throws IOException, AuthenticationException {
      //TODO: Add test for bootstrap
    }

    @Override
    public void startAndCancelPrepare(OzoneConfiguration conf)
        throws IOException, AuthenticationException {
      startAndCancelPrepareCalled = true;
      if (throwOnStartAndCancelPrepare) {
        throw new IOException("Simulated Exception");
      }
    }
  }
}

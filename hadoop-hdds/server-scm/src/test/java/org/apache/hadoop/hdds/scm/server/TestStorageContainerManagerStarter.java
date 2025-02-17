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

package org.apache.hadoop.hdds.scm.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine.ExitCode;

/**
 * This class is used to test the StorageContainerManagerStarter using a mock
 * class to avoid starting any services and hence just test the CLI component.
 */
public class TestStorageContainerManagerStarter {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  private MockSCMStarter mock;

  @BeforeEach
  public void setUpStreams() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
    mock = new MockSCMStarter();
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCallsStartWhenServerStarted() throws Exception {
    assertEquals(ExitCode.OK, executeCommand());
    assertTrue(mock.startCalled);
  }

  @Test
  public void testExceptionThrownWhenStartFails() throws Exception {
    mock.throwOnStart = true;
    assertEquals(GenericCli.EXECUTION_ERROR_EXIT_CODE, executeCommand());
  }

  @Test
  public void testStartNotCalledWithInvalidParam() throws Exception {
    assertEquals(ExitCode.USAGE, executeCommand("--invalid"));
    assertFalse(mock.startCalled);
  }

  @Test
  public void testPassingInitSwitchCallsInit() {
    assertEquals(ExitCode.OK, executeCommand("--init"));
    assertTrue(mock.initCalled);
  }

  @Test
  public void testPassingBootStrapSwitchCallsBootStrap() {
    assertEquals(ExitCode.OK, executeCommand("--bootstrap"));
    assertTrue(mock.bootStrapCalled);
  }

  @Test
  public void testInitSwitchAcceptsClusterIdSSwitch() {
    assertEquals(ExitCode.OK, executeCommand("--init", "--clusterid=abcdefg"));
    assertEquals("abcdefg", mock.clusterId);
  }

  @Test
  public void testInitSwitchWithInvalidParamDoesNotRun() {
    assertEquals(ExitCode.USAGE,
            executeCommand("--init", "--clusterid=abcdefg", "--invalid"));
    assertFalse(mock.initCalled);
  }

  @Test
  public void testBootStrapSwitchWithInvalidParamDoesNotRun() {
    assertEquals(ExitCode.USAGE,
            executeCommand("--bootstrap", "--clusterid=abcdefg", "--invalid"));
    assertFalse(mock.bootStrapCalled);
  }

  @Test
  public void testUnSuccessfulInitThrowsException() {
    mock.throwOnInit = true;
    assertEquals(GenericCli.EXECUTION_ERROR_EXIT_CODE,
            executeCommand("--init"));
  }

  @Test
  public void testUnSuccessfulBootStrapThrowsException() {
    mock.throwOnBootstrap = true;
    assertEquals(GenericCli.EXECUTION_ERROR_EXIT_CODE,
            executeCommand("--bootstrap"));
  }

  @Test
  public void testGenClusterIdRunsGenerate() {
    assertEquals(ExitCode.OK, executeCommand("--genclusterid"));
    assertTrue(mock.generateCalled);
  }

  @Test
  public void testGenClusterIdWithInvalidParamDoesNotRun() {
    assertEquals(ExitCode.USAGE, executeCommand("--genclusterid", "--invalid"));
    assertFalse(mock.generateCalled);
  }

  @Test
  public void testUsagePrintedOnInvalidInput()
      throws UnsupportedEncodingException {
    assertEquals(ExitCode.USAGE, executeCommand("--invalid"));
    Pattern p = Pattern.compile("^Unknown option:.*--invalid.*\nUsage");
    Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  private int executeCommand(String... args) {
    return new StorageContainerManagerStarter(mock).execute(args);
  }

  static class MockSCMStarter implements SCMStarterInterface {

    private boolean initStatus = true;
    private boolean throwOnStart = false;
    private boolean throwOnInit  = false;
    private boolean throwOnBootstrap  = false;
    private boolean startCalled = false;
    private boolean initCalled = false;
    private boolean bootStrapCalled = false;
    private boolean generateCalled = false;
    private String clusterId = null;

    @Override
    public void start(OzoneConfiguration conf) throws Exception {
      if (throwOnStart) {
        throw new Exception("Simulated error on start");
      }
      startCalled = true;
    }

    @Override
    public boolean init(OzoneConfiguration conf, String cid)
        throws IOException {
      if (throwOnInit) {
        throw new IOException("Simulated error on init");
      }
      initCalled = true;
      clusterId = cid;
      return initStatus;
    }

    @Override
    public boolean bootStrap(OzoneConfiguration conf)
        throws IOException {
      if (throwOnBootstrap) {
        throw new IOException("Simulated error on init");
      }
      bootStrapCalled = true;
      return initStatus;
    }

    @Override
    public String generateClusterId() {
      generateCalled = true;
      return "static-cluster-id";
    }
  }
}

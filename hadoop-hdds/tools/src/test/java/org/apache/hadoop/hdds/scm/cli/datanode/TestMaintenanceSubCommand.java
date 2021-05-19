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
package org.apache.hadoop.hdds.scm.cli.datanode;

import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.mockito.Mockito;
import picocli.CommandLine;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.mock;

/**
 * Unit tests to validate the the DecommissionSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestMaintenanceSubCommand {

  private MaintenanceSubCommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @Before
  public void setup() throws UnsupportedEncodingException {
    cmd = new MaintenanceSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @After
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testNoErrorsWhenEnteringMaintenance() throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.startMaintenanceNodes(
        anyListOf(String.class), anyInt()))
        .thenAnswer(invocation -> new ArrayList<DatanodeAdminError>());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("host1", "host2");
    cmd.execute(scmClient);

    Pattern p = Pattern.compile(
        "^Entering\\smaintenance\\smode\\son\\sdatanode\\(s\\)",
        Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host1$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host2$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testErrorsReportedWhenEnteringMaintenance() throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.startMaintenanceNodes(
        anyListOf(String.class), anyInt()))
        .thenAnswer(invocation -> {
          ArrayList<DatanodeAdminError> e = new ArrayList<>();
          e.add(new DatanodeAdminError("host1", "host1 error"));
          return e;
        });

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("host1", "host2");
    try {
      cmd.execute(scmClient);
      fail("Should not succeed without an exception");
    } catch (IOException e) {
      // Expected
    }

    Pattern p = Pattern.compile(
        "^Entering\\smaintenance\\smode\\son\\sdatanode\\(s\\)",
        Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host1$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host2$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^Error: host1: host1 error$", Pattern.MULTILINE);
    m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

}
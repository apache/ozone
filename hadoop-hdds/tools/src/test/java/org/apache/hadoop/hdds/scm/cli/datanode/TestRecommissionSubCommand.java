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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests to validate the the RecommissionSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestRecommissionSubCommand {

  private RecommissionSubCommand cmd;
  private ScmClient scmClient;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new RecommissionSubCommand();
    scmClient = mock(ScmClient.class);
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testMultipleHostnamesCanBeReadFromStdin() throws Exception {
    when(scmClient.decommissionNodes(anyList()))
            .thenAnswer(invocation -> new ArrayList<DatanodeAdminError>());

    String input = "host1\nhost2\nhost3\n";
    System.setIn(new ByteArrayInputStream(input.getBytes(DEFAULT_ENCODING)));
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-");
    cmd.execute(scmClient);

    Pattern p = Pattern.compile(
            "^Started\\srecommissioning\\sdatanode\\(s\\)", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host1$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host2$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host3$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testNoErrorsWhenRecommissioning() throws IOException  {
    when(scmClient.recommissionNodes(anyList()))
        .thenAnswer(invocation -> new ArrayList<DatanodeAdminError>());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("host1", "host2");
    cmd.execute(scmClient);

    Pattern p = Pattern.compile(
        "^Started\\srecommissioning\\sdatanode\\(s\\)", Pattern.MULTILINE);
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
  public void testErrorsReportedWhenRecommissioning() throws IOException  {
    when(scmClient.recommissionNodes(anyList()))
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
        "^Started\\srecommissioning\\sdatanode\\(s\\)", Pattern.MULTILINE);
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

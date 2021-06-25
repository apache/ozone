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

import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStopSubcommand;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStartSubcommand;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStatusSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests to validate the the ContainerBalancerSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestContainerBalancerSubCommand {

  private ContainerBalancerStopSubcommand stopCmd;
  private ContainerBalancerStartSubcommand startCmd;
  private ContainerBalancerStatusSubcommand statusCmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @Before
  public void setup() throws UnsupportedEncodingException {
    stopCmd = new ContainerBalancerStopSubcommand();
    startCmd = new ContainerBalancerStartSubcommand();
    statusCmd = new ContainerBalancerStatusSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @After
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testContainerBalancerStatusSubcommandRunning()
      throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);

    //test status is running
    Mockito.when(scmClient.getContainerBalancerStatus())
        .thenAnswer(invocation -> true);

    statusCmd.execute(scmClient);

    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sRunning.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStatusSubcommandNotRunning()
      throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);

    Mockito.when(scmClient.getContainerBalancerStatus())
        .thenAnswer(invocation -> false);

    statusCmd.execute(scmClient);

    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sNot\\sRunning.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStopSubcommand() throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);
    stopCmd.execute(scmClient);

    Pattern p = Pattern.compile("^Stopping\\sContainerBalancer...");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStartSubcommandWhenBalancerIsNotRunning()
      throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.startContainerBalancer(null, null, null, null))
        .thenAnswer(invocation -> true);
    startCmd.execute(scmClient);

    Pattern p = Pattern.compile("^Starting\\sContainerBalancer" +
        "\\sSuccessfully.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStartSubcommandWhenBalancerIsRunning()
      throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.startContainerBalancer(null, null, null, null))
        .thenAnswer(invocation -> false);
    startCmd.execute(scmClient);

    Pattern p = Pattern.compile("^ContainerBalancer\\sis\\salready\\srunning," +
        "\\sPlease\\sstop\\sit\\sfirst.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

}

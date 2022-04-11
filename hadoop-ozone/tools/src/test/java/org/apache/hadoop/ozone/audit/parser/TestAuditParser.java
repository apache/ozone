/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.audit.parser;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.ParameterException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests AuditParser.
 */
public class TestAuditParser {
  private static File outputBaseDir;
  private static AuditParser parserTool;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestAuditParser.class);
  private static final ByteArrayOutputStream OUT = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_CODING = UTF_8.name();
  private static String dbName;
  private static final String LOGS = TestAuditParser.class
      .getClassLoader().getResource("testaudit.log").getPath();
  private static final String LOGS1 = TestAuditParser.class
      .getClassLoader().getResource("testloadaudit.log").getPath();
  /**
   * Creates output directory which will be used by the test-cases.
   * If a test-case needs a separate directory, it has to create a random
   * directory inside {@code outputBaseDir}.
   *
   * @throws Exception In case of exception while creating output directory.
   */
  @BeforeClass
  public static void init() throws Exception {
    outputBaseDir = getRandomTempDir();
    dbName = getRandomTempDir() + "/testAudit.db";
    parserTool = new AuditParser();
    String[] args = new String[]{dbName, "load", LOGS};
    execute(args, "");
  }

  @Before
  public void setup() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(OUT, false, DEFAULT_CODING));
    System.setErr(new PrintStream(err, false, DEFAULT_CODING));
  }

  @After
  public void reset() {
    // reset stream after each unit test
    OUT.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  /**
   * Cleans up the output base directory.
   */
  @AfterClass
  public static void cleanup() throws IOException {
    FileUtils.deleteDirectory(outputBaseDir);
  }

  private static void execute(String[] args, String msg) {
    List<String> arguments = new ArrayList(Arrays.asList(args));
    LOG.info("Executing shell command with args {}", arguments);
    CommandLine cmd = parserTool.getCmd();

    IExceptionHandler2<List<Object>> exceptionHandler =
        new IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(ParameterException ex,
              String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(ExecutionException ex,
              ParseResult parseResult) {
            throw ex;
          }
        };
    cmd.parseWithHandlers(new CommandLine.RunLast(),
        exceptionHandler, args);
    try {
      Assert.assertTrue(OUT.toString(DEFAULT_CODING).contains(msg));
    } catch (UnsupportedEncodingException ignored) {
    }
  }

  /**
   * Test to find top 5 commands.
   */
  @Test
  public void testTemplateTop5Cmds() {
    String[] args = new String[]{dbName, "template", "top5cmds"};
    execute(args,
        "DELETE_KEY\t3\t\n" +
            "ALLOCATE_KEY\t2\t\n" +
            "COMMIT_KEY\t2\t\n" +
            "CREATE_BUCKET\t2\t\n" +
            "CREATE_VOLUME\t2\t\n\n");
  }

  /**
   * Test to find top 5 users.
   */
  @Test
  public void testTemplateTop5Users() {
    String[] args = new String[]{dbName, "template", "top5users"};
    execute(args, "hadoop\t12\t\n");
  }

  /**
   * Test to find top 5 users.
   */
  @Test
  public void testTemplateTop5ActiveTimeBySeconds() {
    String[] args = new String[]{dbName, "template", "top5activetimebyseconds"};
    execute(args,
        "2018-09-06 01:57:22\t3\t\n" +
            "2018-09-06 01:58:08\t1\t\n" +
            "2018-09-06 01:58:09\t1\t\n" +
            "2018-09-06 01:58:18\t1\t\n" +
            "2018-09-06 01:59:18\t1\t\n");
  }

  /**
   * Test to execute custom query.
   */
  @Test
  public void testQueryCommand() {
    String[] args = new String[]{dbName, "query",
        "select count(*) from audit"};
    execute(args,
        "12");
  }

  /**
   * Test to execute load audit log.
   */
  @Test
  public void testLoadCommand() {
    String[] args1 = new String[]{dbName, "load", LOGS1};
    try{
      execute(args1, "");
      fail("No exception thrown.");
    } catch (Exception e) {
      assertTrue(e.getMessage()
          .contains("java.lang.ArrayIndexOutOfBoundsException: 5"));
    }
  }

  /**
   * Test to check help message.
   * @throws Exception
   */
  @Test
  public void testHelp() throws Exception {
    String[] args = new String[]{"--help"};
    execute(args,
        "Usage: ozone auditparser [-hV] [--verbose] " +
            "[-conf=<configurationPath>]\n" +
            "                         [-D=<String=String>]... <database> " +
            "[COMMAND]");
  }

  private static File getRandomTempDir() throws IOException {
    File tempDir = new File(outputBaseDir,
        RandomStringUtils.randomAlphanumeric(5));
    FileUtils.forceMkdir(tempDir);
    return tempDir;
  }
}

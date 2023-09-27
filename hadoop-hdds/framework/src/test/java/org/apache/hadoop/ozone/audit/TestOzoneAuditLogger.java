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
package org.apache.hadoop.ozone.audit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.audit.AuditEventStatus.FAILURE;
import static org.apache.hadoop.ozone.audit.AuditEventStatus.SUCCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInOrder;


/**
 * Test Ozone Audit Logger.
 */
public class TestOzoneAuditLogger {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneAuditLogger.class.getName());

  static {
    System.setProperty("log4j.configurationFile", "auditlog.properties");
    System.setProperty("log4j2.contextSelector",
        "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
  }

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.OMLOGGER);

  private static final Map<String, String> PARAMS =
      new DummyEntity().toAuditMap();

  private static final String IP_ADDRESS = "192.168.0.1";
  private static final String USER = "john";

  private static final AuditMessage WRITE_FAIL_MSG =
      new AuditMessage.Builder()
          .setUser(USER)
          .atIp(IP_ADDRESS)
          .forOperation(DummyAction.CREATE_VOLUME)
          .withParams(PARAMS)
          .withResult(FAILURE)
          .withException(null).build();

  private static final AuditMessage WRITE_SUCCESS_MSG =
      new AuditMessage.Builder()
          .setUser(USER)
          .atIp(IP_ADDRESS)
          .forOperation(DummyAction.CREATE_VOLUME)
          .withParams(PARAMS)
          .withResult(SUCCESS)
          .withException(null).build();

  private static final AuditMessage READ_FAIL_MSG =
      new AuditMessage.Builder()
          .setUser(USER)
          .atIp(IP_ADDRESS)
          .forOperation(DummyAction.READ_VOLUME)
          .withParams(PARAMS)
          .withResult(FAILURE)
          .withException(null).build();

  private static final AuditMessage READ_SUCCESS_MSG =
      new AuditMessage.Builder()
          .setUser(USER)
          .atIp(IP_ADDRESS)
          .forOperation(DummyAction.READ_VOLUME)
          .withParams(PARAMS)
          .withResult(SUCCESS)
          .withException(null).build();

  private static final AuditMessage AUTH_FAIL_MSG =
      new AuditMessage.Builder()
          .setUser(USER)
          .atIp(IP_ADDRESS)
          .forOperation(DummyAction.READ_VOLUME)
          .withParams(PARAMS)
          .withResult(FAILURE)
          .withException(null).build();

  @AfterAll
  public static void tearDown() {
    File file = new File("audit.log");
    if (FileUtils.deleteQuietly(file)) {
      LOG.info("{} has been deleted as all tests have completed.",
              file.getName());
    } else {
      LOG.info("audit.log could not be deleted.");
    }
  }

  @BeforeEach
  public void init() {
    AUDIT.refreshDebugCmdSet();
  }

  /**
   * Test to verify default log level is INFO when logging WRITE success events.
   */
  @Test
  public void verifyDefaultLogLevelForWriteSuccess() throws IOException {
    AUDIT.logWriteSuccess(WRITE_SUCCESS_MSG);
    String expected =
        "INFO  | OMAudit | ? | " + WRITE_SUCCESS_MSG.getFormattedMessage();
    verifyLog(expected);
  }

  /**
   * Test to verify default log level is ERROR when logging WRITE failure
   * events.
   */
  @Test
  public void verifyDefaultLogLevelForWriteFailure() throws IOException {
    AUDIT.logWriteFailure(WRITE_FAIL_MSG);
    String expected =
        "ERROR | OMAudit | ? | " + WRITE_FAIL_MSG.getFormattedMessage();
    verifyLog(expected);
  }

  /**
   * Test to verify default log level is INFO when logging READ success events.
   */
  @Test
  public void verifyDefaultLogLevelForReadSuccess() throws IOException {
    AUDIT.logReadSuccess(READ_SUCCESS_MSG);
    String expected =
        "INFO  | OMAudit | ? | " + READ_SUCCESS_MSG.getFormattedMessage();
    verifyLog(expected);
  }

  /**
   * Test to verify default log level is ERROR when logging READ failure events.
   */
  @Test
  public void verifyDefaultLogLevelForFailure() throws IOException {
    AUDIT.logReadFailure(READ_FAIL_MSG);
    String expected =
        "ERROR | OMAudit | ? | " + READ_FAIL_MSG.getFormattedMessage();
    verifyLog(expected);
  }

  @Test
  public void verifyDefaultLogLevelForAuthFailure() throws IOException {
    AUDIT.logAuthFailure(AUTH_FAIL_MSG);
    String expected =
        "ERROR | OMAudit | ? | " + AUTH_FAIL_MSG.getFormattedMessage();
    verifyLog(expected);
  }

  @Test
  public void messageIncludesAllParts() {
    String message = WRITE_FAIL_MSG.getFormattedMessage();
    assertTrue(message.contains(USER), message);
    assertTrue(message.contains(IP_ADDRESS), message);
    assertTrue(message.contains(DummyAction.CREATE_VOLUME.name()), message);
    assertTrue(message.contains(PARAMS.toString()), message);
    assertTrue(message.contains(FAILURE.getStatus()), message);
  }

  /**
   * Test to verify no WRITE event is logged.
   */
  @Test
  public void excludedEventNotLogged() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(AuditLogger.AUDIT_LOG_DEBUG_CMD_LIST_PREFIX +
            AuditLoggerType.OMLOGGER.getType().toLowerCase(Locale.ROOT),
        "CREATE_VOLUME");
    AUDIT.refreshDebugCmdSet(conf);
    AUDIT.logWriteSuccess(WRITE_SUCCESS_MSG);
    verifyNoLog();
  }
  
  /**
   * Test to verify if multiline entries can be checked.
   */
  @Test
  public void messageIncludesMultilineException() throws IOException {
    String exceptionMessage = "Dummy exception message";
    TestException testException = new TestException(exceptionMessage);
    AuditMessage exceptionAuditMessage =
        new AuditMessage.Builder()
            .setUser(USER)
            .atIp(IP_ADDRESS)
            .forOperation(DummyAction.CREATE_VOLUME)
            .withParams(PARAMS)
            .withResult(FAILURE)
            .withException(testException).build();
    AUDIT.logWriteFailure(exceptionAuditMessage);
    verifyLog(
        "ERROR | OMAudit | ? | user=john | "
            + "ip=192.168.0.1 | op=CREATE_VOLUME "
            + "{key1=value1, key2=value2} | ret=FAILURE",
        "org.apache.hadoop.ozone.audit."
            + "TestOzoneAuditLogger$TestException: Dummy exception message",
        "at org.apache.hadoop.ozone.audit.TestOzoneAuditLogger"
            + ".messageIncludesMultilineException"
            + "(TestOzoneAuditLogger.java");
  }

  private void verifyLog(String... expectedStrings) throws IOException {
    File file = new File("audit.log");
    List<String> lines = FileUtils.readLines(file, (String)null);
    final int retry = 5;
    int i = 0;
    while (lines.isEmpty() && i < retry) {
      lines = FileUtils.readLines(file, (String)null);
      try {
        Thread.sleep(500 * (i + 1));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
      i++;
    }
    //check if every expected string can be found in the log entry
    assertThat(
        lines.subList(0, expectedStrings.length),
        containsInOrder(expectedStrings)
    );
    //empty the file
    lines.clear();
    FileUtils.writeLines(file, lines, false);
  }

  private void verifyNoLog() throws IOException {
    File file = new File("audit.log");
    List<String> lines = FileUtils.readLines(file, (String)null);
    // When no log entry is expected, the log file must be empty
    assertEquals(0, lines.size());
  }

  private static class TestException extends Exception {
    TestException(String message) {
      super(message);
    }
  }

  private Matcher<Iterable<? extends String>> containsInOrder(
      String[] expectedStrings) {
    return IsIterableContainingInOrder.contains(
        Arrays.stream(expectedStrings)
            .map(str -> containsString(str))
            .collect(Collectors.toList())
    );
  }
}

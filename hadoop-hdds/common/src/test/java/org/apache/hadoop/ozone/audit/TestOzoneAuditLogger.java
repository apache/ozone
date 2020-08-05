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
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.audit.AuditEventStatus.FAILURE;
import static org.apache.hadoop.ozone.audit.AuditEventStatus.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test Ozone Audit Logger.
 */
public class TestOzoneAuditLogger {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneAuditLogger.class.getName());

  static {
    System.setProperty("log4j.configurationFile", "auditlog.properties");
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

  @AfterClass
  public static void tearDown() {
    File file = new File("audit.log");
    if (FileUtils.deleteQuietly(file)) {
      LOG.info("{} has been deleted as all tests have completed.",
              file.getName());
    } else {
      LOG.info("audit.log could not be deleted.");
    }
  }

  /**
   * Test to verify default log level is INFO when logging success events.
   */
  @Test
  public void verifyDefaultLogLevelForSuccess() throws IOException {
    AUDIT.logWriteSuccess(WRITE_SUCCESS_MSG);
    String expected =
        "INFO  | OMAudit | " + WRITE_SUCCESS_MSG.getFormattedMessage();
    verifyLog(expected);
  }

  /**
   * Test to verify default log level is ERROR when logging failure events.
   */
  @Test
  public void verifyDefaultLogLevelForFailure() throws IOException {
    AUDIT.logWriteFailure(WRITE_FAIL_MSG);
    String expected =
        "ERROR | OMAudit | " + WRITE_FAIL_MSG.getFormattedMessage();
    verifyLog(expected);
  }

  @Test
  public void messageIncludesAllParts() {
    String message = WRITE_FAIL_MSG.getFormattedMessage();
    assertTrue(message, message.contains(USER));
    assertTrue(message, message.contains(IP_ADDRESS));
    assertTrue(message, message.contains(DummyAction.CREATE_VOLUME.name()));
    assertTrue(message, message.contains(PARAMS.toString()));
    assertTrue(message, message.contains(FAILURE.getStatus()));
  }

  /**
   * Test to verify no READ event is logged.
   */
  @Test
  public void notLogReadEvents() throws IOException {
    AUDIT.logReadSuccess(READ_SUCCESS_MSG);
    AUDIT.logReadFailure(READ_FAIL_MSG);
    verifyNoLog();
  }

  /**
   * Test to verify if multiline entries can be checked.
   */

  @Test
  public void messageIncludesMultilineException() throws IOException {
      try{
        throw new TestException("Dummy exception");
      } catch(TestException testException)
      {
        AuditMessage exceptionAuditMessage =
                new AuditMessage.Builder()
                        .setUser(USER)
                        .atIp(IP_ADDRESS)
                        .forOperation(DummyAction.CREATE_VOLUME)
                        .withParams(PARAMS)
                        .withResult(FAILURE)
                        .withException(testException).build();
        AUDIT.logWriteFailure(exceptionAuditMessage);
        verifyLog("TestException","org.apache.hadoop.ozone.audit.TestOzoneAuditLogger.messageIncludesMultilineException");

      }
  }

  private void verifyLog(String... expected_strings) throws IOException {
    File file = new File("audit.log");
    List<String> lines = FileUtils.readLines(file, (String)null);
    final int retry = 5;
    int i = 0;
    while (lines.isEmpty() && i < retry) {
      lines = FileUtils.readLines(file, (String)null);
      try {
        Thread.sleep(500 * (i + 1));
      } catch(InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
      i++;
    }
    //check if every expected string can be found in the log entry
    for(String expected : expected_strings)
    {
      assertTrue(contains(lines,expected));
    }
    //empty the file
    lines.clear();
    FileUtils.writeLines(file, lines, false);
  }

  private boolean contains(List<String> lines, String searched){
    for(String line : lines){
      if(line.toLowerCase().contains(searched.toLowerCase())){
        return true;
      }
    }
    return false;
  }

  private void verifyNoLog() throws IOException {
    File file = new File("audit.log");
    List<String> lines = FileUtils.readLines(file, (String)null);
    // When no log entry is expected, the log file must be empty
    assertEquals(0, lines.size());
  }

  private class TestException extends Exception{
    public TestException(String message) {
      super(message);
    }
  }
}

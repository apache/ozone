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

package org.apache.hadoop.ozone.audit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ozone.test.GenericTestUtils.waitFor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;

/**
 * Utility class to read audit logs.
 */
public final class AuditLogTestUtils {
  private static final String AUDITLOG_FILENAME = "audit.log";

  private AuditLogTestUtils() {
  }

  /**
   * Enables audit logging for the mini ozone cluster. Must be called in static
   * block of the test class before starting the cluster.
   */
  public static void enableAuditLog() {
    System.setProperty("log4j.configurationFile", "auditlog.properties");
  }

  /**
   * Searches for the given action in the audit log file.
   */
  public static void verifyAuditLog(AuditAction action,
      AuditEventStatus eventStatus)
      throws InterruptedException, TimeoutException {
    waitFor(
        () -> auditLogContains(action.getAction(), eventStatus.getStatus()),
        1000, 10000);
  }

  public static boolean auditLogContains(String... strings) {
    File file = new File(AUDITLOG_FILENAME);
    try {
      String contents = FileUtils.readFileToString(file, UTF_8);
      for (String s : strings) {
        if (!contents.contains(s)) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  public static void truncateAuditLogFile() throws IOException {
    Files.write(Paths.get(AUDITLOG_FILENAME), new byte[0]);
  }

  public static void deleteAuditLogFile() {
    FileUtils.deleteQuietly(new File(AUDITLOG_FILENAME));
  }
}

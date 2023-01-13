/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.audit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
                                    AuditEventStatus eventStatus) {
    Path file = Paths.get(AUDITLOG_FILENAME);
    try (BufferedReader br = Files.newBufferedReader(file,
            StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.contains(action.getAction()) &&
                line.contains(eventStatus.getStatus())) {
          return;
        }
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    } finally {
      truncateAuditLogFile();
    }
    throw new AssertionError("Audit log file doesn't contain " +
            "the message with params  event=" + action.getAction() +
            " result=" + eventStatus.getStatus());
  }

  private static void truncateAuditLogFile() {
    File auditLogFile = new File(AUDITLOG_FILENAME);
    try {
      new FileOutputStream(auditLogFile).getChannel().truncate(0).close();
    } catch (IOException e) {
      System.out.println("Failed to truncate file: " + AUDITLOG_FILENAME);
    }
  }
}

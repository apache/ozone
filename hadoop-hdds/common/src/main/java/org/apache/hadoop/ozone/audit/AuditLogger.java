/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.audit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


/**
 * Class to define Audit Logger for Ozone.
 */
public class AuditLogger {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogger.class);

  private ExtendedLogger logger;
  private static final String FQCN = AuditLogger.class.getName();
  private static final Marker WRITE_MARKER = AuditMarker.WRITE.getMarker();
  private static final Marker READ_MARKER = AuditMarker.READ.getMarker();
  private static final Marker AUTH_MARKER = AuditMarker.AUTH.getMarker();
  private final AtomicReference<Set<String>> debugCmdSetRef =
      new AtomicReference<>(new HashSet<>());
  public static final String AUDIT_LOG_DEBUG_CMD_LIST_PREFIX =
      "ozone.audit.log.debug.cmd.list.";
  private AuditLoggerType type;

  /**
   * Parametrized Constructor to initialize logger.
   * @param type Audit Logger Type
   */
  public AuditLogger(AuditLoggerType type) {
    initializeLogger(type);
  }

  /**
   * Initializes the logger with specific type.
   * @param loggerType specified one of the values from enum AuditLoggerType.
   */
  private void initializeLogger(AuditLoggerType loggerType) {
    this.logger = LogManager.getContext(false).getLogger(loggerType.getType());
    this.type = loggerType;
    refreshDebugCmdSet();
  }

  @VisibleForTesting
  public ExtendedLogger getLogger() {
    return logger;
  }

  public void logWriteSuccess(AuditMessage msg) {
    if (shouldLogAtDebug(msg)) {
      this.logger.logIfEnabled(FQCN, Level.DEBUG, WRITE_MARKER, msg, null);
    } else {
      this.logger.logIfEnabled(FQCN, Level.INFO, WRITE_MARKER, msg, null);
    }
  }

  public void logWriteFailure(AuditMessage msg) {
    this.logger.logIfEnabled(FQCN, Level.ERROR, WRITE_MARKER, msg,
        msg.getThrowable());
  }

  public void logAuthFailure(AuditMessage msg) {
    this.logger.logIfEnabled(FQCN, Level.ERROR, AUTH_MARKER, msg,
        msg.getThrowable());
  }

  public void logReadSuccess(AuditMessage msg) {
    if (shouldLogAtDebug(msg)) {
      this.logger.logIfEnabled(FQCN, Level.DEBUG, READ_MARKER, msg, null);
    } else {
      this.logger.logIfEnabled(FQCN, Level.INFO, READ_MARKER, msg, null);
    }
  }

  public void logReadFailure(AuditMessage msg) {
    this.logger.logIfEnabled(FQCN, Level.ERROR, READ_MARKER, msg,
        msg.getThrowable());
  }

  public void logWrite(AuditMessage auditMessage) {
    if (auditMessage.getThrowable() == null) {
      logWriteSuccess(auditMessage);
    } else {
      logWriteFailure(auditMessage);
    }
  }

  public void refreshDebugCmdSet() {
    OzoneConfiguration conf = new OzoneConfiguration();
    refreshDebugCmdSet(conf);
  }

  public void refreshDebugCmdSet(OzoneConfiguration conf) {
    Collection<String> cmds = conf.getTrimmedStringCollection(
        AUDIT_LOG_DEBUG_CMD_LIST_PREFIX +
            type.getType().toLowerCase(Locale.ROOT));
    LOG.info("Refresh DebugCmdSet for {} to {}.", type.getType(), cmds);
    debugCmdSetRef.set(
        cmds.stream().map(String::toLowerCase).collect(Collectors.toSet()));
  }

  private boolean shouldLogAtDebug(AuditMessage auditMessage) {
    return debugCmdSetRef.get()
        .contains(auditMessage.getOp().toLowerCase(Locale.ROOT));
  }
}

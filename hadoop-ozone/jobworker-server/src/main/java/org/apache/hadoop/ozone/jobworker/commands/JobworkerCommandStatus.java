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

package org.apache.hadoop.ozone.jobworker.commands;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;

/**
 * A class that is used to communicate status of jobworker commands.
 */

public class JobworkerCommandStatus {

  private final long cmdId;
  private final OMJobworkerCommandProto.Type type;
  private volatile Status status;
  private final String omServiceId;
  private volatile String message;
  private volatile CommandResultCode resultCode;
  private volatile CommandExecutionResultsProto executionResults;
  private String attemptId;
  private Consumer<CommandStatus> persistence;

  void setPersistence(Consumer<CommandStatus> persistence) {
    this.persistence = persistence;
  }

  static JobworkerCommandStatus fromProto(CommandStatus status) {
    JobworkerCommandStatus result = new JobworkerCommandStatus(status.getCmdId(), status.getType(),
        status.getStatus(), status.getOmServiceId(), status.hasMsg() ? status.getMsg() : null,
        status.hasResultCode() ? status.getResultCode() : null);
    result.attemptId = status.getAttemptId();
    result.executionResults = status.hasExecutionResults() ? status.getExecutionResults() : null;
    return result;
  }

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Constructs a JobworkerCommandStatus.
   *
   * @param cmdId   command ID
   * @param type    command type
   * @param status  status enum
   * @param message optional status message
   */
  public JobworkerCommandStatus(long cmdId, OMJobworkerCommandProto.Type type,
                                Status status, String omServiceId, String message, CommandResultCode resultCode) {
    Preconditions.checkArgument(cmdId >= 0, "CmdId must be >= 0");
    Objects.requireNonNull(type, "Type must be not null");
    Objects.requireNonNull(status, "Status must be not null");
    Objects.requireNonNull(omServiceId, "OmServiceId must be not null");
    this.cmdId = cmdId;
    this.type = type;
    this.status = status;
    this.omServiceId = omServiceId;
    this.message = message;
    this.resultCode = resultCode;
  }

  /**
   * Create a new builder.
   *
   * @return Builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns the command ID.
   *
   * @return command ID
   */
  public long getCmdId() {
    return cmdId;
  }

  /**
   * Returns the command type.
   *
   * @return command type
   */
  public OMJobworkerCommandProto.Type getType() {
    return type;
  }

  /**
   * Returns the status.
   *
   * @return status enum
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Check if this command is in a terminal state (SUCCEEDED or FAILED).
   *
   * @return true if the command is in a terminal state
   */
  public boolean isTerminalState() {
    Status currentStatus = status; // Capture once for consistency
    return currentStatus == CommandStatus.Status.SUCCEEDED ||
        currentStatus == CommandStatus.Status.FAILED;
  }

  /**
   * Returns the message.
   *
   * @return message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Atomically updates both status and message to ensure they are seen together
   * by other threads.
   *
   * @param newStatus new status
   * @param msg       new message
   * @param code      the command error code
   */
  public void updateStatusAndMessage(Status newStatus, String msg, CommandResultCode code) {
    Objects.requireNonNull(newStatus, "New status must be not null");
    lock.writeLock().lock();
    try {
      if (persistence != null) {
        CommandStatus.Builder proposed = getProtobufMessage().toBuilder().setStatus(newStatus)
            .clearMsg().clearResultCode();
        if (msg != null) {
          proposed.setMsg(msg);
        }
        if (code != null) {
          proposed.setResultCode(code);
        }
        persistence.accept(proposed.build());
      }
      this.status = newStatus;
      this.message = msg;
      this.resultCode = code;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setExecutionResults(CommandExecutionResultsProto results) {
    lock.writeLock().lock();
    try {
      if (persistence != null) {
        CommandStatus.Builder proposed = getProtobufMessage().toBuilder().clearExecutionResults();
        if (results != null) {
          proposed.setExecutionResults(results);
        }
        persistence.accept(proposed.build());
      }
      this.executionResults = results;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Converts to a protobuf message.
   *
   * @return CommandStatus protobuf message
   */
  public CommandStatus getProtobufMessage() {
    lock.readLock().lock();
    try {
      CommandStatus.Builder builder = CommandStatus.newBuilder()
          .setCmdId(cmdId)
          .setStatus(status)
          .setType(type)
          .setOmServiceId(omServiceId);
      if (attemptId != null) {
        builder.setAttemptId(attemptId);
      }

      if (message != null) {
        builder.setMsg(message);
      }
      if (resultCode != null) {
        builder.setResultCode(resultCode);
      }
      if (executionResults != null) {
        builder.setExecutionResults(executionResults);
      }

      return builder.build();
    } finally {
      lock.readLock().unlock();
    }
  }

  public String getOmServiceId() {
    return omServiceId;
  }

  /**
   * Builder for JobworkerCommandStatus.
   */
  public static class Builder {
    private long cmdId;
    private OMJobworkerCommandProto.Type type;
    private Status status;
    private String omServiceId;
    private String message;
    private CommandResultCode resultCode;

    /**
     * Sets command ID.
     *
     * @param cmdId command ID
     * @return Builder
     */
    public Builder setCmdId(long cmdId) {
      this.cmdId = cmdId;
      return this;
    }

    /**
     * Sets command type.
     *
     * @param type command type
     * @return Builder
     */
    public Builder setType(OMJobworkerCommandProto.Type type) {
      this.type = type;
      return this;
    }

    /**
     * Sets status.
     *
     * @param status status enum
     * @return Builder
     */
    public Builder setStatus(Status status) {
      this.status = status;
      return this;
    }

    /**
     * Sets message.
     *
     * @param msg message
     * @return Builder
     */
    public Builder setMsg(String msg) {
      this.message = msg;
      return this;
    }

    /**
     * Sets omServiceId.
     * @param omServiceId, the command's omServiceId
     * @return Builder
     */
    public Builder setOmServiceId(String omServiceId) {
      this.omServiceId = omServiceId;
      return this;
    }

    /**
     * Sets executionResults.
     * @param resultCode, the command's resultCode
     * @return Builder
     */
    public Builder setResultCode(
        CommandResultCode resultCode) {
      this.resultCode = resultCode;
      return this;
    }

    /**
     * Build the JobworkerCommandStatus.
     *
     * @return JobworkerCommandStatus instance
     */
    public JobworkerCommandStatus build() {
      return new JobworkerCommandStatus(cmdId, type, status, omServiceId, message, resultCode);
    }
  }
}

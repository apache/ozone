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

package org.apache.hadoop.ozone.om.jobworker.command;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;

/**
 * Contains status information for a JobWorker command.
 */
public class JobworkerCommandInfo {
  private final OMJobworkerCommand command;
  private final UUID jobworkerUuid;
  private CommandStatus.Status status;
  private String message;
  private final long creationTime;
  private long lastStatusUpdateTime;
  private CommandResultCode resultCode;

  /**
   * Create a new CommandStatusInfo.
   *
   * @param command       the command type
   * @param jobworkerUuid the UUID of the target JobWorker
   * @param status        the initial status
   */
  public JobworkerCommandInfo(OMJobworkerCommand command,
                              UUID jobworkerUuid,
                              CommandStatus.Status status) {
    Objects.requireNonNull(command, "command id cannot be null");
    Preconditions.checkArgument(command.getId() > 0, "command id cannot be null");
    Objects.requireNonNull(command.getType(), "command type cannot be null");
    Objects.requireNonNull(jobworkerUuid, "jobworkerUuid id cannot be null");
    Objects.requireNonNull(status, "status id cannot be null");
    this.command = command;
    this.jobworkerUuid = jobworkerUuid;
    this.status = status;
    this.message = "";
    this.creationTime = System.currentTimeMillis();
    this.lastStatusUpdateTime = creationTime;
  }

  /**
   * Get the command ID.
   *
   * @return the command ID
   */
  public long getCommandId() {
    return command.getId();
  }

  /**
   * Get the command type.
   *
   * @return the command type
   */
  @Nonnull
  public OMJobworkerCommandProto.Type getCommandType() {
    return command.getType();
  }

  /**
   * Get the UUID of the target JobWorker.
   *
   * @return the JobWorker UUID
   */
  @Nonnull
  public UUID getJobworkerUuid() {
    return jobworkerUuid;
  }

  /**
   * Get the current status.
   *
   * @return the current status
   */
  @Nonnull
  public CommandStatus.Status getStatus() {
    return status;
  }

  /**
   * Set the current status.
   *
   * @param status the new status
   */
  public void setStatus(CommandStatus.Status status) {
    this.status = status;
  }

  /**
   * Get the message associated with the status.
   *
   * @return the status message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Set the message associated with the status.
   *
   * @param message the status message
   */
  public void setMessage(String message) {
    this.message = message;
  }

  public long getLastStatusUpdateTime() {
    return lastStatusUpdateTime;
  }

  public void updateLastStatusUpdateTime() {
    this.lastStatusUpdateTime = System.currentTimeMillis();
  }

  /**
   * Get the creation time of this command status (in milliseconds).
   *
   * @return the creation time
   */
  public long getCreationTime() {
    return creationTime;
  }

  public CommandResultCode getResultCode() {
    return resultCode;
  }

  public void setResultCode(
      CommandResultCode resultCode) {
    this.resultCode = resultCode;
  }

  /**
   * Check if this command is in a terminal state (SUCCEEDED or FAILED).
   *
   * @return true if the command is in a terminal state
   */
  public boolean isTerminalState() {
    return status == CommandStatus.Status.SUCCEEDED ||
        status == CommandStatus.Status.FAILED;
  }

  /**
   * Check if the command has completed.
   *
   * @return true if the command has completed (SUCCEEDED or FAILED),
   *         false otherwise
   */
  public boolean isCompleted() {
    return status == CommandStatus.Status.SUCCEEDED ||
        status == CommandStatus.Status.FAILED;
  }

  /**
   * Check if the command succeeded.
   *
   * @return true if the command has succeeded,
   *         false otherwise
   */
  public boolean isSucceeded() {
    return status == CommandStatus.Status.SUCCEEDED;
  }

  /**
   * Check if the command failed.
   *
   * @return true if the command has failed,
   *         false otherwise
   */
  public boolean isFailed() {
    return status == CommandStatus.Status.FAILED;
  }

  @Nonnull
  public OMJobworkerCommand getCommand() {
    return command;
  }

  @Override
  public String toString() {
    return "CommandStatusInfo{" +
        "commandId=" + command.getId() +
        ", commandType=" + command.getType() +
        ", jobworkerUuid=" + jobworkerUuid +
        ", status=" + status +
        ", message='" + message + '\'' +
        ", creationTime=" + creationTime +
        ", lastStatusUpdateTime=" + lastStatusUpdateTime +
        '}';
  }
}

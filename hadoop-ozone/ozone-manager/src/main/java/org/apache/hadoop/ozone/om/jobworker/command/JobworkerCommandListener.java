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

import jakarta.annotation.Nullable;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;

/**
 * Defines the event listener methods that JobWorker command implementations must support
 * to respond to command state changes. Specifies how commands should handle execution
 * events (executing, succeeded, failed) and timeout notifications.
 * <p>
 * Command tracking is kept only in the memory of the OM leader. After a leadership change the in-flight commands are
 * reported through {@link #onStatusUpdateTimeout}, and a command may run again when the owning workflow re-issues it.
 * Commands must therefore be idempotent, and callbacks must not change persisted state unless this OM is the ready
 * leader.
 */
public interface JobworkerCommandListener {

  /**
   * Called when a command is successfully sent to a JobWorker.
   *
   * @param command the command that was sent
   * @param jobworkerUuid the UUID of the target JobWorker
   */
  void onSendCommand(OMJobworkerCommandProto command, UUID jobworkerUuid);

  /**
   * Called when a command's status changes to SUCCEEDED.
   *
   * @param statusInfo the command status info
   * @param jobworkerDetails the JobWorker that executed the command
   */
  void onCommandSucceeded(JobworkerCommandInfo statusInfo,
      @Nullable CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails);

  /**
   * Called when a command's status changes to FAILED.
   *
   * @param statusInfo the command status info
   * @param jobworkerDetails the JobWorker that failed to execute the command
   */
  void onCommandFailed(JobworkerCommandInfo statusInfo,
      @Nullable CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails);

  /**
   * Called when a command's status changes to EXECUTING.
   *
   * @param statusInfo the command status info
   * @param jobworkerDetails the JobWorker that is executing the command
   */
  void onCommandExecuting(JobworkerCommandInfo statusInfo,
                          JobworkerDetails jobworkerDetails);

  /**
   * Called when a command has not received a status update for the configured timeout period.
   * This is NOT a command status, but an event indicating the command may be stuck or the
   * JobWorker may be unresponsive.
   *
   * @param statusInfo the command status info
   * @param jobworkerUuid the JobWorker uuid, the jobworker may not exist in OM
   */
  void onStatusUpdateTimeout(JobworkerCommandInfo statusInfo,
                             UUID jobworkerUuid);
}

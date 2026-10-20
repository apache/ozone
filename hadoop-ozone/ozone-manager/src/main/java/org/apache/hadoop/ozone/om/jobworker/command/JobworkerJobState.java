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

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.JobworkerJobRecord;

/** Validation shared by the replicated state machine and its Jobworker caller. */
public final class JobworkerJobState {
  private JobworkerJobState() {
  }

  public static String key(OMJobworkerCommandProto.Type type, long id) {
    return type.getNumber() + "/" + id;
  }

  public static boolean terminal(CommandStatus.Status status) {
    return status == CommandStatus.Status.SUCCEEDED || status == CommandStatus.Status.FAILED;
  }

  public static void validate(JobworkerJobRecord record) throws IOException {
    OMJobworkerCommandProto command = OMJobworkerCommandProto.parseFrom(record.getCommand());
    CommandStatus status = CommandStatus.parseFrom(record.getStatus());
    try {
      UUID.fromString(record.getWorkerId());
      UUID.fromString(command.getAttemptId());
      if (!command.hasCmdId() || command.getCmdId() <= 0 || !command.hasAttemptId()
          || command.getCommandType() == OMJobworkerCommandProto.Type.unknownCommand
          || command.getCommandType() == OMJobworkerCommandProto.Type.reregisterCommand
          || !record.getJobKey().equals(key(command.getCommandType(), command.getCmdId()))
          || status.getCmdId() != command.getCmdId() || status.getType() != command.getCommandType()
          || !status.getAttemptId().equals(command.getAttemptId()) || status.getOmServiceId().isEmpty()
          || status.getStatus() == CommandStatus.Status.UNKNOWN_STATUS) {
        throw new IOException("Invalid Jobworker job identity or status");
      }
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid Jobworker UUID", e);
    }
  }

  public static boolean canUpdate(CommandStatus current, CommandStatus proposed) {
    if (terminal(current.getStatus())) {
      return current.equals(proposed);
    }
    if (current.getStatus() == proposed.getStatus()) {
      return true;
    }
    switch (current.getStatus()) {
    case PENDING:
      return proposed.getStatus() != CommandStatus.Status.UNKNOWN_STATUS;
    case EXECUTING:
      return terminal(proposed.getStatus()) || proposed.getStatus() == CommandStatus.Status.UNCERTAIN;
    case UNCERTAIN:
      // The same assigned worker may reconcile its durable receipt after reconnecting.
      return proposed.getStatus() != CommandStatus.Status.UNKNOWN_STATUS;
    default:
      return false;
    }
  }
}

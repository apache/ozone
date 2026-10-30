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

package org.apache.hadoop.ozone.jobworker.report;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandStatus;

/**
 * Publishes CommandStatusReport which will be sent to OM as part of
 * heartbeat. CommandStatusReport consists of the following information:
 * - type    : type of command.
 * - status  : status of command execution (PENDING, EXECUTING, SUCCEEDED, FAILED).
 * - cmdId   : Command id.
 * - msg     : optional message.
 *
 * This class is similar to the CommandStatusReportPublisher in Datanode.
 */
public class JobworkerCommandStatusReportPublisher extends
    JobworkerReportPublisher<CommandStatusReportsProto> {

  @Override
  protected long getReportFrequency() {
    JobworkerConfiguration jwConf = getConf().getObject(JobworkerConfiguration.class);
    long cmdStatusReportIntervalMs = jwConf.getCommandStatusReportInterval().toMillis();
    long heartbeatFrequencyMs = jwConf.getHeartbeatInterval().toMillis();
    Preconditions.checkState(
        heartbeatFrequencyMs <= cmdStatusReportIntervalMs, String.format(
            "Command status report interval %sms cannot be configured lower than heartbeat frequency %sms.",
            cmdStatusReportIntervalMs, heartbeatFrequencyMs));
    return cmdStatusReportIntervalMs;
  }

  @Override
  protected CommandStatusReportsProto getReport() throws IOException {
    Map<String, Map<Long, JobworkerCommandStatus>> commandStatusMap =
        getContext().getCommandManager().getCommandStatusMap();
    CommandStatusReportsProto.Builder builder = CommandStatusReportsProto
        .newBuilder();
    for (Map<Long, JobworkerCommandStatus> map : commandStatusMap.values()) {
      Iterator<Long> iterator = map.keySet().iterator();
      iterator.forEachRemaining(key -> {
        JobworkerCommandStatus cmdStatus = map.get(key);
        if (cmdStatus == null) {
          return;
        }
        // If status is still non-terminal state, then don't remove it from the map as
        // CommandHandler will change its status when it works on this command.
        if (cmdStatus.isTerminalState()) {
          map.remove(key);
        }
        builder.addCmdStatus(cmdStatus.getProtobufMessage());
      });
    }

    return builder.getCmdStatusCount() > 0 ? builder.build() : null;
  }
}

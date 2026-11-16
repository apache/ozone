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

package org.apache.hadoop.ozone.container.common.report;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;

/**
 * Publishes CommandStatusReport which will be sent to SCM as part of
 * heartbeat. CommandStatusReport consist of the following information:
 * - type       : type of command.
 * - status     : status of command execution (PENDING, EXECUTED, FAILURE).
 * - cmdId      : Command id.
 * - msg        : optional message.
 */
public class CommandStatusReportPublisher extends
    ReportPublisher<CommandStatusReportsProto> {

  private long cmdStatusReportInterval = -1;

  @Override
  protected long getReportFrequency() {
    if (cmdStatusReportInterval == -1) {
      cmdStatusReportInterval = getConf().getTimeDuration(
          HDDS_COMMAND_STATUS_REPORT_INTERVAL,
          HDDS_COMMAND_STATUS_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);

      long heartbeatFrequency = HddsServerUtil.getScmHeartbeatInterval(
          getConf());

      Preconditions.checkState(
          heartbeatFrequency <= cmdStatusReportInterval,
          HDDS_COMMAND_STATUS_REPORT_INTERVAL +
              " cannot be configured lower than heartbeat frequency.");
    }
    return cmdStatusReportInterval;
  }

  @Override
  protected List<CommandStatusReportsProto> getReports() {
    Map<Long, CommandStatus> map = this.getContext()
        .getCommandStatusMap();
    Iterator<Long> iterator = map.keySet().iterator();
    List<CommandStatusReportsProto> reports = new ArrayList<>();
    CommandStatusReportsProto.Builder builder = CommandStatusReportsProto
        .newBuilder();
    long reportSize = 0;
    long maxReportSize = (long) getConf().getStorageSize(
        HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE,
        HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE_DEFAULT, BYTES);
    Preconditions.checkArgument(maxReportSize > 0,
        HDDS_HEARTBEAT_COMMAND_STATUS_REPORT_MAX_SIZE + " must be positive");

    while (iterator.hasNext()) {
      Long key = iterator.next();
      CommandStatus cmdStatus = map.get(key);
      if (cmdStatus == null) {
        continue;
      }
      StorageContainerDatanodeProtocolProtos.CommandStatus status =
          cmdStatus.getProtoBufMessage();
      long statusSize = CodedOutputStream.computeMessageSize(
          CommandStatusReportsProto.CMDSTATUS_FIELD_NUMBER, status);
      if (builder.getCmdStatusCount() > 0 &&
          getReportWireSize(reportSize + statusSize) > maxReportSize) {
        reports.add(builder.build());
        builder = CommandStatusReportsProto.newBuilder();
        reportSize = 0;
      }
      builder.addCmdStatus(status);
      reportSize += statusSize;
      // An individual oversized status cannot be split, so publish it alone.
      if (getReportWireSize(reportSize) > maxReportSize) {
        reports.add(builder.build());
        builder = CommandStatusReportsProto.newBuilder();
        reportSize = 0;
      }
      // If status is still pending then don't remove it from map as
      // CommandHandler will change its status when it works on this command.
      if (!cmdStatus.getStatus().equals(Status.PENDING)) {
        map.remove(key);
      }
    }
    if (builder.getCmdStatusCount() > 0) {
      reports.add(builder.build());
    }
    return reports;
  }

  private static long getReportWireSize(long reportSize) {
    return CodedOutputStream.computeTagSize(
        SCMHeartbeatRequestProto.COMMANDSTATUSREPORTS_FIELD_NUMBER)
        + CodedOutputStream.computeUInt64SizeNoTag(reportSize)
        + reportSize;
  }
}

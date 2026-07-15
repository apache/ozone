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

package org.apache.hadoop.hdds.scm.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.CommandStatusReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CommandStatusReports from datanode.
 */
public class CommandStatusReportHandler implements
    EventHandler<CommandStatusReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CommandStatusReportHandler.class);

  @Override
  public void onMessage(CommandStatusReportFromDatanode report,
      EventPublisher publisher) {
    Objects.requireNonNull(report, "report == null");
    List<CommandStatus> cmdStatusList = report.getReport().getCmdStatusList();
    Objects.requireNonNull(cmdStatusList, "cmdStatusList == null");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Processing command status report for dn: {}", report
          .getDatanodeDetails());
    }

    // Route command status to its watchers.
    List<CommandStatus> deleteBlocksCommandStatus = new ArrayList<>();
    cmdStatusList.forEach(cmdStatus -> {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Emitting command status for id:{} type: {}", cmdStatus
            .getCmdId(), cmdStatus.getType());
      }
      if (cmdStatus.getType() == SCMCommandProto.Type.deleteBlocksCommand) {
        deleteBlocksCommandStatus.add(cmdStatus);
      } else {
        LOGGER.debug("CommandStatus of type:{} not handled in " +
            "CommandStatusReportHandler.", cmdStatus.getType());
      }
    });

    /**
     * The purpose of aggregating all CommandStatus to commit is to reduce the
     * Thread switching. When the Datanode queue has a large number of commands
     * , there will have many {@link CommandStatus#Status#PENDING} status
     * CommandStatus in report
     */
    if (!deleteBlocksCommandStatus.isEmpty()) {
      publisher.fireEvent(SCMEvents.DELETE_BLOCK_STATUS, new DeleteBlockStatus(
          deleteBlocksCommandStatus, report.getDatanodeDetails()));
    }
  }

  /**
   * Wrapper event for CommandStatus.
   */
  public static class CommandStatusEvent implements IdentifiableEventPayload {
    private final List<CommandStatus> cmdStatus;

    CommandStatusEvent(List<CommandStatus> cmdStatus) {
      this.cmdStatus = cmdStatus;
    }

    public List<CommandStatus> getCmdStatus() {
      return cmdStatus;
    }

    @Override
    public String toString() {
      return "CommandStatusEvent:" + cmdStatus.toString();
    }

    @Override
    public long getId() {
      return HddsIdFactory.getLongId();
    }
  }

  /**
   * Wrapper event for DeleteBlock Command.
   */
  public static class DeleteBlockStatus extends CommandStatusEvent {
    private final DatanodeDetails datanodeDetails;

    public DeleteBlockStatus(List<CommandStatus> cmdStatus,
        DatanodeDetails datanodeDetails) {
      super(cmdStatus);
      this.datanodeDetails = datanodeDetails;
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }
  }

}

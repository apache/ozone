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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;

import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles pipeline actions from datanode.
 */
public class PipelineActionHandler
    implements EventHandler<PipelineActionsFromDatanode> {

  public static final Logger LOG =
      LoggerFactory.getLogger(PipelineActionHandler.class);

  private final PipelineManager pipelineManager;
  private final ConfigurationSource ozoneConf;

  public PipelineActionHandler(PipelineManager pipelineManager,
      OzoneConfiguration conf) {
    this.pipelineManager = pipelineManager;
    this.ozoneConf = conf;
  }

  @Override
  public void onMessage(PipelineActionsFromDatanode report,
      EventPublisher publisher) {

    report.getReport().getPipelineActionsList().forEach(action ->
        processPipelineAction(report.getDatanodeDetails(), action, publisher));
  }

  /**
   * Process the given PipelineAction.
   *
   * @param datanode the datanode which has sent the PipelineAction
   * @param pipelineAction the PipelineAction
   * @param publisher EventPublisher to fire new events if required
   */
  private void processPipelineAction(final DatanodeDetails datanode,
                                     final PipelineAction pipelineAction,
                                     final EventPublisher publisher) {
    final ClosePipelineInfo info = pipelineAction.getClosePipeline();
    final PipelineAction.Action action = pipelineAction.getAction();
    final PipelineID pid = PipelineID.getFromProtobuf(info.getPipelineID());
    try {
      LOG.info("Received pipeline action {} for {} from datanode {}. " +
          "Reason : {}", action, pid, datanode.getUuidString(),
          info.getDetailedReason());

      if (action == PipelineAction.Action.CLOSE) {
        pipelineManager.finalizeAndDestroyPipeline(
            pipelineManager.getPipeline(pid), true);
      } else {
        LOG.error("unknown pipeline action:{}", action);
      }
    } catch (PipelineNotFoundException e) {
      LOG.warn("Pipeline action {} received for unknown pipeline {}, " +
          "firing close pipeline event.", action, pid);
      publisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(datanode.getUuid(),
              new ClosePipelineCommand(pid)));
    } catch (IOException ioe) {
      LOG.error("Could not execute pipeline action={} pipeline={}",
          action, pid, ioe);
    }
  }

}

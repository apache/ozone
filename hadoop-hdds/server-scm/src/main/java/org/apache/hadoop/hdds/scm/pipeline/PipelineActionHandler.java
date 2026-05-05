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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles pipeline actions from datanode.
 */
public class PipelineActionHandler
    implements EventHandler<PipelineActionsFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineActionHandler.class);

  private final PipelineManager pipelineManager;
  private final SCMContext scmContext;

  public PipelineActionHandler(PipelineManager pipelineManager,
      SCMContext scmContext) {
    this.pipelineManager = pipelineManager;
    this.scmContext = scmContext;
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

    final String logMsg = "Received pipeline action " + action + " for " + pid +
        " from datanode " + datanode + "." +
        " Reason : " + info.getDetailedReason();

    // We can skip processing Pipeline Action if the current SCM is not leader.
    if (!scmContext.isLeader()) {
      LOG.debug(logMsg);
      LOG.debug("Cannot process Pipeline Action for pipeline {} as " +
          "current SCM is not leader.", pid);
      return;
    }

    LOG.info(logMsg);
    try {
      if (action == PipelineAction.Action.CLOSE) {
        pipelineManager.closePipeline(pid);
      } else {
        LOG.error("Received unknown pipeline action {}, for pipeline {} ",
            action, pid);
      }
    } catch (PipelineNotFoundException e) {
      closeUnknownPipeline(publisher, datanode, pid);
    }  catch (SCMException e) {
      if (e.getResult() == SCMException.ResultCodes.SCM_NOT_LEADER) {
        LOG.info("Cannot process Pipeline Action for pipeline {} as " +
            "current SCM is not leader anymore.", pid);
      } else {
        LOG.error("Exception while processing Pipeline Action for Pipeline {}",
            pid, e);
      }
    } catch (IOException e) {
      LOG.error("Exception while processing Pipeline Action for Pipeline {}",
          pid, e);
    }
  }

  private void closeUnknownPipeline(final EventPublisher publisher,
                                    final DatanodeDetails datanode,
                                    final PipelineID pid) {
    try {
      LOG.warn("Pipeline action received for unknown Pipeline {}, " +
          "firing close pipeline event.", pid);
      SCMCommand<?> command = new ClosePipelineCommand(pid);
      command.setTerm(scmContext.getTermOfLeader());
      publisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(datanode, command));
    } catch (NotLeaderException nle) {
      LOG.info("Cannot process Pipeline Action for pipeline {} as " +
          "current SCM is not leader anymore.", pid);
    }
  }
}

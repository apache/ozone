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

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles command status reports from JobWorkers by receiving events and routing them
 * to the appropriate command processor. Implements EventHandler to process JobWorker
 * heartbeat messages containing command status information.
 */
public class JobworkerCommandStatusReportHandler implements
    EventHandler<CommandStatusReportFromJobworker> {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerCommandStatusReportHandler.class);

  private final OMJobworkerCommandManager jobworkerCommandProcessor;
  private final BooleanSupplier isLeaderReady;

  /**
   * Constructs CommandStatusReportHandler.
   *
   * @param jobworkerCommandProcessor the command manager that tracks in-flight commands
   * @param isLeaderReady true if this OM is the ready leader; reports are ignored otherwise
   */
  public JobworkerCommandStatusReportHandler(OMJobworkerCommandManager jobworkerCommandProcessor,
      BooleanSupplier isLeaderReady) {
    Objects.requireNonNull(jobworkerCommandProcessor);
    Objects.requireNonNull(isLeaderReady);
    this.jobworkerCommandProcessor = jobworkerCommandProcessor;
    this.isLeaderReady = isLeaderReady;
  }

  @Override
  public void onMessage(CommandStatusReportFromJobworker report,
                        EventPublisher publisher) {
    Objects.requireNonNull(report);
    JobworkerDetails jobworkerDetails = report.getJobworkerDetails();
    Objects.requireNonNull(jobworkerDetails,
        "CommandStatusReport is missing JobworkerDetails.");
    if (!isLeaderReady.getAsBoolean()) {
      LOG.debug("Skip command status report from JobWorker {} since this OM is not the ready leader.",
          jobworkerDetails);
      return;
    }
    List<CommandStatus> commandStatusList = report.getReport().getCmdStatusList();
    Objects.requireNonNull(commandStatusList);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing command status report for JobWorker: {}",
          jobworkerDetails);
    }

    for (CommandStatus cmdStatus : commandStatusList) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing command status for ID:{} type: {}",
            cmdStatus.getCmdId(), cmdStatus.getType());
      }
      jobworkerCommandProcessor.processStatusUpdate(jobworkerDetails, cmdStatus);
    }
  }
}

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

package org.apache.hadoop.ozone.om.jobworker;

import static org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents.JW_COMMAND_STATUS_REPORT;
import static org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents.JW_NODE_REPORT;

import com.google.protobuf.Message;
import java.util.List;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerReregisterCommand;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for dispatching heartbeat from jobworker to
 * appropriate EventHandler at OM.
 */
public class JobworkerHeartbeatDispatcher {

  public static final Logger LOG =
      LoggerFactory.getLogger(JobworkerHeartbeatDispatcher.class);

  private EventPublisher eventPublisher;
  private final JobworkerNodeManager nodeManager;

  public JobworkerHeartbeatDispatcher(JobworkerNodeManager nodeManager, EventPublisher eventPublisher) {
    this.eventPublisher = eventPublisher;
    this.nodeManager = nodeManager;
  }

  /**
   * Dispatches heartbeat to registered event handlers.
   * @param heartbeat heartbeat to be dispatched.
   * @return list of OMJobworkerCommand
   */
  public List<OMJobworkerCommand> dispatch(SendHeartbeatRequest heartbeat) {
    JobworkerDetails jobworkerDetails =
        JobworkerDetails.getFromProtoBuf(heartbeat.getJobworkerDetails());

    if (!nodeManager.isJobworkerNodeRegistered(jobworkerDetails.getUuid())) {
      LOG.info("OM received heartbeat from an unregistered jobworker {}. " +
          "Asking jobworker to re-register.", jobworkerDetails);
      // TODO(JW): We might want to ensure that a single JobworkerReregisterCommand is sent if there
      //  are multiple reregister commands to the same JW
      nodeManager.addOMJobworkerCommand(jobworkerDetails.getUuid(), new JobworkerReregisterCommand());
    } else {
      LOG.debug("Processing jobworker {} Report.", jobworkerDetails);
      nodeManager.processHeartbeat(jobworkerDetails);

      if (heartbeat.hasJobworkerNodeReport()) {
        LOG.debug("Dispatching Command Node Report.");
        eventPublisher.fireEvent(
            JW_NODE_REPORT,
            new NodeReportFromJobworker(
                jobworkerDetails,
                heartbeat.getJobworkerNodeReport()));
      }

      // Process command status reports
      for (CommandStatusReportsProto report : heartbeat.getCommandStatusReportsList()) {
        LOG.debug("Dispatching Command Status Report from jobworker {}",
            jobworkerDetails.getUuidString());
        eventPublisher.fireEvent(
            JW_COMMAND_STATUS_REPORT,
            new CommandStatusReportFromJobworker(
                jobworkerDetails,
                report));
      }
    }
    return nodeManager.pollJobworkerCommand(jobworkerDetails.getUuid());
  }

  /**
   * Wrapper class for events with the jobworker origin.
   */
  public static class ReportFromJobworker<T extends Message> {

    private final JobworkerDetails jobworkerDetails;

    private T report;

    public ReportFromJobworker(JobworkerDetails jobworkerDetails, T report) {
      this.jobworkerDetails = jobworkerDetails;
      this.report = report;
    }

    public JobworkerDetails getJobworkerDetails() {
      return jobworkerDetails;
    }

    public T getReport() {
      return report;
    }

    public void setReport(T report) {
      this.report = report;
    }
  }

  /**
   * Node report event payload with origin.
   */
  public static class NodeReportFromJobworker
      extends ReportFromJobworker<JobworkerNodeReportProto> {
    public NodeReportFromJobworker(
        JobworkerDetails jobworkerDetails, JobworkerNodeReportProto report) {
      super(jobworkerDetails, report);
    }
  }

  /**
   * Wrapper class for command status reports from JobWorker.
   * Similar to CommandStatusReportFromDatanode in SCM.
   */
  public static class CommandStatusReportFromJobworker
      extends ReportFromJobworker<CommandStatusReportsProto> {

    public CommandStatusReportFromJobworker(JobworkerDetails jobworkerDetails,
                                            CommandStatusReportsProto report) {
      super(jobworkerDetails, report);
    }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.safemode.SafeModeManager;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Handles Pipeline Reports from datanode.
 */
public class PipelineReportHandler implements
    EventHandler<PipelineReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      PipelineReportHandler.class);
  private final PipelineManager pipelineManager;
  private final ConfigurationSource conf;
  private final SafeModeManager scmSafeModeManager;
  private final SCMContext scmContext;
  private final boolean pipelineAvailabilityCheck;
  private final SCMPipelineMetrics metrics;

  public PipelineReportHandler(SafeModeManager scmSafeModeManager,
                               PipelineManager pipelineManager,
                               SCMContext scmContext,
                               ConfigurationSource conf) {
    Preconditions.checkNotNull(pipelineManager);
    this.scmSafeModeManager = scmSafeModeManager;
    this.pipelineManager = pipelineManager;
    this.scmContext = scmContext;
    this.conf = conf;
    this.metrics = SCMPipelineMetrics.create();
    this.pipelineAvailabilityCheck = conf.getBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK_DEFAULT);
  }

  @Override
  public void onMessage(PipelineReportFromDatanode pipelineReportFromDatanode,
      EventPublisher publisher) {
    Preconditions.checkNotNull(pipelineReportFromDatanode);
    DatanodeDetails dn = pipelineReportFromDatanode.getDatanodeDetails();
    PipelineReportsProto pipelineReport =
        pipelineReportFromDatanode.getReport();
    Preconditions.checkNotNull(dn,
        "Pipeline Report is missing DatanodeDetails.");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Processing pipeline report for dn: {}", dn);
    }
    for (PipelineReport report : pipelineReport.getPipelineReportList()) {
      try {
        processPipelineReport(report, dn, publisher);
      } catch (IOException e) {
        LOGGER.error("Could not process pipeline report={} from dn={}.",
            report, dn, e);
      }
    }
  }

  protected void processPipelineReport(PipelineReport report,
      DatanodeDetails dn, EventPublisher publisher) throws IOException {
    PipelineID pipelineID = PipelineID.getFromProtobuf(report.getPipelineID());
    Pipeline pipeline;
    try {
      pipeline = pipelineManager.getPipeline(pipelineID);
    } catch (PipelineNotFoundException e) {
      SCMCommand<?> command = new ClosePipelineCommand(pipelineID);
      command.setTerm(scmContext.getTermOfLeader());
      publisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(dn.getUuid(), command));
      return;
    }

    setReportedDatanode(pipeline, dn);
    setPipelineLeaderId(report, pipeline, dn);

    if (pipeline.getPipelineState() == Pipeline.PipelineState.ALLOCATED) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Pipeline {} {} reported by {}",
            pipeline.getReplicationConfig(),
            pipeline.getId(),
            dn);
      }
      if (pipeline.isHealthy()) {
        pipelineManager.openPipeline(pipelineID);
      }
    }
    if (pipeline.isHealthy()) {
      if (pipelineAvailabilityCheck && scmSafeModeManager.getInSafeMode()) {
        publisher.fireEvent(SCMEvents.OPEN_PIPELINE, pipeline);
      }
    }
  }


  protected void setReportedDatanode(Pipeline pipeline, DatanodeDetails dn)
      throws IOException {
    pipeline.reportDatanode(dn);
  }

  protected void setPipelineLeaderId(PipelineReport report,
                                     Pipeline pipeline,
                                     DatanodeDetails dn) {
    // ONE replica pipeline doesn't have leader flag
    if (report.getIsLeader() ||
        RatisReplicationConfig.hasFactor(pipeline.getReplicationConfig(),
            ReplicationFactor.ONE)) {
      pipeline.setLeaderId(dn.getUuid());
      metrics.incNumPipelineBytesWritten(pipeline, report.getBytesWritten());
    }
  }

  protected PipelineManager getPipelineManager() {
    return pipelineManager;
  }
}

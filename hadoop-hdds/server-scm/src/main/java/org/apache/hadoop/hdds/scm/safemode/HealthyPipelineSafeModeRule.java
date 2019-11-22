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
package org.apache.hadoop.hdds.scm.safemode;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Class defining Safe mode exit criteria for Pipelines.
 *
 * This rule defines percentage of healthy pipelines need to be reported.
 * Once safe mode exit happens, this rules take care of writes can go
 * through in a cluster.
 */
public class HealthyPipelineSafeModeRule
    extends SafeModeExitRule<PipelineReportFromDatanode>{

  public static final Logger LOG =
      LoggerFactory.getLogger(HealthyPipelineSafeModeRule.class);
  private final PipelineManager pipelineManager;
  private int healthyPipelineThresholdCount;
  private int currentHealthyPipelineCount = 0;
  private final Map<PipelineID, Boolean> processedPipelines = new HashMap<>();
  private final double healthyPipelinesPercent;

  HealthyPipelineSafeModeRule(String ruleName, EventQueue eventQueue,
      PipelineManager pipelineManager,
      SCMSafeModeManager manager, Configuration configuration) {
    super(manager, ruleName, eventQueue);
    this.pipelineManager = pipelineManager;
    healthyPipelinesPercent =
        configuration.getDouble(HddsConfigKeys.
                HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT,
            HddsConfigKeys.
                HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT_DEFAULT);

    Preconditions.checkArgument(
        (healthyPipelinesPercent >= 0.0 && healthyPipelinesPercent <= 1.0),
        HddsConfigKeys.
            HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT
            + " value should be >= 0.0 and <= 1.0");

    // As we want to wait for 3 node pipelines
    int pipelineCount =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE).size();

    // This value will be zero when pipeline count is 0.
    // On a fresh installed cluster, there will be zero pipelines in the SCM
    // pipeline DB.
    healthyPipelineThresholdCount =
        (int) Math.ceil(healthyPipelinesPercent * pipelineCount);

    LOG.info(" Total pipeline count is {}, healthy pipeline " +
        "threshold count is {}", pipelineCount, healthyPipelineThresholdCount);

    getSafeModeMetrics().setNumHealthyPipelinesThreshold(
        healthyPipelineThresholdCount);
  }

  @VisibleForTesting
  public void setHealthyPipelineThresholdCount(int actualPipelineCount) {
    healthyPipelineThresholdCount =
        (int) Math.ceil(healthyPipelinesPercent * actualPipelineCount);
  }

  @Override
  protected TypedEvent<PipelineReportFromDatanode> getEventType() {
    return SCMEvents.PROCESSED_PIPELINE_REPORT;
  }

  @Override
  protected boolean validate() {
    if (currentHealthyPipelineCount >= healthyPipelineThresholdCount) {
      return true;
    }
    return false;
  }

  @Override
  protected void process(PipelineReportFromDatanode
      pipelineReportFromDatanode) {

    // When SCM is in safe mode for long time, already registered
    // datanode can send pipeline report again, then pipeline handler fires
    // processed report event, we should not consider this pipeline report
    // from datanode again during threshold calculation.
    Preconditions.checkNotNull(pipelineReportFromDatanode);

    PipelineReportsProto pipelineReport =
        pipelineReportFromDatanode.getReport();

    for (PipelineReport report : pipelineReport.getPipelineReportList()) {
      PipelineID pipelineID = PipelineID.getFromProtobuf(
          report.getPipelineID());
      Pipeline pipeline;
      try {
        pipeline = pipelineManager.getPipeline(pipelineID);
      } catch (PipelineNotFoundException e) {
        continue;
      }

      if (!processedPipelines.containsKey(pipelineID)) {
        if (pipeline.getFactor() == HddsProtos.ReplicationFactor.THREE &&
            report.getIsLeader()) {
          // If the pipeline gets reported with a leader we mark it as healthy
          currentHealthyPipelineCount++;
          getSafeModeMetrics().incCurrentHealthyPipelinesCount();
          processedPipelines.put(pipelineID, Boolean.TRUE);
        }
      }
    }
    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. Healthy pipelines reported count is {}, " +
              "required healthy pipeline reported count is {}",
          currentHealthyPipelineCount, healthyPipelineThresholdCount);
    }
  }

  @Override
  protected void cleanup() {
    processedPipelines.clear();
  }

  @VisibleForTesting
  public int getCurrentHealthyPipelineCount() {
    return currentHealthyPipelineCount;
  }

  @VisibleForTesting
  public int getHealthyPipelineThresholdCount() {
    return healthyPipelineThresholdCount;
  }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * This rule covers whether we have at least one datanode is reported for each
 * pipeline. This rule is for all open containers, we have at least one
 * replica available for read when we exit safe mode.
 */
public class OneReplicaPipelineSafeModeRule extends
    SafeModeExitRule<Pipeline> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OneReplicaPipelineSafeModeRule.class);

  private int thresholdCount;
  private Set<PipelineID> reportedPipelineIDSet = new HashSet<>();
  private Set<PipelineID> oldPipelineIDSet;
  private int oldPipelineReportedCount = 0;
  private int oldPipelineThresholdCount = 0;
  private int newPipelineThresholdCount = 0;
  private int newPipelineReportedCount = 0;


  public OneReplicaPipelineSafeModeRule(String ruleName, EventQueue eventQueue,
      PipelineManager pipelineManager,
      SCMSafeModeManager safeModeManager, Configuration configuration) {
    super(safeModeManager, ruleName, eventQueue);

    double percent =
        configuration.getDouble(
            HddsConfigKeys.HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT,
            HddsConfigKeys.
                HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT_DEFAULT);

    Preconditions.checkArgument((percent >= 0.0 && percent <= 1.0),
        HddsConfigKeys.
            HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT  +
            " value should be >= 0.0 and <= 1.0");

    oldPipelineIDSet =
        ((SCMPipelineManager)pipelineManager).getOldPipelineIdSet();
    int totalPipelineCount =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE).size();
    Preconditions.checkState(totalPipelineCount >= oldPipelineIDSet.size());

    oldPipelineThresholdCount =
        (int) Math.ceil(percent * oldPipelineIDSet.size());
    newPipelineThresholdCount = (int) Math.ceil(
        percent * (totalPipelineCount - oldPipelineIDSet.size()));

    thresholdCount = oldPipelineThresholdCount + newPipelineThresholdCount;

    LOG.info("Total pipeline count is {}, pipeline's with at least one " +
        "datanode reported threshold count is {}", totalPipelineCount,
        thresholdCount);

    getSafeModeMetrics().setNumPipelinesWithAtleastOneReplicaReportedThreshold(
        thresholdCount);
  }

  @Override
  protected TypedEvent<Pipeline> getEventType() {
    return SCMEvents.OPEN_PIPELINE;
  }

  @Override
  protected boolean validate() {
    if (newPipelineReportedCount + oldPipelineReportedCount >= thresholdCount) {
      return true;
    }
    return false;
  }

  @Override
  protected void process(Pipeline pipeline) {
    Preconditions.checkNotNull(pipeline);
    if (pipeline.getType() == HddsProtos.ReplicationType.RATIS &&
        pipeline.getFactor() == HddsProtos.ReplicationFactor.THREE &&
        !reportedPipelineIDSet.contains(pipeline.getId())) {
      if (oldPipelineIDSet.contains(pipeline.getId()) &&
          oldPipelineReportedCount < oldPipelineThresholdCount) {
        getSafeModeMetrics()
            .incCurrentHealthyPipelinesWithAtleastOneReplicaReportedCount();
        oldPipelineReportedCount++;
        reportedPipelineIDSet.add(pipeline.getId());
      } else if (newPipelineReportedCount < newPipelineThresholdCount) {
        getSafeModeMetrics()
            .incCurrentHealthyPipelinesWithAtleastOneReplicaReportedCount();
        newPipelineReportedCount++;
        reportedPipelineIDSet.add(pipeline.getId());
      }
    }

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. Pipelines with at least one datanode reported " +
              "count is {}, required at least one datanode reported per " +
              "pipeline count is {}",
          newPipelineReportedCount + oldPipelineReportedCount, thresholdCount);
    }
  }

  @Override
  protected void cleanup() {
    reportedPipelineIDSet.clear();
  }

  @VisibleForTesting
  public int getThresholdCount() {
    return thresholdCount;
  }

  @VisibleForTesting
  public int getCurrentReportedPipelineCount() {
    return newPipelineReportedCount + oldPipelineReportedCount;
  }
}
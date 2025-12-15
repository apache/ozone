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

package org.apache.hadoop.hdds.scm.safemode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
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

/**
 * This rule covers whether we have at least one datanode is reported for each
 * open pipeline. This rule is for all open containers, we have at least one
 * replica available for read when we exit safe mode.
 */
public class OneReplicaPipelineSafeModeRule extends
    SafeModeExitRule<PipelineReportFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OneReplicaPipelineSafeModeRule.class);

  private int thresholdCount;
  private final Set<PipelineID> reportedPipelineIDSet = new HashSet<>();
  private Set<PipelineID> oldPipelineIDSet;
  private int currentReportedPipelineCount = 0;
  private PipelineManager pipelineManager;
  private final double pipelinePercent;

  public OneReplicaPipelineSafeModeRule(EventQueue eventQueue, PipelineManager pipelineManager,
      SCMSafeModeManager safeModeManager, ConfigurationSource configuration) {
    super(safeModeManager, eventQueue);

    pipelinePercent =
        configuration.getDouble(
            HddsConfigKeys.HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT,
            HddsConfigKeys.
                HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT_DEFAULT);

    Preconditions.checkArgument((pipelinePercent >= 0.0
            && pipelinePercent <= 1.0),
        HddsConfigKeys.
            HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT  +
            " value should be >= 0.0 and <= 1.0");

    this.pipelineManager = pipelineManager;
    initializeRule(false);

  }

  @Override
  protected TypedEvent<PipelineReportFromDatanode> getEventType() {
    return SCMEvents.PIPELINE_REPORT;
  }

  @Override
  protected synchronized boolean validate() {
    if (!validateBasedOnReportProcessing()) {
      updateReportedPipelineSet();
    }
    return currentReportedPipelineCount >= thresholdCount;
  }

  @Override
  protected synchronized void process(PipelineReportFromDatanode report) {
    if (!validateBasedOnReportProcessing()) {
      return;
    }
    Objects.requireNonNull(report, "report == null");
    for (PipelineReport report1 : report.getReport().getPipelineReportList()) {
      Pipeline pipeline;
      try {
        pipeline = pipelineManager.getPipeline(
            PipelineID.getFromProtobuf(report1.getPipelineID()));
      } catch (PipelineNotFoundException pnfe) {
        continue;
      }

      if (RatisReplicationConfig
          .hasFactor(pipeline.getReplicationConfig(), ReplicationFactor.THREE)
          && pipeline.isOpen() &&
          !reportedPipelineIDSet.contains(pipeline.getId())) {
        if (oldPipelineIDSet.contains(pipeline.getId())) {
          getSafeModeMetrics().
              incCurrentHealthyPipelinesWithAtleastOneReplicaReportedCount();
          currentReportedPipelineCount++;
          reportedPipelineIDSet.add(pipeline.getId());
        }
      }
    }

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. Pipelines with at least one datanode reported " +
              "count is {}, required at least one datanode reported per " +
              "pipeline count is {}",
          currentReportedPipelineCount, thresholdCount);
    }
  }

  @Override
  protected synchronized void cleanup() {
    reportedPipelineIDSet.clear();
  }

  @VisibleForTesting
  public synchronized int getThresholdCount() {
    return thresholdCount;
  }

  @VisibleForTesting
  public synchronized int getCurrentReportedPipelineCount() {
    return currentReportedPipelineCount;
  }

  Set<PipelineID> getReportedPipelineIDSet() {
    return Collections.unmodifiableSet(reportedPipelineIDSet);
  }

  @Override
  public String getStatusText() {
    String status = String.format(
        "reported Ratis/THREE pipelines with at least one datanode (=%d) "
            + ">= threshold (=%d)", getCurrentReportedPipelineCount(),
        getThresholdCount());
    status = updateStatusTextWithSamplePipelines(status);
    return status;
  }

  private synchronized String updateStatusTextWithSamplePipelines(
      String status) {
    Set<PipelineID> samplePipelines = oldPipelineIDSet.stream()
        .filter(element -> !reportedPipelineIDSet.contains(element))
        .limit(SAMPLE_PIPELINE_DISPLAY_LIMIT).collect(Collectors.toSet());
    if (!samplePipelines.isEmpty()) {
      String samplePipelineText =
          "Sample pipelines not satisfying the criteria : " + samplePipelines;
      status = status.concat("\n").concat(samplePipelineText);
    }
    return status;
  }

  @Override
  public synchronized void refresh(boolean forceRefresh) {
    if (forceRefresh) {
      initializeRule(true);
    } else {
      if (!validate()) {
        initializeRule(true);
      }
    }
  }

  private void updateReportedPipelineSet() {
    List<Pipeline> openRatisPipelines =
        pipelineManager.getPipelines(RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN);

    for (Pipeline pipeline : openRatisPipelines) {
      PipelineID pipelineID = pipeline.getId();
      if (!pipeline.getNodeSet().isEmpty()
          && oldPipelineIDSet.contains(pipelineID)
          && reportedPipelineIDSet.add(pipelineID)) {
        getSafeModeMetrics().incCurrentHealthyPipelinesWithAtleastOneReplicaReportedCount();
        currentReportedPipelineCount++;
      }
    }
  }

  private void initializeRule(boolean refresh) {

    oldPipelineIDSet = pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        Pipeline.PipelineState.OPEN)
        .stream().map(p -> p.getId()).collect(Collectors.toSet());

    int totalPipelineCount = oldPipelineIDSet.size();

    thresholdCount = (int) Math.ceil(pipelinePercent * totalPipelineCount);

    if (refresh) {
      LOG.info("Refreshed Total pipeline count is {}, pipeline's with at " +
              "least one datanode reported threshold count is {}",
          totalPipelineCount, thresholdCount);
    } else {
      LOG.info("Total pipeline count is {}, pipeline's with at " +
              "least one datanode reported threshold count is {}",
          totalPipelineCount, thresholdCount);
    }

    getSafeModeMetrics().setNumPipelinesWithAtleastOneReplicaReportedThreshold(
        thresholdCount);
  }
}

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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class defining Safe mode exit criteria for Pipelines.
 *
 * This rule defines percentage of healthy pipelines need to be reported.
 * Once safe mode exit happens, this rules take care of writes can go
 * through in a cluster.
 */
public class HealthyPipelineSafeModeRule extends SafeModeExitRule<Pipeline> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HealthyPipelineSafeModeRule.class);

  private int healthyPipelineThresholdCount;
  private int currentHealthyPipelineCount = 0;
  private final double healthyPipelinesPercent;
  private final PipelineManager pipelineManager;
  private final int minHealthyPipelines;
  private final SCMContext scmContext;
  private final NodeManager nodeManager;

  HealthyPipelineSafeModeRule(EventQueue eventQueue,
      PipelineManager pipelineManager, SCMSafeModeManager manager,
      ConfigurationSource configuration, SCMContext scmContext, NodeManager nodeManager) {
    super(manager, eventQueue);
    this.pipelineManager = pipelineManager;
    this.scmContext = scmContext;
    this.nodeManager = nodeManager;
    healthyPipelinesPercent =
        configuration.getDouble(HddsConfigKeys.
                HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT,
            HddsConfigKeys.
                HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT_DEFAULT);

    // We only care about THREE replica pipeline
    minHealthyPipelines = getMinHealthyPipelines(configuration);

    Preconditions.checkArgument(
        (healthyPipelinesPercent >= 0.0 && healthyPipelinesPercent <= 1.0),
        HddsConfigKeys.
            HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT
            + " value should be >= 0.0 and <= 1.0");

    initializeRule(false);
  }

  private int getMinHealthyPipelines(ConfigurationSource config) {
    int minDatanodes = config.getInt(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE_DEFAULT);

    // We only care about THREE replica pipeline
    return minDatanodes / HddsProtos.ReplicationFactor.THREE_VALUE;

  }

  @VisibleForTesting
  public synchronized void setHealthyPipelineThresholdCount(
      int actualPipelineCount) {
    healthyPipelineThresholdCount =
        (int) Math.ceil(healthyPipelinesPercent * actualPipelineCount);
  }

  @Override
  protected TypedEvent<Pipeline> getEventType() {
    return SCMEvents.OPEN_PIPELINE;
  }

  @Override
  protected synchronized boolean validate() {
    boolean shouldRunSafemodeCheck =
        FinalizationManager.shouldCreateNewPipelines(
            scmContext.getFinalizationCheckpoint());
    if (!shouldRunSafemodeCheck) {
      LOG.info("All SCM pipelines are closed due to ongoing upgrade " +
          "finalization. Bypassing healthy pipeline safemode rule.");
      return true;
    }
    // Query PipelineManager directly for healthy pipeline count
    List<Pipeline> openPipelines = pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        Pipeline.PipelineState.OPEN);
    
    LOG.debug("Found {} open RATIS/THREE pipelines", openPipelines.size());

    int pipelineCount = openPipelines.size();
    healthyPipelineThresholdCount = Math.max(minHealthyPipelines,
        (int) Math.ceil(healthyPipelinesPercent * pipelineCount));

    currentHealthyPipelineCount = (int) openPipelines.stream()
        .filter(this::isPipelineHealthy)
        .count();

    getSafeModeMetrics().setNumCurrentHealthyPipelines(currentHealthyPipelineCount);
    boolean isValid = currentHealthyPipelineCount >= healthyPipelineThresholdCount;
    if (scmInSafeMode()) {
      LOG.info("SCM in safe mode. Healthy pipelines: {}, threshold: {}, rule satisfied: {}",
          currentHealthyPipelineCount, healthyPipelineThresholdCount, isValid);
    } else {
      LOG.debug("SCM not in safe mode. Healthy pipelines: {}, threshold: {}",
          currentHealthyPipelineCount, healthyPipelineThresholdCount);
    }
    return isValid;
  }

  boolean isPipelineHealthy(Pipeline pipeline) {
    // Verify pipeline has all 3 nodes
    List<DatanodeDetails> nodes = pipeline.getNodes();
    if (nodes.size() != 3) {
      LOG.debug("Pipeline {} is not healthy: has {} nodes instead of 3",
          pipeline.getId(), nodes.size());
      return false;
    }
    
    // Verify all nodes are healthy
    for (DatanodeDetails dn : nodes) {
      try {
        NodeStatus status = nodeManager.getNodeStatus(dn);
        if (!status.equals(NodeStatus.inServiceHealthy())) {
          LOG.debug("Pipeline {} is not healthy: DN {} has status - Health: {}, Operational State: {}",
              pipeline.getId(), dn.getUuidString(), status.getHealth(), status.getOperationalState());
          return false;
        }
      } catch (NodeNotFoundException e) {
        LOG.warn("Pipeline {} is not healthy: DN {} not found in node manager",
            pipeline.getId(), dn.getUuidString());
        return false;
      }
    }
    return true;
  }

  @Override
  protected synchronized void process(Pipeline pipeline) {
    // No longer processing events, validation is done directly via validate()
    // This method is still called when OPEN_PIPELINE events arrive, but we
    // validate in validate() method instead
    LOG.debug("Received OPEN_PIPELINE event for pipeline {}, validation will happen in validate() method",
        pipeline.getId());
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

  private synchronized void initializeRule(boolean refresh) {
    // Get current open pipeline count from PipelineManager
    List<Pipeline> openPipelines = pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        Pipeline.PipelineState.OPEN);
    
    int pipelineCount = openPipelines.size();

    healthyPipelineThresholdCount = Math.max(minHealthyPipelines,
        (int) Math.ceil(healthyPipelinesPercent * pipelineCount));

    if (refresh) {
      LOG.info("Refreshed total pipeline count is {}, healthy pipeline " +
          "threshold count is {}", pipelineCount,
          healthyPipelineThresholdCount);
    } else {
      LOG.info("Total pipeline count is {}, healthy pipeline " +
          "threshold count is {}", pipelineCount,
          healthyPipelineThresholdCount);
    }

    getSafeModeMetrics().setNumHealthyPipelinesThreshold(
        healthyPipelineThresholdCount);
  }

  @Override
  protected synchronized void cleanup() {
    // No tracking state to clean up since we query PipelineManager directly
  }

  @VisibleForTesting
  public synchronized int getCurrentHealthyPipelineCount() {
    return currentHealthyPipelineCount;
  }

  @VisibleForTesting
  public synchronized int getHealthyPipelineThresholdCount() {
    return healthyPipelineThresholdCount;
  }

  @Override
  public String getStatusText() {
    String status = String.format(
        "healthy Ratis/THREE pipelines (=%d) >= healthyPipelineThresholdCount" +
            " (=%d)", getCurrentHealthyPipelineCount(),
        getHealthyPipelineThresholdCount());
    status = updateStatusTextWithSamplePipelines(status);
    return status;
  }

  private synchronized String updateStatusTextWithSamplePipelines(
      String status) {
    // Get sample pipelines that don't satisfy the healthy criteria
    List<Pipeline> openPipelines = pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        Pipeline.PipelineState.OPEN);
    
    Set<PipelineID> unhealthyPipelines = openPipelines.stream()
        .filter(p -> !isPipelineHealthy(p))
        .map(Pipeline::getId)
        .limit(SAMPLE_PIPELINE_DISPLAY_LIMIT)
        .collect(Collectors.toSet());

    if (!unhealthyPipelines.isEmpty()) {
      String samplePipelineText =
          "Sample pipelines not satisfying the criteria : " + unhealthyPipelines;
      status = status.concat("\n").concat(samplePipelineText);
    }
    return status;
  }
}

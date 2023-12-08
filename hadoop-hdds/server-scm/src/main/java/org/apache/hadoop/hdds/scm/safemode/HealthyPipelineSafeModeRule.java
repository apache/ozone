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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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

  public static final Logger LOG =
      LoggerFactory.getLogger(HealthyPipelineSafeModeRule.class);
  private int healthyPipelineThresholdCount;
  private int currentHealthyPipelineCount = 0;
  private final double healthyPipelinesPercent;
  private final Set<PipelineID> processedPipelineIDs = new HashSet<>();
  private final PipelineManager pipelineManager;
  private final int minHealthyPipelines;
  private final SCMContext scmContext;

  HealthyPipelineSafeModeRule(String ruleName, EventQueue eventQueue,
      PipelineManager pipelineManager, SCMSafeModeManager manager,
      ConfigurationSource configuration, SCMContext scmContext) {
    super(manager, ruleName, eventQueue);
    this.pipelineManager = pipelineManager;
    this.scmContext = scmContext;
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
    } else {
      return currentHealthyPipelineCount >= healthyPipelineThresholdCount;
    }
  }

  @Override
  protected synchronized void process(Pipeline pipeline) {

    // When SCM is in safe mode for long time, already registered
    // datanode can send pipeline report again, or SCMPipelineManager will
    // create new pipelines.
    Preconditions.checkNotNull(pipeline);
    if (pipeline.getType() == HddsProtos.ReplicationType.RATIS &&
        ((RatisReplicationConfig) pipeline.getReplicationConfig())
            .getReplicationFactor() == HddsProtos.ReplicationFactor.THREE &&
        !processedPipelineIDs.contains(pipeline.getId())) {
      getSafeModeMetrics().incCurrentHealthyPipelinesCount();
      currentHealthyPipelineCount++;
      processedPipelineIDs.add(pipeline.getId());
    }

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. Healthy pipelines reported count is {}, " +
              "required healthy pipeline reported count is {}",
          currentHealthyPipelineCount, getHealthyPipelineThresholdCount());

    }
  }


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
    int pipelineCount = pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        Pipeline.PipelineState.OPEN).size();

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
    processedPipelineIDs.clear();
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
    return String.format("healthy Ratis/THREE pipelines (=%d) >= "
            + "healthyPipelineThresholdCount (=%d)",
        getCurrentHealthyPipelineCount(),
        getHealthyPipelineThresholdCount());
  }
}

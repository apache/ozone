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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Implements Api for creating ratis pipelines.
 */
public class RatisPipelineProvider
    extends NCopyPipelineProvider<RatisReplicationConfig> {

  private final int pipelineNumberLimit;
  private final int datanodePipelineLimit;
  private final boolean isRatisStreamingEnabled;

  @VisibleForTesting
  public RatisPipelineProvider(NodeManager nodeManager,
                               PipelineStateManager stateManager,
                               ConfigurationSource conf,
                               EventPublisher eventPublisher,
                               SCMContext scmContext) {
    super(nodeManager, stateManager, conf, eventPublisher, scmContext);
    this.pipelineNumberLimit = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT_DEFAULT);
    this.datanodePipelineLimit = conf.getInt(
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
    this.isRatisStreamingEnabled = conf.getBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED_DEFAULT);
  }

  private boolean exceedPipelineNumberLimit(RatisReplicationConfig replicationConfig) {
    // Apply limits only for replication factor THREE
    if (replicationConfig.getReplicationFactor() != ReplicationFactor.THREE) {
      return false;
    }

    PipelineStateManager pipelineStateManager = getPipelineStateManager();
    int totalActivePipelines = pipelineStateManager.getPipelines(replicationConfig).size();
    int closedPipelines = pipelineStateManager.getPipelines(replicationConfig, PipelineState.CLOSED).size();
    int openPipelines = totalActivePipelines - closedPipelines;
    // Check per-datanode pipeline limit
    if (datanodePipelineLimit > 0) {
      int healthyNodeCount = getNodeManager()
          .getNodeCount(NodeStatus.inServiceHealthy());
      int allowedOpenPipelines = (datanodePipelineLimit * healthyNodeCount)
          / replicationConfig.getRequiredNodes();
      return openPipelines >= allowedOpenPipelines;
    }
    // Check global pipeline limit
    if (pipelineNumberLimit > 0) {
      int factorOnePipelineCount = pipelineStateManager
          .getPipelines(RatisReplicationConfig.getInstance(ReplicationFactor.ONE)).size();
      int allowedOpenPipelines = pipelineNumberLimit - factorOnePipelineCount;
      return openPipelines >= allowedOpenPipelines;
    }
    // No limits are set
    return false;
  }

  @Override
  public synchronized Pipeline create(RatisReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes)
      throws IOException {
    if (exceedPipelineNumberLimit(replicationConfig)) {
      String limitInfo = (datanodePipelineLimit > 0)
          ? String.format("per datanode: %d", datanodePipelineLimit)
          : String.format(": %d", pipelineNumberLimit);

      throw new SCMException(
          String.format("Cannot create pipeline as it would exceed the limit %s replicationConfig: %s",
              limitInfo, replicationConfig),
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE
      );
    }

    final List<DatanodeDetails> dns;
    final ReplicationFactor factor =
        replicationConfig.getReplicationFactor();
    switch (factor) {
    case ONE:
      dns = pickNodesNotUsed(replicationConfig, getMinRatisVolumeSizeBytes(), getContainerSizeBytes());
      break;
    case THREE:
      dns = chooseThreeFactorDatanodes(excludedNodes, favoredNodes, factor.getNumber(),
          isRatisStreamingEnabled, true);

      break;
    default:
      throw new IllegalStateException("Unknown factor: " + factor.name());
    }

    return createPipeline(RatisReplicationConfig.getInstance(factor), dns);
  }
}

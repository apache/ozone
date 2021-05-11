/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms.LeaderChoosePolicy;
import org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms.LeaderChoosePolicyFactory;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements Api for creating ratis pipelines.
 */
public class RatisPipelineProvider
    extends PipelineProvider<RatisReplicationConfig> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisPipelineProvider.class);

  private final ConfigurationSource conf;
  private final EventPublisher eventPublisher;
  private final PipelinePlacementPolicy placementPolicy;
  private int pipelineNumberLimit;
  private int maxPipelinePerDatanode;
  private final LeaderChoosePolicy leaderChoosePolicy;
  private final SCMContext scmContext;

  @VisibleForTesting
  public RatisPipelineProvider(NodeManager nodeManager,
                               StateManager stateManager,
                               ConfigurationSource conf,
                               EventPublisher eventPublisher,
                               SCMContext scmContext) {
    super(nodeManager, stateManager);
    this.conf = conf;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.placementPolicy =
        new PipelinePlacementPolicy(nodeManager, stateManager, conf);
    this.pipelineNumberLimit = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT_DEFAULT);
    String dnLimit = conf.get(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT);
    this.maxPipelinePerDatanode = dnLimit == null ? 0 :
        Integer.parseInt(dnLimit);
    try {
      leaderChoosePolicy = LeaderChoosePolicyFactory
          .getPolicy(conf, nodeManager, stateManager);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean exceedPipelineNumberLimit(
      RatisReplicationConfig replicationConfig) {
    if (replicationConfig.getReplicationFactor() != ReplicationFactor.THREE) {
      // Only put limits for Factor THREE pipelines.
      return false;
    }
    // Per datanode limit
    if (maxPipelinePerDatanode > 0) {
      return (getPipelineStateManager().getPipelines(replicationConfig).size() -
          getPipelineStateManager().getPipelines(replicationConfig,
              PipelineState.CLOSED).size()) > maxPipelinePerDatanode *
          getNodeManager().getNodeCount(NodeStatus.inServiceHealthy()) /
          replicationConfig.getRequiredNodes();
    }

    // Global limit
    if (pipelineNumberLimit > 0) {
      return (getPipelineStateManager().getPipelines(replicationConfig).size() -
          getPipelineStateManager().getPipelines(
              replicationConfig, PipelineState.CLOSED).size()) >
          (pipelineNumberLimit - getPipelineStateManager()
              .getPipelines(new RatisReplicationConfig(ReplicationFactor.ONE))
              .size());
    }

    return false;
  }

  @VisibleForTesting
  public LeaderChoosePolicy getLeaderChoosePolicy() {
    return leaderChoosePolicy;
  }

  @Override
  public synchronized Pipeline create(RatisReplicationConfig replicationConfig)
      throws IOException {
    if (exceedPipelineNumberLimit(replicationConfig)) {
      throw new SCMException("Ratis pipeline number meets the limit: " +
          pipelineNumberLimit + " replicationConfig : " +
          replicationConfig,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    List<DatanodeDetails> dns;

    final ReplicationFactor factor =
        replicationConfig.getReplicationFactor();
    switch (factor) {
    case ONE:
      dns = pickNodesNeverUsed(replicationConfig);
      break;
    case THREE:
      dns = placementPolicy.chooseDatanodes(null,
          null, factor.getNumber(), 0);
      break;
    default:
      throw new IllegalStateException("Unknown factor: " + factor.name());
    }

    DatanodeDetails suggestedLeader = leaderChoosePolicy.chooseLeader(dns);

    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setReplicationConfig(new RatisReplicationConfig(
            factor))
        .setNodes(dns)
        .setSuggestedLeaderId(
            suggestedLeader != null ? suggestedLeader.getUuid() : null)
        .build();

    // Send command to datanodes to create pipeline
    final CreatePipelineCommand createCommand = suggestedLeader != null ?
        new CreatePipelineCommand(pipeline.getId(), pipeline.getType(),
            factor, dns, suggestedLeader) :
        new CreatePipelineCommand(pipeline.getId(), pipeline.getType(),
            factor, dns);

    createCommand.setTerm(scmContext.getTermOfLeader());

    dns.forEach(node -> {
      LOG.info("Sending CreatePipelineCommand for pipeline:{} to datanode:{}",
          pipeline.getId(), node.getUuidString());
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(node.getUuid(), createCommand));
    });

    return pipeline;
  }

  @Override
  public Pipeline create(RatisReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .build();
  }

  @Override
  public void shutdown() {
  }

  /**
   * Removes pipeline from SCM. Sends command to destroy pipeline on all
   * the datanodes.
   *
   * @param pipeline            - Pipeline to be destroyed
   * @throws NotLeaderException - Send datanode command while not leader
   */
  @Override
  public void close(Pipeline pipeline) throws NotLeaderException {
    final ClosePipelineCommand closeCommand =
        new ClosePipelineCommand(pipeline.getId());
    closeCommand.setTerm(scmContext.getTermOfLeader());
    pipeline.getNodes().forEach(node -> {
      final CommandForDatanode<?> datanodeCommand =
          new CommandForDatanode<>(node.getUuid(), closeCommand);
      LOG.info("Send pipeline:{} close command to datanode {}",
          pipeline.getId(), datanodeCommand.getDatanodeId());
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    });
  }
}

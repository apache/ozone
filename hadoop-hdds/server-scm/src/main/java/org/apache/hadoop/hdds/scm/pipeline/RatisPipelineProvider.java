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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements Api for creating ratis pipelines.
 */
public class RatisPipelineProvider extends PipelineProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisPipelineProvider.class);

  private final ConfigurationSource conf;
  private final EventPublisher eventPublisher;
  private final PipelinePlacementPolicy placementPolicy;
  private int pipelineNumberLimit;
  private int maxPipelinePerDatanode;

  RatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager, ConfigurationSource conf,
      EventPublisher eventPublisher) {
    super(nodeManager, stateManager);
    this.conf = conf;
    this.eventPublisher = eventPublisher;
    this.placementPolicy =
        new PipelinePlacementPolicy(nodeManager, stateManager, conf);
    this.pipelineNumberLimit = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT_DEFAULT);
    this.maxPipelinePerDatanode = conf.getInt(
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
  }

  private boolean exceedPipelineNumberLimit(int replication) {
    if (replication != 3) {
      // Only put limits for Factor THREE pipelines.
      return false;
    }
    // Per datanode limit
    if (maxPipelinePerDatanode > 0) {
      return (getPipelineStateManager().getPipelines(
          ReplicationType.RATIS, replication).size() -
          getPipelineStateManager().getPipelines(
              ReplicationType.RATIS, replication,
              PipelineState.CLOSED).size()) > maxPipelinePerDatanode *
          getNodeManager().getNodeCount(HddsProtos.NodeState.HEALTHY) /
          replication;
    }

    // Global limit
    if (pipelineNumberLimit > 0) {
      return (getPipelineStateManager().getPipelines(
          ReplicationType.RATIS, 3).size() -
          getPipelineStateManager().getPipelines(
              ReplicationType.RATIS, 3,
              PipelineState.CLOSED).size()) >
          (pipelineNumberLimit - getPipelineStateManager().getPipelines(
              ReplicationType.RATIS, 1).size());
    }

    return false;
  }

  @Override
  public Pipeline create(int replication) throws IOException {
    if (exceedPipelineNumberLimit(replication)) {
      throw new SCMException("Ratis pipeline number meets the limit: " +
          pipelineNumberLimit + " factor : " +
          replication,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    List<DatanodeDetails> dns;

    switch(replication) {
    case 1:
      dns = pickNodesNeverUsed(ReplicationType.RATIS, 1);
      break;
    case 3:
      dns = placementPolicy.chooseDatanodes(null,
          null, 3, 0);
      break;
    default:
      throw new IllegalStateException("Unknown factor: " + replication);
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setType(ReplicationType.RATIS)
        .setReplication(replication)
        .setNodes(dns)
        .build();

    // Send command to datanodes to create pipeline
    final CreatePipelineCommand createCommand =
        new CreatePipelineCommand(pipeline.getId(), pipeline.getType(),
            replication, dns);

    dns.forEach(node -> {
      LOG.info("Sending CreatePipelineCommand for pipeline:{} to datanode:{}",
          pipeline.getId(), node.getUuidString());
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(node.getUuid(), createCommand));
    });

    return pipeline;
  }

  @Override
  public Pipeline create(int replication,
                         List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setType(ReplicationType.RATIS)
        .setReplication(replication)
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
   * @param pipeline        - Pipeline to be destroyed
   * @throws IOException
   */
  public void close(Pipeline pipeline) {
    final ClosePipelineCommand closeCommand =
        new ClosePipelineCommand(pipeline.getId());
    pipeline.getNodes().stream().forEach(node -> {
      final CommandForDatanode datanodeCommand =
          new CommandForDatanode<>(node.getUuid(), closeCommand);
      LOG.info("Send pipeline:{} close command to datanode {}",
          pipeline.getId(), datanodeCommand.getDatanodeId());
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    });
  }
}

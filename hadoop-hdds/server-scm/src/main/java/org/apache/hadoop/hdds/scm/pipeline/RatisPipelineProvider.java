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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implements Api for creating ratis pipelines.
 */
public class RatisPipelineProvider implements PipelineProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisPipelineProvider.class);

  private final NodeManager nodeManager;
  private final PipelineStateManager stateManager;
  private final Configuration conf;
  private final EventPublisher eventPublisher;
  private final PipelinePlacementPolicy placementPolicy;
  private int pipelineNumberLimit;
  private int maxPipelinePerDatanode;

  // Set parallelism at 3, as now in Ratis we create 1 and 3 node pipelines.
  private final int parallelismForPool = 3;

  private final ForkJoinPool.ForkJoinWorkerThreadFactory factory =
      (pool -> {
        final ForkJoinWorkerThread worker = ForkJoinPool.
            defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName("RATISCREATEPIPELINE" + worker.getPoolIndex());
        return worker;
      });

  private final ForkJoinPool forkJoinPool = new ForkJoinPool(
      parallelismForPool, factory, null, false);

  RatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager, Configuration conf,
      EventPublisher eventPublisher) {
    this.nodeManager = nodeManager;
    this.stateManager = stateManager;
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

  private List<DatanodeDetails> pickNodesNeverUsed(ReplicationFactor factor)
      throws SCMException {
    Set<DatanodeDetails> dnsUsed = new HashSet<>();
    stateManager.getPipelines(ReplicationType.RATIS, factor)
        .stream().filter(
          p -> p.getPipelineState().equals(PipelineState.OPEN) ||
              p.getPipelineState().equals(PipelineState.DORMANT) ||
              p.getPipelineState().equals(PipelineState.ALLOCATED))
        .forEach(p -> dnsUsed.addAll(p.getNodes()));

    // Get list of healthy nodes
    List<DatanodeDetails> dns = nodeManager
        .getNodes(HddsProtos.NodeState.HEALTHY)
        .parallelStream()
        .filter(dn -> !dnsUsed.contains(dn))
        .limit(factor.getNumber())
        .collect(Collectors.toList());
    if (dns.size() < factor.getNumber()) {
      String e = String
          .format("Cannot create pipeline of factor %d using %d nodes." +
                  " Used %d nodes. Healthy nodes %d", factor.getNumber(),
              dns.size(), dnsUsed.size(),
              nodeManager.getNodes(HddsProtos.NodeState.HEALTHY).size());
      throw new SCMException(e,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return dns;
  }

  private boolean exceedPipelineNumberLimit(ReplicationFactor factor) {
    if (factor != ReplicationFactor.THREE) {
      // Only put limits for Factor THREE pipelines.
      return false;
    }
    // Per datanode limit
    if (maxPipelinePerDatanode > 0) {
      return (stateManager.getPipelines(ReplicationType.RATIS, factor).size() -
          stateManager.getPipelines(ReplicationType.RATIS, factor,
              Pipeline.PipelineState.CLOSED).size()) > maxPipelinePerDatanode *
          nodeManager.getNodeCount(HddsProtos.NodeState.HEALTHY) /
          factor.getNumber();
    }

    // Global limit
    if (pipelineNumberLimit > 0) {
      return (stateManager.getPipelines(ReplicationType.RATIS,
          ReplicationFactor.THREE).size() - stateManager.getPipelines(
          ReplicationType.RATIS, ReplicationFactor.THREE,
          Pipeline.PipelineState.CLOSED).size()) >
          (pipelineNumberLimit - stateManager.getPipelines(
              ReplicationType.RATIS, ReplicationFactor.ONE).size());
    }

    return false;
  }

  @Override
  public Pipeline create(ReplicationFactor factor) throws IOException {
    if (exceedPipelineNumberLimit(factor)) {
      throw new SCMException("Ratis pipeline number meets the limit: " +
          pipelineNumberLimit + " factor : " +
          factor.getNumber(),
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    List<DatanodeDetails> dns;
    int nodeIdHash = 0;

    switch(factor) {
    case ONE:
      dns = pickNodesNeverUsed(ReplicationFactor.ONE);
      break;
    case THREE:
      dns = placementPolicy.chooseDatanodes(null,
          null, factor.getNumber(), 0);
      nodeIdHash = RatisPipelineUtils.encodeNodeIdsOfFactorThreePipeline(dns);
      break;
    default:
      throw new IllegalStateException("Unknown factor: " + factor.name());
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setType(ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(dns)
        .setNodeIdsHash(nodeIdHash)
        .build();

    // Send command to datanodes to create pipeline
    final CreatePipelineCommand createCommand =
        new CreatePipelineCommand(pipeline.getId(), pipeline.getType(),
            factor, dns);

    dns.forEach(node -> {
      LOG.info("Sending CreatePipelineCommand for pipeline:{} to datanode:{}",
          pipeline.getId(), node.getUuidString());
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(node.getUuid(), createCommand));
    });

    return pipeline;
  }

  @Override
  public Pipeline create(ReplicationFactor factor,
                         List<DatanodeDetails> nodes) {
    int nodeIdHash = 0;
    if (factor == ReplicationFactor.THREE) {
      nodeIdHash = RatisPipelineUtils.encodeNodeIdsOfFactorThreePipeline(nodes);
    }
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setType(ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(nodes)
        .setNodeIdsHash(nodeIdHash)
        .build();
  }

  @Override
  public void shutdown() {
    forkJoinPool.shutdownNow();
    try {
      forkJoinPool.awaitTermination(60, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Unexpected exception occurred during shutdown of " +
              "RatisPipelineProvider", e);
    }
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

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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.NodeUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms.LeaderChoosePolicy;
import org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms.LeaderChoosePolicyFactory;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements Api for creating ratis pipelines.
 */
public class RatisPipelineProvider
    extends PipelineProvider<RatisReplicationConfig> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisPipelineProvider.class);

  private final EventPublisher eventPublisher;
  private final PlacementPolicy placementPolicy;
  private final int pipelineNumberLimit;
  private final int datanodePipelineLimit;
  private final LeaderChoosePolicy leaderChoosePolicy;
  private final SCMContext scmContext;
  private final long containerSizeBytes;
  private final long minRatisVolumeSizeBytes;

  @VisibleForTesting
  public RatisPipelineProvider(NodeManager nodeManager,
                               PipelineStateManager stateManager,
                               ConfigurationSource conf,
                               EventPublisher eventPublisher,
                               SCMContext scmContext) {
    super(nodeManager, stateManager);
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.placementPolicy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);
    this.pipelineNumberLimit = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT_DEFAULT);
    this.datanodePipelineLimit = conf.getInt(
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
    this.containerSizeBytes = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);
    this.minRatisVolumeSizeBytes = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN_DEFAULT,
        StorageUnit.BYTES);
    try {
      leaderChoosePolicy = LeaderChoosePolicyFactory
          .getPolicy(conf, nodeManager, stateManager);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  @VisibleForTesting
  public LeaderChoosePolicy getLeaderChoosePolicy() {
    return leaderChoosePolicy;
  }

  @Override
  public synchronized Pipeline create(RatisReplicationConfig replicationConfig,
      StorageTier storageTier)
      throws IOException {
    return create(replicationConfig, Collections.emptyList(),
        Collections.emptyList(), storageTier);
  }

  @Override
  public synchronized Pipeline create(RatisReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes, StorageTier storageTier)
      throws IOException {
    if (exceedPipelineNumberLimit(replicationConfig)) {
      String limitInfo = (datanodePipelineLimit > 0)
          ? String.format("per datanode: %d", datanodePipelineLimit)
          : String.format(": %d", pipelineNumberLimit);

      throw new SCMException(
          String.format("Cannot create pipeline for StorageTier %s as it would exceed the limit %s " +
                  "replicationConfig: %s", storageTier, limitInfo, replicationConfig),
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE
      );
    }

    final List<DatanodeDetails> dns;
    final ReplicationFactor factor =
        replicationConfig.getReplicationFactor();
    switch (factor) {
    case ONE:
      dns = pickNodesNotUsed(replicationConfig, minRatisVolumeSizeBytes, containerSizeBytes, storageTier);
      break;
    case THREE:
      StorageTierUtil.validateNotEmpty(storageTier);
      StorageType storageType = storageTier.getUniformStorageType();
      List<DatanodeDetails> excludeDueToEngagement = filterPipelineEngagement();
      if (!excludeDueToEngagement.isEmpty()) {
        if (excludedNodes.isEmpty()) {
          excludedNodes = excludeDueToEngagement;
        } else {
          excludedNodes.addAll(excludeDueToEngagement);
        }
      }
      dns = placementPolicy.chooseDatanodes(excludedNodes,
          favoredNodes, factor.getNumber(), minRatisVolumeSizeBytes,
          containerSizeBytes, storageType);
      break;
    default:
      throw new IllegalStateException("Unknown factor: " + factor.name());
    }

    DatanodeDetails suggestedLeader = leaderChoosePolicy.chooseLeader(dns);

    List<StorageTier> storageTiers = NodeUtils.getDatanodesStorageTypes(dns, getNodeManager());
    if (!storageTiers.contains(storageTier)) {
      throw new SCMException(String.format("Cannot create pipeline for StorageTier %s replicationConfig: %s",
              storageTier, replicationConfig), SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    Preconditions.checkArgument(storageTiers.contains(storageTier));
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setReplicationConfig(RatisReplicationConfig.getInstance(factor))
        .setNodes(dns)
        .setSuggestedLeaderId(
            suggestedLeader != null ? suggestedLeader.getID() : null)
        .setSupportedStorageTier(storageTiers)
        .build();

    // Send command to datanodes to create pipeline
    final CreatePipelineCommand createCommand = suggestedLeader != null ?
        new CreatePipelineCommand(pipeline.getId(), pipeline.getType(),
            factor, dns, suggestedLeader) :
        new CreatePipelineCommand(pipeline.getId(), pipeline.getType(),
            factor, dns);

    createCommand.setTerm(scmContext.getTermOfLeader());

    dns.forEach(node -> {
      LOG.info("Sending CreatePipelineCommand for pipeline:{} to datanode:{} storageTier:{}",
          pipeline.getId(), node, storageTier);
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(node, createCommand));
    });

    return pipeline;
  }

  @Override
  public Pipeline create(RatisReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes) {
    List<StorageTier> storageTiers = NodeUtils.getDatanodesStorageTypes(nodes, getNodeManager());
    return createPipelineInternal(replicationConfig, nodes, storageTiers);
  }

  private Pipeline createPipelineInternal(RatisReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes, List<StorageTier> storageTiers) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.ALLOCATED)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .setSupportedStorageTier(storageTiers)
        .build();
  }

  @Override
  public Pipeline createForRead(
      RatisReplicationConfig replicationConfig,
      Set<ContainerReplica> replicas) {
    // Read Pipelines do not require storage tiers, so the calculation of storage tiers can be omitted.
    return createPipelineInternal(replicationConfig, replicas
        .stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList()), new ArrayList<>());
  }

  private List<DatanodeDetails> filterPipelineEngagement() {
    final NodeManager nodeManager = getNodeManager();
    final PipelineStateManager stateManager = getPipelineStateManager();
    final List<DatanodeDetails> healthyNodes = nodeManager.getNodes(NodeStatus.inServiceHealthy());
    final List<DatanodeDetails> excluded = new ArrayList<>();
    for (DatanodeDetails d : healthyNodes) {
      final int count = PipelinePlacementPolicy.currentRatisThreePipelineCount(nodeManager, stateManager, d);
      if (count >= nodeManager.pipelineLimit(d)) {
        excluded.add(d);
      }
    }
    return excluded;
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
          new CommandForDatanode<>(node, closeCommand);
      LOG.info("Send pipeline:{} close command to datanode {}",
          pipeline.getId(), datanodeCommand.getDatanodeId());
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    });
  }
}

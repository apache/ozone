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

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.RATIS_DATASTREAM;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
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
 * Base class for providers of pipelines with N copies, such as RATIS and CHAINED pipelines.
 * The datanodes are chosen by {@link PipelinePlacementPolicy},
 * a leader is suggested by {@link LeaderChoosePolicy}
 * and the pipelines are created and closed on the datanodes using
 * {@link CreatePipelineCommand} and {@link ClosePipelineCommand}.
 */
public abstract class NCopyPipelineProvider<C extends ReplicatedReplicationConfig> extends PipelineProvider<C> {

  private static final Logger LOG = LoggerFactory.getLogger(NCopyPipelineProvider.class);

  private final EventPublisher eventPublisher;
  private final PlacementPolicy placementPolicy;
  private final LeaderChoosePolicy leaderChoosePolicy;
  private final SCMContext scmContext;
  private final long containerSizeBytes;
  private final long minRatisVolumeSizeBytes;

  protected NCopyPipelineProvider(NodeManager nodeManager,
                                  PipelineStateManager stateManager,
                                  ConfigurationSource conf,
                                  EventPublisher eventPublisher,
                                  SCMContext scmContext) {
    super(nodeManager, stateManager);
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.placementPolicy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);
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

  @VisibleForTesting
  public LeaderChoosePolicy getLeaderChoosePolicy() {
    return leaderChoosePolicy;
  }

  long getContainerSizeBytes() {
    return containerSizeBytes;
  }

  long getMinRatisVolumeSizeBytes() {
    return minRatisVolumeSizeBytes;
  }

  @Override
  public synchronized Pipeline create(C replicationConfig) throws IOException {
    return create(replicationConfig, Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public Pipeline create(C replicationConfig, List<DatanodeDetails> nodes) {
    return newPipelineBuilder(replicationConfig, nodes)
        .setId(PipelineID.randomId())
        .build();
  }

  @Override
  public Pipeline createForRead(C replicationConfig, Set<ContainerReplica> replicas) {
    // Use insecureRandomId for throwaway read pipeline IDs to avoid
    // contention on the shared SecureRandom instance.
    return newPipelineBuilder(replicationConfig, ContainerReplica.toDatanodeDetailsList(replicas))
        .setId(PipelineID.insecureRandomId())
        .build();
  }

  /**
   * Build a new pipeline with the given datanodes and a suggested leader.
   * Then, send {@link CreatePipelineCommand} to each datanode.
   */
  Pipeline createPipeline(C replicationConfig, List<DatanodeDetails> dns) throws NotLeaderException {
    DatanodeDetails suggestedLeader = leaderChoosePolicy.chooseLeader(dns);

    Pipeline pipeline = newPipelineBuilder(replicationConfig, dns)
        .setId(PipelineID.randomId())
        .setSuggestedLeaderId(suggestedLeader != null ? suggestedLeader.getID() : null)
        .build();

    // Send command to datanodes to create pipeline
    final ReplicationFactor factor = replicationConfig.getReplicationFactor();
    final CreatePipelineCommand createCommand = new CreatePipelineCommand(
        pipeline.getId(), pipeline.getType(), factor, dns, suggestedLeader);
    createCommand.setTerm(scmContext.getTermOfLeader());

    dns.forEach(node -> {
      LOG.info("Sending CreatePipelineCommand for pipeline:{} to datanode:{}",
          pipeline.getId(), node);
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
          new CommandForDatanode<>(node, createCommand));
    });

    return pipeline;
  }

  /**
   * Choose datanodes for a factor THREE pipeline,
   * excluding the datanodes which have reached their pipeline limit.
   *
   * @param excludeNonStreamNodes Should the datanodes without the {@link DatanodeDetails.Port.Name#RATIS_DATASTREAM}
   *                              port be excluded?
   * @param fallback When excludeNonStreamNodes is true, should it fall back to choose from all the datanodes
   *                 if there are not enough datanodes with the stream port?
   */
  List<DatanodeDetails> chooseThreeFactorDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes, int requiredNode,
      boolean excludeNonStreamNodes, boolean fallback)
      throws IOException {
    final NodeManager nodeManager = getNodeManager();
    final PipelineStateManager stateManager = getPipelineStateManager();
    final List<DatanodeDetails> healthyNodes = nodeManager.getNodes(NodeStatus.inServiceHealthy());
    excludedNodes = excludedNodes.isEmpty() ? null : excludedNodes;
    List<DatanodeDetails> additionalExcludedNodes = null;
    for (DatanodeDetails d : healthyNodes) {
      final int count = PipelinePlacementPolicy.currentRatisThreePipelineCount(nodeManager, stateManager, d);
      if (count >= nodeManager.pipelineLimit(d)) {
        if (excludedNodes == null) {
          excludedNodes = new ArrayList<>();
        }
        excludedNodes.add(d);
      } else if (excludeNonStreamNodes && !d.hasPort(RATIS_DATASTREAM)) {
        if (additionalExcludedNodes == null) {
          additionalExcludedNodes = new ArrayList<>();
        }
        additionalExcludedNodes.add(d);
      }
    }
    // If the cluster does not support Ratis streaming, or if all nodes support it, no fallback will occur.
    if (additionalExcludedNodes != null) {
      if (excludedNodes != null) {
        additionalExcludedNodes.addAll(excludedNodes);
      }
      try {
        return placementPolicy.chooseDatanodes(additionalExcludedNodes,
            favoredNodes, requiredNode, minRatisVolumeSizeBytes,
            containerSizeBytes);
      } catch (SCMException scmException) {
        if (!fallback) {
          throw scmException;
        }
        LOG.debug("Failed to allocate datanodes with strict exclusion (non-streaming nodes excluded)." +
            " Falling back.", scmException);
      }
    }
    return placementPolicy.chooseDatanodes(excludedNodes,
          favoredNodes, requiredNode, minRatisVolumeSizeBytes,
          containerSizeBytes);
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
          pipeline.getId(), node);
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    });
  }
}

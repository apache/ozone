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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;

/**
 * Creates pipeline based on replication type.
 */
public class PipelineFactory {

  private Map<ReplicationType, PipelineProvider> providers;

  PipelineFactory(NodeManager nodeManager, PipelineStateManager stateManager,
                  ConfigurationSource conf, EventPublisher eventPublisher,
                  SCMContext scmContext) {
    providers = new HashMap<>();
    providers.put(ReplicationType.STAND_ALONE,
        new SimplePipelineProvider(nodeManager, stateManager));
    providers.put(ReplicationType.RATIS,
        new RatisPipelineProvider(nodeManager,
            stateManager, conf,
            eventPublisher, scmContext));
    PlacementPolicy ecPlacementPolicy;
    try {
      ecPlacementPolicy = ContainerPlacementPolicyFactory.getECPolicy(conf,
          nodeManager, nodeManager.getClusterNetworkTopologyMap(), true,
          SCMContainerPlacementMetrics.create());
    } catch (SCMException e) {
      throw new RuntimeException("Unable to get the container placement policy",
          e);
    }
    providers.put(ReplicationType.EC,
        new ECPipelineProvider(nodeManager, stateManager, conf,
            ecPlacementPolicy));
  }

  protected PipelineFactory() {
  }

  @VisibleForTesting
  void setProvider(
      ReplicationType replicationType,
      PipelineProvider provider
  ) {
    providers.put(replicationType, provider);
  }

  public Pipeline create(
      ReplicationConfig replicationConfig, List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> favoredNodes)
      throws IOException {
    Pipeline pipeline = providers.get(replicationConfig.getReplicationType())
        .create(replicationConfig, excludedNodes, favoredNodes);
    checkPipeline(pipeline);
    return pipeline;
  }

  private void checkPipeline(Pipeline pipeline) throws IOException {
    // In case in case if provided pipeline provider returns null.
    if (pipeline == null) {
      throw new SCMException("Pipeline cannot be null",
          SCMException.ResultCodes.INTERNAL_ERROR);
    }
    // In case if provided pipeline returns less number of nodes than
    // required.
    if (pipeline.getNodes().size() != pipeline.getReplicationConfig()
        .getRequiredNodes()) {
      throw new SCMException("Nodes size= " + pipeline.getNodes()
          .size() + ", replication factor= " + pipeline.getReplicationConfig()
          .getRequiredNodes() + " do not match",
          SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES);
    }
  }

  public Pipeline create(ReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes
  ) {
    return providers.get(replicationConfig.getReplicationType())
        .create(replicationConfig, nodes);
  }

  public Pipeline createForRead(ReplicationConfig replicationConfig,
      Set<ContainerReplica> replicas) {
    return providers.get(replicationConfig.getReplicationType())
        .createForRead(replicationConfig, replicas);
  }

  public void close(ReplicationType type, Pipeline pipeline)
      throws IOException {
    providers.get(type).close(pipeline);
  }

  @VisibleForTesting
  public Map<ReplicationType, PipelineProvider> getProviders() {
    return providers;
  }

  protected void setProviders(
      Map<ReplicationType, PipelineProvider> providers) {
    this.providers = providers;
  }
}

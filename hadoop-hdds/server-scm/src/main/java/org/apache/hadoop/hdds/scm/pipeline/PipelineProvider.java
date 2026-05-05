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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for creating pipelines.
 */
public abstract class PipelineProvider<REPLICATION_CONFIG
    extends ReplicationConfig> {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineProvider.class);
  private final NodeManager nodeManager;
  private final PipelineStateManager stateManager;

  public PipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager) {
    this.nodeManager = nodeManager;
    this.stateManager = stateManager;
  }

  public PipelineProvider() {
    this.nodeManager = null;
    this.stateManager = null;
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public PipelineStateManager getPipelineStateManager() {
    return stateManager;
  }

  protected abstract Pipeline create(REPLICATION_CONFIG replicationConfig)
      throws IOException;

  protected abstract Pipeline create(REPLICATION_CONFIG replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes)
      throws IOException;

  protected abstract Pipeline create(
      REPLICATION_CONFIG replicationConfig,
      List<DatanodeDetails> nodes
  );

  protected abstract Pipeline createForRead(
      REPLICATION_CONFIG replicationConfig,
      Set<ContainerReplica> replicas
  );

  protected abstract void close(Pipeline pipeline) throws IOException;

  List<DatanodeDetails> pickNodesNotUsed(REPLICATION_CONFIG replicationConfig,
                                         long metadataSizeRequired,
                                         long dataSizeRequired)
      throws SCMException {
    int nodesRequired = replicationConfig.getRequiredNodes();
    List<DatanodeDetails> healthyDNs = pickAllNodesNotUsed(replicationConfig);
    List<DatanodeDetails> healthyDNsWithSpace = healthyDNs.stream()
        .filter(dn -> SCMCommonPlacementPolicy.hasEnoughSpace(dn, metadataSizeRequired, dataSizeRequired))
        .limit(nodesRequired)
        .collect(Collectors.toList());

    if (healthyDNsWithSpace.size() < nodesRequired) {
      String msg = String.format("Unable to find enough nodes that meet the " +
              "space requirement of %d bytes for metadata and %d bytes for " +
              "data in healthy node set. Nodes required: %d Found: %d",
          metadataSizeRequired, dataSizeRequired, nodesRequired,
          healthyDNsWithSpace.size());
      LOG.warn(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE);
    }

    return healthyDNsWithSpace;
  }

  List<DatanodeDetails> pickNodesNotUsed(REPLICATION_CONFIG replicationConfig)
      throws SCMException {
    return pickAllNodesNotUsed(replicationConfig).stream()
        .limit(replicationConfig.getRequiredNodes())
        .collect(Collectors.toList());
  }

  List<DatanodeDetails> pickAllNodesNotUsed(
      REPLICATION_CONFIG replicationConfig) throws SCMException {
    Set<DatanodeDetails> dnsUsed = new HashSet<>();
    stateManager.getPipelines(replicationConfig).stream().filter(
        p -> p.getPipelineState().equals(Pipeline.PipelineState.OPEN) ||
            p.getPipelineState().equals(Pipeline.PipelineState.DORMANT) ||
            p.getPipelineState().equals(Pipeline.PipelineState.ALLOCATED))
        .forEach(p -> dnsUsed.addAll(p.getNodes()));

    // Get list of healthy nodes
    List<DatanodeDetails> dns = nodeManager
        .getNodes(NodeStatus.inServiceHealthy())
        .parallelStream()
        .filter(dn -> !dnsUsed.contains(dn))
        .collect(Collectors.toList());
    if (dns.size() < replicationConfig.getRequiredNodes()) {
      String e = String
          .format("Cannot create pipeline %s using %d nodes." +
                  " Used %d nodes. Healthy nodes %d",
              replicationConfig.toString(),
              dns.size(), dnsUsed.size(),
              nodeManager.getNodes(NodeStatus.inServiceHealthy()).size());
      throw new SCMException(e,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return dns;
  }
}

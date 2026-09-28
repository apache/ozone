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

package org.apache.hadoop.hdds.scm.simulation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

/**
 * SCM's rack aware placement of RATIS container replicas, which tells the running simulation about placements that fail
 * although enough healthy in-service datanodes are neither used nor excluded. SCM creates the policy with fallback, so
 * it should then succeed on some rack. SCM loads it through {@code ozone.scm.container.placement.impl}.
 */
public class SimContainerPlacementPolicy implements PlacementPolicy {

  private final PlacementPolicy placement;
  private final NodeManager nodeManager;

  public SimContainerPlacementPolicy(NodeManager nodeManager, ConfigurationSource conf, NetworkTopology topology,
      boolean fallback, SCMContainerPlacementMetrics metrics) {
    this.placement = new SCMContainerPlacementRackAware(nodeManager, conf, topology, fallback, metrics);
    this.nodeManager = nodeManager;
  }

  @Override
  public List<DatanodeDetails> chooseDatanodes(List<DatanodeDetails> usedNodes, List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> favoredNodes, int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
      throws IOException {
    try {
      return placement.chooseDatanodes(usedNodes, excludedNodes, favoredNodes, nodesRequired, metadataSizeRequired,
          dataSizeRequired);
    } catch (IOException e) {
      ScmSimulation simulation = ScmSimulation.running();
      if (simulation != null) {
        simulation.placementFailed(eligible(usedNodes, excludedNodes), nodesRequired, e);
      }
      throw e;
    }
  }

  /** Healthy in-service datanodes that are neither used nor excluded. */
  private List<DatanodeDetails> eligible(List<DatanodeDetails> usedNodes, List<DatanodeDetails> excludedNodes) {
    Set<DatanodeID> unavailable = new HashSet<>();
    for (List<DatanodeDetails> nodes : Arrays.asList(usedNodes, excludedNodes)) {
      if (nodes != null) {
        nodes.forEach(node -> unavailable.add(node.getID()));
      }
    }
    List<DatanodeDetails> eligible = new ArrayList<>();
    for (DatanodeDetails node : nodeManager.getNodes(NodeStatus.inServiceHealthy())) {
      if (!unavailable.contains(node.getID())) {
        eligible.add(node);
      }
    }
    return eligible;
  }

  @Override
  public ContainerPlacementStatus validateContainerPlacement(List<DatanodeDetails> dns, int replicas) {
    return placement.validateContainerPlacement(dns, replicas);
  }

  @Override
  public Set<ContainerReplica> replicasToCopyToFixMisreplication(Map<ContainerReplica, Boolean> replicas) {
    return placement.replicasToCopyToFixMisreplication(replicas);
  }

  @Override
  public Set<ContainerReplica> replicasToRemoveToFixOverreplication(Set<ContainerReplica> replicas,
      int expectedCountPerUniqueReplica) {
    return placement.replicasToRemoveToFixOverreplication(replicas, expectedCountPerUniqueReplica);
  }
}

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.NodeUtils;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to create pipelines for EC containers.
 */
public class ECPipelineProvider extends PipelineProvider<ECReplicationConfig> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECPipelineProvider.class);

  static final Comparator<NodeStatus> CREATE_FOR_READ_COMPARATOR = (left, right) -> {
    final int healthy = Boolean.compare(right.isHealthy(), left.isHealthy());
    if (healthy != 0) {
      return healthy;
    }
    final int dead = Boolean.compare(left.isDead(), right.isDead());
    return dead != 0 ? dead : left.getOperationalState().compareTo(right.getOperationalState());
  };

  // TODO - EC Placement Policy. Standard Network Aware topology will not work
  //        for EC as it stands. We may want an "as many racks as possible"
  //        policy. HDDS-5326.

  private final ConfigurationSource conf;
  private final PlacementPolicy placementPolicy;
  private final long containerSizeBytes;

  public ECPipelineProvider(NodeManager nodeManager,
                            PipelineStateManager stateManager,
                            ConfigurationSource conf,
                            PlacementPolicy placementPolicy) {
    super(nodeManager, stateManager);
    this.conf = conf;
    this.placementPolicy = placementPolicy;
    this.containerSizeBytes = (long) this.conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  @Override
  public synchronized Pipeline create(ECReplicationConfig replicationConfig, StorageTier storageTier)
      throws IOException {
    return create(replicationConfig, Collections.emptyList(),
        Collections.emptyList(), storageTier);
  }

  @Override
  protected Pipeline create(ECReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      StorageTier storageTier)
      throws IOException {
    StorageType storageType = storageTier.getUniformStorageType();
    List<DatanodeDetails> dns = placementPolicy
        .chooseDatanodes(excludedNodes, favoredNodes,
            replicationConfig.getRequiredNodes(), 0, this.containerSizeBytes, storageType);
    return create(replicationConfig, dns, storageTier);
  }

  @Override
  protected Pipeline create(ECReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes, StorageTier storageTier) throws IOException {
    List<StorageTier> storageTiers = NodeUtils.getDatanodesStorageTypes(nodes, getNodeManager());
    if (!storageTiers.contains(storageTier)) {
      throw new SCMException(String.format("Cannot create pipeline for "
              + "StorageTier %s replicationConfig: %s",
          storageTier, replicationConfig),
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    Map<DatanodeDetails, Integer> dnIndexes = new HashMap<>();
    int ecIndex = 1;
    for (DatanodeDetails dn : nodes) {
      dnIndexes.put(dn, ecIndex);
      ecIndex++;
    }

    return newPipelineBuilder(replicationConfig, nodes)
        .setId(PipelineID.randomId())
        .setReplicaIndexes(dnIndexes)
        .setSupportedStorageTier(storageTier)
        .build();
  }

  @Override
  public Pipeline createForRead(
      ECReplicationConfig replicationConfig,
      Set<ContainerReplica> replicas) {
    Map<DatanodeDetails, Integer> map = new HashMap<>();
    List<DatanodeDetails> dns = new ArrayList<>(replicas.size());
    Map<DatanodeDetails, NodeStatus> nodeStatusMap = new HashMap<>();

    for (ContainerReplica r : replicas) {
      DatanodeDetails dn = r.getDatanodeDetails();
      try {
        NodeStatus nodeStatus = getNodeManager().getNodeStatus(dn);
        if (!nodeStatus.isDead()) {
          map.put(dn, r.getReplicaIndex());
          dns.add(dn);
          nodeStatusMap.put(dn, nodeStatus);
        }
      } catch (NodeNotFoundException e) {
        LOG.error("Failed to getNodeStatus for {}", dn, e);
      }
    }

    dns.sort(Comparator.comparing(nodeStatusMap::get, CREATE_FOR_READ_COMPARATOR));

    // Use insecureRandomId for throwaway read pipeline IDs to avoid
    // contention on the shared SecureRandom instance.
    // Read Pipelines do not require storage tiers, so the calculation of storage tiers can be omitted.
    return newPipelineBuilder(replicationConfig, dns)
        .setId(PipelineID.insecureRandomId())
        .setReplicaIndexes(map)
        .setSupportedStorageTier(null)
        .build();
  }

  @Override
  protected void close(Pipeline pipeline) throws IOException {
  }

}

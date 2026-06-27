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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;

/**
 * Implements Api for creating stand alone pipelines.
 */
public class SimplePipelineProvider
    extends PipelineProvider<StandaloneReplicationConfig> {

  public SimplePipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager) {
    super(nodeManager, stateManager);
  }

  @Override
  public Pipeline create(StandaloneReplicationConfig replicationConfig, StorageTier storageTier)
      throws IOException {
    StorageTierUtil.validateNotEmpty(storageTier);
    return create(replicationConfig, Collections.emptyList(),
        Collections.emptyList(), storageTier);
  }

  @Override
  public Pipeline create(StandaloneReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes, StorageTier storageTier)
      throws IOException {
    StorageType storageType = storageTier.getUniformStorageType();
    List<DatanodeDetails> dns = pickNodesNotUsed(replicationConfig);
    dns = dns.stream().filter(dn ->
            ((DatanodeInfo) dn).getStorageReports().stream()
            .anyMatch(reportProto ->
                StorageTypeUtils.getFromProtobuf(reportProto.getStorageType()).equals(storageType)))
        .collect(Collectors.toList());
    int available = dns.size();
    int required = replicationConfig.getRequiredNodes();
    if (available < required) {
      String msg = String.format(
          "Cannot create pipeline of factor %d using %d nodes storageTier %s",
          required, available, storageTier);
      throw new InsufficientDatanodesException(required, available, msg);
    }

    Collections.shuffle(dns);
    List<StorageTier> storageTiers = NodeUtils.getDatanodesStorageTypes(dns, getNodeManager());
    if (!storageTiers.contains(storageTier)) {
      throw new SCMException(String.format("Cannot create pipeline for StorageTier %s replicationConfig: %s",
          storageTier, replicationConfig), SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(replicationConfig)
        .setNodes(dns.subList(0,
            replicationConfig.getReplicationFactor().getNumber()))
        .setSupportedStorageTier(storageTiers)
        .build();
  }

  private Pipeline createPipelineInternal(StandaloneReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes, List<StorageTier> storageTiers) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .setSupportedStorageTier(storageTiers)
        .build();
  }

  @Override
  public Pipeline create(StandaloneReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes) {
    List<StorageTier> storageTiers = NodeUtils.getDatanodesStorageTypes(nodes, getNodeManager());
    return createPipelineInternal(replicationConfig, nodes, storageTiers);
  }

  @Override
  public Pipeline createForRead(StandaloneReplicationConfig replicationConfig,
      Set<ContainerReplica> replicas) {
    // Read Pipelines do not require storage tiers, so the calculation of storage tiers can be omitted.
    return createPipelineInternal(replicationConfig, replicas
        .stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList()), new ArrayList<>());
  }

  @Override
  public void close(Pipeline pipeline) throws IOException {

  }

}

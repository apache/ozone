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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;
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
  public Pipeline create(StandaloneReplicationConfig replicationConfig)
      throws IOException {
    return create(replicationConfig, Collections.emptyList(),
        Collections.emptyList());
  }

  @Override
  public Pipeline create(StandaloneReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes)
      throws IOException {
    List<DatanodeDetails> dns = pickNodesNotUsed(replicationConfig);
    int available = dns.size();
    int required = replicationConfig.getRequiredNodes();
    if (available < required) {
      String msg = String.format(
          "Cannot create pipeline of factor %d using %d nodes.",
          required, available);
      throw new InsufficientDatanodesException(required, available, msg);
    }

    Collections.shuffle(dns);
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(replicationConfig)
        .setNodes(dns.subList(0,
            replicationConfig.getReplicationFactor().getNumber()))
        .build();
  }

  @Override
  public Pipeline create(StandaloneReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .build();
  }

  @Override
  public Pipeline createForRead(StandaloneReplicationConfig replicationConfig,
      Set<ContainerReplica> replicas) {
    return create(replicationConfig, replicas
        .stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList()));
  }

  @Override
  public void close(Pipeline pipeline) throws IOException {

  }

}

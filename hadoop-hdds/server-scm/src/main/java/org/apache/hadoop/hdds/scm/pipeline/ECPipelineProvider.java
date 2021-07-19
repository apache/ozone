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

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.node.NodeManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to create pipelines for EC containers.
 */
public class ECPipelineProvider extends PipelineProvider<ECReplicationConfig> {

  // TODO - EC Placement Policy. Standard Network Aware topology will not work
  //        for EC as it stands. We may want an "as many racks as possible"
  //        policy. HDDS-5326.

  private final ConfigurationSource conf;
  private final PlacementPolicy placementPolicy;

  public ECPipelineProvider(NodeManager nodeManager,
                            StateManager stateManager,
                            ConfigurationSource conf,
                            PlacementPolicy placementPolicy) {
    super(nodeManager, stateManager);
    this.conf = conf;
    this.placementPolicy = placementPolicy;
  }

  @Override
  public synchronized Pipeline create(ECReplicationConfig replicationConfig)
      throws IOException {
    return create(replicationConfig, Collections.emptyList(),
        Collections.emptyList());
  }

  @Override
  protected Pipeline create(ECReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes)
      throws IOException {
    List<DatanodeDetails> dns = placementPolicy.chooseDatanodes(excludedNodes,
        favoredNodes, replicationConfig.getRequiredNodes(), 0);
    return create(replicationConfig, dns);
  }

  @Override
  protected Pipeline create(ECReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes) {

    Map<DatanodeDetails, Integer> dnIndexes = new HashMap<>();
    int ecIndex = 1;
    for (DatanodeDetails dn : nodes) {
      dnIndexes.put(dn, ecIndex);
      ecIndex++;
    }

    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.ALLOCATED)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .setReplicaIndexes(dnIndexes)
        .build();
  }

  @Override
  protected void close(Pipeline pipeline) throws IOException {
  }

  @Override
  protected void shutdown() {
  }

}

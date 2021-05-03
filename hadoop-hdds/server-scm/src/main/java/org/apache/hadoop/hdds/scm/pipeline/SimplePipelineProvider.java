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

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Implements Api for creating stand alone pipelines.
 */
public class SimplePipelineProvider
    extends PipelineProvider<StandaloneReplicationConfig> {

  public SimplePipelineProvider(NodeManager nodeManager,
      StateManager stateManager) {
    super(nodeManager, stateManager);
  }

  @Override
  public Pipeline create(StandaloneReplicationConfig replicationConfig)
      throws IOException {
    List<DatanodeDetails> dns = pickNodesNeverUsed(replicationConfig);
    if (dns.size() < replicationConfig.getRequiredNodes()) {
      String e = String
          .format("Cannot create pipeline of factor %d using %d nodes.",
              replicationConfig.getRequiredNodes(), dns.size());
      throw new InsufficientDatanodesException(e);
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
  public void close(Pipeline pipeline) throws IOException {

  }

  @Override
  public void shutdown() {
    // Do nothing.
  }
}

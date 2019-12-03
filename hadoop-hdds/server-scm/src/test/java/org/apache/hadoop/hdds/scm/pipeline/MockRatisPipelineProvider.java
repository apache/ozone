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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;

import java.io.IOException;
import java.util.List;

/**
 * Mock Ratis Pipeline Provider for Mock Nodes.
 */
public class MockRatisPipelineProvider extends RatisPipelineProvider {

  private boolean autoOpenPipeline;

  public MockRatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager, Configuration conf,
      EventPublisher eventPublisher, boolean autoOpen) {
    super(nodeManager, stateManager, conf, eventPublisher);
    autoOpenPipeline = autoOpen;
  }

  public MockRatisPipelineProvider(NodeManager nodeManager,
                            PipelineStateManager stateManager,
                            Configuration conf) {
    super(nodeManager, stateManager, conf, new EventQueue());
  }

  public MockRatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager, Configuration conf,
      EventPublisher eventPublisher) {
    super(nodeManager, stateManager, conf, eventPublisher);
    autoOpenPipeline = true;
  }

  protected void initializePipeline(Pipeline pipeline) throws IOException {
    // do nothing as the datanodes do not exists
  }

  @Override
  public Pipeline create(HddsProtos.ReplicationFactor factor)
      throws IOException {
    if (autoOpenPipeline) {
      return super.create(factor);
    } else {
      Pipeline initialPipeline = super.create(factor);
      return Pipeline.newBuilder()
          .setId(initialPipeline.getId())
          // overwrite pipeline state to main ALLOCATED
          .setState(Pipeline.PipelineState.ALLOCATED)
          .setType(initialPipeline.getType())
          .setFactor(factor)
          .setNodes(initialPipeline.getNodes())
          .build();
    }
  }

  @Override
  public void shutdown() {
    // Do nothing.
  }

  @Override
  public Pipeline create(HddsProtos.ReplicationFactor factor,
                         List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.OPEN)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(nodes)
        .build();
  }
}

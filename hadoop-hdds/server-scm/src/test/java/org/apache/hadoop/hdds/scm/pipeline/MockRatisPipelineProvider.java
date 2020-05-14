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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;

/**
 * Mock Ratis Pipeline Provider for Mock Nodes.
 */
public class MockRatisPipelineProvider extends RatisPipelineProvider {

  private boolean autoOpenPipeline;
  private  boolean isHealthy;

  public MockRatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager, ConfigurationSource conf,
      EventPublisher eventPublisher, boolean autoOpen) {
    super(nodeManager, stateManager, conf, eventPublisher);
    autoOpenPipeline = autoOpen;
  }

  public MockRatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager,
      ConfigurationSource conf) {
    super(nodeManager, stateManager, conf, new EventQueue());
  }

  public MockRatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager,
      ConfigurationSource conf, boolean isHealthy) {
    super(nodeManager, stateManager, conf, new EventQueue());
    this.isHealthy = isHealthy;
  }

  public MockRatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager, ConfigurationSource conf,
      EventPublisher eventPublisher) {
    super(nodeManager, stateManager, conf, eventPublisher);
    autoOpenPipeline = true;
  }

  protected void initializePipeline(Pipeline pipeline) throws IOException {
    // do nothing as the datanodes do not exists
  }

  @Override
  public Pipeline create(int replication)
      throws IOException {
    if (autoOpenPipeline) {
      return super.create(replication);
    } else {
      Pipeline initialPipeline = super.create(replication);
      Pipeline pipeline = Pipeline.newBuilder()
          .setId(initialPipeline.getId())
          // overwrite pipeline state to main ALLOCATED
          .setState(Pipeline.PipelineState.ALLOCATED)
          .setType(initialPipeline.getType())
          .setReplication(replication)
          .setNodes(initialPipeline.getNodes())
          .build();
      if (isHealthy) {
        for (DatanodeDetails datanodeDetails : initialPipeline.getNodes()) {
          pipeline.reportDatanode(datanodeDetails);
        }
        pipeline.setLeaderId(initialPipeline.getFirstNode().getUuid());
      }
      return pipeline;
    }
  }

  @Override
  public void shutdown() {
    // Do nothing.
  }

  @Override
  public Pipeline create(int replication,
                         List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.OPEN)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setReplication(replication)
        .setNodes(nodes)
        .build();
  }
}

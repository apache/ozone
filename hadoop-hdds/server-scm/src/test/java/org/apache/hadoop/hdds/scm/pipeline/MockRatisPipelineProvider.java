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
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;

/**
 * Mock Ratis Pipeline Provider for Mock Nodes.
 */
public class MockRatisPipelineProvider extends RatisPipelineProvider {

  private boolean autoOpenPipeline;

  public MockRatisPipelineProvider(
      NodeManager nodeManager, PipelineStateManager stateManager,
      ConfigurationSource conf, EventPublisher eventPublisher,
      boolean autoOpen) {
    super(nodeManager, stateManager,
        conf, eventPublisher, SCMContext.emptyContext());
    autoOpenPipeline = autoOpen;
  }

  public MockRatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager,
      ConfigurationSource conf) {
    super(nodeManager, stateManager,
        conf, new EventQueue(), SCMContext.emptyContext());
  }

  public MockRatisPipelineProvider(
      NodeManager nodeManager, PipelineStateManager stateManager,
      ConfigurationSource conf, EventPublisher eventPublisher) {
    super(nodeManager, stateManager,
        conf, eventPublisher, SCMContext.emptyContext());
    autoOpenPipeline = true;
  }

  protected void initializePipeline(Pipeline pipeline) throws IOException {
    // do nothing as the datanodes do not exists
  }

  @Override
  public Pipeline create(RatisReplicationConfig replicationConfig)
      throws IOException {
    if (autoOpenPipeline) {
      return super.create(replicationConfig);
    } else {
      Pipeline initialPipeline = super.create(replicationConfig);
      Pipeline pipeline = Pipeline.newBuilder()
          .setId(initialPipeline.getId())
          // overwrite pipeline state to main ALLOCATED
          .setState(Pipeline.PipelineState.ALLOCATED)
          .setReplicationConfig(ReplicationConfig
              .fromProtoTypeAndFactor(initialPipeline.getType(),
                  replicationConfig.getReplicationFactor()))
          .setNodes(initialPipeline.getNodes())
          .build();
      return pipeline;
    }
  }

  public static void markPipelineHealthy(Pipeline pipeline)
      throws IOException {
    for (DatanodeDetails datanodeDetails : pipeline.getNodes()) {
      pipeline.reportDatanode(datanodeDetails);
    }
    pipeline.setLeaderId(pipeline.getFirstNode().getID());
  }

  @Override
  public Pipeline create(RatisReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .build();
  }
}

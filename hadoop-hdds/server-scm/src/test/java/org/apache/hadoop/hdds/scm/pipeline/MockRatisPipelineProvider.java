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
import org.apache.hadoop.hdds.scm.node.NodeManager;

import java.io.IOException;

/**
 * Mock Ratis Pipeline Provider for Mock Nodes.
 */
public class MockRatisPipelineProvider extends RatisPipelineProvider {

  public MockRatisPipelineProvider(NodeManager nodeManager,
                            PipelineStateManager stateManager,
                            Configuration conf) {
    super(nodeManager, stateManager, conf);
  }

  protected void initializePipeline(Pipeline pipeline) throws IOException {
    // do nothing as the datanodes do not exists
  }

  @Override
  public void shutdown() {
    // Do nothing.
  }
}

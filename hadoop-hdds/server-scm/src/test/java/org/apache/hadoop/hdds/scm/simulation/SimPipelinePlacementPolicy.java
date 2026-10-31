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

import java.util.Random;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelinePlacementPolicy;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;

/**
 * Pipeline placement whose random choices come from a seed in the configuration, so that the simulation places
 * pipelines reproducibly. SCM loads it through {@code ozone.scm.pipeline.placement.impl}.
 */
public class SimPipelinePlacementPolicy extends PipelinePlacementPolicy {

  static final String SEED_KEY = "ozone.scm.simulation.pipeline.placement.seed";

  private final Random random;

  public SimPipelinePlacementPolicy(NodeManager nodeManager, PipelineStateManager stateManager,
      ConfigurationSource conf) {
    super(nodeManager, stateManager, conf);
    random = new Random(conf.getLong(SEED_KEY, 0));
  }

  @Override
  public Random getRand() {
    return random;
  }
}

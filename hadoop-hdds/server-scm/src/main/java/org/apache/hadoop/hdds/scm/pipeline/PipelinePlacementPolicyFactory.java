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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.node.NodeManager;

/**
 * Pipeline placement factor for pipeline providers to create placement instance
 * based on configuration property.
 * {@link ScmConfigKeys#OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY}
 */
public final class PipelinePlacementPolicyFactory {

  private PipelinePlacementPolicyFactory() {
  }

  public static PlacementPolicy getPolicy(NodeManager nodeManager,
      PipelineStateManager stateManager, ConfigurationSource conf) {
    final Class<? extends PlacementPolicy> clazz
        = conf.getClass(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        PipelinePlacementPolicy.class, PlacementPolicy.class);

    try {
      return clazz.getDeclaredConstructor(NodeManager.class,
              PipelineStateManager.class, ConfigurationSource.class)
          .newInstance(nodeManager, stateManager, conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to getPolicy for " + clazz, e);
    }
  }
}

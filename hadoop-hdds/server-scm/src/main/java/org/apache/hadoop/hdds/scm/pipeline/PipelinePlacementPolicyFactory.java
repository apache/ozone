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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY;

/**
 * Pipeline placement factor for pipeline providers to create placement instance
 * based on configuration property.
 * {@link ScmConfigKeys#OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY}
 */
public final class PipelinePlacementPolicyFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelinePlacementPolicyFactory.class);

  private static final Class<? extends PlacementPolicy>
      OZONE_SCM_CONTAINER_PLACEMENT_IMPL_DEFAULT =
      SCMContainerPlacementRackScatter.class;

  private PipelinePlacementPolicyFactory() {
  }

  public static PlacementPolicy getPolicy(
      ConfigurationSource conf, final NodeManager nodeManager,
      final PipelineStateManager stateManager,
      NetworkTopology clusterMap, final boolean fallback,
      SCMContainerPlacementMetrics metrics) throws SCMException {

    Class<? extends PlacementPolicy> placementClass =
        PipelinePlacementPolicy.class;
    Constructor<? extends PlacementPolicy> constructor;

    try {
      if (conf.get(OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY)
          .equals("org.apache.hadoop.hdds.scm.container.placement" +
              ".algorithms.SCMContainerPlacementRackScatter")) {
        placementClass = conf.getClass(ScmConfigKeys
                .OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
            OZONE_SCM_CONTAINER_PLACEMENT_IMPL_DEFAULT,
            PlacementPolicy.class);

        constructor = placementClass.getDeclaredConstructor(NodeManager.class,
            ConfigurationSource.class, NetworkTopology.class, boolean.class,
            SCMContainerPlacementMetrics.class);
        LOG.info("Create pipeline placement policy of type {}",
            placementClass.getCanonicalName());
        return (constructor.newInstance(nodeManager, conf, clusterMap,
            fallback, metrics));
      }
      constructor = placementClass.getDeclaredConstructor(NodeManager.class,
          PipelineStateManager.class, ConfigurationSource.class);
      LOG.info("Create pipeline placement policy of type {}",
          placementClass.getCanonicalName());
      return (constructor.newInstance(nodeManager, stateManager, conf));
    } catch (InstantiationException |
             IllegalAccessException | NoSuchMethodException |
             InvocationTargetException e) {
      throw new RuntimeException("Failed to create PipelinePlacementPolicy, "
          + placementClass.getName());
    }
  }
}

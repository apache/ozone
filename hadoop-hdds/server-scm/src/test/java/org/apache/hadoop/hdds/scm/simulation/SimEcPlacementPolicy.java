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

import java.util.List;
import java.util.Random;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;

/**
 * SCM's rack scatter placement of EC container replicas, with its random choices seeded from the configuration. Like
 * {@link SimContainerPlacementPolicy}, it tells the running simulation about placements that fail although enough
 * datanodes are eligible. SCM loads it through {@code ozone.scm.container.placement.ec.impl}.
 */
public class SimEcPlacementPolicy extends SCMContainerPlacementRackScatter {

  static final String SEED_KEY = "ozone.scm.simulation.ec.placement.seed";

  private final NodeManager nodeManager;
  private final Random random;

  public SimEcPlacementPolicy(NodeManager nodeManager, ConfigurationSource conf, NetworkTopology topology,
      boolean fallback, SCMContainerPlacementMetrics metrics) {
    super(nodeManager, conf, topology, fallback, metrics);
    this.nodeManager = nodeManager;
    this.random = new Random(conf.getLong(SEED_KEY, 0));
  }

  @Override
  public Random getRand() {
    return random;
  }

  @Override
  protected List<DatanodeDetails> chooseDatanodesInternal(List<DatanodeDetails> usedNodes,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes, int nodesRequired,
      long metadataSizeRequired, long dataSizeRequired) throws SCMException {
    try {
      return super.chooseDatanodesInternal(usedNodes, excludedNodes, favoredNodes, nodesRequired, metadataSizeRequired,
          dataSizeRequired);
    } catch (SCMException e) {
      ScmSimulation simulation = ScmSimulation.running();
      if (simulation != null) {
        simulation.placementFailed(SimContainerPlacementPolicy.eligible(nodeManager, usedNodes, excludedNodes),
            nodesRequired, e);
      }
      throw e;
    }
  }
}

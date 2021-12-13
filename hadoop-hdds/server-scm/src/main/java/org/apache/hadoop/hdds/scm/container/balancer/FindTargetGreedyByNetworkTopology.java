/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.LoggerFactory;


import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * an implementation of FindTargetGreedy, which will always select the
 * target with the shortest distance according to network topology
 * distance to the give source datanode.
 */
public class FindTargetGreedyByNetworkTopology
    extends AbstractFindTargetGreedy {

  private NetworkTopology networkTopology;
  private DatanodeDetails prevSource;
  private List potentialTargets;

  public FindTargetGreedyByNetworkTopology(
      ContainerManager containerManager,
      PlacementPolicy placementPolicy,
      NodeManager nodeManager,
      NetworkTopology networkTopology) {
    super(containerManager, placementPolicy, nodeManager);
    setLogger(LoggerFactory.getLogger(FindTargetGreedyByNetworkTopology.class));
    potentialTargets = new LinkedList<>();
    setPotentialTargets(potentialTargets);
    this.networkTopology = networkTopology;
    prevSource = null;
  }

  /**
   * sort potentialTargets for specified source datanode according to
   * network topology if enabled.
   * @param source the specified source datanode
   */
  protected void sortTargetForSource(DatanodeDetails source) {
    if (source.equals(prevSource)) {
      return;
    }
    Collections.sort(potentialTargets,
        (DatanodeUsageInfo da, DatanodeUsageInfo db) -> {
        DatanodeDetails a = da.getDatanodeDetails();
        DatanodeDetails b = db.getDatanodeDetails();
        // sort by network topology first
        int distanceToA = networkTopology.getDistanceCost(source, a);
        int distanceToB = networkTopology.getDistanceCost(source, b);
        if (distanceToA != distanceToB) {
          return distanceToA - distanceToB;
        }
        // if distance to source is equal , sort by usage
        return compareByUsage(da, db);
      });
  }
}

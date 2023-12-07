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

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;

/**
 * an implementation of FindTargetGreedy, which will always select the
 * target with the shortest distance according to network topology
 * distance to the give source datanode.
 */
class FindTargetGreedyByNetworkTopology extends AbstractFindTargetGreedy {
  private final NetworkTopology networkTopology;
  private final ArrayList<DatanodeUsageInfo> potentialTargets;

  FindTargetGreedyByNetworkTopology(
      @Nonnull StorageContainerManager scm
  ) {
    super(scm, FindTargetGreedyByNetworkTopology.class);
    networkTopology = scm.getClusterMap();
    potentialTargets = new ArrayList<>();
  }

  @Override
  public @Nonnull Collection<DatanodeUsageInfo> getPotentialTargets() {
    return potentialTargets;
  }

  /**
   * Sort potentialTargets for specified source datanode according to network
   * topology.
   *
   * @param source the specified source datanode
   */
  @VisibleForTesting
  public void sortTargetForSource(@Nonnull DatanodeDetails source) {
    potentialTargets.sort((DatanodeUsageInfo da, DatanodeUsageInfo db) -> {
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

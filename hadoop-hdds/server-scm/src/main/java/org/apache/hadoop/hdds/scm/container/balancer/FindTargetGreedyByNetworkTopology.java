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

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * an implementation of FindTargetGreedy, which will always select the
 * target with the shortest distance according to network topology
 * distance to the give source datanode.
 */
public class FindTargetGreedyByNetworkTopology
    extends AbstractFindTargetGreedy {
  private static final Logger LOG =
      LoggerFactory.getLogger(FindTargetGreedyByNetworkTopology.class);

  private NetworkTopology networkTopology;
  private List potentialTargets;

  public FindTargetGreedyByNetworkTopology(
      ContainerManager containerManager,
      PlacementPolicyValidateProxy placementPolicyValidateProxy,
      NodeManager nodeManager,
      NetworkTopology networkTopology) {
    super(containerManager, placementPolicyValidateProxy, nodeManager);
    setLogger(LOG);
    potentialTargets = new LinkedList<>();
    setPotentialTargets(potentialTargets);
    this.networkTopology = networkTopology;
  }

  /**
   * sort potentialTargets for specified source datanode according to
   * network topology.
   * @param source the specified source datanode
   */
  @Override
  @VisibleForTesting
  public void sortTargetForSource(DatanodeDetails source) {
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

  /**
   * Resets the collection of target datanode usage info that will be
   * considered for balancing. Gets the latest usage info from node manager.
   * @param targets collection of target {@link DatanodeDetails} that
   *                containers can move to
   */
  @Override
  public void resetPotentialTargets(
      @Nonnull Collection<DatanodeDetails> targets) {
    // create DatanodeUsageInfo from DatanodeDetails
    List<DatanodeUsageInfo> usageInfos = new ArrayList<>(targets.size());
    targets.forEach(datanodeDetails -> usageInfos.add(
        getNodeManager().getUsageInfo(datanodeDetails)));

    super.resetTargets(usageInfos);
  }

}

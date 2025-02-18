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

import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;

/**
 * This interface can be used to implement strategies to find a target for a
 * source.
 */
public interface FindTargetStrategy {

  /**
   * Finds target for a source for container move, given a collection of
   * potential target datanodes, a set of candidate containers, and a
   * functional interface with a method that returns true if a given size can
   * enter a potential target.
   *
   * @param source Datanode to find a target for
   * @param candidateContainer candidate containers satisfying
   *                            selection criteria
   *                            {@link ContainerBalancerSelectionCriteria}
   * (DatanodeDetails, Long) method returns true if the size specified in the
   * second argument can enter the specified DatanodeDetails node
   * @return {@link ContainerMoveSelection} containing the target node and
   * selected container
   */
  ContainerMoveSelection findTargetForContainerMove(
      DatanodeDetails source, ContainerID candidateContainer);

  /**
   * increase the Entering size of a candidate target data node.
   */
  void increaseSizeEntering(DatanodeDetails target, long size);

  /**
   * reInitialize FindTargetStrategy.
   */
  void reInitialize(List<DatanodeUsageInfo> potentialDataNodes,
                    ContainerBalancerConfiguration config, Double upperLimit);

  /**
   * Resets the collection of target {@link DatanodeUsageInfo} that can be
   * selected for balancing.
   * @param targets collection of target {@link DatanodeDetails}
   *               that containers can be moved to
   */
  void resetPotentialTargets(@Nonnull Collection<DatanodeDetails> targets);

  /**
   * Get a map of the node IDs and the corresponding data sizes moved to each node.
   * @return nodeId to size entering from node map
   */
  Map<DatanodeDetails, Long> getSizeEnteringNodes();

  /**
   * Clear the map of node IDs and their corresponding data sizes that were moved to each node.
   */
  void clearSizeEnteringNodes();
}

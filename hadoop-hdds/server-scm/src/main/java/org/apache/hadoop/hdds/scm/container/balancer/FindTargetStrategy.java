/**
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
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import java.util.Collection;
import java.util.Set;
import java.util.function.BiFunction;

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
   * @param potentialTargets Collection of potential target datanodes
   * @param candidateContainers Set of candidate containers satisfying
   *                            selection criteria
   *                            {@link ContainerBalancerSelectionCriteria}
   * @param canSizeEnterTarget A functional interface whose apply
   * (DatanodeDetails, Long) method returns true if the size specified in the
   * second argument can enter the specified DatanodeDetails node
   * @return {@link ContainerMoveSelection} containing the target node and
   * selected container
   */
  ContainerMoveSelection findTargetForContainerMove(
      DatanodeDetails source, Collection<DatanodeDetails> potentialTargets,
      Set<ContainerID> candidateContainers,
      BiFunction<DatanodeDetails, Long, Boolean> canSizeEnterTarget);

  /**
   * Checks whether moving the specified container from the specified source
   * to target datanode will satisfy the placement policy.
   *
   * @param containerID Container to be moved from source to target
   * @param replicas Set of replicas of the given container
   * @param source Source datanode for container move
   * @param target Target datanode for container move
   * @return true if placement policy is satisfied
   */
  boolean containerMoveSatisfiesPlacementPolicy(ContainerID containerID,
                                                Set<ContainerReplica> replicas,
                                                DatanodeDetails source,
                                                DatanodeDetails target);
}

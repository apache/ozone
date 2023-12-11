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

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerMoveSelection;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * This interface can be used to implement strategies to find a target for a source.
 */
interface FindTargetStrategy {

  /**
   * Finds target for a source for container move, given a collection of potential target datanodes,
   * a set of candidate containers, and a functional interface with a method that returns true
   * if a given size can enter a potential target.
   *
   * @param source                datanode to find a target for
   * @param containersToMove      set of candidate containers satisfying selection criteria
   *                              {@link ContainerSelectionCriteria}
   * @param maxSizeEnteringTarget the maximum size that can enter a target datanode in each iteration while balancing.
   * @param upperLimit            the value of upper limit for node utilization:
   *                              clusterAvgUtilisation + threshold
   * @return {@link ContainerMoveSelection} containing the target node and selected container
   */
  @Nullable ContainerMoveSelection findTargetForContainerMove(
      @Nonnull DatanodeDetails source,
      @Nonnull Set<ContainerID> containersToMove,
      long maxSizeEnteringTarget,
      double upperLimit
  );

  /**
   * increase the Entering size of a candidate target data node.
   */
  void increaseSizeEntering(@Nonnull DatanodeDetails target, long size, long maxSizeEnteringTarget);

  /**
   * reInitialize FindTargetStrategy.
   */
  void reInitialize(@Nonnull List<DatanodeUsageInfo> potentialDataNodes);

  /**
   * Resets the collection of target {@link DatanodeUsageInfo} that can be selected for balancing.
   *
   * @param targets collection of target {@link DatanodeDetails} that containers can be moved to
   */
  void resetPotentialTargets(@Nonnull Collection<DatanodeDetails> targets);
}

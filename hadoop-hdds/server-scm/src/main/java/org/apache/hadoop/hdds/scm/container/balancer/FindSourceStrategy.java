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
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;

/**
 * This interface can be used to implement strategies to get a
 * source datanode.
 */
public interface FindSourceStrategy {

  /**
   * get the next candidate source data node according to
   * the strategy.
   *
   * @return the nex candidate source data node.
   */
  DatanodeDetails getNextCandidateSourceDataNode();

  /**
   * remove the specified data node from candidate source
   * data nodes.
   */
  void removeCandidateSourceDataNode(DatanodeDetails dui);

  /**
   * add the specified data node to the candidate source
   * data nodes.
   * This method does not check whether the specified Datanode is already present in the Collection.
   * Callers must take the responsibility of checking and removing the Datanode before adding, if required.
   *
   * @param dn datanode to be added to potentialSources
   */
  void addBackSourceDataNode(DatanodeDetails dn);

  /**
   * increase the Leaving size of a candidate source data node.
   */
  void increaseSizeLeaving(DatanodeDetails dui, long size);

  /**
   * Checks if specified size can leave a specified source datanode
   * according to {@link ContainerBalancerConfiguration}
   * "size.entering.target.max".
   *
   * @param source target datanode in which size is entering
   * @param size   size in bytes
   * @return true if size can leave, else false
   */
  boolean canSizeLeaveSource(DatanodeDetails source, long size);

  /**
   * reInitialize FindSourceStrategy.
   */
  void reInitialize(List<DatanodeUsageInfo> potentialDataNodes,
                    ContainerBalancerConfiguration config, Double lowerLimit);

  /**
   * Resets the collection of source {@link DatanodeUsageInfo} that can be
   * selected for balancing.
   *
   * @param sources collection of source
   *                {@link DatanodeDetails} that containers can move from
   */
  void resetPotentialSources(@Nonnull Collection<DatanodeDetails> sources);

  /**
   * Get a map of the node IDs and the corresponding data sizes moved from each node.
   * @return nodeId to size leaving from node map
   */
  Map<DatanodeDetails, Long> getSizeLeavingNodes();

  /**
   * Clear the map of node IDs and their corresponding data sizes that were moved from each node.
   */
  void clearSizeLeavingNodes();
}

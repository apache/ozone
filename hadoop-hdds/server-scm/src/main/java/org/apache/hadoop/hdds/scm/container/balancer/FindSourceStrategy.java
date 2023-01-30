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
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

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
}

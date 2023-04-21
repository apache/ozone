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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Utility class for ReplicationManager.
 */
public final class ReplicationManagerUtil {

  private ReplicationManagerUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      ReplicationManagerUtil.class);

  /**
   * Using the passed placement policy attempt to select a list of datanodes to
   * use as new targets. If the placement policy is unable to select enough
   * nodes, the number of nodes requested will be reduced by 1 and the placement
   * policy will be called again. This will continue until the placement policy
   * is able to select enough nodes or the number of nodes requested is reduced
   * to zero when an exception will be thrown.
   * @param policy The placement policy to use to select nodes.
   * @param requiredNodes The number of nodes required
   * @param usedNodes Any nodes already used by the container
   * @param excludedNodes Any Excluded nodes which cannot be selected
   * @param defaultContainerSize The cluster default max container size
   * @param container The container to select new replicas for
   * @return A list of up to requiredNodes datanodes to use as targets for new
   *         replicas. Note the number of nodes returned may be less than the
   *         number of nodes requested if the placement policy is unable to
   *         return enough nodes.
   * @throws SCMException If no nodes can be selected.
   */
  public static List<DatanodeDetails> getTargetDatanodes(PlacementPolicy policy,
      int requiredNodes, List<DatanodeDetails> usedNodes,
      List<DatanodeDetails> excludedNodes, long defaultContainerSize,
      ContainerInfo container) throws SCMException {

    // Ensure that target datanodes have enough space to hold a complete
    // container.
    final long dataSizeRequired =
        Math.max(container.getUsedBytes(), defaultContainerSize);

    int mutableRequiredNodes = requiredNodes;
    while (mutableRequiredNodes > 0) {
      try {
        if (usedNodes == null) {
          return policy.chooseDatanodes(excludedNodes, null,
              mutableRequiredNodes, 0, dataSizeRequired);
        } else {
          return policy.chooseDatanodes(usedNodes, excludedNodes, null,
              mutableRequiredNodes, 0, dataSizeRequired);
        }
      } catch (IOException e) {
        LOG.debug("Placement policy was not able to return {} nodes for " +
            "container {}.",
            mutableRequiredNodes, container.getContainerID(), e);
        mutableRequiredNodes--;
      }
    }
    throw new SCMException(String.format("Placement Policy: %s did not return"
            + " any nodes. Number of required Nodes %d, Datasize Required: %d",
        policy.getClass(), requiredNodes, dataSizeRequired),
        SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
  }

}

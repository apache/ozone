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

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container placement policy that randomly chooses healthy datanodes.
 * This is very similar to current HDFS placement. That is we
 * just randomly place containers without any considerations of utilization.
 * <p>
 * That means we rely on balancer to achieve even distribution of data.
 * Balancer will need to support containers as a feature before this class
 * can be practically used.
 */
public final class SCMContainerPlacementRandom extends SCMCommonPlacementPolicy
    implements PlacementPolicy {
  @VisibleForTesting
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerPlacementRandom.class);

  private final SCMContainerPlacementMetrics metrics;

  /**
   * Construct a random Block Placement policy.
   *
   * @param nodeManager nodeManager
   * @param conf Config
   */
  public SCMContainerPlacementRandom(final NodeManager nodeManager,
      final ConfigurationSource conf, final NetworkTopology networkTopology,
      final boolean fallback, final SCMContainerPlacementMetrics metrics) {
    super(nodeManager, conf);
    this.metrics = metrics;
  }

  /**
   * Choose datanodes called by the SCM to choose the datanode.
   *
   * @param usedNodes - list of the datanodes to already chosen in the
   *                      pipeline.
   * @param excludedNodes - list of the datanodes to exclude.
   * @param favoredNodes - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return List of Datanodes.
   * @throws SCMException  SCMException
   */
  @Override
  protected List<DatanodeDetails> chooseDatanodesInternal(
          List<DatanodeDetails> usedNodes,
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes, final int nodesRequired,
          long metadataSizeRequired, long dataSizeRequired)
          throws SCMException {
    metrics.incrDatanodeRequestCount(nodesRequired);
    List<DatanodeDetails> healthyNodes =
        super.chooseDatanodesInternal(usedNodes, excludedNodes, favoredNodes,
                nodesRequired, metadataSizeRequired, dataSizeRequired);

    if (healthyNodes.size() == nodesRequired) {
      return healthyNodes;
    }
    return getResultSet(nodesRequired, healthyNodes);
  }

  /**
   * Just chose a node randomly and remove it from the set of nodes we can
   * chose from.
   *
   * @param healthyNodes - all healthy datanodes.
   * @return one randomly chosen datanode that from two randomly chosen datanode
   */
  @Override
  public DatanodeDetails chooseNode(final List<DatanodeDetails> healthyNodes) {
    metrics.incrDatanodeChooseAttemptCount();
    DatanodeDetails selectedNode =
        healthyNodes.get(getRand().nextInt(healthyNodes.size()));
    healthyNodes.remove(selectedNode);
    metrics.incrDatanodeChooseSuccessCount();
    return selectedNode;
  }
}

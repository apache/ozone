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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The selection criteria for selecting source datanodes , the containers of
 * which will be moved out.
 */
public class FindSourceGreedy implements FindSourceStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(FindSourceGreedy.class);
  private Map<DatanodeDetails, Long> sizeLeavingNode;
  private PriorityQueue<DatanodeUsageInfo> potentialSources;
  private NodeManager nodeManager;
  private ContainerBalancerConfiguration config;
  private Double lowerLimit;

  FindSourceGreedy(NodeManager nodeManager) {
    sizeLeavingNode = new ConcurrentHashMap<>();
    potentialSources = new PriorityQueue<>((a, b) -> {
      double currentUsageOfA = a.calculateUtilization(
          -sizeLeavingNode.get(a.getDatanodeDetails()));
      double currentUsageOfB = b.calculateUtilization(
          -sizeLeavingNode.get(b.getDatanodeDetails()));
      //in descending order
      int ret = Double.compare(currentUsageOfB, currentUsageOfA);
      if (ret != 0) {
        return ret;
      }
      return a.getDatanodeID().compareTo(b.getDatanodeID());
    });
    this.nodeManager = nodeManager;
  }

  private void setLowerLimit(Double lowerLimit) {
    this.lowerLimit = lowerLimit;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetPotentialSources(
      @Nonnull Collection<DatanodeDetails> sources) {
    List<DatanodeUsageInfo> usageInfos = new ArrayList<>(sources.size());
    sources.forEach(source -> usageInfos.add(nodeManager.getUsageInfo(source)));
    resetSources(usageInfos);
  }

  /**
   * Resets the collection of source datanodes that are considered when
   * selecting the next source datanode.
   * @param sources potential sources
   */
  private void resetSources(Collection<DatanodeUsageInfo> sources) {
    potentialSources.clear();
    /* since sizeLeavingNode map is being used to track how much data has
    left a DN in an iteration, put keys only if they don't already exist
     */
    sources.forEach(source -> {
      sizeLeavingNode.putIfAbsent(source.getDatanodeDetails(), 0L);
      potentialSources.add(source);
    });
  }

  private void setConfiguration(ContainerBalancerConfiguration conf) {
    this.config = conf;
  }

  /**
   * increase the Leaving size of a candidate source data node.
   */
  @Override
  public void increaseSizeLeaving(DatanodeDetails dui, long size) {
    Long currentSize = sizeLeavingNode.get(dui);
    if (currentSize != null) {
      sizeLeavingNode.put(dui, currentSize + size);
      //reorder according to the latest sizeLeavingNode
      addBackSourceDataNode(dui);
      return;
    }
    LOG.warn("Cannot find datanode {} in candidate source datanodes", dui);
  }

  /**
   * get the next candidate source data node according to
   * the strategy.
   *
   * @return the nex candidate source data node.
   */
  @Override
  public DatanodeDetails getNextCandidateSourceDataNode() {
    if (potentialSources.isEmpty()) {
      LOG.info("no more candidate source data node");
      return null;
    }
    return potentialSources.poll().getDatanodeDetails();
  }

  /**
   * remove the specified data node from candidate source
   * data nodes.
   */
  @Override
  public void removeCandidateSourceDataNode(DatanodeDetails dui) {
    potentialSources.removeIf(a -> a.getDatanodeDetails().equals(dui));
  }

  @Override
  public void addBackSourceDataNode(DatanodeDetails dn) {
    DatanodeUsageInfo dui = nodeManager.getUsageInfo(dn);
    potentialSources.add(dui);
  }

  /**
   * Checks if specified size can leave a specified target datanode
   * according to {@link ContainerBalancerConfiguration}
   * "size.entering.target.max".
   *
   * @param source target datanode in which size is entering
   * @param size   size in bytes
   * @return true if size can leave, else false
   */
  @Override
  public boolean canSizeLeaveSource(DatanodeDetails source, long size) {
    if (sizeLeavingNode.containsKey(source)) {
      long sizeLeavingAfterMove = sizeLeavingNode.get(source) + size;
      //size can be moved out of source datanode only when the following
      //three conditions are met.
      //1 size should be greater than zero bytes
      //2 sizeLeavingAfterMove does not succeed the configured
      // MaxSizeLeavingTarget
      //3 after subtracting sizeLeavingAfterMove, the usage is bigger
      // than or equal to lowerLimit
      if (size <= 0) {
        LOG.debug("{} bytes container cannot leave datanode {}", size, source);
        return false;
      }
      if (sizeLeavingAfterMove > config.getMaxSizeLeavingSource()) {
        LOG.debug("{} bytes cannot leave datanode {} because 'size.leaving" +
                ".source.max' limit is {} and {} bytes have already left.",
            size, source, config.getMaxSizeLeavingSource(),
            sizeLeavingNode.get(source));
        return false;
      }
      if (Double.compare(nodeManager.getUsageInfo(source)
          .calculateUtilization(-sizeLeavingAfterMove), lowerLimit) < 0) {
        LOG.debug("{} bytes cannot leave datanode {} because its utilization " +
                "will drop below the lower limit of {}.", size, source, lowerLimit);
        return false;
      }
      return true;
    }

    LOG.warn("No record of how much size has left datanode {}", source);
    return false;
  }

  /**
   * reInitialize FindSourceStrategy.
   */
  @Override
  public void reInitialize(List<DatanodeUsageInfo> potentialDataNodes,
                           ContainerBalancerConfiguration conf,
                           Double lowLimit) {
    setConfiguration(conf);
    setLowerLimit(lowLimit);
    sizeLeavingNode.clear();
    resetSources(potentialDataNodes);
  }

  @Override
  public Map<DatanodeDetails, Long> getSizeLeavingNodes() {
    return sizeLeavingNode;
  }

  @Override
  public void clearSizeLeavingNodes() {
    sizeLeavingNode.clear();
  }
}

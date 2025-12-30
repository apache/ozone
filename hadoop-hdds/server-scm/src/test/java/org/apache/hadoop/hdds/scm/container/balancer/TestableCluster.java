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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class is used for creating test cluster with a required number of datanodes.
 * 1. Fill the cluster by generating some data.
 * 2. Nodes in the cluster have utilization values determined by generateUtilization method.
 */
public final class TestableCluster {
  static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
  private static final Logger LOG = LoggerFactory.getLogger(TestableCluster.class);
  private final int nodeCount;
  private final double[] nodeUtilizationList;
  private final DatanodeUsageInfo[] nodesInCluster;
  private final Map<ContainerID, ContainerInfo> cidToInfoMap = new HashMap<>();
  private final Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap = new HashMap<>();
  private final Map<DatanodeUsageInfo, Set<ContainerID>> dnUsageToContainersMap = new HashMap<>();
  private final double averageUtilization;

  TestableCluster(int numberOfNodes, long storageUnit) {
    nodeCount = numberOfNodes;
    nodeUtilizationList = createUtilizationList(nodeCount);
    nodesInCluster = new DatanodeUsageInfo[nodeCount];

    generateData(storageUnit);
    createReplicasForContainers();
    long clusterCapacity = 0, clusterUsedSpace = 0;

    // For each node utilization, calculate that datanode's used space and capacity.
    for (int i = 0; i < nodeUtilizationList.length; i++) {
      Set<ContainerID> containerIDSet = dnUsageToContainersMap.get(nodesInCluster[i]);
      long datanodeUsedSpace = 0;
      for (ContainerID containerID : containerIDSet) {
        datanodeUsedSpace += cidToInfoMap.get(containerID).getUsedBytes();
      }
      // Use node utilization and used space to determine node capacity.
      long datanodeCapacity = (nodeUtilizationList[i] == 0)
          ? storageUnit * RANDOM.nextInt(10, 60)
          : (long) (datanodeUsedSpace / nodeUtilizationList[i]);

      SCMNodeStat stat = new SCMNodeStat(datanodeCapacity, datanodeUsedSpace,
          datanodeCapacity - datanodeUsedSpace, 0,
          datanodeCapacity - datanodeUsedSpace - 1, 0);
      nodesInCluster[i].setScmNodeStat(stat);
      clusterUsedSpace += datanodeUsedSpace;
      clusterCapacity += datanodeCapacity;
    }

    averageUtilization = (double) clusterUsedSpace / clusterCapacity;
  }

  @Override
  public String toString() {
    return "cluster of " + nodeCount + " nodes";
  }

  @Nonnull Map<DatanodeUsageInfo, Set<ContainerID>> getDatanodeToContainersMap() {
    return dnUsageToContainersMap;
  }

  @Nonnull Map<ContainerID, ContainerInfo> getCidToInfoMap() {
    return cidToInfoMap;
  }

  int getNodeCount() {
    return nodeCount;
  }

  double getAverageUtilization() {
    return averageUtilization;
  }

  @Nonnull DatanodeUsageInfo[] getNodesInCluster() {
    return nodesInCluster;
  }

  double[] getNodeUtilizationList() {
    return nodeUtilizationList;
  }

  @Nonnull Map<ContainerID, Set<ContainerReplica>> getCidToReplicasMap() {
    return cidToReplicasMap;
  }

  /**
   * Determines unBalanced nodes, that is, over and under utilized nodes,
   * according to the generated utilization values for nodes and the threshold.
   *
   * @param threshold a percentage in the range 0 to 100
   * @return list of DatanodeUsageInfo containing the expected(correct) unBalanced nodes.
   */
  @Nonnull List<DatanodeUsageInfo> getUnBalancedNodes(double threshold) {
    threshold /= 100;
    double lowerLimit = averageUtilization - threshold;
    double upperLimit = averageUtilization + threshold;

    // Use node utilization to determine over and under utilized nodes.
    List<DatanodeUsageInfo> expectedUnBalancedNodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      if (nodeUtilizationList[nodeCount - i - 1] > upperLimit) {
        expectedUnBalancedNodes.add(nodesInCluster[nodeCount - i - 1]);
      }
    }
    for (int i = 0; i < nodeCount; i++) {
      if (nodeUtilizationList[i] < lowerLimit) {
        expectedUnBalancedNodes.add(nodesInCluster[i]);
      }
    }
    return expectedUnBalancedNodes;
  }

  /**
   * Create some datanodes and containers for each node.
   */
  private void generateData(long storageUnit) {
    // Create datanodes and add containers to them.
    for (int i = 0; i < nodeCount; i++) {
      DatanodeUsageInfo usageInfo =
          new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(), new SCMNodeStat());
      nodesInCluster[i] = usageInfo;

      // Create containers with varying used space.
      Set<ContainerID> containerIDSet = new HashSet<>();
      int sizeMultiple = 0;
      for (int j = 0; j < i; j++) {
        sizeMultiple %= 5;
        sizeMultiple++;
        ContainerInfo container = createContainer((long) i * i + j, storageUnit * sizeMultiple);

        cidToInfoMap.put(container.containerID(), container);
        containerIDSet.add(container.containerID());

        // Create initial replica for this container and add it.
        Set<ContainerReplica> containerReplicaSet = new HashSet<>();
        containerReplicaSet.add(
            createReplica(container.containerID(), usageInfo.getDatanodeDetails(), container.getUsedBytes()));
        cidToReplicasMap.put(container.containerID(), containerReplicaSet);
      }
      dnUsageToContainersMap.put(usageInfo, containerIDSet);
    }
  }

  private @Nonnull ContainerInfo createContainer(long id, long usedBytes) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder()
        .setContainerID(id)
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setOwner("TestContainerBalancer")
        .setUsedBytes(usedBytes);

    // Make it a RATIS container if id is even, else make it an EC container.
    ReplicationConfig config = (id % 2 == 0)
        ? RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE)
        : new ECReplicationConfig(3, 2);

    builder.setReplicationConfig(config);
    return builder.build();
  }

  /**
   * Create the required number of replicas for each container. Note that one replica already exists and
   * nodes with utilization value 0 should not have any replicas.
   */
  private void createReplicasForContainers() {
    for (ContainerInfo container : cidToInfoMap.values()) {
      // One replica already exists; create the remaining ones.
      ReplicationConfig replicationConfig = container.getReplicationConfig();
      ContainerID key = container.containerID();
      for (int i = 0; i < replicationConfig.getRequiredNodes() - 1; i++) {
        // Randomly pick a datanode for this replica.
        int dnIndex = RANDOM.nextInt(0, nodeCount);
        // Don't put replicas in DNs that are supposed to have 0 utilization.
        if (Math.abs(nodeUtilizationList[dnIndex] - 0.0d) > 0.00001) {
          DatanodeDetails node = nodesInCluster[dnIndex].getDatanodeDetails();
          Set<ContainerReplica> replicas = cidToReplicasMap.get(key);
          replicas.add(createReplica(key, node, container.getUsedBytes()));
          cidToReplicasMap.put(key, replicas);
          dnUsageToContainersMap.get(nodesInCluster[dnIndex]).add(key);
        }
      }
    }
  }

  /**
   * Generates a range of equally spaced utilization(that is, used / capacity) values from 0 to 1.
   *
   * @param count Number of values to generate. Count must be greater than or equal to 1.
   * @return double array of node utilization values
   * @throws IllegalArgumentException If the value of the parameter count is less than 1.
   */
  private static double[] createUtilizationList(int count) throws IllegalArgumentException {
    if (count < 1) {
      LOG.warn("The value of argument count is {}. However, count must be greater than 0.", count);
      throw new IllegalArgumentException();
    }
    double[] result = new double[count];
    for (int i = 0; i < count; i++) {
      result[i] = (i / (double) count);
    }
    return result;
  }

  private @Nonnull ContainerReplica createReplica(
      @Nonnull ContainerID containerID,
      @Nonnull DatanodeDetails datanodeDetails,
      long usedBytes
  ) {
    return ContainerReplica.newBuilder()
        .setContainerID(containerID)
        .setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(datanodeDetails.getID())
        .setSequenceId(1000L)
        .setBytesUsed(usedBytes)
        .build();
  }
}

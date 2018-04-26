/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.testutils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodePoolManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerInfo;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .HEALTHY;

/**
 * This class  manages the state of datanode
 * in conjunction with the node pool and node managers.
 */
public class ReplicationDatanodeStateManager {
  private final NodeManager nodeManager;
  private final NodePoolManager poolManager;
  private final Random r;

  /**
   * The datanode state Manager.
   *
   * @param nodeManager
   * @param poolManager
   */
  public ReplicationDatanodeStateManager(NodeManager nodeManager,
      NodePoolManager poolManager) {
    this.nodeManager = nodeManager;
    this.poolManager = poolManager;
    r = new Random();
  }

  /**
   * Get Container Report as if it is from a datanode in the cluster.
   * @param containerName - Container Name.
   * @param poolName - Pool Name.
   * @param dataNodeCount - Datanode Count.
   * @return List of Container Reports.
   */
  public List<ContainerReportsRequestProto> getContainerReport(
      String containerName, String poolName, int dataNodeCount) {
    List<ContainerReportsRequestProto> containerList = new LinkedList<>();
    List<DatanodeDetails> nodesInPool = poolManager.getNodes(poolName);

    if (nodesInPool == null) {
      return containerList;
    }

    if (nodesInPool.size() < dataNodeCount) {
      throw new IllegalStateException("Not enough datanodes to create " +
          "required container reports");
    }

    int containerID = 1;
    while (containerList.size() < dataNodeCount && nodesInPool.size() > 0) {
      DatanodeDetails id = nodesInPool.get(r.nextInt(nodesInPool.size()));
      nodesInPool.remove(id);
      containerID++;
      // We return container reports only for nodes that are healthy.
      if (nodeManager.getNodeState(id) == HEALTHY) {
        ContainerInfo info = ContainerInfo.newBuilder()
            .setContainerName(containerName)
            .setFinalhash(DigestUtils.sha256Hex(containerName))
            .setContainerID(containerID)
            .build();
        ContainerReportsRequestProto containerReport =
            ContainerReportsRequestProto.newBuilder().addReports(info)
            .setDatanodeDetails(id.getProtoBufMessage())
            .setType(ContainerReportsRequestProto.reportType.fullReport)
            .build();
        containerList.add(containerReport);
      }
    }
    return containerList;
  }
}

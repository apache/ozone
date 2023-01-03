/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Handles the Ratis mis replication processing and forming the respective SCM
 * commands.
 */
public class RatisMisReplicationHandler extends MisReplicationHandler {

  public RatisMisReplicationHandler(
          PlacementPolicy<ContainerReplica> containerPlacement,
          ConfigurationSource conf, NodeManager nodeManager) {
    super(containerPlacement, conf, nodeManager);
  }

  @Override
  protected ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo containerInfo, Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> pendingOps, int minHealthyForMaintenance)
      throws IOException {
    if (containerInfo.getReplicationType() !=
            HddsProtos.ReplicationType.RATIS) {
      throw new IOException(String.format("Invalid Container Replication Type :"
              + " %s.Expected Container Replication Type : RATIS",
              containerInfo.getReplicationType().toString()));
    }
    // count pending adds and deletes
    int pendingAdd = 0, pendingDelete = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdd++;
      } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      }
    }
    return new RatisContainerReplicaCount(
            containerInfo, replicas, pendingAdd,
            pendingDelete, containerInfo.getReplicationFactor().getNumber(),
            minHealthyForMaintenance);
  }

  @Override
  protected ReplicateContainerCommand getReplicateCommand(
          ContainerInfo containerInfo, ContainerReplica replica) {
    return new ReplicateContainerCommand(containerInfo.getContainerID(),
            Collections.singletonList(replica.getDatanodeDetails()));
  }
}

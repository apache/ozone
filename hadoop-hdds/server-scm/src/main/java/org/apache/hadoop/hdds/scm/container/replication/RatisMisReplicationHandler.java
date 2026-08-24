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

package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * Handles the Ratis mis replication processing and forming the respective SCM
 * commands.
 */
public class RatisMisReplicationHandler extends MisReplicationHandler {

  public RatisMisReplicationHandler(
          PlacementPolicy containerPlacement,
          ConfigurationSource conf, ReplicationManager replicationManager) {
    super(containerPlacement, conf, replicationManager);
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
    return new RatisContainerReplicaCount(containerInfo, replicas, pendingOps,
        minHealthyForMaintenance, true);
  }

  @Override
  protected int sendReplicateCommands(
      ContainerInfo containerInfo,
      Set<ContainerReplica> replicasToBeReplicated,
      List<DatanodeDetails> sources, List<DatanodeDetails> targetDns)
      throws CommandTargetOverloadedException, NotLeaderException {
    ReplicationManager replicationManager = getReplicationManager();
    long containerID = containerInfo.getContainerID();

    int commandsSent = 0;
    for (DatanodeDetails target : targetDns) {
      if (replicationManager.getConfig().isPush()) {
        replicationManager.sendThrottledReplicationCommand(containerInfo,
            sources, target, 0);
      } else {
        ReplicateContainerCommand cmd = ReplicateContainerCommand
            .fromSources(containerID, sources);
        replicationManager.sendDatanodeCommand(cmd, containerInfo, target);
      }
      commandsSent++;
    }

    return commandsSent;
  }
}

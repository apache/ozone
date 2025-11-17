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
import java.util.Collections;
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
 * Handles the EC Mis replication processing and forming the respective SCM
 * commands.
 */
public class ECMisReplicationHandler extends MisReplicationHandler {
  public ECMisReplicationHandler(
          PlacementPolicy containerPlacement,
          ConfigurationSource conf, ReplicationManager replicationManager) {
    super(containerPlacement, conf, replicationManager);
  }

  @Override
  protected ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo containerInfo, Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> pendingOps, int remainingMaintenanceRedundancy)
      throws IOException {
    if (containerInfo.getReplicationType() != HddsProtos.ReplicationType.EC) {
      throw new IOException(String.format("Invalid Container Replication Type :"
                      + " %s.Expected Container Replication Type : EC",
              containerInfo.getReplicationType().toString()));
    }
    return new ECContainerReplicaCount(containerInfo, replicas, pendingOps,
            remainingMaintenanceRedundancy);
  }

  @Override
  protected int sendReplicateCommands(
      ContainerInfo containerInfo,
      Set<ContainerReplica> replicasToBeReplicated,
      List<DatanodeDetails> sources, List<DatanodeDetails> targetDns)
      throws CommandTargetOverloadedException, NotLeaderException {
    ReplicationManager replicationManager = getReplicationManager();
    int commandsSent = 0;
    int datanodeIdx = 0;
    CommandTargetOverloadedException overloadedException = null;
    for (ContainerReplica replica : replicasToBeReplicated) {
      if (datanodeIdx == targetDns.size()) {
        break;
      }
      long containerID = containerInfo.getContainerID();
      DatanodeDetails source = replica.getDatanodeDetails();
      DatanodeDetails target = targetDns.get(datanodeIdx);
      try {
        if (replicationManager.getConfig().isPush()) {
          replicationManager.sendThrottledReplicationCommand(containerInfo,
              Collections.singletonList(source), target,
              replica.getReplicaIndex());
        } else {
          ReplicateContainerCommand cmd = ReplicateContainerCommand
              .fromSources(containerID, Collections.singletonList(source));
          // For EC containers, we need to track the replica index which is
          // to be replicated, so add it to the command.
          cmd.setReplicaIndex(replica.getReplicaIndex());
          replicationManager.sendDatanodeCommand(cmd, containerInfo, target);
        }
        commandsSent++;
      } catch (CommandTargetOverloadedException e) {
        LOG.debug("Unable to replicate container {} and index {} from {} to {}"
                + " because the source is overloaded",
            containerID, replica.getReplicaIndex(), source, target);
        overloadedException = e;
      }
      datanodeIdx += 1;
    }
    if (overloadedException != null) {
      throw overloadedException;
    }
    return commandsSent;
  }
}

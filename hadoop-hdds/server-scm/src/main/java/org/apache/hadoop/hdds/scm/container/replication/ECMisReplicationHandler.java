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
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Handles the EC Mis replication processing and forming the respective SCM
 * commands.
 */
public class ECMisReplicationHandler extends MisReplicationHandler {
  public ECMisReplicationHandler(
          PlacementPolicy<ContainerReplica> containerPlacement,
          ConfigurationSource conf, ReplicationManager replicationManager,
          boolean push) {
    super(containerPlacement, conf, replicationManager, push);
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
  protected ReplicateContainerCommand updateReplicateCommand(
          ReplicateContainerCommand command, ContainerReplica replica) {
    // For EC containers, we need to track the replica index which is
    // to be replicated, so add it to the command.
    command.setReplicaIndex(replica.getReplicaIndex());
    return command;
  }


}

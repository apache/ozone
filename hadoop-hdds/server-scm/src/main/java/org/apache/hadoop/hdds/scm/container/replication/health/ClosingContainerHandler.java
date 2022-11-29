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

package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;

/**
 * Class used in Replication Manager to close replicas of CLOSING containers.
 */
public class ClosingContainerHandler extends AbstractCheck {
  private final ReplicationManager replicationManager;

  public ClosingContainerHandler(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * If the container is in CLOSING state, send close commands to replicas
   * that are not UNHEALTHY.
   * @param request ContainerCheckRequest object representing the container
   * @return false if the specified container is not CLOSING, otherwise true
   * @see
   * <a href="https://issues.apache.org/jira/browse/HDDS-5708">HDDS-5708</a>
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();

    if (containerInfo.getState() != HddsProtos.LifeCycleState.CLOSING) {
      return false;
    }

    boolean forceClose = request.getContainerInfo().getReplicationConfig()
        .getReplicationType() != HddsProtos.ReplicationType.RATIS;

    for (ContainerReplica replica : request.getContainerReplicas()) {
      if (replica.getState() != ContainerReplicaProto.State.UNHEALTHY) {
        replicationManager.sendCloseContainerReplicaCommand(
            containerInfo, replica.getDatanodeDetails(), forceClose);
      }
    }
    return true;
  }
}
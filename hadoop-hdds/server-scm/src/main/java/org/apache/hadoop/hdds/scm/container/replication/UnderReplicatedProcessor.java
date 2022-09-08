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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Class used to pick messages from the ReplicationManager under replicated
 * queue, calculate the reconstruction commands and assign to the datanodes
 * via the eventQueue.
 */
public class UnderReplicatedProcessor {

  private static final Logger LOG = LoggerFactory
      .getLogger(UnderReplicatedProcessor.class);
  private final ReplicationManager replicationManager;
  private final ContainerReplicaPendingOps pendingOps;
  private final EventPublisher eventPublisher;

  public UnderReplicatedProcessor(ReplicationManager replicationManager,
      ContainerReplicaPendingOps pendingOps,
      EventPublisher eventPublisher) {
    this.replicationManager = replicationManager;
    this.pendingOps = pendingOps;
    this.eventPublisher = eventPublisher;
  }

  /**
   * Read messages from the ReplicationManager under replicated queue and,
   * form commands to correct the under replication. The commands are added
   * to the event queue and the PendingReplicaOps are adjusted.
   *
   * Note: this is a temporary implementation of this feature. A future
   * version will need to limit the amount of messages assigned to each
   * datanode, so they are not assigned too much work.
   */
  public void processAll() {
    int processed = 0;
    int failed = 0;
    while (true) {
      ContainerHealthResult.UnderReplicatedHealthResult underRep =
          replicationManager.dequeueUnderReplicatedContainer();
      if (underRep == null) {
        break;
      }
      try {
        processContainer(underRep);
        processed++;
      } catch (Exception e) {
        LOG.error("Error processing under replicated container {}",
            underRep.getContainerInfo(), e);
        failed++;
        replicationManager.requeueUnderReplicatedContainer(underRep);
      }
    }
    LOG.info("Processed {} under replicated containers, failed processing {}",
        processed, failed);
  }

  protected void processContainer(ContainerHealthResult
      .UnderReplicatedHealthResult underRep) throws IOException {
    Map<DatanodeDetails, SCMCommand<?>> cmds = replicationManager
        .processUnderReplicatedContainer(underRep);
    for (Map.Entry<DatanodeDetails, SCMCommand<?>> cmd : cmds.entrySet()) {
      SCMCommand<?> scmCmd = cmd.getValue();
      scmCmd.setTerm(replicationManager.getScmTerm());
      final CommandForDatanode<?> datanodeCommand =
          new CommandForDatanode<>(cmd.getKey().getUuid(), scmCmd);
      eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
      adjustPendingOps(underRep.getContainerInfo().containerID(),
          scmCmd, cmd.getKey());
    }
  }

  private void adjustPendingOps(ContainerID containerID, SCMCommand<?> cmd,
      DatanodeDetails targetDatanode)
      throws IOException {
    if (cmd.getType() == StorageContainerDatanodeProtocolProtos
        .SCMCommandProto.Type.reconstructECContainersCommand) {
      ReconstructECContainersCommand rcc = (ReconstructECContainersCommand) cmd;
      List<DatanodeDetails> targets = rcc.getTargetDatanodes();
      byte[] targetIndexes = rcc.getMissingContainerIndexes();
      for (int i = 0; i < targetIndexes.length; i++) {
        pendingOps.scheduleAddReplica(containerID, targets.get(i),
            targetIndexes[i]);
      }
    } else if (cmd.getType() == StorageContainerDatanodeProtocolProtos
        .SCMCommandProto.Type.replicateContainerCommand) {
      ReplicateContainerCommand rcc = (ReplicateContainerCommand) cmd;
      pendingOps.scheduleAddReplica(
          containerID, targetDatanode, rcc.getReplicaIndex());
    } else {
      throw new IOException("Unexpected command type " + cmd.getType());
    }
  }
}

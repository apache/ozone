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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ContainerReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Command handler to push containers to a target datanode.
 */
public class ReplicateContainerCommandHandler implements CommandHandler {

  private ReplicationSupervisor supervisor;

  private ContainerReplicator pushReplicator;

  private static final String METRIC_NAME = ReplicationTask.METRIC_NAME;

  public ReplicateContainerCommandHandler(ReplicationSupervisor supervisor, ContainerReplicator pushReplicator) {
    this.supervisor = supervisor;
    this.pushReplicator = pushReplicator;
  }

  public String getMetricsName() {
    return METRIC_NAME;
  }

  @Override
  public void handle(SCMCommand<?> command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {

    final ReplicateContainerCommand replicateCommand =
        (ReplicateContainerCommand) command;
    final long containerID = replicateCommand.getContainerID();

    Preconditions.checkArgument(replicateCommand.getTargetDatanode() != null,
        "Replication command received for container %s without a target datanode.",
        containerID);

    ReplicationTask task = new ReplicationTask(replicateCommand, pushReplicator);
    supervisor.addTask(task);
  }

  @Override
  public int getQueuedCount() {
    return (int) this.supervisor.getReplicationQueuedCount(METRIC_NAME);
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return Type.replicateContainerCommand;
  }

  @Override
  public int getInvocationCount() {
    return (int) this.supervisor.getReplicationRequestCount(METRIC_NAME);
  }

  @Override
  public long getAverageRunTime() {
    return this.supervisor.getReplicationRequestAvgTime(METRIC_NAME);
  }

  @Override
  public long getTotalRunTime() {
    return this.supervisor.getReplicationRequestTotalTime(METRIC_NAME);
  }
}

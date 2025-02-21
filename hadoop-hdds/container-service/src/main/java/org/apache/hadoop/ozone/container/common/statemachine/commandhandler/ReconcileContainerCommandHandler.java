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

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.checksum.ReconcileContainerTask;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Handles commands from SCM to reconcile a container replica on this datanode with the replicas on its peers.
 */
public class ReconcileContainerCommandHandler implements CommandHandler {
  private final ReplicationSupervisor supervisor;
  private final DNContainerOperationClient dnClient;
  private String metricsName;

  public ReconcileContainerCommandHandler(ReplicationSupervisor supervisor, DNContainerOperationClient dnClient) {
    this.supervisor = supervisor;
    this.dnClient = dnClient;
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container, StateContext context,
      SCMConnectionManager connectionManager) {
    ReconcileContainerCommand reconcileCommand = (ReconcileContainerCommand) command;
    ReconcileContainerTask task = new ReconcileContainerTask(container.getController(), dnClient, reconcileCommand);
    if (metricsName == null) {
      metricsName = task.getMetricName();
    }
    supervisor.addTask(task);
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.reconcileContainerCommand;
  }

  @Override
  public int getQueuedCount() {
    return this.metricsName == null ? 0 : (int) this.supervisor
        .getReplicationQueuedCount(metricsName);
  }

  @Override
  public int getInvocationCount() {
    return this.metricsName == null ? 0 : (int) this.supervisor
        .getReplicationRequestCount(metricsName);
  }

  @Override
  public long getAverageRunTime() {
    return this.metricsName == null ? 0 : (int) this.supervisor
        .getReplicationRequestAvgTime(metricsName);
  }

  @Override
  public long getTotalRunTime() {
    return this.metricsName == null ? 0 : this.supervisor
        .getReplicationRequestTotalTime(metricsName);
  }

  public String getMetricsName() {
    return this.metricsName;
  }
}

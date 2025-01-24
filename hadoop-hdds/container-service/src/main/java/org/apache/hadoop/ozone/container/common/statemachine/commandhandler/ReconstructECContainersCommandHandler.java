/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCommandInfo;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinator;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Command handler for reconstructing the lost EC containers.
 */
public class ReconstructECContainersCommandHandler implements CommandHandler {
  private final ReplicationSupervisor supervisor;
  private final ECReconstructionCoordinator coordinator;
  private final ConfigurationSource conf;
  private String metricsName;

  public ReconstructECContainersCommandHandler(ConfigurationSource conf,
      ReplicationSupervisor supervisor,
      ECReconstructionCoordinator coordinator) {
    this.conf = conf;
    this.supervisor = supervisor;
    this.coordinator = coordinator;
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    ReconstructECContainersCommand ecContainersCommand =
        (ReconstructECContainersCommand) command;
    ECReconstructionCommandInfo reconstructionCommandInfo =
        new ECReconstructionCommandInfo(ecContainersCommand);
    ECReconstructionCoordinatorTask task = new ECReconstructionCoordinatorTask(
        coordinator, reconstructionCommandInfo);
    if (this.metricsName == null) {
      this.metricsName = task.getMetricName();
    }
    this.supervisor.addTask(task);
  }

  public String getMetricsName() {
    return this.metricsName;
  }

  @Override
  public Type getCommandType() {
    return Type.reconstructECContainersCommand;
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

  @Override
  public int getQueuedCount() {
    return this.metricsName == null ? 0 : (int) this.supervisor
        .getReplicationQueuedCount(metricsName);
  }

  public ConfigurationSource getConf() {
    return conf;
  }
}

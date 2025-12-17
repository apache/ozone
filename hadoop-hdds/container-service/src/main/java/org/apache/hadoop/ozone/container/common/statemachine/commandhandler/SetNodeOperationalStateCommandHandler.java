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

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SetNodeOperationalStateCommandProto;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle the SetNodeOperationalStateCommand sent from SCM to the datanode
 * to persist the current operational state.
 */
public class SetNodeOperationalStateCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SetNodeOperationalStateCommandHandler.class);
  private final ConfigurationSource conf;
  private final Consumer<HddsProtos.NodeOperationalState> replicationSupervisor;
  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final MutableRate opsLatencyMs;

  /**
   * Set Node State command handler.
   *
   * @param conf - Configuration for the datanode.
   */
  public SetNodeOperationalStateCommandHandler(ConfigurationSource conf,
      Consumer<HddsProtos.NodeOperationalState> replicationSupervisor) {
    this.conf = conf;
    this.replicationSupervisor = replicationSupervisor;
    MetricsRegistry registry = new MetricsRegistry(
        SetNodeOperationalStateCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(Type.setNodeOperationalStateCommand + "Ms");
  }

  /**
   * Handles a given SCM command.
   *
   * @param command - SCM Command
   * @param container - Ozone Container.
   * @param context - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand<?> command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    long startTime = Time.monotonicNow();
    invocationCount.incrementAndGet();
    if (command.getType() != Type.setNodeOperationalStateCommand) {
      LOG.warn("Skipping handling command, expected command "
              + "type {} but found {}",
          Type.setNodeOperationalStateCommand, command.getType());
      return;
    }
    SetNodeOperationalStateCommand setNodeCmd =
        (SetNodeOperationalStateCommand) command;
    SetNodeOperationalStateCommandProto setNodeCmdProto = setNodeCmd.getProto();
    DatanodeDetails dni = context.getParent().getDatanodeDetails();
    HddsProtos.NodeOperationalState state =
        setNodeCmdProto.getNodeOperationalState();
    try {
      persistUpdatedDatanodeDetails(dni, state, setNodeCmd.getStateExpiryEpochSeconds());
    } catch (IOException ioe) {
      LOG.error("Failed to persist the datanode state", ioe);
      // TODO - this should probably be raised, but it will break the command
      //      handler interface.
    }
    replicationSupervisor.accept(state);
    this.opsLatencyMs.add(Time.monotonicNow() - startTime);
  }

  private void persistUpdatedDatanodeDetails(
      DatanodeDetails dnDetails, HddsProtos.NodeOperationalState state, long stateExpiryEpochSeconds)
      throws IOException {
    DatanodeDetails persistedDni = new DatanodeDetails(dnDetails);
    persistedDni.setPersistedOpState(state);
    persistedDni.setPersistedOpStateExpiryEpochSec(stateExpiryEpochSeconds);
    persistDatanodeDetails(persistedDni);
    dnDetails.setPersistedOpState(state);
    dnDetails.setPersistedOpStateExpiryEpochSec(stateExpiryEpochSeconds);
  }

  // TODO - this duplicates code in HddsDatanodeService and InitDatanodeState
  //        Need to refactor.
  private void persistDatanodeDetails(DatanodeDetails dnDetails)
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    Objects.requireNonNull(idFilePath, "idFilePath == null");
    File idFile = new File(idFilePath);
    ContainerUtils.writeDatanodeDetailsTo(dnDetails, idFile, conf);
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public Type getCommandType() {
    return Type.setNodeOperationalStateCommand;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return invocationCount.intValue();
  }

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  @Override
  public long getAverageRunTime() {
    return (long) this.opsLatencyMs.lastStat().mean();
  }

  @Override
  public long getTotalRunTime() {
    return (long) this.opsLatencyMs.lastStat().total();
  }

  @Override
  public int getQueuedCount() {
    return 0;
  }
}

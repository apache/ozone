/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;


import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Handle the SetNodeOperationalStateCommand sent from SCM to the datanode
 * to persist the current operational state.
 */
public class SetNodeOperationalStateCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SetNodeOperationalStateCommandHandler.class);
  private final ConfigurationSource conf;
  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final AtomicLong totalTime = new AtomicLong(0);

  /**
   * Set Node State command handler.
   *
   * @param conf - Configuration for the datanode.
   */
  public SetNodeOperationalStateCommandHandler(ConfigurationSource conf) {
    this.conf = conf;
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
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    long startTime = Time.monotonicNow();
    invocationCount.incrementAndGet();
    StorageContainerDatanodeProtocolProtos.SetNodeOperationalStateCommandProto
        setNodeCmdProto = null;

    if (command.getType() != Type.setNodeOperationalStateCommand) {
      LOG.warn("Skipping handling command, expected command "
              + "type {} but found {}",
          Type.setNodeOperationalStateCommand, command.getType());
      return;
    }
    SetNodeOperationalStateCommand setNodeCmd =
        (SetNodeOperationalStateCommand) command;
    setNodeCmdProto = setNodeCmd.getProto();
    DatanodeDetails dni = context.getParent().getDatanodeDetails();
    dni.setPersistedOpState(setNodeCmdProto.getNodeOperationalState());
    dni.setPersistedOpStateExpiryEpochSec(
        setNodeCmd.getStateExpiryEpochSeconds());
    try {
      persistDatanodeDetails(dni);
    } catch (IOException ioe) {
      LOG.error("Failed to persist the datanode state", ioe);
      // TODO - this should probably be raised, but it will break the command
      //      handler interface.
    }
    totalTime.addAndGet(Time.monotonicNow() - startTime);
  }

  // TODO - this duplicates code in HddsDatanodeService and InitDatanodeState
  //        Need to refactor.
  private void persistDatanodeDetails(DatanodeDetails dnDetails)
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    if (idFilePath == null || idFilePath.isEmpty()) {
      LOG.error("A valid path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR);
      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR +
              " must be defined. See" +
              " https://wiki.apache.org/hadoop/Ozone#Configuration" +
              " for details on configuring Ozone.");
    }

    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    ContainerUtils.writeDatanodeDetailsTo(dnDetails, idFile);
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type
      getCommandType() {
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
    final int invocations = invocationCount.get();
    return invocations == 0 ?
        0 : totalTime.get() / invocations;
  }
}
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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Handler for close container command received from SCM.
 */
public class CloseContainerCommandHandler implements CommandHandler {
  static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerCommandHandler.class);
  private int invocationCount;
  private long totalTime;
  private boolean cmdExecuted;

  /**
   * Constructs a ContainerReport handler.
   */
  public CloseContainerCommandHandler() {
  }

  /**
   * Handles a given SCM command.
   *
   * @param command           - SCM Command
   * @param container         - Ozone Container.
   * @param context           - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    LOG.debug("Processing Close Container command.");
    invocationCount++;
    cmdExecuted = false;
    long startTime = Time.monotonicNow();
    // TODO: define this as INVALID_CONTAINER_ID in HddsConsts.java (TBA)
    long containerID = -1;
    try {

      CloseContainerCommandProto
          closeContainerProto =
          CloseContainerCommandProto
              .parseFrom(command.getProtoBufMessage());
      containerID = closeContainerProto.getContainerID();
      HddsProtos.ReplicationType replicationType =
          closeContainerProto.getReplicationType();

      ContainerProtos.CloseContainerRequestProto.Builder closeRequest =
          ContainerProtos.CloseContainerRequestProto.newBuilder();
      closeRequest.setContainerID(containerID);

      ContainerProtos.ContainerCommandRequestProto.Builder request =
          ContainerProtos.ContainerCommandRequestProto.newBuilder();
      request.setCmdType(ContainerProtos.Type.CloseContainer);
      request.setCloseContainer(closeRequest);
      request.setTraceID(UUID.randomUUID().toString());
      request.setDatanodeUuid(
          context.getParent().getDatanodeDetails().getUuidString());
      // submit the close container request for the XceiverServer to handle
      container.submitContainerRequest(
          request.build(), replicationType);
      cmdExecuted = true;
    } catch (Exception e) {
      LOG.error("Can't close container " + containerID, e);
    } finally {
      updateCommandStatus(context, command, cmdExecuted, LOG);
      long endTime = Time.monotonicNow();
      totalTime += endTime - startTime;
    }
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.closeContainerCommand;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return invocationCount;
  }

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  @Override
  public long getAverageRunTime() {
    if (invocationCount > 0) {
      return totalTime / invocationCount;
    }
    return 0;
  }
}

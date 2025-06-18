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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerInfo;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerService.DiskBalancerOperationalState;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.DiskBalancerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for DiskBalancer command received from SCM.
 */
public class DiskBalancerCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerCommandHandler.class);

  private AtomicLong invocationCount = new AtomicLong(0);
  private long totalTime;

  /**
   * Constructs a diskBalancerCommand handler.
   */
  public DiskBalancerCommandHandler() {
  }

  /**
   * Handles a given SCM command.
   *
   * @param command           - SCM Command
   * @param ozoneContainer    - Ozone Container.
   * @param context           - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand command, OzoneContainer ozoneContainer,
      StateContext context, SCMConnectionManager connectionManager) {
    invocationCount.incrementAndGet();
    final long startTime = Time.monotonicNow();
    DiskBalancerCommand diskBalancerCommand = (DiskBalancerCommand) command;

    final HddsProtos.DiskBalancerOpType opType =
        diskBalancerCommand.getOpType();
    final DiskBalancerConfiguration diskBalancerConf =
        diskBalancerCommand.getDiskBalancerConfiguration();

    DiskBalancerInfo diskBalancerInfo = ozoneContainer.getDiskBalancerInfo();
    LOG.info("Processing {}", diskBalancerCommand);
    try {
      switch (opType) {
      case START:
        HddsProtos.NodeOperationalState state = context.getParent().getDatanodeDetails().getPersistedOpState();

        if (state == HddsProtos.NodeOperationalState.IN_SERVICE) {
          diskBalancerInfo.setOperationalState(DiskBalancerOperationalState.RUNNING);
        } else {
          LOG.warn("Cannot start DiskBalancer as node is in {} state. Pausing instead.", state);
          diskBalancerInfo.setOperationalState(DiskBalancerOperationalState.PAUSED_BY_NODE_STATE);
        }
        diskBalancerInfo.updateFromConf(diskBalancerConf);
        break;
      case STOP:
        diskBalancerInfo.setOperationalState(DiskBalancerOperationalState.STOPPED);
        break;
      case UPDATE:
        diskBalancerInfo.updateFromConf(diskBalancerConf);
        break;
      default:
        throw new IOException("Unexpected type " + opType);
      }
      ozoneContainer.getDiskBalancerService().refresh(diskBalancerInfo);
    } catch (IOException e) {
      LOG.error("Can't handle command type: {}", opType, e);
    } finally {
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
    return SCMCommandProto.Type.diskBalancerCommand;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return (int)invocationCount.get();
  }

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  @Override
  public long getAverageRunTime() {
    if (invocationCount.get() > 0) {
      return totalTime / invocationCount.get();
    }
    return 0;
  }

  @Override
  public long getTotalRunTime() {
    return totalTime;
  }

  @Override
  public int getQueuedCount() {
    return 0;
  }
}

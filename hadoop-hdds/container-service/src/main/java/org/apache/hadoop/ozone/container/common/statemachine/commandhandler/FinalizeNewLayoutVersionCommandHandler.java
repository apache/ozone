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

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_REQUIRED;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FinalizeNewLayoutVersionCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.FinalizeNewLayoutVersionCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for FinalizeNewLayoutVersion command received from SCM.
 */
public class FinalizeNewLayoutVersionCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(FinalizeNewLayoutVersionCommandHandler.class);

  private AtomicLong invocationCount = new AtomicLong(0);
  private final MutableRate opsLatencyMs;

  /**
   * Constructs a FinalizeNewLayoutVersionCommandHandler.
   */
  public FinalizeNewLayoutVersionCommandHandler() {
    MetricsRegistry registry = new MetricsRegistry(
        FinalizeNewLayoutVersionCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(SCMCommandProto.Type.finalizeNewLayoutVersionCommand + "Ms");
  }

  /**
   * Handles a given SCM command.
   *
   * @param command           - SCM Command
   * @param ozoneContainer         - Ozone Container.
   * @param context           - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand<?> command, OzoneContainer ozoneContainer,
      StateContext context, SCMConnectionManager connectionManager) {
    LOG.info("Processing FinalizeNewLayoutVersionCommandHandler command.");
    invocationCount.incrementAndGet();
    final long startTime = Time.monotonicNow();
    DatanodeStateMachine dsm = context.getParent();
    final FinalizeNewLayoutVersionCommandProto finalizeCommand =
        ((FinalizeNewLayoutVersionCommand)command).getProto();
    try {
      if (finalizeCommand.getFinalizeNewLayoutVersion()) {
        // SCM is asking datanode to finalize
        if (dsm.getLayoutVersionManager().getUpgradeState() ==
            FINALIZATION_REQUIRED) {
          // SCM will keep sending Finalize command until datanode mlv == slv
          // we need to avoid multiple invocations of finalizeUpgrade.
          LOG.info("Finalize Upgrade called!");
          dsm.finalizeUpgrade();
        }
      }
    } catch (Exception e) {
      LOG.error("Exception during finalization.", e);
    } finally {
      long endTime = Time.monotonicNow();
      this.opsLatencyMs.add(endTime - startTime);
    }
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.finalizeNewLayoutVersionCommand;
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

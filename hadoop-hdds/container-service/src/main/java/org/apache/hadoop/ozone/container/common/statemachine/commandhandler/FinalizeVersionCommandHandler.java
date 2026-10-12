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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FinalizeNewDatanodeVersionCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.FinalizeVersionCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for FinalizeVersion command received from SCM.
 */
public class FinalizeVersionCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(FinalizeVersionCommandHandler.class);

  private final AtomicLong invocationCount = new AtomicLong(0);
  private final MutableRate opsLatencyMs;

  /**
   * Constructs a FinalizeVersionCommandHandler.
   */
  public FinalizeVersionCommandHandler() {
    MetricsRegistry registry = new MetricsRegistry(
        FinalizeVersionCommandHandler.class.getSimpleName());
    this.opsLatencyMs =
        registry.newRate(SCMCommandProto.Type.finalizeNewDatanodeVersionCommand + "Ms");
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
    LOG.info("Processing FinalizeVersionCommandHandler command.");
    invocationCount.incrementAndGet();
    final long startTime = Time.monotonicNow();
    DatanodeStateMachine dsm = context.getParent();
    final FinalizeNewDatanodeVersionCommandProto finalizeCommand =
        ((FinalizeVersionCommand) command).getProto();
    try {
      int dnSoftwareVersion = dsm.getVersionManager().getSoftwareVersion().serialize();
      int expectedSoftwareVersion = finalizeCommand.getExpectedSoftwareVersion();
      if (dnSoftwareVersion != expectedSoftwareVersion) {
        // Version mismatch should not happen here: the datanode is rejected
        // at registration and SCM only finalizes after its own version checks.
        // Crash defensively rather than finalize on an unexpected version.
        String msg = String.format("Datanode software version %d does not match the software version %d expected by " +
            "SCM. Terminating the datanode.", dnSoftwareVersion, expectedSoftwareVersion);
        ExitUtils.terminate(1, msg, LOG);
      }
      if (dsm.getVersionManager().needsFinalization()) {
        LOG.info("Finalize upgrade called.");
        dsm.getVersionManager().finalizeUpgrade();
      }
    } catch (UpgradeException e) {
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
    return SCMCommandProto.Type.finalizeNewDatanodeVersionCommand;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return (int) invocationCount.get();
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

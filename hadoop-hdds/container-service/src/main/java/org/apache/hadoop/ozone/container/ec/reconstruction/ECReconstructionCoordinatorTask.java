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
package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the actual EC reconstruction coordination task.
 */
public class ECReconstructionCoordinatorTask implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ECReconstructionCoordinatorTask.class);
  private final ConcurrentHashMap.KeySetView<Object, Boolean> inprogressCounter;
  private final ECReconstructionCoordinator reconstructionCoordinator;
  private final ECReconstructionCommandInfo reconstructionCommandInfo;
  private final Clock clock;

  public ECReconstructionCoordinatorTask(
      ECReconstructionCoordinator coordinator,
      ECReconstructionCommandInfo reconstructionCommandInfo,
      ConcurrentHashMap.KeySetView<Object, Boolean>
          inprogressReconstructionCoordinatorCounter,
      Clock clock) {
    this.reconstructionCoordinator = coordinator;
    this.reconstructionCommandInfo = reconstructionCommandInfo;
    this.inprogressCounter = inprogressReconstructionCoordinatorCounter;
    this.clock = clock;
  }

  @Override
  public void run() {
    // Implement the coordinator logic to handle a container group
    // reconstruction.

    // 1. Read container block meta info from the available min required good
    // containers. ( Full block set should be available with 1st or parity
    // indexes containers)
    // 2. Find out the total number of blocks
    // 3. Loop each block and use the ReconstructedInputStreams(HDDS-6665) and
    // recover.
    // 4. Write the recovered chunks to given targets/write locally to
    // respective container. HDDS-6582
    // 5. Close/finalize the recovered containers.
    long containerID = this.reconstructionCommandInfo.getContainerID();
    long start = Time.monotonicNow();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting the EC reconstruction of the container {}",
          containerID);
    }
    try {
      if (reconstructionCommandInfo.getDeadline() > 0
          && clock.millis() > reconstructionCommandInfo.getDeadline()) {
        LOG.info("Ignoring this reconstruct container command for container" +
                " {} since the current time {}ms is past the deadline {}ms",
            containerID, clock.millis(),
            reconstructionCommandInfo.getDeadline());
        return;
      }

      final OptionalLong currentTerm =
          reconstructionCoordinator.getTermOfLeaderSCM();
      final long taskTerm = reconstructionCommandInfo.getTerm();
      if (currentTerm.isPresent() && taskTerm < currentTerm.getAsLong()) {
        LOG.info("Ignoring {} since SCM leader has new term ({} < {})",
            reconstructionCommandInfo, taskTerm, currentTerm.getAsLong());
        return;
      }

      reconstructionCoordinator.reconstructECContainerGroup(
          reconstructionCommandInfo.getContainerID(),
          reconstructionCommandInfo.getEcReplicationConfig(),
          reconstructionCommandInfo.getSourceNodeMap(),
          reconstructionCommandInfo.getTargetNodeMap());
      long elapsed = Time.monotonicNow() - start;
      LOG.info("Completed {} in {} ms", reconstructionCommandInfo, elapsed);
    } catch (IOException e) {
      long elapsed = Time.monotonicNow() - start;
      LOG.warn("Failed {} after {} ms", reconstructionCommandInfo, elapsed, e);
    } finally {
      this.inprogressCounter.remove(containerID);
    }
  }

  @Override
  public String toString() {
    return "ECReconstructionTask{info=" + reconstructionCommandInfo + '}';
  }
}

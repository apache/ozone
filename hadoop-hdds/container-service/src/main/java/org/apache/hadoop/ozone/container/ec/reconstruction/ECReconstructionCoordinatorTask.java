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

import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * This is the actual EC reconstruction coordination task.
 */
public class ECReconstructionCoordinatorTask
    extends AbstractReplicationTask implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ECReconstructionCoordinatorTask.class);
  private final ECReconstructionCoordinator reconstructionCoordinator;
  private final ECReconstructionCommandInfo reconstructionCommandInfo;

  public ECReconstructionCoordinatorTask(
      ECReconstructionCoordinator coordinator,
      ECReconstructionCommandInfo reconstructionCommandInfo) {
    super(reconstructionCommandInfo.getContainerID(),
        reconstructionCommandInfo.getDeadline(),
        reconstructionCommandInfo.getTerm());
    this.reconstructionCoordinator = coordinator;
    this.reconstructionCommandInfo = reconstructionCommandInfo;
  }

  @Override
  public void runTask() {
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
      reconstructionCoordinator.reconstructECContainerGroup(
          reconstructionCommandInfo.getContainerID(),
          reconstructionCommandInfo.getEcReplicationConfig(),
          reconstructionCommandInfo.getSourceNodeMap(),
          reconstructionCommandInfo.getTargetNodeMap());
      long elapsed = Time.monotonicNow() - start;
      LOG.info("Completed {} in {} ms", reconstructionCommandInfo, elapsed);
      setStatus(Status.DONE);
    } catch (IOException e) {
      long elapsed = Time.monotonicNow() - start;
      LOG.warn("Failed {} after {} ms", reconstructionCommandInfo, elapsed, e);
      setStatus(Status.FAILED);
    }
  }

  @Override
  public String toString() {
    return "ECReconstructionTask{info=" + reconstructionCommandInfo + '}';
  }

  @Override
  public void run() {
    runTask();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ECReconstructionCoordinatorTask that = (ECReconstructionCoordinatorTask) o;
    return getContainerId() == that.getContainerId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerId());
  }
}

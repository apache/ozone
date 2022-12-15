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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This is the actual EC reconstruction coordination task.
 */
public class ECReconstructionCoordinatorTask implements Runnable {
  static final Logger LOG =
      LoggerFactory.getLogger(ECReconstructionCoordinatorTask.class);
  private final ConcurrentHashMap.KeySetView<Object, Boolean> inprogressCounter;
  private final ECReconstructionCoordinator reconstructionCoordinator;
  private final ECReconstructionCommandInfo reconstructionCommandInfo;
  private long deadlineMsSinceEpoch = 0;
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

      SortedMap<Integer, DatanodeDetails> sourceNodeMap =
          reconstructionCommandInfo.getSources().stream().collect(Collectors
              .toMap(DatanodeDetailsAndReplicaIndex::getReplicaIndex,
                  DatanodeDetailsAndReplicaIndex::getDnDetails, (v1, v2) -> v1,
                  TreeMap::new));
      SortedMap<Integer, DatanodeDetails> targetNodeMap = IntStream
          .range(0, reconstructionCommandInfo.getTargetDatanodes().size())
          .boxed().collect(Collectors.toMap(i -> (int) reconstructionCommandInfo
                  .getMissingContainerIndexes()[i],
              i -> reconstructionCommandInfo.getTargetDatanodes().get(i),
              (v1, v2) -> v1, TreeMap::new));

      reconstructionCoordinator.reconstructECContainerGroup(
          reconstructionCommandInfo.getContainerID(),
          reconstructionCommandInfo.getEcReplicationConfig(), sourceNodeMap,
          targetNodeMap);
      LOG.info("Completed the EC reconstruction of the container {}",
          reconstructionCommandInfo.getContainerID());
    } catch (IOException e) {
      LOG.warn(
          "Failed to complete the reconstruction task for the container: "
              + reconstructionCommandInfo.getContainerID(), e);
    } finally {
      this.inprogressCounter.remove(containerID);
    }
  }

  @Override
  public String toString() {
    return "ECReconstructionCoordinatorTask{" + "reconstructionCommandInfo="
        + reconstructionCommandInfo + '}';
  }
}

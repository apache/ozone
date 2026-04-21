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

package org.apache.hadoop.ozone.container.ec.reconstruction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the actual EC reconstruction coordination task.
 */
public class ECReconstructionCoordinatorTask
    extends AbstractReplicationTask implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ECReconstructionCoordinatorTask.class);
  private final ECReconstructionCoordinator reconstructionCoordinator;
  private final ECReconstructionCommandInfo reconstructionCommandInfo;
  private final ContainerController containerController;
  private final DatanodeDetails self;
  private final String debugString;
  public static final String METRIC_NAME = "ECReconstructions";
  public static final String METRIC_DESCRIPTION_SEGMENT = "EC reconstructions";

  public ECReconstructionCoordinatorTask(
      ECReconstructionCoordinator coordinator,
      ECReconstructionCommandInfo reconstructionCommandInfo,
      ContainerController containerController,
      DatanodeDetails self) {
    super(reconstructionCommandInfo.getContainerID(),
        reconstructionCommandInfo.getDeadline(),
        reconstructionCommandInfo.getTerm());
    this.reconstructionCoordinator = coordinator;
    this.reconstructionCommandInfo = reconstructionCommandInfo;
    this.containerController = containerController;
    this.self = self;
    debugString = reconstructionCommandInfo.toString();
  }

  @Override
  public String getMetricName() {
    return METRIC_NAME;
  }

  @Override
  public String getMetricDescriptionSegment() {
    return METRIC_DESCRIPTION_SEGMENT;
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
    long start = Time.monotonicNow();

    LOG.info("{}", this);

    try {
      reconstructionCoordinator.reconstructECContainerGroup(
          reconstructionCommandInfo.getContainerID(),
          reconstructionCommandInfo.getEcReplicationConfig(),
          reconstructionCommandInfo.getSourceNodeMap(),
          reconstructionCommandInfo.getTargetNodeMap());
      long elapsed = Time.monotonicNow() - start;
      setStatus(Status.DONE);
      LOG.info("{} in {} ms", this, elapsed);
    } catch (Exception e) {
      long elapsed = Time.monotonicNow() - start;
      setStatus(Status.FAILED);
      LOG.warn("{} after {} ms", this, elapsed, e);
    }
  }

  @Override
  protected Object getCommandForDebug() {
    return debugString;
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

  @Override
  public Collection<HddsVolume> getVolumes() {
    if (containerController == null || self == null) {
      return Collections.emptyList();
    }
    List<HddsVolume> volumes = new ArrayList<>();
    SortedMap<Integer, DatanodeDetails> sources =
        reconstructionCommandInfo.getSourceNodeMap();
    boolean hasLocalSource = false;
    for (DatanodeDetails dn : sources.values()) {
      if (dn.equals(self)) {
        hasLocalSource = true;
        break;
      }
    }
    if (hasLocalSource) {
      org.apache.hadoop.ozone.container.common.interfaces.Container<?>
          container = containerController.getContainer(getContainerId());
      if (container != null) {
        HddsVolume vol = (HddsVolume) container.getContainerData().getVolume();
        if (vol != null) {
          volumes.add(vol);
        }
      }
    }
    return volumes;
  }
}

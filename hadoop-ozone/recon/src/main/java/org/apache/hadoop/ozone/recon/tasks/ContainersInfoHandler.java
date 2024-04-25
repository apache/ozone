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

package org.apache.hadoop.ozone.recon.tasks;

import jakarta.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.scm.ReconScmMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages Containers Table info of SCM MetaData DB, updates Recon's in-memory
 * Container manager objects and global stats table to update container stats.
 */
public class ContainersInfoHandler implements SCMMetaDataTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainersInfoHandler.class);

  private ReconContainerMetadataManager reconContainerMetadataManager;
  private ReconScmMetadataManager scmMetadataManager;
  private Map<LifeCycleState, Long> containerStateCountMap;

  public ContainersInfoHandler(ReconContainerMetadataManager reconContainerMetadataManager,
                               ReconScmMetadataManager scmMetadataManager,
                               Map<LifeCycleState, Long> containerStateCountMap) {
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.scmMetadataManager = scmMetadataManager;
    this.containerStateCountMap = containerStateCountMap;
  }

  private void handleUpdateContainerEvent(ContainerInfo containerInfo) throws IOException {
    scmMetadataManager.getOzoneStorageContainerManager().getContainerManager()
        .deleteContainer(ContainerID.valueOf(containerInfo.getContainerID()));
    scmMetadataManager.getOzoneStorageContainerManager().getContainerManager().initialize(containerInfo);
  }

  private void handleDeleteContainerEvent(ContainerInfo containerInfo)
      throws IOException {
    containerStateCountMap.compute(containerInfo.getState(), (k, v) -> (v != null ? v : 0L) - 1);
    scmMetadataManager.getOzoneStorageContainerManager().getContainerManager()
        .deleteContainer(ContainerID.valueOf(containerInfo.getContainerID()));
  }

  private void handlePutContainerEvent(ContainerInfo containerInfo) throws IOException {
    containerStateCountMap.compute(containerInfo.getState(), (k, v) -> (v != null ? v : 0L) + 1);
    scmMetadataManager.getOzoneStorageContainerManager().getContainerManager().initialize(containerInfo);

  }

  /**
   * Handles a PUT event on scm metadata DB tables.
   *
   * @param event The PUT event to be processed.
   */
  @Override
  public void handlePutEvent(RocksDBUpdateEvent<?, Object> event) {
    ContainerID containerId = (ContainerID) event.getKey();
    ContainerInfo containerInfo = (ContainerInfo) event.getValue();
    try {
      handlePutContainerEvent(containerInfo);
    } catch (IOException ioe) {
      LOG.error("Unexpected error while handling add new container event and processing container stats for" +
          " containerId: {} - ", containerId, ioe);
    }
  }

  /**
   * Handles a DELETE event on scm metadata DB tables.
   *
   * @param event The DELETE event to be processed.
   */
  @Override
  public void handleDeleteEvent(RocksDBUpdateEvent<?, Object> event) {
    ContainerID containerId = (ContainerID) event.getKey();
    ContainerInfo containerInfo = (ContainerInfo) event.getValue();
    try {
      handleDeleteContainerEvent(containerInfo);
    } catch (IOException ioe) {
      LOG.error("Unexpected error while handling delete container event and processing container stats for" +
          " containerId: {} - ", containerId, ioe);
    }
  }

  /**
   * Handles an UPDATE event on scm metadata DB tables.
   *
   * @param event The UPDATE event to be processed.
   */
  @Override
  public void handleUpdateEvent(RocksDBUpdateEvent<?, Object> event) {
    ContainerID containerId = (ContainerID) event.getKey();
    ContainerInfo containerInfo = (ContainerInfo) event.getValue();
    try {
      handleUpdateContainerEvent(containerInfo);
    } catch (Exception ioe) {
      LOG.error("Unexpected error while handling update of container event and processing container stats for" +
          " containerId: {} - ", containerId, ioe);
    }
  }

  /**
   * Iterates all the rows of desired SCM metadata DB table to capture
   * and process the information further by sending to any downstream class.
   *
   * @param reconScmMetadataManager
   * @return Pair represents the success (true) or failure (false) for the task.
   * @throws IOException
   */
  @Override
  public Pair<String, Boolean> reprocess(ReconScmMetadataManager reconScmMetadataManager) throws IOException {
    long containerCount = 0;
    Map<LifeCycleState, Long> containerStateCntMap = new HashMap<>();
    try {
      LOG.info("Starting a 'reprocess' run of {}", getHandlerName());
      Instant start = Instant.now();

      // reset total count of deleted containers to zero
      reconContainerMetadataManager.clearContainerStats();
      Table<ContainerID, ContainerInfo> containerTable = reconScmMetadataManager.getContainerTable();
      try (
          TableIterator<ContainerID, ? extends Table.KeyValue<ContainerID, ContainerInfo>> containerTableIterator =
              containerTable.iterator()) {
        while (containerTableIterator.hasNext()) {
          Table.KeyValue<ContainerID, ContainerInfo> keyValue = containerTableIterator.next();
          ContainerInfo containerInfo = keyValue.getValue();
          handleContainerStatsReprocess(containerInfo, containerStateCntMap);
          containerCount++;
        }
        ImmutablePair<String, Boolean> result = saveContainerStats(containerStateCntMap);
        if (result != null) {
          return result;
        }
      }
      LOG.info("Completed 'reprocess' of ContainerStatsTask.");
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("It took me {} seconds to process stats of {} containers.",
          (double) duration / 1000.0, containerCount);
    } catch (IOException ioEx) {
      LOG.error("Unable to populate Container Stats data in Recon DB. ",
          ioEx);
      return new ImmutablePair<>(getHandlerName(), false);
    }
    return new ImmutablePair<>(getHandlerName(), true);
  }

  @Nullable
  private ImmutablePair<String, Boolean> saveContainerStats(
      Map<LifeCycleState, Long> containerStateCntMap) {
    if (!containerStateCntMap.isEmpty()) {
      if (!reconContainerMetadataManager.storeContainerStatesStats(containerStateCntMap)) {
        LOG.error("Unable to store container stats information to the GlobalStats table.");
        return new ImmutablePair<>(getHandlerName(), false);
      }
    }
    return null;
  }

  private void handleContainerStatsReprocess(ContainerInfo containerInfo,
                                             Map<LifeCycleState, Long> containerStateCntMap) {
    containerStateCntMap.compute(containerInfo.getState(), (k, v) -> (v != null ? v : 0L) + 1);
  }

  @Override
  public String getHandlerName() {
    return "ContainersInfoHandler";
  }
}

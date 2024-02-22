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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.scm.ReconSCMMetadataProcessingTask;
import org.apache.hadoop.ozone.recon.scm.ReconScmMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class to iterate over the OM DB and populate the Recon container DB with
 * the container -> Key reverse mapping.
 */
public class ContainerStatsTask implements ReconSCMMetadataProcessingTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerStatsTask.class);

  private OzoneConfiguration configuration;
  private ReconScmMetadataManager scmMetadataManager;
  private ReconContainerMetadataManager reconContainerMetadataManager;

  @Inject
  public ContainerStatsTask(ReconScmMetadataManager scmMetadataManager,
                            ReconContainerMetadataManager reconContainerMetadataManager,
                            OzoneConfiguration configuration) {
    this.scmMetadataManager = scmMetadataManager;
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.configuration = configuration;
  }

  /**
   * Read container table data and compute stats like DELETED container count,
   * number of blocks per container etc.
   */
  @Override
  public Pair<String, Boolean> reprocess(ReconScmMetadataManager reconScmMetadataManager) {
    long containerCount = 0;
    Map<HddsProtos.LifeCycleState, Long> containerCountMap = new HashMap<>();
    try {
      LOG.info("Starting a 'reprocess' run of ContainerStatsTask.");
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
          handleContainerStatsReprocess(containerInfo, containerCountMap);
          containerCount++;
        }
        ImmutablePair<String, Boolean> result = saveContainerStats(containerCountMap);
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
      return new ImmutablePair<>(getTaskName(), false);
    }
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Nullable
  private ImmutablePair<String, Boolean> saveContainerStats(Map<HddsProtos.LifeCycleState, Long> containerCountMap) {
    if (!containerCountMap.isEmpty()) {
      if (!reconContainerMetadataManager.storeContainerStatesStats(containerCountMap)) {
        LOG.error("Unable to store container stats information to the GlobalStats table.");
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    return null;
  }

  private void handleContainerStatsReprocess(ContainerInfo containerInfo,
                                             Map<HddsProtos.LifeCycleState, Long> containerCountMap) {
    containerCountMap.compute(containerInfo.getState(), (k, v) -> (v != null ? v : 0L) + 1);
  }

  @Override
  public String getTaskName() {
    return "ContainerStatsTask";
  }

  public Collection<String> getTaskTables() throws IOException {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(scmMetadataManager.getContainerTable().getName());
    return taskTables;
  }

  @Override
  public Pair<String, Boolean> process(RocksDBUpdateEventBatch events) {
    Iterator<RocksDBUpdateEvent> eventIterator = events.getIterator();
    int eventCount = 0;
    Collection<String> taskTables = Collections.EMPTY_SET;
    Map<HddsProtos.LifeCycleState, Long> containerCountMap = new HashMap<>();
    try {
      taskTables = getTaskTables();


      while (eventIterator.hasNext()) {
        RocksDBUpdateEvent<ContainerID, ContainerInfo> rocksDBUpdateEvent = eventIterator.next();
        if (!taskTables.contains(rocksDBUpdateEvent.getTable())) {
          continue;
        }
        ContainerID containerId = rocksDBUpdateEvent.getKey();
        ContainerInfo containerInfo = rocksDBUpdateEvent.getValue();

        switch (rocksDBUpdateEvent.getAction()) {
        case PUT:
          handlePutContainerEvent(containerInfo, containerCountMap);
          break;

        case DELETE:
          handleDeleteContainerEvent(containerInfo, containerCountMap);
          break;

        case UPDATE:
          if (rocksDBUpdateEvent.getOldValue() != null) {
            handleDeleteContainerEvent(rocksDBUpdateEvent.getOldValue(), containerCountMap);
          } else {
            LOG.warn("Update event does not have the old container info for {}.",
                containerId);
          }
          handlePutContainerEvent(containerInfo, containerCountMap);
          break;

        default:
          LOG.debug("Skipping DB update event : {}",
              rocksDBUpdateEvent.getAction());
        }
        eventCount++;
      }
    } catch (IOException ioe) {
      LOG.error("Unexpected exception while processing processing container data from SCM - {}", ioe);
    }
    writeToTheDB(containerCountMap);
    LOG.info("{} successfully processed {} OM DB update event(s).",
        getTaskName(), eventCount);
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void writeToTheDB(Map<HddsProtos.LifeCycleState, Long> containerCountMap) {
    saveContainerStats(containerCountMap);
  }

  private void handleDeleteContainerEvent(ContainerInfo containerInfo,
                                          Map<HddsProtos.LifeCycleState, Long> containerCountMap) {
    containerCountMap.compute(containerInfo.getState(), (k, v) -> (v != null ? v : 0L) - 1);
  }

  private void handlePutContainerEvent(ContainerInfo containerInfo,
                                       Map<HddsProtos.LifeCycleState, Long> containerCountMap) throws IOException {
    containerCountMap.compute(containerInfo.getState(), (k, v) -> (v != null ? v : 0L) + 1);
    scmMetadataManager.getOzoneStorageContainerManager().getContainerManager().initialize(containerInfo);

    // TODO: Initialize other managers also (sequenceIdGen, NodeManager, PipelineManager)
  }
}

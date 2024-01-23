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

package org.apache.hadoop.ozone.recon.fsck;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.util.Time;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.hadoop.ozone.recon.schema.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.TOTAL_KEYS;
import static org.apache.hadoop.ozone.recon.ReconConstants.TOTAL_USED_BYTES;


/**
 * Class that scans the list of containers and keeps track of containers with
 * no replicas in a SQL table.
 */
public class ContainerHealthTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthTask.class);
  public static final int FETCH_COUNT = Integer.parseInt(DEFAULT_FETCH_COUNT);

  private ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private StorageContainerServiceProvider scmClient;
  private ContainerManager containerManager;
  private ContainerHealthSchemaManager containerHealthSchemaManager;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private PlacementPolicy placementPolicy;
  private final long interval;

  private Set<ContainerInfo> processedContainers = new HashSet<>();

  public ContainerHealthTask(
      ContainerManager containerManager,
      StorageContainerServiceProvider scmClient,
      ReconTaskStatusDao reconTaskStatusDao,
      ContainerHealthSchemaManager containerHealthSchemaManager,
      PlacementPolicy placementPolicy,
      ReconTaskConfig reconTaskConfig,
      ReconContainerMetadataManager reconContainerMetadataManager) {
    super(reconTaskStatusDao);
    this.scmClient = scmClient;
    this.containerHealthSchemaManager = containerHealthSchemaManager;
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.placementPolicy = placementPolicy;
    this.containerManager = containerManager;
    interval = reconTaskConfig.getMissingContainerTaskInterval().toMillis();
  }

  @Override
  public void run() {
    try {
      while (canRun()) {
        triggerContainerHealthCheck();
        Thread.sleep(interval);
      }
    } catch (Throwable t) {
      LOG.error("Exception in Missing Container task Thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void triggerContainerHealthCheck() {
    lock.writeLock().lock();
    // Map contains all UNHEALTHY STATES as keys and value is another map
    // with 3 keys (CONTAINER_COUNT, TOTAL_KEYS, TOTAL_USED_BYTES) and value
    // is count for each of these 3 stats.
    // E.g. <MISSING, <CONTAINER_COUNT, 1>>, <MISSING, <TOTAL_KEYS, 10>>,
    // <MISSING, <TOTAL_USED_BYTES, 2048>>,
    // <EMPTY_MISSING, <CONTAINER_COUNT, 10>>, <EMPTY_MISSING, <TOTAL_KEYS, 2>>,
    // <EMPTY_MISSING, <TOTAL_USED_BYTES, 2048>>
    Map<UnHealthyContainerStates, Map<String, Long>>
        unhealthyContainerStateStatsMap;
    try {
      unhealthyContainerStateStatsMap = new HashMap<>(Collections.emptyMap());
      initializeUnhealthyContainerStateStatsMap(
          unhealthyContainerStateStatsMap);
      long start = Time.monotonicNow();
      long currentTime = System.currentTimeMillis();
      long existingCount = processExistingDBRecords(currentTime,
          unhealthyContainerStateStatsMap);
      LOG.info("Container Health task thread took {} milliseconds to" +
              " process {} existing database records.",
          Time.monotonicNow() - start, existingCount);

      checkAndProcessContainers(unhealthyContainerStateStatsMap, currentTime);
      processedContainers.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void checkAndProcessContainers(
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap, long currentTime) {
    ContainerID startID = ContainerID.valueOf(1);
    List<ContainerInfo> containers = containerManager.getContainers(startID,
        FETCH_COUNT);
    long start;
    long iterationCount = 0;
    while (!containers.isEmpty()) {
      start = Time.monotonicNow();
      containers.stream()
          .filter(c -> !processedContainers.contains(c))
          .forEach(c -> processContainer(c, currentTime,
              unhealthyContainerStateStatsMap));
      recordSingleRunCompletion();
      LOG.info("Container Health task thread took {} milliseconds for" +
              " processing {} containers.", Time.monotonicNow() - start,
          containers.size());
      logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
      if (containers.size() >= FETCH_COUNT) {
        startID = ContainerID.valueOf(
            containers.get(containers.size() - 1).getContainerID() + 1);
        containers = containerManager.getContainers(startID, FETCH_COUNT);
      } else {
        containers.clear();
      }
      iterationCount++;
    }
    LOG.info(
        "Container Health task thread took {} iterations to fetch all " +
            "containers using batched approach with batch size of {}",
        iterationCount, FETCH_COUNT);
  }

  private void logUnhealthyContainerStats(
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap) {
    // If any EMPTY_MISSING containers, then it is possible that such
    // containers got stuck in the closing state which never got
    // any replicas created on the datanodes. In this case, we log it as
    // EMPTY, and insert as EMPTY_MISSING in UNHEALTHY_CONTAINERS table.
    unhealthyContainerStateStatsMap.entrySet().forEach(stateEntry -> {
      UnHealthyContainerStates unhealthyContainerState = stateEntry.getKey();
      Map<String, Long> containerStateStatsMap = stateEntry.getValue();
      StringBuilder logMsgBuilder =
          new StringBuilder(unhealthyContainerState.toString());
      logMsgBuilder.append(" **Container State Stats:** \n\t");
      containerStateStatsMap.entrySet().forEach(statsEntry -> {
        logMsgBuilder.append(statsEntry.getKey());
        logMsgBuilder.append(" -> ");
        logMsgBuilder.append(statsEntry.getValue());
        logMsgBuilder.append(" , ");
      });
      LOG.info(logMsgBuilder.toString());
    });
  }

  private void initializeUnhealthyContainerStateStatsMap(
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap) {
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.MISSING, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.EMPTY_MISSING, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.UNDER_REPLICATED, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.OVER_REPLICATED, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.MIS_REPLICATED, new HashMap<>());
  }

  private ContainerHealthStatus setCurrentContainer(long recordId)
      throws ContainerNotFoundException {
    ContainerInfo container =
        containerManager.getContainer(ContainerID.valueOf(recordId));
    Set<ContainerReplica> replicas =
        containerManager.getContainerReplicas(container.containerID());
    return new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager);
  }

  private void completeProcessingContainer(
      ContainerHealthStatus container,
      Set<String> existingRecords,
      long currentTime,
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateCountMap) {
    containerHealthSchemaManager.insertUnhealthyContainerRecords(
        ContainerHealthRecords.generateUnhealthyRecords(
            container, existingRecords, currentTime,
            unhealthyContainerStateCountMap));
    processedContainers.add(container.getContainer());
  }

  /**
   * This method reads all existing records in the UnhealthyContainers table.
   * The container records are read sorted by Container ID, as there can be
   * more than 1 record per container.
   * Each record is checked to see if it should be retained or deleted, and if
   * any of the replica counts have changed the record is updated. Each record
   * for a container is collected into a Set and when the next container id
   * changes, indicating the end of the records for the current container,
   * completeProcessingContainer is called. This will check to see if any
   * additional records need to be added to the database.
   *
   * @param currentTime Timestamp to place on all records generated by this run
   * @param unhealthyContainerStateCountMap
   * @return Count of records processed
   */
  private long processExistingDBRecords(long currentTime,
                                        Map<UnHealthyContainerStates,
                                            Map<String, Long>>
                                            unhealthyContainerStateCountMap) {
    long recordCount = 0;
    try (Cursor<UnhealthyContainersRecord> cursor =
             containerHealthSchemaManager.getAllUnhealthyRecordsCursor()) {
      ContainerHealthStatus currentContainer = null;
      Set<String> existingRecords = new HashSet<>();
      while (cursor.hasNext()) {
        recordCount++;
        UnhealthyContainersRecord rec = cursor.fetchNext();
        try {
          if (currentContainer == null) {
            currentContainer = setCurrentContainer(rec.getContainerId());
          }
          if (currentContainer.getContainerID() != rec.getContainerId()) {
            completeProcessingContainer(
                currentContainer, existingRecords, currentTime,
                unhealthyContainerStateCountMap);
            existingRecords.clear();
            currentContainer = setCurrentContainer(rec.getContainerId());
          }
          if (ContainerHealthRecords
              .retainOrUpdateRecord(currentContainer, rec
              )) {
            // Check if the missing container is deleted in SCM
            if (currentContainer.isMissing() &&
                containerDeletedInSCM(currentContainer.getContainer())) {
              rec.delete();
            }
            existingRecords.add(rec.getContainerState());
            if (rec.changed()) {
              rec.update();
            }
          } else {
            rec.delete();
          }
        } catch (ContainerNotFoundException cnf) {
          rec.delete();
          currentContainer = null;
        }
      }
      // Remember to finish processing the last container
      if (currentContainer != null) {
        completeProcessingContainer(
            currentContainer, existingRecords, currentTime,
            unhealthyContainerStateCountMap);
      }
    }
    return recordCount;
  }

  private void processContainer(ContainerInfo container, long currentTime,
                                Map<UnHealthyContainerStates,
                                    Map<String, Long>>
                                      unhealthyContainerStateStatsMap) {
    try {
      Set<ContainerReplica> containerReplicas =
          containerManager.getContainerReplicas(container.containerID());
      ContainerHealthStatus h = new ContainerHealthStatus(container,
          containerReplicas, placementPolicy, reconContainerMetadataManager);
      if (h.isHealthy() || h.isDeleted()) {
        return;
      }
      // For containers deleted in SCM, we sync the container state here.
      if (h.isMissing() && containerDeletedInSCM(container)) {
        return;
      }
      containerHealthSchemaManager.insertUnhealthyContainerRecords(
          ContainerHealthRecords.generateUnhealthyRecords(h, currentTime,
              unhealthyContainerStateStatsMap));
    } catch (ContainerNotFoundException e) {
      LOG.error("Container not found while processing container in Container " +
          "Health task", e);
    }
  }

  private boolean containerDeletedInSCM(ContainerInfo containerInfo) {
    try {
      ContainerWithPipeline containerWithPipeline =
          scmClient.getContainerWithPipeline(containerInfo.getContainerID());
      if (containerWithPipeline.getContainerInfo().getState() ==
          HddsProtos.LifeCycleState.DELETED) {
        if (containerInfo.getState() == HddsProtos.LifeCycleState.CLOSED) {
          containerManager.updateContainerState(containerInfo.containerID(),
              HddsProtos.LifeCycleEvent.DELETE);
        }
        if (containerInfo.getState() == HddsProtos.LifeCycleState.DELETING &&
            containerManager.getContainerReplicas(containerInfo.containerID())
                .size() == 0
        ) {
          containerManager.updateContainerState(containerInfo.containerID(),
              HddsProtos.LifeCycleEvent.CLEANUP);
        }
        return true;
      }
    } catch (InvalidStateTransitionException e) {
      LOG.error("Failed to transition Container state while processing " +
          "container in Container Health task", e);
    } catch (IOException e) {
      LOG.error("Got exception while processing container in" +
          " Container Health task", e);
    }
    return false;
  }

  /**
   * Helper methods to generate and update the required database records for
   * unhealthy containers.
   */
  public static class ContainerHealthRecords {

    /**
     * Given an existing database record and a ContainerHealthStatus object,
     * this method will check if the database record should be retained or not.
     * Eg, if a missing record exists, and the ContainerHealthStatus indicates
     * the container is still missing, the method will return true, indicating
     * the record should be retained. If the container is no longer missing,
     * it will return false, indicating the record should be deleted.
     * If the record is to be retained, the fields in the record for actual
     * replica count, delta and reason will be updated if their counts have
     * changed.
     *
     * @param container ContainerHealthStatus representing the
     *                  health state of the container.
     * @param rec       Existing database record from the
     *                  UnhealthyContainers table.
     * @return returns true or false if need to retain or update the unhealthy
     * container record
     */
    public static boolean retainOrUpdateRecord(
        ContainerHealthStatus container, UnhealthyContainersRecord rec) {
      boolean returnValue = false;
      switch (UnHealthyContainerStates.valueOf(rec.getContainerState())) {
      case MISSING:
        returnValue = container.isMissing();
        break;
      case MIS_REPLICATED:
        returnValue = keepMisReplicatedRecord(container, rec);
        break;
      case UNDER_REPLICATED:
        returnValue = keepUnderReplicatedRecord(container, rec);
        break;
      case OVER_REPLICATED:
        returnValue = keepOverReplicatedRecord(container, rec);
        break;
      default:
        returnValue = false;
      }
      return returnValue;
    }

    public static List<UnhealthyContainers> generateUnhealthyRecords(
        ContainerHealthStatus container, long time,
        Map<UnHealthyContainerStates, Map<String, Long>>
            unhealthyContainerStateStatsMap) {
      return generateUnhealthyRecords(container, new HashSet<>(), time,
          unhealthyContainerStateStatsMap);
    }

    /**
     * Check the status of the container and generate any database records that
     * need to be recorded. This method also considers the records seen by the
     * method retainOrUpdateRecord. If a record has been seen by that method
     * then it will not be emitted here. Therefore this method returns only the
     * missing records which have not been seen already.
     * @return List of UnhealthyContainer records to be stored in the DB
     */
    public static List<UnhealthyContainers> generateUnhealthyRecords(
        ContainerHealthStatus container, Set<String> recordForStateExists,
        long time,
        Map<UnHealthyContainerStates, Map<String, Long>>
            unhealthyContainerStateStatsMap) {
      List<UnhealthyContainers> records = new ArrayList<>();
      if (container.isHealthy() || container.isDeleted()) {
        return records;
      }

      if (container.isMissing()
          && !recordForStateExists.contains(
          UnHealthyContainerStates.MISSING.toString())) {
        if (!container.isEmpty()) {
          LOG.info("Non-empty container {} is missing. It has {} " +
                  "keys and {} bytes used according to SCM metadata. " +
                  "Please visit Recon's missing container page for a list of " +
                  "keys (and their metadata) mapped to this container.",
              container.getContainerID(), container.getNumKeys(),
              container.getContainer().getUsedBytes());
          records.add(
              recordForState(container, UnHealthyContainerStates.MISSING,
                  time));
          populateContainerStats(container, UnHealthyContainerStates.MISSING,
              unhealthyContainerStateStatsMap);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Empty container {} is missing. Kindly check the " +
                "consolidated container stats per UNHEALTHY state logged as " +
                "starting with **Container State Stats:**");
          }
          records.add(
              recordForState(container, UnHealthyContainerStates.EMPTY_MISSING,
                  time));
          populateContainerStats(container,
              UnHealthyContainerStates.EMPTY_MISSING,
              unhealthyContainerStateStatsMap);
        }
        // A container cannot have any other records if it is missing so return
        return records;
      }

      if (container.isUnderReplicated()
          && !recordForStateExists.contains(
              UnHealthyContainerStates.UNDER_REPLICATED.toString())) {
        records.add(recordForState(
            container, UnHealthyContainerStates.UNDER_REPLICATED, time));
        populateContainerStats(container,
            UnHealthyContainerStates.UNDER_REPLICATED,
            unhealthyContainerStateStatsMap);
      }

      if (container.isOverReplicated()
          && !recordForStateExists.contains(
              UnHealthyContainerStates.OVER_REPLICATED.toString())) {
        records.add(recordForState(
            container, UnHealthyContainerStates.OVER_REPLICATED, time));
        populateContainerStats(container,
            UnHealthyContainerStates.OVER_REPLICATED,
            unhealthyContainerStateStatsMap);
      }

      if (container.isMisReplicated()
          && !recordForStateExists.contains(
              UnHealthyContainerStates.MIS_REPLICATED.toString())) {
        records.add(recordForState(
            container, UnHealthyContainerStates.MIS_REPLICATED, time));
        populateContainerStats(container,
            UnHealthyContainerStates.MIS_REPLICATED,
            unhealthyContainerStateStatsMap);
      }

      return records;
    }

    private static UnhealthyContainers recordForState(
        ContainerHealthStatus container, UnHealthyContainerStates state,
        long time) {
      UnhealthyContainers rec = new UnhealthyContainers();
      rec.setContainerId(container.getContainerID());
      if (state == UnHealthyContainerStates.MIS_REPLICATED) {
        rec.setExpectedReplicaCount(container.expectedPlacementCount());
        rec.setActualReplicaCount(container.actualPlacementCount());
        rec.setReplicaDelta(container.misReplicatedDelta());
        rec.setReason(container.misReplicatedReason());
      } else {
        rec.setExpectedReplicaCount(container.getReplicationFactor());
        rec.setActualReplicaCount(container.getReplicaCount());
        rec.setReplicaDelta(container.replicaDelta());
      }
      rec.setContainerState(state.toString());
      rec.setInStateSince(time);
      return rec;
    }

    private static boolean keepOverReplicatedRecord(
        ContainerHealthStatus container, UnhealthyContainersRecord rec) {
      if (container.isOverReplicated()) {
        updateExpectedReplicaCount(rec, container.getReplicationFactor());
        updateActualReplicaCount(rec, container.getReplicaCount());
        updateReplicaDelta(rec, container.replicaDelta());
        return true;
      }
      return false;
    }

    private static boolean keepUnderReplicatedRecord(
        ContainerHealthStatus container, UnhealthyContainersRecord rec) {
      if (container.isUnderReplicated()) {
        updateExpectedReplicaCount(rec, container.getReplicationFactor());
        updateActualReplicaCount(rec, container.getReplicaCount());
        updateReplicaDelta(rec, container.replicaDelta());
        return true;
      }
      return false;
    }

    private static boolean keepMisReplicatedRecord(
        ContainerHealthStatus container, UnhealthyContainersRecord rec) {
      if (container.isMisReplicated()) {
        updateExpectedReplicaCount(rec, container.expectedPlacementCount());
        updateActualReplicaCount(rec, container.actualPlacementCount());
        updateReplicaDelta(rec, container.misReplicatedDelta());
        updateReason(rec, container.misReplicatedReason());
        return true;
      }
      return false;
    }

    /**
     * With a Jooq record, if you update any field in the record, the record
     * is marked as changed, even if you updated it to the same value as it is
     * already set to. We only need to run a DB update statement if the record
     * has really changed. The methods below ensure we do not update the Jooq
     * record unless the values have changed and hence save a DB execution
     */
    private static void updateExpectedReplicaCount(
        UnhealthyContainersRecord rec, int expectedCount) {
      if (rec.getExpectedReplicaCount() != expectedCount) {
        rec.setExpectedReplicaCount(expectedCount);
      }
    }

    private static void updateActualReplicaCount(
        UnhealthyContainersRecord rec, int actualCount) {
      if (rec.getActualReplicaCount() != actualCount) {
        rec.setActualReplicaCount(actualCount);
      }
    }

    private static void updateReplicaDelta(
        UnhealthyContainersRecord rec, int delta) {
      if (rec.getReplicaDelta() != delta) {
        rec.setReplicaDelta(delta);
      }
    }

    private static void updateReason(
        UnhealthyContainersRecord rec, String reason) {
      if (!rec.getReason().equals(reason)) {
        rec.setReason(reason);
      }
    }
  }

  private static void populateContainerStats(
      ContainerHealthStatus container,
      UnHealthyContainerStates unhealthyState,
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap) {
    if (unhealthyContainerStateStatsMap.containsKey(unhealthyState)) {
      Map<String, Long> containerStatsMap =
          unhealthyContainerStateStatsMap.get(unhealthyState);
      containerStatsMap.compute(CONTAINER_COUNT,
          (containerCount, value) -> (value == null) ? 1 : (value + 1));
      containerStatsMap.compute(TOTAL_KEYS,
          (totalKeyCount, value) -> (value == null) ? container.getNumKeys() :
              (value + container.getNumKeys()));
      containerStatsMap.compute(TOTAL_USED_BYTES,
          (totalUsedBytes, value) -> (value == null) ?
              container.getContainer().getUsedBytes() :
              (value + container.getContainer().getUsedBytes()));
    }
  }
}

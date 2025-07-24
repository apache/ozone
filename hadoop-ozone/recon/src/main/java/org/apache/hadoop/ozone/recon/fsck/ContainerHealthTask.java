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

package org.apache.hadoop.ozone.recon.fsck;

import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.TOTAL_KEYS;
import static org.apache.hadoop.ozone.recon.ReconConstants.TOTAL_USED_BYTES;
import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.recon.metrics.ContainerHealthMetrics;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.util.Time;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that scans the list of containers and keeps track of containers with
 * no replicas in a SQL table.
 */
public class ContainerHealthTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthTask.class);
  public static final int FETCH_COUNT = Integer.parseInt(DEFAULT_FETCH_COUNT);

  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private final StorageContainerServiceProvider scmClient;
  private final ContainerManager containerManager;
  private final ContainerHealthSchemaManager containerHealthSchemaManager;
  private final ReconContainerMetadataManager reconContainerMetadataManager;
  private final PlacementPolicy placementPolicy;
  private final long interval;
  private Map<UnHealthyContainerStates, Map<String, Long>>
      unhealthyContainerStateStatsMapForTesting;

  private final Set<ContainerInfo> processedContainers = new HashSet<>();

  private final OzoneConfiguration conf;

  private final ReconTaskStatusUpdater taskStatusUpdater;
  private final ContainerHealthMetrics containerHealthMetrics;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ContainerHealthTask(
      ContainerManager containerManager,
      StorageContainerServiceProvider scmClient,
      ContainerHealthSchemaManager containerHealthSchemaManager,
      PlacementPolicy placementPolicy,
      ReconTaskConfig reconTaskConfig,
      ReconContainerMetadataManager reconContainerMetadataManager,
      OzoneConfiguration conf, ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {
    super(taskStatusUpdaterManager);
    this.scmClient = scmClient;
    this.containerHealthSchemaManager = containerHealthSchemaManager;
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.placementPolicy = placementPolicy;
    this.containerManager = containerManager;
    this.conf = conf;
    interval = reconTaskConfig.getMissingContainerTaskInterval().toMillis();
    this.taskStatusUpdater = getTaskStatusUpdater();
    this.containerHealthMetrics = ContainerHealthMetrics.create();
  }

  @Override
  public void run() {
    try {
      while (canRun()) {
        initializeAndRunTask();
        Thread.sleep(interval);
      }
    } catch (Throwable t) {
      LOG.error("Exception in Container Health task thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      taskStatusUpdater.setLastTaskRunStatus(-1);
      taskStatusUpdater.recordRunCompletion();
    }
  }

  @Override
  protected void runTask() throws Exception {
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
      LOG.debug("Container Health task thread took {} milliseconds to" +
              " process {} existing database records.",
          Time.monotonicNow() - start, existingCount);

      start = Time.monotonicNow();
      checkAndProcessContainers(unhealthyContainerStateStatsMap, currentTime);
      LOG.debug("Container Health Task thread took {} milliseconds to process containers",
          Time.monotonicNow() - start);
      taskStatusUpdater.setLastTaskRunStatus(0);
      processedContainers.clear();
      logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
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
      LOG.debug("Container Health task thread took {} milliseconds for" +
              " processing {} containers.", Time.monotonicNow() - start,
          containers.size());
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
      Map<UnHealthyContainerStates, Map<String, Long>> unhealthyContainerStateStatsMap) {

    unhealthyContainerStateStatsMapForTesting = new HashMap<>(unhealthyContainerStateStatsMap);

    // If any EMPTY_MISSING containers, then it is possible that such
    // containers got stuck in the closing state which never got
    // any replicas created on the datanodes. In this case, we log it as
    // EMPTY_MISSING in unhealthy container statistics but do not add it to the table.
    unhealthyContainerStateStatsMap.forEach((unhealthyContainerState, containerStateStatsMap) -> {
      // Reset metrics to zero if the map is empty for MISSING or UNDER_REPLICATED
      Optional.of(containerStateStatsMap)
          .filter(Map::isEmpty)
          .ifPresent(emptyMap -> resetContainerHealthMetrics(unhealthyContainerState));

      // Process and log the container state statistics
      String logMessage = containerStateStatsMap.entrySet().stream()
          .peek(entry -> updateContainerHealthMetrics(unhealthyContainerState, entry))
          .map(entry -> entry.getKey() + " -> " + entry.getValue())
          .collect(Collectors.joining(" , ", unhealthyContainerState + " **Container State Stats:** \n\t", ""));

      if (!containerStateStatsMap.isEmpty()) {
        LOG.info(logMessage);
      }
    });
  }

  /**
   * Helper method to update container health metrics using functional approach.
   */
  private void updateContainerHealthMetrics(UnHealthyContainerStates state, Map.Entry<String, Long> entry) {
    Map<UnHealthyContainerStates, Consumer<Long>> metricUpdaters = new HashMap<>();
    metricUpdaters.put(UnHealthyContainerStates.MISSING, containerHealthMetrics::setMissingContainerCount);
    metricUpdaters.put(UnHealthyContainerStates.UNDER_REPLICATED,
        containerHealthMetrics::setUnderReplicatedContainerCount);

    Optional.ofNullable(metricUpdaters.get(state))
        .filter(updater -> CONTAINER_COUNT.equals(entry.getKey()))
        .ifPresent(updater -> updater.accept(entry.getValue()));
  }

  /**
   * Resets container health metrics to zero using a functional approach.
   */
  private void resetContainerHealthMetrics(UnHealthyContainerStates state) {
    Map<UnHealthyContainerStates, Consumer<Long>> resetActions = new HashMap<>();
    resetActions.put(UnHealthyContainerStates.MISSING, containerHealthMetrics::setMissingContainerCount);
    resetActions.put(UnHealthyContainerStates.UNDER_REPLICATED,
        containerHealthMetrics::setUnderReplicatedContainerCount);

    Optional.ofNullable(resetActions.get(state)).ifPresent(action -> action.accept(0L));
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
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.NEGATIVE_SIZE, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
            UnHealthyContainerStates.REPLICA_MISMATCH, new HashMap<>());
  }

  private ContainerHealthStatus setCurrentContainer(long recordId)
      throws ContainerNotFoundException {
    ContainerInfo container =
        containerManager.getContainer(ContainerID.valueOf(recordId));
    Set<ContainerReplica> replicas =
        containerManager.getContainerReplicas(container.containerID());
    return new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, conf);
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
   * If a container is identified as missing, empty-missing, under-replicated,
   * over-replicated or mis-replicated, the method checks with SCM to determine
   * if it has been deleted, using {@code containerDeletedInSCM}. If the container is
   * deleted in SCM, the corresponding record is removed from Recon.
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
          // Set the current container if it's not already set
          if (currentContainer == null) {
            currentContainer = setCurrentContainer(rec.getContainerId());
          }
          // If the container ID has changed, finish processing the previous one
          if (currentContainer.getContainerID() != rec.getContainerId()) {
            completeProcessingContainer(
                currentContainer, existingRecords, currentTime,
                unhealthyContainerStateCountMap);
            existingRecords.clear();
            currentContainer = setCurrentContainer(rec.getContainerId());
          }

          //  Unhealthy Containers such as MISSING, UNDER_REPLICATED,
          // OVER_REPLICATED, MIS_REPLICATED can have their unhealthy states changed or retained.
          if (!ContainerHealthRecords.retainOrUpdateRecord(currentContainer, rec)) {
            rec.delete();
            LOG.info("DELETED existing unhealthy container record...for Container: {}",
                currentContainer.getContainerID());
          }

          // If the container is marked as MISSING and it's deleted in SCM, remove the record
          if (currentContainer.isMissing() && containerDeletedInSCM(currentContainer.getContainer())) {
            rec.delete();
            LOG.info("DELETED existing MISSING unhealthy container record...as container deleted " +
                    "in SCM as well: {}", currentContainer.getContainerID());
          }

          existingRecords.add(rec.getContainerState());
          // If the record was changed, update it
          if (rec.changed()) {
            rec.update();
          }
        } catch (ContainerNotFoundException cnf) {
          // If the container is not found, delete the record and reset currentContainer
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
          containerReplicas, placementPolicy,
          reconContainerMetadataManager, conf);

      if ((h.isHealthilyReplicated() && !h.areChecksumsMismatched()) || h.isDeleted()) {
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

  /**
   * Ensures the container's state in Recon is updated to match its state in SCM.
   *
   * If SCM reports the container as DELETED, this method attempts to transition
   * the container's state in Recon from CLOSED to DELETING, or from DELETING to
   * DELETED, based on the current state in Recon. It logs each transition attempt
   * and handles any exceptions that may occur.
   *
   * @param containerInfo the container whose state is being checked and potentially updated.
   * @return {@code true} if the container was found to be DELETED in SCM and the
   *         state transition was attempted in Recon; {@code false} otherwise.
   */
  private boolean containerDeletedInSCM(ContainerInfo containerInfo) {
    try {
      ContainerWithPipeline containerWithPipeline =
          scmClient.getContainerWithPipeline(containerInfo.getContainerID());
      if (containerWithPipeline.getContainerInfo().getState() ==
          HddsProtos.LifeCycleState.DELETED) {
        if (containerInfo.getState() == HddsProtos.LifeCycleState.CLOSED) {
          containerManager.updateContainerState(containerInfo.containerID(),
              HddsProtos.LifeCycleEvent.DELETE);
          LOG.debug("Successfully changed container {} state from CLOSED to DELETING.",
              containerInfo.containerID());
        }
        if (containerInfo.getState() == HddsProtos.LifeCycleState.DELETING &&
            containerManager.getContainerReplicas(containerInfo.containerID()).isEmpty()
        ) {
          containerManager.updateContainerState(containerInfo.containerID(),
              HddsProtos.LifeCycleEvent.CLEANUP);
          LOG.info("Successfully Deleted container {} from Recon.", containerInfo.containerID());
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
   * This method is used to handle containers with negative sizes. It logs an
   * error message.
   * @param containerHealthStatus
   * @param currentTime
   * @param unhealthyContainerStateStatsMap
   */
  private static void handleNegativeSizedContainers(
      ContainerHealthStatus containerHealthStatus, long currentTime,
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap) {
    // NEGATIVE_SIZE containers are also not inserted into the database.
    // This condition usually arises due to corrupted or invalid metadata, where
    // the container's size is inaccurately recorded as negative. Since this does not
    // represent a typical unhealthy scenario and may not have any meaningful
    // impact on system health, such containers are logged for investigation but
    // excluded from the UNHEALTHY_CONTAINERS table to maintain data integrity.
    ContainerInfo container = containerHealthStatus.getContainer();
    LOG.error("Container {} has negative size.", container.getContainerID());
    populateContainerStats(containerHealthStatus, UnHealthyContainerStates.NEGATIVE_SIZE,
        unhealthyContainerStateStatsMap);
  }

  /**
   * This method is used to handle containers that are empty and missing. It logs
   * a debug message.
   * @param containerHealthStatus
   * @param currentTime
   * @param unhealthyContainerStateStatsMap
   */
  private static void handleEmptyMissingContainers(
      ContainerHealthStatus containerHealthStatus, long currentTime,
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap) {
    // EMPTY_MISSING containers are not inserted into the database.
    // These containers typically represent those that were never written to
    // or remain in an incomplete state. Tracking such containers as unhealthy
    // would not provide valuable insights since they don't pose a risk or issue
    // to the system. Instead, they are logged for awareness, but not stored in
    // the UNHEALTHY_CONTAINERS table to avoid unnecessary entries.
    ContainerInfo container = containerHealthStatus.getContainer();
    LOG.debug("Empty container {} is missing. It will be logged in the " +
        "unhealthy container statistics, but no record will be created in the " +
        "UNHEALTHY_CONTAINERS table.", container.getContainerID());
    populateContainerStats(containerHealthStatus, EMPTY_MISSING,
        unhealthyContainerStateStatsMap);
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
      boolean returnValue;
      switch (UnHealthyContainerStates.valueOf(rec.getContainerState())) {
      case MISSING:
        returnValue = container.isMissing() && !container.isEmpty();
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
      case REPLICA_MISMATCH:
        returnValue = keepReplicaMismatchRecord(container, rec);
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
      if ((container.isHealthilyReplicated() && !container.areChecksumsMismatched()) || container.isDeleted()) {
        return records;
      }

      if (container.isMissing()) {
        boolean shouldAddRecord = !recordForStateExists.contains(UnHealthyContainerStates.MISSING.toString());
        if (!container.isEmpty()) {
          LOG.info("Non-empty container {} is missing. It has {} " +
                  "keys and {} bytes used according to SCM metadata. " +
                  "Please visit Recon's missing container page for a list of " +
                  "keys (and their metadata) mapped to this container.",
              container.getContainerID(), container.getNumKeys(),
              container.getContainer().getUsedBytes());

          if (shouldAddRecord) {
            records.add(recordForState(container, UnHealthyContainerStates.MISSING, time));
          }
          populateContainerStats(container, UnHealthyContainerStates.MISSING, unhealthyContainerStateStatsMap);
        } else {
          handleEmptyMissingContainers(container, time, unhealthyContainerStateStatsMap);
        }
        // A container cannot have any other records if it is missing, so return
        return records;
      }

      // For Negative sized containers we only log but not insert into DB
      if (container.getContainer().getUsedBytes() < 0) {
        handleNegativeSizedContainers(container, time,
            unhealthyContainerStateStatsMap);
      }

      if (container.isUnderReplicated()) {
        boolean shouldAddRecord = !recordForStateExists.contains(UnHealthyContainerStates.UNDER_REPLICATED.toString());
        if (shouldAddRecord) {
          records.add(recordForState(container, UnHealthyContainerStates.UNDER_REPLICATED, time));
        }
        populateContainerStats(container, UnHealthyContainerStates.UNDER_REPLICATED, unhealthyContainerStateStatsMap);
      }

      if (container.isOverReplicated()) {
        boolean shouldAddRecord = !recordForStateExists.contains(UnHealthyContainerStates.OVER_REPLICATED.toString());
        if (shouldAddRecord) {
          records.add(recordForState(container, UnHealthyContainerStates.OVER_REPLICATED, time));
        }
        populateContainerStats(container, UnHealthyContainerStates.OVER_REPLICATED, unhealthyContainerStateStatsMap);
      }

      if (container.areChecksumsMismatched()
              && !recordForStateExists.contains(
              UnHealthyContainerStates.REPLICA_MISMATCH.toString())) {
        records.add(recordForState(
                container, UnHealthyContainerStates.REPLICA_MISMATCH, time));
        populateContainerStats(container,
                UnHealthyContainerStates.REPLICA_MISMATCH,
                unhealthyContainerStateStatsMap);
      }

      if (container.isMisReplicated()) {
        boolean shouldAddRecord = !recordForStateExists.contains(UnHealthyContainerStates.MIS_REPLICATED.toString());
        if (shouldAddRecord) {
          records.add(recordForState(container, UnHealthyContainerStates.MIS_REPLICATED, time));
        }
        populateContainerStats(container, UnHealthyContainerStates.MIS_REPLICATED, unhealthyContainerStateStatsMap);
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

    private static boolean keepReplicaMismatchRecord(
            ContainerHealthStatus container, UnhealthyContainersRecord rec) {
      if (container.areChecksumsMismatched()) {
        updateExpectedReplicaCount(rec, container.getReplicationFactor());
        updateActualReplicaCount(rec, container.getReplicaCount());
        updateReplicaDelta(rec, container.replicaDelta());
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

  @Override
  public synchronized void stop() {
    super.stop();
    this.containerHealthMetrics.unRegister();
  }

  /**
   * Expose the unhealthyContainerStateStatsMap for testing purposes.
   */
  @VisibleForTesting
  public Map<UnHealthyContainerStates, Map<String, Long>> getUnhealthyContainerStateStatsMap() {
    return unhealthyContainerStateStatsMapForTesting;
  }

}

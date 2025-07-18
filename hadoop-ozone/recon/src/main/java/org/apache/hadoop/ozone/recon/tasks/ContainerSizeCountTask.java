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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;
import static org.apache.ozone.recon.schema.generated.tables.ContainerCountBySizeTable.CONTAINER_COUNT_BY_SIZE;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.UtilizationSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.daos.ContainerCountBySizeDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ContainerCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that scans the list of containers and keeps track of container sizes
 * binned into ranges (1KB, 2Kb…,4MB,…1GB,…5GB…) to the Recon
 * containerSize DB.
 */
public class ContainerSizeCountTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerSizeCountTask.class);

  private ContainerManager containerManager;
  private final long interval;
  private ContainerCountBySizeDao containerCountBySizeDao;
  private DSLContext dslContext;
  private HashMap<ContainerID, Long> processedContainers = new HashMap<>();
  private ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final ReconTaskStatusUpdater taskStatusUpdater;

  public ContainerSizeCountTask(
      ContainerManager containerManager,
      ReconTaskConfig reconTaskConfig,
      ContainerCountBySizeDao containerCountBySizeDao,
      UtilizationSchemaDefinition utilizationSchemaDefinition,
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {
    super(taskStatusUpdaterManager);
    this.containerManager = containerManager;
    this.containerCountBySizeDao = containerCountBySizeDao;
    this.dslContext = utilizationSchemaDefinition.getDSLContext();
    interval = reconTaskConfig.getContainerSizeCountTaskInterval().toMillis();
    this.taskStatusUpdater = getTaskStatusUpdater();
  }


  /**
   * The run() method is the main loop of the ContainerSizeCountTask class.
   * It periodically retrieves a list of containers from the containerManager,
   * and then calls the process() function to update the containerSizeCountMap.
   */
  @Override
  protected synchronized void run() {
    try {
      while (canRun()) {
        wait(interval);
        long startTime, endTime, duration, durationMilliseconds;
        startTime = System.nanoTime();
        initializeAndRunTask();
        endTime = System.nanoTime();
        duration = endTime - startTime;
        durationMilliseconds = duration / 1_000_000;
        LOG.debug("Elapsed Time in milliseconds for Process() execution: {}",
            durationMilliseconds);
      }
    } catch (Throwable t) {
      LOG.debug("Exception in Container Size Distribution task Thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      taskStatusUpdater.setLastTaskRunStatus(-1);
      taskStatusUpdater.recordRunCompletion();
    }
  }

  private void process(ContainerInfo container,
                       Map<ContainerSizeCountKey, Long> map) {
    final ContainerID id = container.containerID();
    final long usedBytes = container.getUsedBytes();
    final long currentSize;

    if (usedBytes < 0) {
      LOG.debug("Negative usedBytes ({}) for container {}, treating it as 0",
          usedBytes, id);
      currentSize = 0;
    } else {
      currentSize = usedBytes;
    }

    final Long previousSize = processedContainers.put(id, currentSize);
    if (previousSize != null) {
      decrementContainerSizeCount(previousSize, map);
    }
    incrementContainerSizeCount(currentSize, map);
  }

  @Override
  protected void runTask() throws Exception {
    final List<ContainerInfo> containers = containerManager.getContainers();
    if (processedContainers.isEmpty()) {
      int execute = dslContext.truncate(CONTAINER_COUNT_BY_SIZE).execute();
      LOG.debug("Deleted {} records from {}", execute, CONTAINER_COUNT_BY_SIZE);
    }
    processContainers(containers);
  }

  /**
   * The processContainers() function is responsible for updating the counts of
   * containers being tracked in a containerSizeCountMap based on the
   * ContainerInfo objects in the list containers. It then iterates through
   * the list of containers and does the following for each container:
   *
   * 1) If the container's state is not "deleted," it will be processed:
   *    - If the container is not present in processedContainers, it is a new
   *      container. Therefore, it is added to the processedContainers map, and
   *      the count for its size in the containerSizeCountMap is incremented by
   *      1 using the handlePutKeyEvent() function.
   *    - If the container is present in processedContainers but its size has
   *      been updated to a new size, the count for the old size in the
   *      containerSizeCountMap is decremented by 1 using the
   *      handleDeleteKeyEvent() function. Subsequently, the count for the new
   *      size is incremented by 1 using the handlePutKeyEvent() function.
   *
   * 2) If the container's state is "deleted," it is skipped, as deleted
   *    containers are not processed.
   *
   * After processing, the remaining containers inside the deletedContainers map
   * are those that are not in the cluster and need to be deleted from the total
   * size counts. Finally, the counts in the containerSizeCountMap are written
   * to the database using the writeCountsToDB() function.
   */
  @VisibleForTesting
  public void processContainers(List<ContainerInfo> containers) {
    lock.writeLock().lock();
    boolean processingFailed = false;

    try {
      final Map<ContainerSizeCountKey, Long> containerSizeCountMap
          = new HashMap<>();
      final Map<ContainerID, Long> deletedContainers
          = new HashMap<>(processedContainers);

      // Loop to handle container create and size-update operations
      for (ContainerInfo container : containers) {
        if (container.getState().equals(DELETED)) {
          continue; // Skip deleted containers
        }
        deletedContainers.remove(container.containerID());
        // For New Container being created
        try {
          process(container, containerSizeCountMap);
        } catch (Exception e) {
          processingFailed = true;
          // FIXME: it is a bug if there is an exception.
          LOG.error("FIXME: Failed to process " + container, e);
        }
      }

      // Method to handle Container delete operations
      handleContainerDeleteOperations(deletedContainers, containerSizeCountMap);

      // Write to the database
      writeCountsToDB(false, containerSizeCountMap);
      containerSizeCountMap.clear();
      LOG.debug("Completed a 'process' run of ContainerSizeCountTask.");
      taskStatusUpdater.setLastTaskRunStatus(processingFailed ? -1 : 0);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Populate DB with the counts of container sizes calculated
   * using the dao.
   * <p>
   * The writeCountsToDB function updates the database with the count of
   * container sizes. It does this by creating two lists of records to be
   * inserted or updated in the database. It iterates over the keys of the
   * containerSizeCountMap and creates a new record for each key. It then
   * checks whether the database has been truncated or not. If it has not been
   * truncated, it attempts to find the current count for the container size
   * in the database and either inserts a new record or updates the current
   * record with the updated count. If the database has been truncated,
   * it only inserts a new record if the count is non-zero. Finally, it
   * uses the containerCountBySizeDao to insert the new records and update
   * the existing records in the database.
   *
   * @param isDbTruncated that checks if the database has been truncated or not.
   * @param containerSizeCountMap stores counts of container sizes
   */
  private void writeCountsToDB(boolean isDbTruncated,
                               Map<ContainerSizeCountKey, Long>
                                   containerSizeCountMap) {
    List<ContainerCountBySize> insertToDb = new ArrayList<>();
    List<ContainerCountBySize> updateInDb = new ArrayList<>();

    containerSizeCountMap.keySet().forEach((ContainerSizeCountKey key) -> {
      ContainerCountBySize newRecord = new ContainerCountBySize();
      newRecord.setContainerSize(key.containerSizeUpperBound);
      newRecord.setCount(containerSizeCountMap.get(key));
      if (!isDbTruncated) {
        // Get the current count from database and update
        Record1<Long> recordToFind =
            dslContext.newRecord(
                    CONTAINER_COUNT_BY_SIZE.CONTAINER_SIZE)
                .value1(key.containerSizeUpperBound);
        ContainerCountBySize containerCountRecord =
            containerCountBySizeDao.findById(recordToFind.value1());
        if (containerCountRecord == null && newRecord.getCount() > 0L) {
          // insert new row only for non-zero counts.
          insertToDb.add(newRecord);
        } else if (containerCountRecord != null) {
          newRecord.setCount(containerCountRecord.getCount() +
              containerSizeCountMap.get(key));
          updateInDb.add(newRecord);
        }
      } else if (newRecord.getCount() > 0) {
        // insert new row only for non-zero counts.
        insertToDb.add(newRecord);
      }
    });
    containerCountBySizeDao.insert(insertToDb);
    containerCountBySizeDao.update(updateInDb);
  }

  /**
   *
   * Handles the deletion of containers by updating the tracking of processed containers
   * and adjusting the count of containers based on their sizes. When a container is deleted,
   * it is removed from the tracking of processed containers, and the count of containers
   * corresponding to its size is decremented in the container size count map.
   *
   * Used by process()
   *
   * @param deletedContainers  Map that stores the info of deleted containers
   * @param containerSizeCountMap Map that stores the counts of container sizes
   */
  private void handleContainerDeleteOperations(
      Map<ContainerID, Long> deletedContainers,
      Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    for (Map.Entry<ContainerID, Long> containerId :
        deletedContainers.entrySet()) {
      // processedContainers will only keep a track of all containers that have
      // been processed except DELETED containers.
      processedContainers.remove(containerId.getKey());
      long containerSize = deletedContainers.get(containerId.getKey());
      decrementContainerSizeCount(containerSize, containerSizeCountMap);
    }
  }

  /**
   * Calculate and update the count of containers being tracked by
   * containerSizeCountMap.
   *
   * The function calculates the upper size bound of the size range that the
   * given container size belongs to, using the getContainerSizeCountKey
   * function. It then increments the count of containers belonging to that
   * size range by 1. If the map does not contain an entry for the calculated
   * size range, the count is set to +1. The updated count is then stored in
   * the map under the calculated size range. This function is used to handle
   * a create event, i.e., when a container is created in the cluster.
   *
   * Used by process().
   *
   * @param containerSize to calculate the upperSizeBound
   */
  private static void incrementContainerSizeCount(long containerSize,
                    Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    updateContainerSizeCount(containerSize, 1, containerSizeCountMap);
  }

  /**
   * Calculate and update the count of container being tracked by
   * containerSizeCountMap.
   *
   * The function calculates the upper size bound of the size range that the
   * given container size belongs to, using the getContainerSizeCountKey
   * function. It then decrements the count of containers belonging to that
   * size range by 1. If the map does not contain an entry for the calculated
   * size range, the count is set to -1. The updated count is then stored in
   * the map under the calculated size range. This function is used to handle
   * a delete event, i.e., when a container is deleted from the cluster.
   *
   * Used by process().
   *
   * @param containerSize to calculate the upperSizeBound
   */
  private static void decrementContainerSizeCount(long containerSize,
                    Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    updateContainerSizeCount(containerSize, -1, containerSizeCountMap);
  }

  private static void updateContainerSizeCount(long containerSize, int delta,
                    Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    ContainerSizeCountKey key = getContainerSizeCountKey(containerSize);
    containerSizeCountMap.compute(key,
        (k, previous) -> previous != null ? previous + delta : delta);
  }

  /**
   * The purpose of this function is to categorize containers into different
   * size ranges, or "bins," based on their size.
   * The ContainerSizeCountKey object is used to store the upper bound value
   * for each size range, and is later used to lookup the count of containers
   * in that size range within a Map.
   *
   * If the container size is 0, the method sets the size of
   * ContainerSizeCountKey as zero without calculating the upper bound. Used by
   * decrementContainerSizeCount() and incrementContainerSizeCount()
   *
   * @param containerSize to calculate the upperSizeBound
   */
  private static ContainerSizeCountKey getContainerSizeCountKey(
      long containerSize) {
    // If containerSize is 0, return a ContainerSizeCountKey with size 0
    if (containerSize == 0) {
      return new ContainerSizeCountKey(0L);
    }

    // Otherwise, calculate the upperSizeBound
    return new ContainerSizeCountKey(
        ReconUtils.getContainerSizeUpperBound(containerSize));
  }

  /**
   *  The ContainerSizeCountKey class is a simple key class that has a single
   *  field, containerSizeUpperBound, which is a Long representing the upper
   *  bound of the container size range.
   */
  private static class ContainerSizeCountKey {

    private Long containerSizeUpperBound;

    ContainerSizeCountKey(
        Long containerSizeUpperBound) {
      this.containerSizeUpperBound = containerSizeUpperBound;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ContainerSizeCountKey) {
        ContainerSizeCountTask.ContainerSizeCountKey
            s = (ContainerSizeCountTask.ContainerSizeCountKey) obj;
        return
            containerSizeUpperBound.equals(s.containerSizeUpperBound);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (containerSizeUpperBound).hashCode();
    }
  }

}

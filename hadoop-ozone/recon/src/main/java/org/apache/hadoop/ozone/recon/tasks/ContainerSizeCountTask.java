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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.spi.ReconContainerSizeMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
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
  private ReconContainerSizeMetadataManager reconContainerSizeMetadataManager;
  private HashMap<ContainerID, Long> processedContainers = new HashMap<>();
  private ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final ReconTaskStatusUpdater taskStatusUpdater;

  public ContainerSizeCountTask(
      ContainerManager containerManager,
      ReconTaskConfig reconTaskConfig,
      ReconContainerSizeMetadataManager reconContainerSizeMetadataManager,
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {
    super(taskStatusUpdaterManager);
    this.containerManager = containerManager;
    this.reconContainerSizeMetadataManager = reconContainerSizeMetadataManager;
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
      // Clear RocksDB table instead of truncating Derby
      reconContainerSizeMetadataManager.clearContainerCountTable();
      LOG.debug("Cleared container count table in RocksDB");
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
   * Populate RocksDB with the counts of container sizes using batch operations.
   * <p>
   * The writeCountsToDB function updates RocksDB with the count of
   * container sizes. It uses batch operations for atomic writes. If the database
   * has not been truncated, it reads the current count from RocksDB, adds the
   * delta, and either updates the entry (if new count > 0) or deletes it
   * (if new count = 0). If the database has been truncated, it only inserts
   * entries with non-zero counts.
   *
   * @param isDbTruncated that checks if the database has been truncated or not.
   * @param containerSizeCountMap stores counts of container sizes
   */
  private void writeCountsToDB(boolean isDbTruncated,
                               Map<ContainerSizeCountKey, Long>
                                   containerSizeCountMap) {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<ContainerSizeCountKey, Long> entry :
          containerSizeCountMap.entrySet()) {
        ContainerSizeCountKey key = entry.getKey();
        Long delta = entry.getValue();

        if (!isDbTruncated) {
          // Get current count from RocksDB
          Long currentCount = reconContainerSizeMetadataManager
              .getContainerSizeCount(key);
          long newCount = (currentCount != null ? currentCount : 0L) + delta;

          if (newCount > 0) {
            reconContainerSizeMetadataManager.batchStoreContainerSizeCount(
                rdbBatchOperation, key, newCount);
          } else if (newCount == 0 && currentCount != null) {
            // Delete the entry if count reaches zero
            reconContainerSizeMetadataManager.batchDeleteContainerSizeCount(
                rdbBatchOperation, key);
          }
        } else if (delta > 0) {
          // After truncate, just insert non-zero counts
          reconContainerSizeMetadataManager.batchStoreContainerSizeCount(
              rdbBatchOperation, key, delta);
        }
      }
      reconContainerSizeMetadataManager.commitBatchOperation(rdbBatchOperation);
    } catch (IOException e) {
      LOG.error("Failed to write container size counts to RocksDB", e);
      throw new RuntimeException(e);
    }
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

}

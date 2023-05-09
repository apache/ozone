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


import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.hadoop.ozone.recon.schema.tables.ContainerCountBySizeTable.CONTAINER_COUNT_BY_SIZE;


/**
 * Class that scans the list of containers and keeps track of container sizes
 * binned into ranges (1KB, 2Kb…,4MB,…1GB,…5GB…) to the Recon
 * containerSize DB.
 */
public class ContainerSizeCountTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerSizeCountTask.class);

  private StorageContainerServiceProvider scmClient;
  private ContainerManager containerManager;
  private final long interval;
  private ContainerCountBySizeDao containerCountBySizeDao;
  private DSLContext dslContext;
  private HashMap<ContainerID, Long> processedContainers = new HashMap<>();
  private ReadWriteLock lock = new ReentrantReadWriteLock(true);

  public ContainerSizeCountTask(
      ContainerManager containerManager,
      StorageContainerServiceProvider scmClient,
      ReconTaskStatusDao reconTaskStatusDao,
      ReconTaskConfig reconTaskConfig,
      ContainerCountBySizeDao containerCountBySizeDao,
      UtilizationSchemaDefinition utilizationSchemaDefinition) {
    super(reconTaskStatusDao);
    this.scmClient = scmClient;
    this.containerManager = containerManager;
    this.containerCountBySizeDao = containerCountBySizeDao;
    this.dslContext = utilizationSchemaDefinition.getDSLContext();
    interval = reconTaskConfig.getContainerSizeCountTaskInterval().toMillis();
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
        final List<ContainerInfo> containers = containerManager.getContainers();
        if (processedContainers.isEmpty()) {
          try {
            int execute =
                dslContext.truncate(CONTAINER_COUNT_BY_SIZE).execute();
            LOG.info("Deleted {} records from {}", execute,
                CONTAINER_COUNT_BY_SIZE);
          } catch (Exception e) {
            LOG.error("An error occurred while truncating the table {}: {}",
                CONTAINER_COUNT_BY_SIZE, e.getMessage(), e);
            return;
          }
        }
        startTime = System.nanoTime();
        process(containers);
        endTime = System.nanoTime();
        duration = endTime - startTime;
        durationMilliseconds = duration / 1_000_000;
        LOG.info("Elapsed Time in milliseconds for Process() execution: {}",
            durationMilliseconds);
      }
    } catch (Throwable t) {
      LOG.error("Exception in Container Size Distribution task Thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }


  /**
   * The process() function is responsible for updating the counts of
   * containers being tracked in a containerSizeCountMap based on the
   * ContainerInfo objects in the list containers.It then iterates through
   * the list of containers and does the following for each container:
   *
   * 1) If the container is not present in processedContainers,
   * it is a new container, so it is added to the processedContainers map
   * and the count for its size in the containerSizeCountMap is incremented
   * by 1 using the handlePutKeyEvent() function.
   * 2) If the container is present in processedContainers but its size has
   * been updated to the new size then the count for the old size in the
   * containerSizeCountMap is decremented by 1 using the
   * handleDeleteKeyEvent() function. The count for the new size is then
   * incremented by 1 using the handlePutKeyEvent() function.
   * 3) If the container is not present in containers list, it means the
   * container has been deleted.
   * The remaining containers inside the deletedContainers map are the ones
   * that are not in the cluster and need to be deleted. Finally, the counts in
   * the containerSizeCountMap are written to the database using the
   * writeCountsToDB() function.
   */
  public void process(List<ContainerInfo> containers) {
    lock.writeLock().lock();
    try {
      HashMap<ContainerSizeCountKey, Long> containerSizeCountMap =
          new HashMap<>();
      HashMap<ContainerID, Long> deletedContainers = new HashMap<>();
      deletedContainers.putAll(processedContainers);

      // Loop to handle container create and size-update operations
      for (ContainerInfo container : containers) {
        // The containers present in the cache hence it is not yet deleted
        deletedContainers.remove(container.containerID());
        // For New Container being created
        if (!processedContainers.containsKey(container.containerID())) {
          incrementContainerSizeCount(container.getUsedBytes(),
              containerSizeCountMap);
          processedContainers.put(container.containerID(),
              container.getUsedBytes());
        } else if (processedContainers.get(container.containerID()) !=
            container.getUsedBytes()) { // If the Container Size is Updated
          decrementContainerSizeCount(
              processedContainers.get(container.containerID()),
              containerSizeCountMap);
          incrementContainerSizeCount(container.getUsedBytes(),
              containerSizeCountMap);
          processedContainers.put(container.containerID(),
              container.getUsedBytes());
        }
      }

      // Method to handle Container delete operations
      handleContainerDeleteOperations(deletedContainers, containerSizeCountMap);

      // Write to the database
      writeCountsToDB(false, containerSizeCountMap);
      containerSizeCountMap.clear();
      LOG.info("Completed a 'process' run of ContainerSizeCountTask.");
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

  @Override
  public String getTaskName() {
    return "ContainerSizeCountTask";
  }

  /**
   *
   * The handleContainerDeleteOperations() function loops through the entries
   * in the deletedContainers map and calls the handleDeleteKeyEvent() function
   * for each one. This will decrement the size counts of those containers by
   * one which are no longer present in the cluster
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
  private void incrementContainerSizeCount(long containerSize,
                   Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    ContainerSizeCountKey key = getContainerSizeCountKey(containerSize);
    Long count = containerSizeCountMap.containsKey(key) ?
        containerSizeCountMap.get(key) + 1L : 1L;
    containerSizeCountMap.put(key, count);
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
  private void decrementContainerSizeCount(long containerSize,
                   Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    ContainerSizeCountKey key = getContainerSizeCountKey(containerSize);
    Long count = containerSizeCountMap.containsKey(key) ?
        containerSizeCountMap.get(key) - 1L : -1L;
    containerSizeCountMap.put(key, count);
  }

  /**
   *
   * The purpose of this function is to categorize containers into different
   * size ranges, or "bins," based on their size.
   * The ContainerSizeCountKey object is used to store the upper bound value
   * for each size range, and is later used to lookup the count of containers
   * in that size range within a Map.
   *
   * Used by decrementContainerSizeCount() and incrementContainerSizeCount()
   *
   * @param containerSize to calculate the upperSizeBound
   */
  private ContainerSizeCountKey getContainerSizeCountKey(
      long containerSize) {
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

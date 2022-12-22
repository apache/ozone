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

import java.util.*;

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


  @Override
  protected synchronized void run() {
    try {
      while (canRun()) {
        wait(interval);

        final List<ContainerInfo> containers = containerManager.getContainers();
        if (processedContainers.isEmpty()) {
          long start = System.currentTimeMillis();
          reprocess(containers);
          long end = System.currentTimeMillis();
          LOG.info("Elapsed Time in milli seconds for Reprocess() execution: ",
              (end - start));
        } else {
          long start = System.currentTimeMillis();
          process(containers);
          long end = System.currentTimeMillis();
          LOG.info("Elapsed Time in milli seconds for Process() execution: ",
              (end - start));
        }
      }
    } catch (Throwable t) {
      LOG.error("Exception in Container Size Distribution task Thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void process(List<ContainerInfo> containers) {
    Map<ContainerSizeCountKey, Long> containerSizeCountMap = new HashMap<>();
    Map<ContainerID, Long> deletedContainers = new HashMap<>();
    deletedContainers.putAll(processedContainers);
    // Loop to handle container create and size-update operations
    for (ContainerInfo container : containers) {
      // The containers present in both the processed containers map and
      // also in cache are the ones that have not been deleted
      deletedContainers.remove(container);
      if (!processedContainers.containsKey( // For New Container being created
          container.getContainerID())) {
        processedContainers.put(container.containerID(),
            container.getUsedBytes());
        handlePutKeyEvent(container.getUsedBytes(),
            containerSizeCountMap);
      } else if (processedContainers.get(container.containerID()) !=
          container.getUsedBytes()) { // If the Container Size is Updated
        processedContainers.put(container.containerID(),
            container.getUsedBytes());
        handleDeleteKeyEvent(container.getUsedBytes(),
            containerSizeCountMap);
        handlePutKeyEvent(container.getUsedBytes(),
            containerSizeCountMap);
      }
    }
    // Loop to handle Container delete operations
    for (ContainerID containerId : deletedContainers.keySet()) {
      // The containers which were not present in cache but were still there
      // in the processed containers map are the ones that have been deleted
      processedContainers.remove(containerId);
      handleDeleteKeyEvent(deletedContainers.get(containerId),
          containerSizeCountMap); // Decreases the old value of range by one
    }
    writeCountsToDB(false, containerSizeCountMap);
    LOG.info("Completed a 'process' run of ContainerSizeCountTask.");
  }

  public void reprocess(List<ContainerInfo> containers) {
    Map<ContainerSizeCountKey, Long> containerSizeCountMap = new HashMap<>();
    // Truncate table before inserting new rows
    int execute = dslContext.delete(CONTAINER_COUNT_BY_SIZE).execute();
    LOG.info("Deleted {} records from {}", execute,
        CONTAINER_COUNT_BY_SIZE);
    for (int i = 0; i < containers.size(); i++) {
      processedContainers.put(containers.get(i).containerID(),
          containers.get(i).getUsedBytes());
      handlePutKeyEvent(containers.get(i).getUsedBytes(),
          containerSizeCountMap);
    }
    writeCountsToDB(true, containerSizeCountMap);
    LOG.info("Completed a 'reprocess' run of ContainerSizeCountTask.");
  }


  /**
   * Populate DB with the counts of container sizes calculated
   * using the dao.
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
   * Calculate and update the count of containers being tracked by
   * containerSizeCountMap.
   * Used by reprocess() and process().
   *
   * @param containerSize to calculate the upperSizeBound
   */
  private void handlePutKeyEvent(long containerSize,
                     Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    ContainerSizeCountKey key = getContainerSizeCountKey(containerSize);
    Long count = containerSizeCountMap.containsKey(key) ?
        containerSizeCountMap.get(key) + 1L : 1L;
    containerSizeCountMap.put(key, count);
  }

  /**
   * Calculate and update the count of container being tracked by
   * containerSizeCountMap.
   * Used by process().
   *
   * @param containerSize to calculate the upperSizeBound
   */
  private void handleDeleteKeyEvent(long containerSize,
                    Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    ContainerSizeCountKey key = getContainerSizeCountKey(containerSize);
    Long count = containerSizeCountMap.containsKey(key) ?
        containerSizeCountMap.get(key) - 1L : -1L;
    containerSizeCountMap.put(key, count);
  }

  private ContainerSizeCountKey getContainerSizeCountKey(
      long containerSize) {
    return new ContainerSizeCountKey(
        // Using the FileSize UpperBound Calculator for now, we can replace it
        // with a new UpperBound Calculator for containers only
        ReconUtils.getFileSizeUpperBound(containerSize));
  }


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

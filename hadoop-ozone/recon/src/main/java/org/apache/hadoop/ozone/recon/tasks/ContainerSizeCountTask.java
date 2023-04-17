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


import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerCountBySize;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hadoop.ozone.recon.schema.tables.ContainerCountBySizeTable.CONTAINER_COUNT_BY_SIZE;


/**
 * Class that scans the list of containers and keeps track of container sizes
 * binned into ranges (1KB, 2Kb..,4MB,.., 1TB,..1PB) to the Recon
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


  public ContainerSizeCountTask(
      ContainerManager containerManager,
      StorageContainerServiceProvider scmClient,
      ReconTaskStatusDao reconTaskStatusDao,
      ReconTaskConfig reconTaskConfig,
      ContainerCountBySizeDao containerCountBySizeDao,
      DSLContext dslContext) {
    super(reconTaskStatusDao);
    this.scmClient = scmClient;
    this.containerManager = containerManager;
    this.containerCountBySizeDao = containerCountBySizeDao;
    this.dslContext = dslContext;
    interval = reconTaskConfig.getContainerSizeCountTaskInterval().toMillis();
  }


  @Override
  protected synchronized void run() {
    try {
      while (canRun()) {
        wait(interval);

        final List<ContainerInfo> containers = containerManager.getContainers();
        Map<ContainerSizeCountKey, Long> containerSizeCountMap =
            new HashMap<>();
        for (int i = 0; i < containers.size(); i++) {
          handlePutKeyEvent(containers.get(i), containerSizeCountMap);
        }
        // Truncate table before inserting new rows
        int execute = dslContext.delete(CONTAINER_COUNT_BY_SIZE).execute();
        LOG.info("Deleted {} records from {}", execute,
            CONTAINER_COUNT_BY_SIZE);

        writeCountsToDB(true, containerSizeCountMap);

        LOG.info("Completed a 'reprocess' run of ContainerSizeCountTask.");
      }
    } catch (Throwable t) {
      LOG.error("Exception in Container Size Distribution task Thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Populate DB with the counts of container sizes calculated
   * using the dao.
   */
  private void writeCountsToDB(boolean isDbTruncated,
                               Map<ContainerSizeCountKey, Long>
                                   containerSizeCountMap) {
    List<ContainerCountBySize> insertToDb = new ArrayList<>();

    containerSizeCountMap.keySet().forEach((ContainerSizeCountKey key) -> {
      ContainerCountBySize newRecord = new ContainerCountBySize();
      newRecord.setContainerSize(key.containerSizeUpperBound);
      newRecord.setCount(containerSizeCountMap.get(key));
      if (!isDbTruncated) {
        insertToDb.add(newRecord);
      }
    });
    containerCountBySizeDao.insert(insertToDb);
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
   * @param containerInfo OmKey being updated for count
   */
  private void handlePutKeyEvent(ContainerInfo containerInfo,
                     Map<ContainerSizeCountKey, Long> containerSizeCountMap) {
    ContainerSizeCountKey key = getContainerSizeCountKey(containerInfo);
    Long count = containerSizeCountMap.containsKey(key) ?
        containerSizeCountMap.get(key) + 1L : 1L;
    containerSizeCountMap.put(key, count);
  }

  private ContainerSizeCountKey getContainerSizeCountKey(
      ContainerInfo containerInfo) {
    return new ContainerSizeCountKey(
        ReconUtils.getFileSizeUpperBound(containerInfo.getUsedBytes()));
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

/**
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

import java.util.Set;

import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.hadoop.ozone.recon.schema.tables.daos.MissingContainersDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.MissingContainers;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * Class that scans the list of containers and keeps track of containers with
 * no replicas in a SQL table.
 */
public class MissingContainerTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(MissingContainerTask.class);

  private OzoneStorageContainerManager scm;
  private MissingContainersDao missingContainersDao;
  private ReconTaskStatusDao reconTaskStatusDao;

  private Thread fsckMonitor;
  private volatile boolean running;

  public MissingContainerTask(ReconStorageContainerManagerFacade scm,
                              Configuration sqlConfiguration) {
    this.scm = scm;
    this.missingContainersDao = new MissingContainersDao(sqlConfiguration);
    this.reconTaskStatusDao = new ReconTaskStatusDao(sqlConfiguration);
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
        getClass().getName(), 0L, 0L);
    if (!reconTaskStatusDao.existsById(getClass().getName())) {
      reconTaskStatusDao.insert(reconTaskStatusRecord);
      LOG.info("Registered {} task ", getClass().getName());
    }
  }

  public void start() {
    if (!isRunning()) {
      LOG.info("Starting Missing Container Monitor Thread.");
      running = true;
      fsckMonitor = new Thread(this::run);
      fsckMonitor.setName("MissingContainerMonitor");
      fsckMonitor.setDaemon(true);
      fsckMonitor.start();
    } else {
      LOG.info("Missing Container Monitor Thread is already running.");
    }
  }

  /**
   * Stops Replication Monitor thread.
   */
  public synchronized void stop() {
    if (running) {
      LOG.info("Stopping Missing Container Monitor Thread.");
      running = false;
      notifyAll();
    } else {
      LOG.info("Missing Container Monitor Thread is not running.");
    }
  }

  public void run() {
    long currentTime = System.currentTimeMillis();
    ContainerManager containerManager = scm.getContainerManager();
    containerManager.getContainerIDs().forEach(containerID -> {
      try {
        Set<ContainerReplica> containerReplicas =
            containerManager.getContainerReplicas(containerID);
        if (CollectionUtils.isEmpty(containerReplicas)) {
          if (!missingContainersDao.existsById(containerID.getId())) {
            MissingContainers newRecord =
                new MissingContainers(containerID.getId(), currentTime);
            missingContainersDao.insert(newRecord);
          }
        } else {
          if (missingContainersDao.existsById(containerID.getId())) {
            missingContainersDao.deleteById(containerID.getId());
          }
        }
        reconTaskStatusDao.update(new ReconTaskStatus(getClass().getName(),
            System.currentTimeMillis(), 0L));
      } catch (ContainerNotFoundException e) {
        LOG.error("Container not found while finding missing containers", e);
      }
    });
  }

  public boolean isRunning() {
    if (!running) {
      synchronized (this) {
        return fsckMonitor != null
            && fsckMonitor.isAlive();
      }
    }
    return true;
  }
}

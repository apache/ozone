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

import java.util.Set;

import javax.inject.Inject;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.persistence.ContainerSchemaManager;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.util.Time;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * Class that scans the list of containers and keeps track of containers with
 * no replicas in a SQL table.
 */
public class MissingContainerTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(MissingContainerTask.class);

  private ContainerManager containerManager;
  private ContainerSchemaManager containerSchemaManager;
  private static final long INTERVAL = 5 * 60 * 1000L;

  @Inject
  public MissingContainerTask(
      OzoneStorageContainerManager ozoneStorageContainerManager,
      ReconTaskStatusDao reconTaskStatusDao,
      ContainerSchemaManager containerSchemaManager) {
    super(reconTaskStatusDao);
    this.containerSchemaManager = containerSchemaManager;
    this.containerManager = ozoneStorageContainerManager.getContainerManager();
  }

  public synchronized void run() {
    try {
      while (canRun()) {
        long start = Time.monotonicNow();
        long currentTime = System.currentTimeMillis();
        final Set<ContainerID> containerIds =
            containerManager.getContainerIDs();
        containerIds.forEach(containerID ->
            processContainer(containerID, currentTime));
        recordSingleRunCompletion();
        LOG.info("Missing Container task Thread took {} milliseconds for" +
                " processing {} containers.", Time.monotonicNow() - start,
            containerIds.size());
        wait(INTERVAL);
      }
    } catch (Throwable t) {
      LOG.error("Exception in Missing Container task Thread.", t);
    }
  }

  private void processContainer(ContainerID containerID, long currentTime) {
    try {
      Set<ContainerReplica> containerReplicas =
          containerManager.getContainerReplicas(containerID);
      // check if a container has 0 replicas or if all available replicas
      // are marked UNHEALTHY.
      boolean isAllUnhealthy =
          containerReplicas.stream().allMatch(replica ->
              replica.getState().equals(State.UNHEALTHY));
      boolean isMissingContainer =
          containerSchemaManager.isMissingContainer(containerID.getId());
      if (CollectionUtils.isEmpty(containerReplicas) || isAllUnhealthy) {
        if (!isMissingContainer) {
          LOG.info("Found a missing container with ID {}. Adding it to the " +
              "database", containerID.getId());
          containerSchemaManager.addMissingContainer(containerID.getId(),
              currentTime);
        }
      } else {
        if (isMissingContainer) {
          containerSchemaManager.deleteMissingContainer(containerID.getId());
        }
      }
    } catch (ContainerNotFoundException e) {
      LOG.error("Container not found while finding missing containers", e);
    }
  }
}

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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FINALIZE;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.persistence.ContainerSchemaManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's overriding implementation of SCM's Container Manager.
 */
public class ReconContainerManager extends SCMContainerManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerManager.class);
  private StorageContainerServiceProvider scmClient;
  private ContainerSchemaManager containerSchemaManager;

  /**
   * Constructs a mapping class that creates mapping between container names
   * and pipelines.
   * <p>
   * passed to LevelDB and this memory is allocated in Native code space.
   * CacheSize is specified
   * in MB.
   *
   * @throws IOException on Failure.
   */
  public ReconContainerManager(
      ConfigurationSource conf,
      Table<ContainerID, ContainerInfo> containerStore,
      BatchOperationHandler batchHandler,
      PipelineManager pipelineManager,
      StorageContainerServiceProvider scm,
      ContainerSchemaManager containerSchemaManager) throws IOException {
    super(conf, containerStore, batchHandler, pipelineManager);
    this.scmClient = scm;
    this.containerSchemaManager = containerSchemaManager;
  }

  /**
   * Check and add new container if not already present in Recon.
   *
   * @param containerID     containerID to check.
   * @param datanodeDetails Datanode from where we got this container.
   * @throws IOException on Error.
   */
  public void checkAndAddNewContainer(ContainerID containerID,
      ContainerReplicaProto.State replicaState,
      DatanodeDetails datanodeDetails)
      throws IOException {
    if (!exists(containerID)) {
      LOG.info("New container {} got from {}.", containerID,
          datanodeDetails.getHostName());
      ContainerWithPipeline containerWithPipeline =
          scmClient.getContainerWithPipeline(containerID.getId());
      LOG.debug("Verified new container from SCM {}, {} ",
          containerID, containerWithPipeline.getPipeline().getId());
      // If no other client added this, go ahead and add this container.
      if (!exists(containerID)) {
        addNewContainer(containerID.getId(), containerWithPipeline);
      }
    } else {
      // Check if container state is not open. In SCM, container state
      // changes to CLOSING first, and then the close command is pushed down
      // to Datanodes. Recon 'learns' this from DN, and hence replica state
      // will move container state to 'CLOSING'.
      ContainerInfo containerInfo = getContainer(containerID);
      if (containerInfo.getState().equals(HddsProtos.LifeCycleState.OPEN)
          && !replicaState.equals(ContainerReplicaProto.State.OPEN)
          && isHealthy(replicaState)) {
        LOG.info("Container {} has state OPEN, but Replica has State {}.",
            containerID, replicaState);
        updateContainerState(containerID, FINALIZE);
      }
    }
  }

  private boolean isHealthy(ContainerReplicaProto.State replicaState) {
    return replicaState != ContainerReplicaProto.State.UNHEALTHY
        && replicaState != ContainerReplicaProto.State.INVALID
        && replicaState != ContainerReplicaProto.State.DELETED;
  }

  /**
   * Adds a new container to Recon's container manager.
   * @param containerId id
   * @param containerWithPipeline containerInfo with pipeline info
   * @throws IOException on Error.
   */
  public void addNewContainer(long containerId,
                              ContainerWithPipeline containerWithPipeline)
      throws IOException {
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();
    getLock().lock();
    try {
      boolean success = false;
      if (containerInfo.getState().equals(HddsProtos.LifeCycleState.OPEN)) {
        PipelineID pipelineID = containerWithPipeline.getPipeline().getId();
        if (getPipelineManager().containsPipeline(pipelineID)) {
          getContainerStateManager().addContainerInfo(containerId,
              containerInfo, getPipelineManager(),
              containerWithPipeline.getPipeline());
          success = true;
        } else {
          // Get open container for a pipeline that Recon does not know
          // about yet. Cannot update internal state until pipeline is synced.
          LOG.warn(String.format(
              "Pipeline %s not found. Cannot add container %s",
              pipelineID, containerInfo.containerID()));
        }
      } else {
        // Non 'Open' Container. No need to worry about pipeline since SCM
        // returns a random pipelineID.
        getContainerStateManager().addContainerInfo(containerId,
            containerInfo, getPipelineManager(), null);
        success = true;
      }
      if (success) {
        addContainerToDB(containerInfo);
        LOG.info("Successfully added container {} to Recon.",
            containerInfo.containerID());
      }
    } catch (IOException ex) {
      LOG.info("Exception while adding container {} .",
          containerInfo.containerID(), ex);
      getPipelineManager().removeContainerFromPipeline(
          containerInfo.getPipelineID(),
          new ContainerID(containerInfo.getContainerID()));
      throw ex;
    } finally {
      getLock().unlock();
    }
  }

  /**
   * Add a container Replica for given DataNode.
   *
   * @param containerID
   * @param replica
   */
  @Override
  public void updateContainerReplica(ContainerID containerID,
      ContainerReplica replica)
      throws ContainerNotFoundException {
    super.updateContainerReplica(containerID, replica);
    // Update container_history table
    long currentTime = System.currentTimeMillis();
    String datanodeHost = replica.getDatanodeDetails().getHostName();
    containerSchemaManager.upsertContainerHistory(containerID.getId(),
        datanodeHost, currentTime);
  }

  public ContainerSchemaManager getContainerSchemaManager() {
    return containerSchemaManager;
  }
}

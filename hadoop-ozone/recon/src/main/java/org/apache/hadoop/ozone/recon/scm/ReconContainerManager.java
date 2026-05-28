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

package org.apache.hadoop.ozone.recon.scm;

import static java.util.Comparator.comparingLong;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.CLEANUP;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.DELETE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FINALIZE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.QUASI_CLOSE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's overriding implementation of SCM's Container Manager.
 */
public class ReconContainerManager extends ContainerManagerImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerManager.class);
  private final StorageContainerServiceProvider scmClient;
  private final PipelineManager pipelineManager;
  private final ContainerHealthSchemaManager containerHealthSchemaManager;
  private final ReconContainerMetadataManager cdbServiceProvider;
  private final Table<DatanodeID, DatanodeDetails> nodeDB;
  // Container ID -> Datanode UUID -> Timestamp
  private final Map<Long, Map<UUID, ContainerReplicaHistory>> replicaHistoryMap;
  // Pipeline -> # of open containers
  private final Map<PipelineID, Integer> pipelineToOpenContainer;

  @SuppressWarnings("parameternumber")
  public ReconContainerManager(
      Configuration conf,
      DBStore store,
      Table<ContainerID, ContainerInfo> containerStore,
      PipelineManager pipelineManager,
      StorageContainerServiceProvider scm,
      ContainerHealthSchemaManager containerHealthSchemaManager,
      ReconContainerMetadataManager reconContainerMetadataManager,
      SCMHAManager scmhaManager,
      SequenceIdGenerator sequenceIdGen,
      ContainerReplicaPendingOps pendingOps)
      throws IOException {
    super(conf, scmhaManager, sequenceIdGen, pipelineManager, containerStore,
        pendingOps);
    this.scmClient = scm;
    this.pipelineManager = pipelineManager;
    this.containerHealthSchemaManager = containerHealthSchemaManager;
    this.cdbServiceProvider = reconContainerMetadataManager;
    this.nodeDB = ReconSCMDBDefinition.NODES.getTable(store);
    this.replicaHistoryMap = new ConcurrentHashMap<>();
    this.pipelineToOpenContainer = new ConcurrentHashMap<>();
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
      throws IOException, InvalidStateTransitionException {
    if (!containerExist(containerID)) {
      LOG.info("New container {} got from {}.", containerID,
          datanodeDetails.getHostName());
      ContainerWithPipeline containerWithPipeline =
          scmClient.getContainerWithPipeline(containerID.getId());
      Pipeline pipeline = containerWithPipeline.getPipeline();
      LOG.debug("Verified new container from SCM {}, {} ",
          containerID, pipeline != null ? pipeline.getId() : "<null-pipeline>");
      // no need call "containerExist" to check, because
      // 1 containerExist and addNewContainer can not be atomic
      // 2 addNewContainer will double check the existence
      addNewContainer(containerWithPipeline);
    } else {
      checkContainerStateAndUpdate(containerID, replicaState);
    }
  }

  /**
   * Check and add new containers in batch if not already present in Recon.
   *
   * @param containerReplicaProtoList list of containerReplicaProtos.
   */
  public void checkAndAddNewContainerBatch(
      List<ContainerReplicaProto> containerReplicaProtoList) {
    Map<Boolean, List<ContainerReplicaProto>> containers =
        containerReplicaProtoList.parallelStream()
        .collect(Collectors.groupingBy(c ->
            containerExist(ContainerID.valueOf(c.getContainerID()))));

    List<ContainerReplicaProto> existContainers = null;
    if (containers.containsKey(true)) {
      existContainers = containers.get(true);
    }
    List<Long> noExistContainers = null;
    if (containers.containsKey(false)) {
      noExistContainers = containers.get(false).parallelStream().
          map(ContainerReplicaProto::getContainerID)
          .collect(Collectors.toList());
    }

    if (null != noExistContainers) {
      List<ContainerWithPipeline> verifiedContainerPipeline =
          scmClient.getExistContainerWithPipelinesInBatch(noExistContainers);
      LOG.debug("{} new containers have been verified by SCM , " +
              "{} containers not found at SCM",
          verifiedContainerPipeline.size(),
          noExistContainers.size() - verifiedContainerPipeline.size());
      for (ContainerWithPipeline cwp : verifiedContainerPipeline) {
        try {
          addNewContainer(cwp);
        } catch (IOException ioe) {
          LOG.error("Exception while checking and adding new container.", ioe);
        }
      }
    }

    if (null != existContainers) {
      for (ContainerReplicaProto crp : existContainers) {
        ContainerID cID = ContainerID.valueOf(crp.getContainerID());
        ContainerReplicaProto.State crpState = crp.getState();
        try {
          checkContainerStateAndUpdate(cID, crpState);
        } catch (Exception ioe) {
          LOG.error("Exception while " +
              "checkContainerStateAndUpdate container", ioe);
        }
      }
    }
  }

  /**
   * Transitions a container from OPEN to CLOSING, keeping the per-pipeline
   * open-container count in {@link #pipelineToOpenContainer} accurate.
   *
   * <p>Must be called whenever an OPEN container is moved to CLOSING so that
   * the pipeline's open-container count stays consistent. Both the DN-report
   * driven path ({@link #checkContainerStateAndUpdate}) and the periodic
   * targeted sync path use this method to avoid divergence in the count exposed
   * to the Recon Node API.
   *
   * <p>If the container was recorded without a pipeline, the count decrement is
   * safely skipped.
   *
   * @param containerID   container to advance from OPEN to CLOSING
   * @param containerInfo already-fetched ContainerInfo for the container
   * @throws IOException                     if the state update fails
   * @throws InvalidStateTransitionException if the state transition is invalid
   */
  void transitionOpenToClosing(ContainerID containerID,
                               ContainerInfo containerInfo)
      throws IOException, InvalidStateTransitionException {
    PipelineID pipelineID = containerInfo.getPipelineID();
    if (pipelineID != null) {
      int curCnt = pipelineToOpenContainer.getOrDefault(pipelineID, 0);
      if (curCnt == 1) {
        pipelineToOpenContainer.remove(pipelineID);
      } else if (curCnt > 0) {
        pipelineToOpenContainer.put(pipelineID, curCnt - 1);
      }
    }
    updateContainerState(containerID, FINALIZE);
  }

  /**
   * Check if Recon's container lifecycle state can be corrected based on a DN
   * replica report and SCM's authoritative state.
   *
   * <p>OPEN uses the DN replica state as the signal: only a healthy non-OPEN
   * replica can move Recon to CLOSING. For CLOSING, the DN report is only a
   * wake-up signal and Recon queries SCM before advancing. For DELETED, Recon
   * recovers only on a live terminal replica report and only when SCM still
   * reports the container as live.
   *
   * @param containerID containerID to check
   * @param state replica state reported by a DataNode
   */
  private void checkContainerStateAndUpdate(ContainerID containerID,
                                            ContainerReplicaProto.State state)
      throws IOException, InvalidStateTransitionException {
    ContainerInfo containerInfo = getContainer(containerID);
    HddsProtos.LifeCycleState reconState = containerInfo.getState();

    if (reconState == HddsProtos.LifeCycleState.OPEN) {
      if (state.equals(ContainerReplicaProto.State.OPEN) || !isHealthy(state)) {
        return;
      }
      LOG.info("Container {} has state OPEN, but given state is {}.",
          containerID, state);
      transitionOpenToClosing(containerID, containerInfo);
      return;
    }

    if (reconState == HddsProtos.LifeCycleState.CLOSING) {
      reconcileClosingContainerFromScm(containerID);
      return;
    }

    if (reconState == HddsProtos.LifeCycleState.DELETED) {
      recoverDeletedContainerFromScm(containerID, state);
    }
  }

  private void reconcileClosingContainerFromScm(ContainerID containerID)
      throws IOException, InvalidStateTransitionException {
    HddsProtos.LifeCycleState scmState = getScmContainerState(containerID);

    if (scmState == HddsProtos.LifeCycleState.QUASI_CLOSED) {
      updateContainerState(containerID, QUASI_CLOSE);
      LOG.info("Container {} advanced to QUASI_CLOSED in Recon "
          + "based on SCM state.", containerID);
    } else if (scmState == HddsProtos.LifeCycleState.CLOSED) {
      updateContainerState(containerID, CLOSE);
      LOG.info("Container {} advanced to CLOSED in Recon based on SCM state.",
          containerID);
    } else if (scmState == HddsProtos.LifeCycleState.DELETING
        || scmState == HddsProtos.LifeCycleState.DELETED) {
      updateContainerState(containerID, CLOSE);
      updateContainerState(containerID, DELETE);
      if (scmState == HddsProtos.LifeCycleState.DELETED) {
        updateContainerState(containerID, CLEANUP);
      }
      LOG.info("Container {} advanced to {} in Recon based on SCM state.",
          containerID, scmState);
    }
  }

  private void recoverDeletedContainerFromScm(ContainerID containerID,
      ContainerReplicaProto.State replicaState) throws IOException {
    if (replicaState != ContainerReplicaProto.State.CLOSED
        && replicaState != ContainerReplicaProto.State.QUASI_CLOSED) {
      return;
    }

    ContainerWithPipeline scmContainer =
        scmClient.getContainerWithPipeline(containerID.getId());
    HddsProtos.LifeCycleState scmState =
        scmContainer.getContainerInfo().getState();
    if (scmState != HddsProtos.LifeCycleState.CLOSED
        && scmState != HddsProtos.LifeCycleState.QUASI_CLOSED) {
      LOG.info("Container {} is DELETED in Recon and DN reported {}, "
          + "but SCM state is {}. Skipping recovery.",
          containerID, replicaState, scmState);
      return;
    }

    deleteContainer(containerID);
    addNewContainer(scmContainer);
    LOG.info("Recovered container {} from DELETED in Recon to {} based on "
        + "DN report {} and SCM state {}.",
        containerID, scmState, replicaState, scmState);
  }

  private HddsProtos.LifeCycleState getScmContainerState(ContainerID containerID)
      throws IOException {
    return scmClient.getContainerWithPipeline(containerID.getId())
        .getContainerInfo().getState();
  }

  private boolean isHealthy(ContainerReplicaProto.State replicaState) {
    return replicaState != ContainerReplicaProto.State.UNHEALTHY
        && replicaState != ContainerReplicaProto.State.INVALID
        && replicaState != ContainerReplicaProto.State.DELETED;
  }

  /**
   * Adds a new container to Recon's container manager.
   *
   * <p>For OPEN containers a valid pipeline is expected. If the pipeline is
   * {@code null} (e.g., returned by SCM when the pipeline has already been
   * cleaned up for a QUASI_CLOSED container that arrived via the sync path),
   * the container is still recorded in the state manager without pipeline
   * tracking so that it is not permanently absent from Recon.
   *
   * @param containerWithPipeline containerInfo with pipeline info
   *                              (pipeline may be null)
   * @throws IOException on Error.
   */
  public void addNewContainer(ContainerWithPipeline containerWithPipeline)
      throws IOException {
    ReconPipelineManager reconPipelineManager = (ReconPipelineManager) pipelineManager;
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();
    try {
      if (containerInfo.getState().equals(HddsProtos.LifeCycleState.OPEN)) {
        Pipeline pipeline = containerWithPipeline.getPipeline();
        if (pipeline != null) {
          PipelineID pipelineID = pipeline.getId();
          // Check if the pipeline is present in Recon; add it if not.
          if (reconPipelineManager.addPipeline(pipeline)) {
            LOG.info("Added new pipeline {} to Recon pipeline metadata from "
                + "SCM.", pipelineID);
          }
          getContainerStateManager().addContainer(containerInfo.getProtobuf());
          pipelineManager.addContainerToPipeline(pipelineID,
              containerInfo.containerID());
          // Update open container count on all datanodes on this pipeline.
          pipelineToOpenContainer.put(pipelineID,
              pipelineToOpenContainer.getOrDefault(pipelineID, 0) + 1);
          LOG.info("Successfully added OPEN container {} with pipeline {} to "
              + "Recon.", containerInfo.containerID(), pipelineID);
        } else {
          // Pipeline not available (cleaned up in SCM). Record the container
          // without pipeline tracking so it is not permanently absent from Recon.
          getContainerStateManager().addContainer(containerInfo.getProtobuf());
          LOG.warn("Added OPEN container {} to Recon without pipeline "
              + "(pipeline was null - likely cleaned up on SCM side). "
              + "Pipeline tracking unavailable for this container.",
              containerInfo.containerID());
        }
      } else {
        getContainerStateManager().addContainer(containerInfo.getProtobuf());
        LOG.info("Successfully added container {} in state {} to Recon.",
            containerInfo.containerID(), containerInfo.getState());
      }
    } catch (IOException ex) {
      LOG.info("Exception while adding container {}.",
          containerInfo.containerID(), ex);
      PipelineID pipelineID = containerInfo.getPipelineID();
      if (pipelineID != null) {
        pipelineManager.removeContainerFromPipeline(
            pipelineID, ContainerID.valueOf(containerInfo.getContainerID()));
      }
      throw ex;
    }
  }

  /**
   * Add a container Replica for given DataNode.
   */
  @Override
  public void updateContainerReplica(ContainerID containerID,
      ContainerReplica replica)
      throws ContainerNotFoundException {
    super.updateContainerReplica(containerID, replica);

    final long currTime = System.currentTimeMillis();
    final long id = containerID.getId();
    final DatanodeDetails dnInfo = replica.getDatanodeDetails();
    final UUID uuid = dnInfo.getUuid();

    // Map from DataNode UUID to replica last seen time
    final Map<UUID, ContainerReplicaHistory> replicaLastSeenMap =
        replicaHistoryMap.get(id);

    boolean flushToDB = false;
    long bcsId = replica.getSequenceId() != null ? replica.getSequenceId() : -1;
    String state = replica.getState().toString();
    ContainerChecksums checksums = replica.getChecksums();

    // If replica doesn't exist in in-memory map, add to DB and add to map
    if (replicaLastSeenMap == null) {
      // putIfAbsent to avoid TOCTOU
      replicaHistoryMap.putIfAbsent(id,
          new ConcurrentHashMap<UUID, ContainerReplicaHistory>() {{
            put(uuid, new ContainerReplicaHistory(uuid, currTime, currTime,
                bcsId, state, checksums));
          }});
      flushToDB = true;
    } else {
      // ContainerID exists, update timestamp in memory
      final ContainerReplicaHistory ts = replicaLastSeenMap.get(uuid);
      if (ts == null) {
        // New Datanode
        replicaLastSeenMap.put(uuid,
            new ContainerReplicaHistory(uuid, currTime, currTime, bcsId,
                state, checksums));
        flushToDB = true;
      } else {
        // if the object exists, only update the last seen time & bcsId fields
        ts.setLastSeenTime(currTime);
        ts.setBcsId(bcsId);
        ts.setState(state);
        ts.setChecksums(checksums);
      }
    }

    if (flushToDB) {
      upsertContainerHistory(id, uuid, currTime, bcsId, state, checksums);
    }
  }

  /**
   * Remove a Container Replica of a given DataNode.
   */
  @Override
  public void removeContainerReplica(ContainerID containerID,
      ContainerReplica replica) throws ContainerNotFoundException,
      ContainerReplicaNotFoundException {
    super.removeContainerReplica(containerID, replica);

    final long id = containerID.getId();
    final DatanodeDetails dnInfo = replica.getDatanodeDetails();
    final UUID uuid = dnInfo.getUuid();
    String state = replica.getState().toString();

    final Map<UUID, ContainerReplicaHistory> replicaLastSeenMap =
        replicaHistoryMap.get(id);
    if (replicaLastSeenMap != null) {
      final ContainerReplicaHistory ts = replicaLastSeenMap.get(uuid);
      if (ts != null) {
        // Flush to DB, then remove from in-memory map
        upsertContainerHistory(id, uuid, ts.getLastSeenTime(), ts.getBcsId(),
            state, ts.getChecksums());
        replicaLastSeenMap.remove(uuid);
      }
    }
  }

  @VisibleForTesting
  public ContainerHealthSchemaManager getContainerSchemaManager() {
    return containerHealthSchemaManager;
  }

  @VisibleForTesting
  public Map<Long, Map<UUID, ContainerReplicaHistory>> getReplicaHistoryMap() {
    return replicaHistoryMap;
  }

  public List<ContainerHistory> getAllContainerHistory(long containerID) {
    // First, get the existing entries from DB
    Map<UUID, ContainerReplicaHistory> resMap;
    try {
      resMap = cdbServiceProvider.getContainerReplicaHistory(containerID);
    } catch (IOException ex) {
      resMap = new HashMap<>();
      LOG.debug("Unable to retrieve container replica history from RDB.");
    }

    // Then, update the entries with the latest in-memory info, if available
    if (replicaHistoryMap != null) {
      Map<UUID, ContainerReplicaHistory> replicaLastSeenMap =
          replicaHistoryMap.get(containerID);
      if (replicaLastSeenMap != null) {
        Map<UUID, ContainerReplicaHistory> finalResMap = resMap;
        replicaLastSeenMap.forEach((k, v) ->
            finalResMap.merge(k, v, (old, latest) -> latest));
        resMap = finalResMap;
      }
    }

    // Finally, convert map to list for output
    List<ContainerHistory> resList = new ArrayList<>();
    for (Map.Entry<UUID, ContainerReplicaHistory> entry : resMap.entrySet()) {
      final UUID uuid = entry.getKey();
      String hostname = "N/A";
      // Attempt to retrieve hostname from NODES table
      if (nodeDB != null) {
        try {
          final DatanodeDetails dnDetails = nodeDB.get(DatanodeID.of(uuid));
          if (dnDetails != null) {
            hostname = dnDetails.getHostName();
          }
        } catch (IOException ex) {
          LOG.debug("Unable to retrieve from NODES table of node {}. {}",
              uuid, ex.getMessage());
        }
      }
      final long firstSeenTime = entry.getValue().getFirstSeenTime();
      final long lastSeenTime = entry.getValue().getLastSeenTime();
      long bcsId = entry.getValue().getBcsId();
      String state = entry.getValue().getState();
      long dataChecksum = entry.getValue().getDataChecksum();

      resList.add(new ContainerHistory(containerID, uuid.toString(), hostname,
          firstSeenTime, lastSeenTime, bcsId, state, dataChecksum));
    }
    return resList;
  }

  public List<ContainerHistory> getLatestContainerHistory(long containerID,
      int limit) {
    List<ContainerHistory> res = getAllContainerHistory(containerID);
    res.sort(comparingLong(ContainerHistory::getLastSeenTime).reversed());
    return res.stream().limit(limit).collect(Collectors.toList());
  }

  /**
   * Flush the container replica history in-memory map to DB.
   * Expected to be called on Recon graceful shutdown.
   * @param clearMap true to clear the in-memory map after flushing completes.
   */
  public void flushReplicaHistoryMapToDB(boolean clearMap) {
    if (replicaHistoryMap == null) {
      return;
    }
    synchronized (replicaHistoryMap) {
      try {
        cdbServiceProvider.batchStoreContainerReplicaHistory(replicaHistoryMap);
      } catch (IOException e) {
        LOG.debug("Error flushing container replica history to DB. {}",
            e.getMessage());
      }
      if (clearMap) {
        replicaHistoryMap.clear();
      }
    }
  }

  public void upsertContainerHistory(long containerID, UUID uuid, long time,
                                     long bcsId, String state, ContainerChecksums checksums) {
    Map<UUID, ContainerReplicaHistory> tsMap;
    try {
      tsMap = cdbServiceProvider.getContainerReplicaHistory(containerID);
      ContainerReplicaHistory ts = tsMap.get(uuid);
      if (ts == null) {
        // New entry
        tsMap.put(uuid, new ContainerReplicaHistory(uuid, time, time, bcsId,
            state, checksums));
      } else {
        // Entry exists, update last seen time and put it back to DB.
        ts.setLastSeenTime(time);
        ts.setState(state);
        ts.setChecksums(checksums);
      }
      cdbServiceProvider.storeContainerReplicaHistory(containerID, tsMap);
    } catch (IOException e) {
      LOG.debug("Error on DB operations. {}", e.getMessage());
    }
  }

  public Table<DatanodeID, DatanodeDetails> getNodeDB() {
    return nodeDB;
  }

  public Map<PipelineID, Integer> getPipelineToOpenContainer() {
    return pipelineToOpenContainer;
  }

  @VisibleForTesting
  public StorageContainerServiceProvider getScmClient() {
    return scmClient;
  }
}

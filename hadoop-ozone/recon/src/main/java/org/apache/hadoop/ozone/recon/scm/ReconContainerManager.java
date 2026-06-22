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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FINALIZE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  // Container ID -> DatanodeID -> Timestamp
  private final Map<Long, Map<DatanodeID, ContainerReplicaHistory>> replicaHistoryMap;
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
   * <p>If the container was recorded without a pipeline (null pipeline at
   * {@code addNewContainer} time) the count decrement is safely skipped.
   *
   * @param containerID   container to advance from OPEN to CLOSING
   * @param containerInfo already-fetched {@code ContainerInfo} for the container
   *                      (avoids a redundant lookup inside this method)
   * @throws IOException                     if the state update fails
   * @throws InvalidStateTransitionException if the container is not in OPEN state
   */
  void transitionOpenToClosing(ContainerID containerID, ContainerInfo containerInfo)
      throws IOException, InvalidStateTransitionException {
    PipelineID pipelineID = containerInfo.getPipelineID();
    updateContainerState(containerID, FINALIZE);  // OPEN → CLOSING
    if (pipelineID != null) {
      int curCnt = pipelineToOpenContainer.getOrDefault(pipelineID, 0);
      if (curCnt == 1) {
        pipelineToOpenContainer.remove(pipelineID);
      } else if (curCnt > 0) {
        pipelineToOpenContainer.put(pipelineID, curCnt - 1);
      }
    }
  }

  /**
   * Check if Recon's container lifecycle state needs the Recon-specific
   * pre-processing required before SCM's shared report handler processes the
   * replica.
   *
   * <p>Recon only handles OPEN to CLOSING here to keep the per-pipeline open
   * container count accurate. All other known-container lifecycle transitions
   * are left to SCM's common ICR/FCR state machine, which is invoked after this
   * method by Recon's report handlers.
   *
   * @param containerID containerID to check
   * @param replicaState replica state reported by a DataNode
   */
  private void checkContainerStateAndUpdate(ContainerID containerID,
                                            ContainerReplicaProto.State replicaState)
      throws IOException, InvalidStateTransitionException {
    ContainerInfo containerInfo = getContainer(containerID);
    HddsProtos.LifeCycleState reconState = containerInfo.getState();

    if (reconState == HddsProtos.LifeCycleState.OPEN
        && replicaState != ContainerReplicaProto.State.OPEN && isHealthy(replicaState)) {
      LOG.info("Container {} has state OPEN, but given state is {}.",
          containerID, replicaState);
      transitionOpenToClosing(containerID, containerInfo);
    }
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
   * @param containerWithPipeline containerInfo with pipeline info (pipeline may be null)
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
            LOG.info("Added new pipeline {} to Recon pipeline metadata from SCM.", pipelineID);
          }
          getContainerStateManager().addContainer(containerInfo.getProtobuf());
          pipelineManager.addContainerToPipeline(pipelineID, containerInfo.containerID());
          // Update open container count on all datanodes on this pipeline.
          pipelineToOpenContainer.put(pipelineID,
              pipelineToOpenContainer.getOrDefault(pipelineID, 0) + 1);
          LOG.info("Successfully added OPEN container {} with pipeline {} to Recon.",
              containerInfo.containerID(), pipelineID);
        } else {
          // Pipeline not available (cleaned up in SCM). Record the container
          // without pipeline tracking so it is not permanently absent from Recon.
          getContainerStateManager().addContainer(containerInfo.getProtobuf());
          LOG.warn("Added OPEN container {} to Recon without pipeline "
              + "(pipeline was null — likely cleaned up on SCM side). "
              + "Pipeline tracking unavailable for this container.",
              containerInfo.containerID());
        }
      } else {
        getContainerStateManager().addContainer(containerInfo.getProtobuf());
        LOG.info("Successfully added container {} in state {} to Recon.",
            containerInfo.containerID(), containerInfo.getState());
      }
    } catch (IOException ex) {
      LOG.info("Exception while adding container {}.", containerInfo.containerID(), ex);
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
    final long cid = containerID.getId();
    final DatanodeDetails dnInfo = replica.getDatanodeDetails();
    final DatanodeID id = dnInfo.getID();

    // Map from DataNode ID to replica last seen time
    final Map<DatanodeID, ContainerReplicaHistory> replicaLastSeenMap =
        replicaHistoryMap.get(cid);

    boolean flushToDB = false;
    long bcsId = replica.getSequenceId() != null ? replica.getSequenceId() : -1;
    String state = replica.getState().toString();
    ContainerChecksums checksums = replica.getChecksums();

    // If replica doesn't exist in in-memory map, add to DB and add to map
    if (replicaLastSeenMap == null) {
      // putIfAbsent to avoid TOCTOU
      replicaHistoryMap.putIfAbsent(cid,
          new ConcurrentHashMap<DatanodeID, ContainerReplicaHistory>() {{
            put(id, new ContainerReplicaHistory(id, currTime, currTime,
                bcsId, state, checksums));
          }});
      flushToDB = true;
    } else {
      // ContainerID exists, update timestamp in memory
      final ContainerReplicaHistory ts = replicaLastSeenMap.get(id);
      if (ts == null) {
        // New Datanode
        replicaLastSeenMap.put(id,
            new ContainerReplicaHistory(id, currTime, currTime, bcsId,
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
      upsertContainerHistory(cid, id, currTime, bcsId, state, checksums);
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

    final long cid = containerID.getId();
    final DatanodeDetails dnInfo = replica.getDatanodeDetails();
    final DatanodeID id = dnInfo.getID();
    String state = replica.getState().toString();

    final Map<DatanodeID, ContainerReplicaHistory> replicaLastSeenMap =
        replicaHistoryMap.get(cid);
    if (replicaLastSeenMap != null) {
      final ContainerReplicaHistory ts = replicaLastSeenMap.get(id);
      if (ts != null) {
        // Flush to DB, then remove from in-memory map
        upsertContainerHistory(cid, id, ts.getLastSeenTime(), ts.getBcsId(),
            state, ts.getChecksums());
        replicaLastSeenMap.remove(id);
      }
    }
  }

  @VisibleForTesting
  public ContainerHealthSchemaManager getContainerSchemaManager() {
    return containerHealthSchemaManager;
  }

  @VisibleForTesting
  public Map<Long, Map<DatanodeID, ContainerReplicaHistory>> getReplicaHistoryMap() {
    return replicaHistoryMap;
  }

  public List<ContainerHistory> getAllContainerHistory(long containerID) {
    // First, get the existing entries from DB
    Map<DatanodeID, ContainerReplicaHistory> resMap;
    try {
      resMap = cdbServiceProvider.getContainerReplicaHistory(containerID);
    } catch (IOException ex) {
      resMap = new HashMap<>();
      LOG.debug("Unable to retrieve container replica history from RDB.");
    }

    // Then, update the entries with the latest in-memory info, if available
    if (replicaHistoryMap != null) {
      Map<DatanodeID, ContainerReplicaHistory> replicaLastSeenMap =
          replicaHistoryMap.get(containerID);
      if (replicaLastSeenMap != null) {
        Map<DatanodeID, ContainerReplicaHistory> finalResMap = resMap;
        replicaLastSeenMap.forEach((k, v) ->
            finalResMap.merge(k, v, (old, latest) -> latest));
        resMap = finalResMap;
      }
    }

    // Finally, convert map to list for output
    List<ContainerHistory> resList = new ArrayList<>();
    for (Map.Entry<DatanodeID, ContainerReplicaHistory> entry : resMap.entrySet()) {
      final DatanodeID id = entry.getKey();
      String hostname = "N/A";
      // Attempt to retrieve hostname from NODES table
      if (nodeDB != null) {
        try {
          final DatanodeDetails dnDetails = nodeDB.get(id);
          if (dnDetails != null) {
            hostname = dnDetails.getHostName();
          }
        } catch (IOException ex) {
          LOG.debug("Unable to retrieve from NODES table of node {}. {}",
              id, ex.getMessage());
        }
      }
      final long firstSeenTime = entry.getValue().getFirstSeenTime();
      final long lastSeenTime = entry.getValue().getLastSeenTime();
      long bcsId = entry.getValue().getBcsId();
      String state = entry.getValue().getState();
      long dataChecksum = entry.getValue().getDataChecksum();

      resList.add(new ContainerHistory(containerID, id.toString(), hostname,
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

  public void upsertContainerHistory(long containerID, DatanodeID id, long time,
                                     long bcsId, String state, ContainerChecksums checksums) {
    Map<DatanodeID, ContainerReplicaHistory> tsMap;
    try {
      tsMap = cdbServiceProvider.getContainerReplicaHistory(containerID);
      ContainerReplicaHistory ts = tsMap.get(id);
      if (ts == null) {
        // New entry
        tsMap.put(id, new ContainerReplicaHistory(id, time, time, bcsId,
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

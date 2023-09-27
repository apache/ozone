/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.metrics.SCMContainerManagerMetrics;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Comparator.reverseOrder;
import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.CONTAINER_ID;
import static org.apache.hadoop.hdds.utils.CollectionUtils.findTopN;

/**
 * {@link ContainerManager} implementation in SCM server.
 */
public class ContainerManagerImpl implements ContainerManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerManagerImpl.class);

  /**
   * Limit the number of on-going ratis operations.
   */
  private final Lock lock;

  private final PipelineManager pipelineManager;

  private final ContainerStateManager containerStateManager;

  private final SCMHAManager haManager;
  private final SequenceIdGenerator sequenceIdGen;

  // TODO: Revisit this.
  // Metrics related to operations should be moved to ProtocolServer
  private final SCMContainerManagerMetrics scmContainerManagerMetrics;

  private final int numContainerPerVolume;

  @SuppressWarnings("java:S2245") // no need for secure random
  private final Random random = new Random();

  /**
   *
   */
  public ContainerManagerImpl(
      final Configuration conf,
      final SCMHAManager scmHaManager,
      final SequenceIdGenerator sequenceIdGen,
      final PipelineManager pipelineManager,
      final Table<ContainerID, ContainerInfo> containerStore,
      final ContainerReplicaPendingOps containerReplicaPendingOps)
      throws IOException {
    // Introduce builder for this class?
    this.lock = new ReentrantLock();
    this.pipelineManager = pipelineManager;
    this.haManager = scmHaManager;
    this.sequenceIdGen = sequenceIdGen;
    this.containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmHaManager.getRatisServer())
        .setContainerStore(containerStore)
        .setSCMDBTransactionBuffer(scmHaManager.getDBTransactionBuffer())
        .setContainerReplicaPendingOps(containerReplicaPendingOps)
        .build();

    this.numContainerPerVolume = conf
        .getInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
            ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);

    this.scmContainerManagerMetrics = SCMContainerManagerMetrics.create();
  }

  @Override
  public void reinitialize(Table<ContainerID, ContainerInfo> containerStore)
      throws IOException {
    lock.lock();
    try {
      containerStateManager.reinitialize(containerStore);
    } catch (IOException ioe) {
      LOG.error("Failed to reinitialize containerManager", ioe);
      throw ioe;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ContainerInfo getContainer(final ContainerID id)
      throws ContainerNotFoundException {
    return Optional.ofNullable(containerStateManager
        .getContainer(id))
        .orElseThrow(() -> new ContainerNotFoundException("ID " + id));
  }

  @Override
  public List<ContainerInfo> getContainers(final ContainerID startID,
                                           final int count) {
    scmContainerManagerMetrics.incNumListContainersOps();
    return toContainers(filterSortAndLimit(startID, count,
        containerStateManager.getContainerIDs()));
  }

  @Override
  public List<ContainerInfo> getContainers(final LifeCycleState state) {
    scmContainerManagerMetrics.incNumListContainersOps();
    return toContainers(containerStateManager.getContainerIDs(state));
  }

  @Override
  public List<ContainerInfo> getContainers(final ContainerID startID,
                                           final int count,
                                           final LifeCycleState state) {
    scmContainerManagerMetrics.incNumListContainersOps();
    return toContainers(filterSortAndLimit(startID, count,
        containerStateManager.getContainerIDs(state)));
  }

  @Override
  public int getContainerStateCount(final LifeCycleState state) {
    return containerStateManager.getContainerIDs(state).size();
  }

  @Override
  public ContainerInfo allocateContainer(
      final ReplicationConfig replicationConfig, final String owner)
      throws IOException {
    // Acquire pipeline manager lock, to avoid any updates to pipeline
    // while allocate container happens. This is to avoid scenario like
    // mentioned in HDDS-5655.
    pipelineManager.acquireReadLock();
    lock.lock();
    List<Pipeline> pipelines;
    Pipeline pipeline;
    ContainerInfo containerInfo = null;
    try {
      pipelines = pipelineManager
          .getPipelines(replicationConfig, Pipeline.PipelineState.OPEN);
      if (!pipelines.isEmpty()) {
        pipeline = pipelines.get(random.nextInt(pipelines.size()));
        containerInfo = createContainer(pipeline, owner);
      }
    } finally {
      lock.unlock();
      pipelineManager.releaseReadLock();
    }

    if (pipelines.isEmpty()) {
      try {
        pipeline = pipelineManager.createPipeline(replicationConfig);
        pipelineManager.waitPipelineReady(pipeline.getId(), 0);
      } catch (IOException e) {
        scmContainerManagerMetrics.incNumFailureCreateContainers();
        throw new IOException("Could not allocate container. Cannot get any" +
            " matching pipeline for replicationConfig: " + replicationConfig
            + ", State:PipelineState.OPEN", e);
      }
      pipelineManager.acquireReadLock();
      lock.lock();
      try {
        pipelines = pipelineManager
            .getPipelines(replicationConfig, Pipeline.PipelineState.OPEN);
        if (!pipelines.isEmpty()) {
          pipeline = pipelines.get(random.nextInt(pipelines.size()));
          containerInfo = createContainer(pipeline, owner);
        } else {
          throw new IOException("Could not allocate container. Cannot get any" +
              " matching pipeline for replicationConfig: " + replicationConfig
              + ", State:PipelineState.OPEN");
        }
      } finally {
        lock.unlock();
        pipelineManager.releaseReadLock();
      }
    }
    return containerInfo;
  }

  private ContainerInfo createContainer(Pipeline pipeline, String owner)
      throws IOException {
    final ContainerInfo containerInfo = allocateContainer(pipeline, owner);
    if (LOG.isTraceEnabled()) {
      LOG.trace("New container allocated: {}", containerInfo);
    }
    return containerInfo;
  }

  private ContainerInfo allocateContainer(final Pipeline pipeline,
                                          final String owner)
      throws IOException {
    final long uniqueId = sequenceIdGen.getNextId(CONTAINER_ID);
    Preconditions.checkState(uniqueId > 0,
        "Cannot allocate container, negative container id" +
            " generated. %s.", uniqueId);
    final ContainerID containerID = ContainerID.valueOf(uniqueId);
    final ContainerInfoProto.Builder containerInfoBuilder = ContainerInfoProto
        .newBuilder()
        .setState(LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId().getProtobuf())
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.now())
        .setOwner(owner)
        .setContainerID(containerID.getId())
        .setDeleteTransactionId(0)
        .setReplicationType(pipeline.getType());

    if (pipeline.getReplicationConfig() instanceof ECReplicationConfig) {
      containerInfoBuilder.setEcReplicationConfig(
          ((ECReplicationConfig) pipeline.getReplicationConfig()).toProto());
    } else {
      containerInfoBuilder.setReplicationFactor(
          ReplicationConfig.getLegacyFactor(pipeline.getReplicationConfig()));
    }

    containerStateManager.addContainer(containerInfoBuilder.build());
    scmContainerManagerMetrics.incNumSuccessfulCreateContainers();
    return containerStateManager.getContainer(containerID);
  }

  @Override
  public void updateContainerState(final ContainerID cid,
                                   final LifeCycleEvent event)
      throws IOException, InvalidStateTransitionException {
    HddsProtos.ContainerID protoId = cid.getProtobuf();
    lock.lock();
    try {
      if (containerExist(cid)) {
        containerStateManager.updateContainerState(protoId, event);
      } else {
        throwContainerNotFoundException(cid);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<ContainerReplica> getContainerReplicas(final ContainerID id)
      throws ContainerNotFoundException {
    return Optional.ofNullable(containerStateManager
        .getContainerReplicas(id))
        .orElseThrow(() -> new ContainerNotFoundException("ID " + id));
  }

  @Override
  public void updateContainerReplica(final ContainerID cid,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException {
    if (containerExist(cid)) {
      containerStateManager.updateContainerReplica(cid, replica);
    } else {
      throwContainerNotFoundException(cid);
    }
  }

  @Override
  public void removeContainerReplica(final ContainerID cid,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    if (containerExist(cid)) {
      containerStateManager.removeContainerReplica(cid, replica);
    } else {
      throwContainerNotFoundException(cid);
    }
  }

  @Override
  public void updateDeleteTransactionId(
      final Map<ContainerID, Long> deleteTransactionMap) throws IOException {
    containerStateManager.updateDeleteTransactionId(deleteTransactionMap);
  }

  @Override
  public ContainerInfo getMatchingContainer(final long size, final String owner,
      final Pipeline pipeline, final Set<ContainerID> excludedContainerIDs) {
    NavigableSet<ContainerID> containerIDs;
    ContainerInfo containerInfo;
    try {
      synchronized (pipeline.getId()) {
        containerIDs = getContainersForOwner(pipeline, owner);
        if (containerIDs.size() < getOpenContainerCountPerPipeline(pipeline)) {
          allocateContainer(pipeline, owner);
          containerIDs = getContainersForOwner(pipeline, owner);
        }
        containerIDs.removeAll(excludedContainerIDs);
        containerInfo = containerStateManager.getMatchingContainer(
            size, owner, pipeline.getId(), containerIDs);
        if (containerInfo == null) {
          containerInfo = allocateContainer(pipeline, owner);
        }
        return containerInfo;
      }
    } catch (Exception e) {
      LOG.warn("Container allocation failed on pipeline={}", pipeline, e);
      return null;
    }
  }

  private int getOpenContainerCountPerPipeline(Pipeline pipeline) {
    int minContainerCountPerDn = numContainerPerVolume *
        pipelineManager.minHealthyVolumeNum(pipeline);
    int minPipelineCountPerDn = pipelineManager.minPipelineLimit(pipeline);
    return (int) Math.ceil(
        ((double) minContainerCountPerDn / minPipelineCountPerDn));
  }

  /**
   * Returns the container ID's matching with specified owner.
   * @param pipeline
   * @param owner
   * @return NavigableSet<ContainerID>
   */
  private NavigableSet<ContainerID> getContainersForOwner(
      Pipeline pipeline, String owner) throws IOException {
    NavigableSet<ContainerID> containerIDs =
        pipelineManager.getContainersInPipeline(pipeline.getId());
    Iterator<ContainerID> containerIDIterator = containerIDs.iterator();
    while (containerIDIterator.hasNext()) {
      ContainerID cid = containerIDIterator.next();
      try {
        if (!getContainer(cid).getOwner().equals(owner)) {
          containerIDIterator.remove();
        }
      } catch (ContainerNotFoundException e) {
        LOG.error("Could not find container info for container {}", cid, e);
        containerIDIterator.remove();
      }
    }
    return containerIDs;
  }

  @Override
  public void notifyContainerReportProcessing(final boolean isFullReport,
                                              final boolean success) {
    if (isFullReport) {
      if (success) {
        scmContainerManagerMetrics.incNumContainerReportsProcessedSuccessful();
      } else {
        scmContainerManagerMetrics.incNumContainerReportsProcessedFailed();
      }
    } else {
      if (success) {
        scmContainerManagerMetrics.incNumICRReportsProcessedSuccessful();
      } else {
        scmContainerManagerMetrics.incNumICRReportsProcessedFailed();
      }
    }
  }

  @Override
  public void deleteContainer(final ContainerID cid)
      throws IOException {
    HddsProtos.ContainerID protoId = cid.getProtobuf();

    final boolean found;
    lock.lock();
    try {
      found = containerExist(cid);
      if (found) {
        containerStateManager.removeContainer(protoId);
      }
    } finally {
      lock.unlock();
    }

    if (found) {
      scmContainerManagerMetrics.incNumSuccessfulDeleteContainers();
    } else {
      scmContainerManagerMetrics.incNumFailureDeleteContainers();
      throwContainerNotFoundException(cid);
    }
  }

  @Override
  public boolean containerExist(final ContainerID id) {
    return containerStateManager.contains(id);
  }

  private void throwContainerNotFoundException(final ContainerID id)
      throws ContainerNotFoundException {
    throw new ContainerNotFoundException("Container with id " +
        id + " not found.");
  }

  @Override
  public void close() throws IOException {
    containerStateManager.close();
  }

  // Remove this after fixing Recon
  @VisibleForTesting
  public ContainerStateManager getContainerStateManager() {
    return containerStateManager;
  }

  @VisibleForTesting
  public SCMHAManager getSCMHAManager() {
    return haManager;
  }

  private static List<ContainerID> filterSortAndLimit(
      ContainerID startID, int count, Set<ContainerID> set) {

    if (ContainerID.MIN.equals(startID) && count >= set.size()) {
      List<ContainerID> list = new ArrayList<>(set);
      Collections.sort(list);
      return list;
    }

    return findTopN(set, count, reverseOrder(),
        id -> id.compareTo(startID) >= 0);
  }

  /**
   * Returns a list of all containers identified by {@code ids}.
   */
  private List<ContainerInfo> toContainers(Collection<ContainerID> ids) {
    List<ContainerInfo> containers = new ArrayList<>(ids.size());

    for (ContainerID id : ids) {
      ContainerInfo container = containerStateManager.getContainer(id);
      if (container != null) {
        containers.add(container);
      }
    }

    return containers;
  }
}

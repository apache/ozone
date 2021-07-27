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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.metrics.SCMContainerManagerMetrics;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.CONTAINER_ID;

/**
 * TODO: Add javadoc.
 */
public class ContainerManagerImpl implements ContainerManagerV2 {

  /*
   * TODO: Introduce container level locks.
   */

  /**
   *
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerManagerImpl.class);

  /**
   *
   */
  // Limit the number of on-going ratis operations.
  private final Lock lock;

  /**
   *
   */
  private final PipelineManager pipelineManager;

  /**
   *
   */
  private final ContainerStateManagerV2 containerStateManager;

  private final SCMHAManager haManager;
  private final SequenceIdGenerator sequenceIdGen;

  // TODO: Revisit this.
  // Metrics related to operations should be moved to ProtocolServer
  private final SCMContainerManagerMetrics scmContainerManagerMetrics;

  private final int numContainerPerVolume;
  private final Random random = new Random();

  /**
   *
   */
  public ContainerManagerImpl(
      final Configuration conf,
      final SCMHAManager scmHaManager,
      final SequenceIdGenerator sequenceIdGen,
      final PipelineManager pipelineManager,
      final Table<ContainerID, ContainerInfo> containerStore)
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
        .getContainer(id.getProtobuf()))
        .orElseThrow(() -> new ContainerNotFoundException("ID " + id));
  }

  @Override
  public List<ContainerInfo> getContainers(final ContainerID startID,
                                           final int count) {
    scmContainerManagerMetrics.incNumListContainersOps();
    // TODO: Remove the null check, startID should not be null. Fix the unit
    //  test before removing the check.
    final long start = startID == null ? 0 : startID.getId();
    final List<ContainerID> containersIds =
        new ArrayList<>(containerStateManager.getContainerIDs());
    Collections.sort(containersIds);
    return containersIds.stream()
        .filter(id -> id.getId() >= start).limit(count)
        .map(ContainerID::getProtobuf)
        .map(containerStateManager::getContainer)
        .collect(Collectors.toList());
  }

  @Override
  public List<ContainerInfo> getContainers(final LifeCycleState state) {
    return containerStateManager.getContainerIDs(state).stream()
        .map(ContainerID::getProtobuf)
        .map(containerStateManager::getContainer)
        .filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public ContainerInfo allocateContainer(
      final ReplicationConfig replicationConfig, final String owner)
      throws IOException {
    lock.lock();
    try {
      final List<Pipeline> pipelines = pipelineManager
          .getPipelines(replicationConfig, Pipeline.PipelineState.OPEN);

      final Pipeline pipeline;
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
      } else {
        pipeline = pipelines.get(random.nextInt(pipelines.size()));
      }
      final ContainerInfo containerInfo = allocateContainer(pipeline, owner);
      if (LOG.isTraceEnabled()) {
        LOG.trace("New container allocated: {}", containerInfo);
      }
      return containerInfo;
    } finally {
      lock.unlock();
    }
  }

  private ContainerInfo allocateContainer(final Pipeline pipeline,
                                          final String owner)
      throws IOException {
    final long uniqueId = sequenceIdGen.getNextId(CONTAINER_ID);
    Preconditions.checkState(uniqueId > 0,
        "Cannot allocate container, negative container id" +
            " generated. %s.", uniqueId);
    final ContainerID containerID = ContainerID.valueOf(uniqueId);
    final ContainerInfoProto containerInfo = ContainerInfoProto.newBuilder()
        .setState(LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId().getProtobuf())
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.now())
        .setOwner(owner)
        .setContainerID(containerID.getId())
        .setDeleteTransactionId(0)
        .setReplicationFactor(
            ReplicationConfig.getLegacyFactor(pipeline.getReplicationConfig()))
        .setReplicationType(pipeline.getType())
        .build();
    containerStateManager.addContainer(containerInfo);
    scmContainerManagerMetrics.incNumSuccessfulCreateContainers();
    return containerStateManager.getContainer(containerID.getProtobuf());
  }

  @Override
  public void updateContainerState(final ContainerID id,
                                   final LifeCycleEvent event)
      throws IOException, InvalidStateTransitionException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
    lock.lock();
    try {
      if (containerExist(cid)) {
        containerStateManager.updateContainerState(cid, event);
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
        .getContainerReplicas(id.getProtobuf()))
        .orElseThrow(() -> new ContainerNotFoundException("ID " + id));
  }

  @Override
  public void updateContainerReplica(final ContainerID id,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
    if (containerExist(cid)) {
      containerStateManager.updateContainerReplica(cid, replica);
    } else {
      throwContainerNotFoundException(cid);
    }
  }

  @Override
  public void removeContainerReplica(final ContainerID id,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
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
  public void deleteContainer(final ContainerID id)
      throws IOException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
    lock.lock();
    try {
      if (containerExist(cid)) {
        containerStateManager.removeContainer(cid);
        scmContainerManagerMetrics.incNumSuccessfulDeleteContainers();
      } else {
        scmContainerManagerMetrics.incNumFailureDeleteContainers();
        throwContainerNotFoundException(cid);
      }
    } finally {
      lock.unlock();
    }
  }

  @Deprecated
  private void checkIfContainerExist(final HddsProtos.ContainerID id)
      throws ContainerNotFoundException {
    if (!containerStateManager.contains(id)) {
      throw new ContainerNotFoundException("Container with id #" +
          id.getId() + " not found.");
    }
  }

  @Override
  public boolean containerExist(final ContainerID id) {
    return containerExist(id.getProtobuf());
  }

  private boolean containerExist(final HddsProtos.ContainerID id) {
    return containerStateManager.contains(id);
  }

  private void throwContainerNotFoundException(final HddsProtos.ContainerID id)
      throws ContainerNotFoundException {
    throw new ContainerNotFoundException("Container with id #" +
        id.getId() + " not found.");
  }

  @Override
  public void close() throws IOException {
    containerStateManager.close();
  }

  // Remove this after fixing Recon
  @Deprecated
  protected ContainerStateManagerV2 getContainerStateManager() {
    return containerStateManager;
  }

  @VisibleForTesting
  public SCMHAManager getSCMHAManager() {
    return haManager;
  }

  public Set<ContainerID> getContainerIDs() {
    return containerStateManager.getContainerIDs();
  }
}

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  //Can we move this lock to ContainerStateManager?
  private final ReadWriteLock lock;

  /**
   *
   */
  private final PipelineManager pipelineManager;

  /**
   *
   */
  private final ContainerStateManagerV2 containerStateManager;

  /**
   *
   */
  public ContainerManagerImpl(
      final Configuration conf,
      final SCMHAManager scmHaManager,
      final PipelineManager pipelineManager,
      final Table<ContainerID, ContainerInfo> containerStore)
      throws IOException {
    // Introduce builder for this class?
    this.lock = new ReentrantReadWriteLock();
    this.pipelineManager = pipelineManager;
    this.containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmHaManager.getRatisServer())
        .setContainerStore(containerStore)
        .build();
  }

  @Override
  public ContainerInfo getContainer(final ContainerID id)
      throws ContainerNotFoundException {
    lock.readLock().lock();
    try {
      return Optional.ofNullable(containerStateManager
          .getContainer(id.getProtobuf()))
          .orElseThrow(() -> new ContainerNotFoundException("ID " + id));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<ContainerInfo> listContainers(final ContainerID startID,
                                            final int count) {
    lock.readLock().lock();
    try {
      final long start = startID == null ? 0 : startID.getId();
      final List<ContainerID> containersIds =
          new ArrayList<>(containerStateManager.getContainerIDs());
      Collections.sort(containersIds);
      return containersIds.stream()
          .filter(id -> id.getId() > start).limit(count)
          .map(ContainerID::getProtobuf)
          .map(containerStateManager::getContainer)
          .collect(Collectors.toList());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<ContainerInfo> listContainers(final LifeCycleState state) {
    lock.readLock().lock();
    try {
      return containerStateManager.getContainerIDs(state).stream()
          .map(ContainerID::getProtobuf)
          .map(containerStateManager::getContainer)
          .filter(Objects::nonNull).collect(Collectors.toList());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public ContainerInfo allocateContainer(final ReplicationType type,
      final ReplicationFactor replicationFactor, final String owner)
      throws IOException {
    lock.writeLock().lock();
    try {
      final List<Pipeline> pipelines = pipelineManager
          .getPipelines(type, replicationFactor, Pipeline.PipelineState.OPEN);

      if (pipelines.isEmpty()) {
        throw new IOException("Could not allocate container. Cannot get any" +
            " matching pipeline for Type:" + type + ", Factor:" +
            replicationFactor + ", State:PipelineState.OPEN");
      }

      // TODO: Replace this with Distributed unique id generator.
      final ContainerID containerID = ContainerID.valueOf(UniqueId.next());
      final Pipeline pipeline = pipelines.get(
          (int) containerID.getId() % pipelines.size());

      final ContainerInfoProto containerInfo = ContainerInfoProto.newBuilder()
          .setState(LifeCycleState.OPEN)
          .setPipelineID(pipeline.getId().getProtobuf())
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.now())
          .setOwner(owner)
          .setContainerID(containerID.getId())
          .setDeleteTransactionId(0)
          .setReplicationFactor(pipeline.getFactor())
          .setReplicationType(pipeline.getType())
          .build();
      containerStateManager.addContainer(containerInfo);
      if (LOG.isTraceEnabled()) {
        LOG.trace("New container allocated: {}", containerInfo);
      }
      return containerStateManager.getContainer(containerID.getProtobuf());
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void updateContainerState(final ContainerID id,
                                   final LifeCycleEvent event)
      throws IOException, InvalidStateTransitionException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
    lock.writeLock().lock();
    try {
      checkIfContainerExist(cid);
      containerStateManager.updateContainerState(cid, event);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Set<ContainerReplica> getContainerReplicas(final ContainerID id)
      throws ContainerNotFoundException {
    lock.readLock().lock();
    try {
      return Optional.ofNullable(containerStateManager
          .getContainerReplicas(id.getProtobuf()))
          .orElseThrow(() -> new ContainerNotFoundException("ID " + id));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void updateContainerReplica(final ContainerID id,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
    lock.writeLock().lock();
    try {
      checkIfContainerExist(cid);
      containerStateManager.updateContainerReplica(cid, replica);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeContainerReplica(final ContainerID id,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
    lock.writeLock().lock();
    try {
      checkIfContainerExist(cid);
      containerStateManager.removeContainerReplica(cid, replica);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void updateDeleteTransactionId(
      final Map<ContainerID, Long> deleteTransactionMap) throws IOException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public ContainerInfo getMatchingContainer(final long size, final String owner,
      final Pipeline pipeline, final List<ContainerID> excludedContainerIDS) {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public void notifyContainerReportProcessing(final boolean isFullReport,
                                              final boolean success) {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public void deleteContainer(final ContainerID id)
      throws IOException {
    final HddsProtos.ContainerID cid = id.getProtobuf();
    lock.writeLock().lock();
    try {
      checkIfContainerExist(cid);
      containerStateManager.removeContainer(cid);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void checkIfContainerExist(final HddsProtos.ContainerID id)
      throws ContainerNotFoundException {
    if (!containerStateManager.contains(id)) {
      throw new ContainerNotFoundException("Container with id #" +
          id.getId() + " not found.");
    }
  }

  @Override
  public void close() throws Exception {
    containerStateManager.close();
  }

}

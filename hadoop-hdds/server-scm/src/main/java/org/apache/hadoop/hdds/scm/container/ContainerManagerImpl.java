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
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Add javadoc.
 */
public class ContainerManagerImpl implements ContainerManagerV2 {

  /**
   *
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerManagerImpl.class);

  /**
   *
   */
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
      // Introduce builder for this class?
      final Configuration conf, final PipelineManager pipelineManager,
      final SCMHAManager scmhaManager,
      final Table<ContainerID, ContainerInfo> containerStore)
      throws IOException {
    this.lock = new ReentrantReadWriteLock();
    this.pipelineManager = pipelineManager;
    this.containerStateManager =  ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(containerStore)
        .build();
  }

  @Override
  public Set<ContainerID> getContainerIDs() {
    lock.readLock().lock();
    try {
      return containerStateManager.getContainerIDs();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<ContainerInfo> getContainers() {
    lock.readLock().lock();
    try {
      return containerStateManager.getContainerIDs().stream().map(id -> {
        try {
          return containerStateManager.getContainer(id);
        } catch (ContainerNotFoundException e) {
          // How can this happen? o_O
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toSet());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public ContainerInfo getContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    lock.readLock().lock();
    try {
      return containerStateManager.getContainer(containerID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<ContainerInfo> getContainers(final LifeCycleState state) {
    lock.readLock().lock();
    try {
      return containerStateManager.getContainerIDs(state).stream().map(id -> {
        try {
          return containerStateManager.getContainer(id);
        } catch (ContainerNotFoundException e) {
          // How can this happen? o_O
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toSet());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean exists(final ContainerID containerID) {
    lock.readLock().lock();
    try {
      return (containerStateManager.getContainer(containerID) != null);
    } catch (ContainerNotFoundException ex) {
      return false;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<ContainerInfo> listContainers(final ContainerID startID,
                                            final int count) {
    lock.readLock().lock();
    try {
      final long startId = startID == null ? 0 : startID.getId();
      final List<ContainerID> containersIds =
          new ArrayList<>(containerStateManager.getContainerIDs());
      Collections.sort(containersIds);
      return containersIds.stream()
          .filter(id -> id.getId() > startId)
          .limit(count)
          .map(id -> {
            try {
              return containerStateManager.getContainer(id);
            } catch (ContainerNotFoundException ex) {
              // This can never happen, as we hold lock no one else can remove
              // the container after we got the container ids.
              LOG.warn("Container Missing.", ex);
              return null;
            }
          }).collect(Collectors.toList());
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

      final ContainerID containerID = containerStateManager
          .getNextContainerID();
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
      return containerStateManager.getContainer(containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deleteContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public void updateContainerState(final ContainerID containerID,
                                   final LifeCycleEvent event)
      throws ContainerNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public Set<ContainerReplica> getContainerReplicas(
      final ContainerID containerID) throws ContainerNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public void updateContainerReplica(final ContainerID containerID,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public void removeContainerReplica(final ContainerID containerID,
                                     final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented!");
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
  public void close() throws IOException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

}

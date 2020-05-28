/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.states.ContainerState;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of ContainerStateManager. This implementation
 * holds the Container States in-memory which is backed by a persistent store.
 * The persistent store is always kept in sync with the in-memory state changes.
 */
public final class ContainerStateManagerImpl
    implements ContainerStateManagerV2 {

  /**
   * Logger instance of ContainerStateManagerImpl.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerStateManagerImpl.class);

  /**
   * Configured container size.
   */
  private final long containerSize;

  /**
   * The container ID sequence which is used to create new container.
   * This will be removed once we have a Distributed Sequence ID Generator.
   */
  @Deprecated
  private final AtomicLong nextContainerID;

  /**
   * In-memory representation of Container States.
   */
  private final ContainerStateMap containers;

  /**
   * Persistent store for Container States.
   */
  private Table<ContainerID, ContainerInfo> containerStore;

  /**
   * PipelineManager instance.
   */
  private final PipelineManager pipelineManager;

  /**
   * Container lifecycle state machine.
   */
  private final StateMachine<LifeCycleState, LifeCycleEvent> stateMachine;

  /**
   * We use the containers in round-robin fashion for operations like block
   * allocation. This map is used for remembering the last used container.
   */
  private final ConcurrentHashMap<ContainerState, ContainerID> lastUsedMap;

  /**
   * constructs ContainerStateManagerImpl instance and loads the containers
   * form the persistent storage.
   *
   * @param conf the Configuration
   * @param pipelineManager the {@link PipelineManager} instance
   * @param containerStore the persistent storage
   * @throws IOException in case of error while loading the containers
   */
  private ContainerStateManagerImpl(final Configuration conf,
      final PipelineManager pipelineManager,
      final Table<ContainerID, ContainerInfo> containerStore)
      throws IOException {
    this.pipelineManager = pipelineManager;
    this.containerStore = containerStore;
    this.stateMachine = newStateMachine();
    this.containerSize = getConfiguredContainerSize(conf);
    this.nextContainerID = new AtomicLong(1L);
    this.containers = new ContainerStateMap();
    this.lastUsedMap = new ConcurrentHashMap<>();

    initialize();
  }

  /**
   * Creates and initializes a new Container Lifecycle StateMachine.
   *
   * @return the Container Lifecycle StateMachine
   */
  private StateMachine<LifeCycleState, LifeCycleEvent> newStateMachine() {

    final Set<LifeCycleState> finalStates = new HashSet<>();

    // These are the steady states of a container.
    finalStates.add(LifeCycleState.OPEN);
    finalStates.add(LifeCycleState.CLOSED);
    finalStates.add(LifeCycleState.DELETED);

    final StateMachine<LifeCycleState, LifeCycleEvent> containerLifecycleSM =
        new StateMachine<>(LifeCycleState.OPEN, finalStates);

    containerLifecycleSM.addTransition(LifeCycleState.OPEN,
        LifeCycleState.CLOSING,
        LifeCycleEvent.FINALIZE);

    containerLifecycleSM.addTransition(LifeCycleState.CLOSING,
        LifeCycleState.QUASI_CLOSED,
        LifeCycleEvent.QUASI_CLOSE);

    containerLifecycleSM.addTransition(LifeCycleState.CLOSING,
        LifeCycleState.CLOSED,
        LifeCycleEvent.CLOSE);

    containerLifecycleSM.addTransition(LifeCycleState.QUASI_CLOSED,
        LifeCycleState.CLOSED,
        LifeCycleEvent.FORCE_CLOSE);

    containerLifecycleSM.addTransition(LifeCycleState.CLOSED,
        LifeCycleState.DELETING,
        LifeCycleEvent.DELETE);

    containerLifecycleSM.addTransition(LifeCycleState.DELETING,
        LifeCycleState.DELETED,
        LifeCycleEvent.CLEANUP);

    return containerLifecycleSM;
  }

  /**
   * Returns the configured container size.
   *
   * @return the max size of container
   */
  private long getConfiguredContainerSize(final Configuration conf) {
    return (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);
  }

  /**
   * Loads the containers from container store into memory.
   *
   * @throws IOException in case of error while loading the containers
   */
  private void initialize() throws IOException {
    TableIterator<ContainerID, ? extends KeyValue<ContainerID, ContainerInfo>>
        iterator = containerStore.iterator();

    while (iterator.hasNext()) {
      final ContainerInfo container = iterator.next().getValue();
      Preconditions.checkNotNull(container);
      containers.addContainer(container);
      nextContainerID.set(Long.max(container.containerID().getId(),
          nextContainerID.get()));
      if (container.getState() == LifeCycleState.OPEN) {
        try {
          pipelineManager.addContainerToPipeline(container.getPipelineID(),
              ContainerID.valueof(container.getContainerID()));
        } catch (PipelineNotFoundException ex) {
          LOG.warn("Found container {} which is in OPEN state with " +
                  "pipeline {} that does not exist. Marking container for " +
                  "closing.", container, container.getPipelineID());
          updateContainerState(container.containerID(),
              LifeCycleEvent.FINALIZE);
        }
      }
    }
  }

  @Override
  public ContainerID getNextContainerID() {
    return ContainerID.valueof(nextContainerID.get());
  }

  @Override
  public Set<ContainerID> getContainerIDs() {
    return containers.getAllContainerIDs();
  }

  @Override
  public Set<ContainerID> getContainerIDs(final LifeCycleState state) {
    return containers.getContainerIDsByState(state);
  }

  @Override
  public ContainerInfo getContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    return containers.getContainerInfo(containerID);
  }

  @Override
  public Set<ContainerReplica> getContainerReplicas(
      final ContainerID containerID) throws ContainerNotFoundException {
    return containers.getContainerReplicas(containerID);
  }

  @Override
  public void addContainer(final ContainerInfoProto containerInfo)
      throws IOException {

    // Change the exception thrown to PipelineNotFound and
    // ClosedPipelineException once ClosedPipelineException is introduced
    // in PipelineManager.

    Preconditions.checkNotNull(containerInfo);
    final ContainerInfo container = ContainerInfo.fromProtobuf(containerInfo);
    final ContainerID containerID = container.containerID();
    final PipelineID pipelineID = container.getPipelineID();

    /*
     * TODO:
     *  Check if the container already exist in in ContainerStateManager.
     *  This optimization can be done after moving ContainerNotFoundException
     *  from ContainerStateMap to ContainerManagerImpl.
     */

    containerStore.put(containerID, container);
    containers.addContainer(container);
    pipelineManager.addContainerToPipeline(pipelineID, containerID);
    nextContainerID.incrementAndGet();
  }

  void updateContainerState(final ContainerID containerID,
                            final LifeCycleEvent event)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }


  void updateContainerReplica(final ContainerID containerID,
                              final ContainerReplica replica)
      throws ContainerNotFoundException {
    containers.updateContainerReplica(containerID, replica);
  }


  void updateDeleteTransactionId(
      final Map<ContainerID, Long> deleteTransactionMap) {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  ContainerInfo getMatchingContainer(final long size, String owner,
      PipelineID pipelineID, NavigableSet<ContainerID> containerIDs) {
    throw new UnsupportedOperationException("Not yet implemented!");
  }


  NavigableSet<ContainerID> getMatchingContainerIDs(final String owner,
      final ReplicationType type, final ReplicationFactor factor,
      final LifeCycleState state) {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  void removeContainerReplica(final ContainerID containerID,
                              final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }


  void removeContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented!");
  }

  @Override
  public void close() throws Exception {
    containerStore.close();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for ContainerStateManager.
   */
  public static class Builder {
    private Configuration conf;
    private PipelineManager pipelineMgr;
    private SCMRatisServer scmRatisServer;
    private Table<ContainerID, ContainerInfo> table;

    public Builder setConfiguration(final Configuration config) {
      conf = config;
      return this;
    }

    public Builder setPipelineManager(final PipelineManager pipelineManager) {
      pipelineMgr = pipelineManager;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setContainerStore(
        final Table<ContainerID, ContainerInfo> containerStore) {
      table = containerStore;
      return this;
    }

    public ContainerStateManagerV2 build() throws IOException {
      Preconditions.checkNotNull(conf);
      Preconditions.checkNotNull(pipelineMgr);
      Preconditions.checkNotNull(scmRatisServer);
      Preconditions.checkNotNull(table);

      final ContainerStateManagerV2 csm = new ContainerStateManagerImpl(
          conf, pipelineMgr, table);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(RequestType.CONTAINER, csm,
              scmRatisServer);

      return (ContainerStateManagerV2) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{ContainerStateManagerV2.class}, invocationHandler);
    }

  }
}

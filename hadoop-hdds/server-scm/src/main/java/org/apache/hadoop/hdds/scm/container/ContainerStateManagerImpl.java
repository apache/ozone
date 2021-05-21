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
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.states.ContainerState;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.ha.CheckedConsumer;
import org.apache.hadoop.hdds.scm.ha.ExecutionUtil;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FINALIZE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.QUASI_CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FORCE_CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.DELETE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.CLEANUP;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;

/**
 * Default implementation of ContainerStateManager. This implementation
 * holds the Container States in-memory which is backed by a persistent store.
 * The persistent store is always kept in sync with the in-memory state changes.
 *
 * This class is NOT thread safe. All the calls are idempotent.
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
   * In-memory representation of Container States.
   */
  private ContainerStateMap containers;

  /**
   * Persistent store for Container States.
   */
  private Table<ContainerID, ContainerInfo> containerStore;

  private final DBTransactionBuffer transactionBuffer;

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
  private ConcurrentHashMap<ContainerState, ContainerID> lastUsedMap;

  private final Map<LifeCycleEvent, CheckedConsumer<ContainerInfo, IOException>>
      containerStateChangeActions;

  // Protect containers and containerStore against the potential
  // contentions between RaftServer and ContainerManager.
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
      final Table<ContainerID, ContainerInfo> containerStore,
      final DBTransactionBuffer buffer)
      throws IOException {
    this.pipelineManager = pipelineManager;
    this.containerStore = containerStore;
    this.stateMachine = newStateMachine();
    this.containerSize = getConfiguredContainerSize(conf);
    this.containers = new ContainerStateMap();
    this.lastUsedMap = new ConcurrentHashMap<>();
    this.containerStateChangeActions = getContainerStateChangeActions();
    this.transactionBuffer = buffer;

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
    finalStates.add(CLOSED);
    finalStates.add(DELETED);

    final StateMachine<LifeCycleState, LifeCycleEvent> containerLifecycleSM =
        new StateMachine<>(OPEN, finalStates);

    containerLifecycleSM.addTransition(OPEN, CLOSING, FINALIZE);
    containerLifecycleSM.addTransition(CLOSING, QUASI_CLOSED, QUASI_CLOSE);
    containerLifecycleSM.addTransition(CLOSING, CLOSED, CLOSE);
    containerLifecycleSM.addTransition(QUASI_CLOSED, CLOSED, FORCE_CLOSE);
    containerLifecycleSM.addTransition(CLOSED, DELETING, DELETE);
    containerLifecycleSM.addTransition(DELETING, DELETED, CLEANUP);

    /* The following set of transitions are to make state machine
     * transition idempotent.
     */
    makeStateTransitionIdempotent(containerLifecycleSM, FINALIZE,
        CLOSING, QUASI_CLOSED, CLOSED, DELETING, DELETED);
    makeStateTransitionIdempotent(containerLifecycleSM, QUASI_CLOSE,
        QUASI_CLOSED, CLOSED, DELETING, DELETED);
    makeStateTransitionIdempotent(containerLifecycleSM, CLOSE,
        CLOSED, DELETING, DELETED);
    makeStateTransitionIdempotent(containerLifecycleSM, FORCE_CLOSE,
        CLOSED, DELETING, DELETED);
    makeStateTransitionIdempotent(containerLifecycleSM, DELETE,
        DELETING, DELETED);
    makeStateTransitionIdempotent(containerLifecycleSM, CLEANUP, DELETED);

    return containerLifecycleSM;
  }

  private void makeStateTransitionIdempotent(
      final StateMachine<LifeCycleState, LifeCycleEvent> sm,
      final LifeCycleEvent event, final LifeCycleState... states) {
    for (LifeCycleState state : states) {
      sm.addTransition(state, state, event);
    }
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
      if (container.getState() == LifeCycleState.OPEN) {
        try {
          pipelineManager.addContainerToPipeline(container.getPipelineID(),
              container.containerID());
        } catch (PipelineNotFoundException ex) {
          LOG.warn("Found container {} which is in OPEN state with " +
                  "pipeline {} that does not exist. Marking container for " +
                  "closing.", container, container.getPipelineID());
          try {
            updateContainerState(container.containerID().getProtobuf(),
                LifeCycleEvent.FINALIZE);
          } catch (InvalidStateTransitionException e) {
            // This cannot happen.
            LOG.warn("Unable to finalize Container {}.", container);
          }
        }
      }
    }
  }

  private Map<LifeCycleEvent, CheckedConsumer<ContainerInfo, IOException>>
      getContainerStateChangeActions() {
    final Map<LifeCycleEvent, CheckedConsumer<ContainerInfo, IOException>>
        actions = new EnumMap<>(LifeCycleEvent.class);
    actions.put(FINALIZE, info -> pipelineManager
        .removeContainerFromPipeline(info.getPipelineID(), info.containerID()));
    return actions;
  }

  @Override
  public Set<ContainerID> getContainerIDs() {
    lock.readLock().lock();
    try {
      return containers.getAllContainerIDs();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<ContainerID> getContainerIDs(final LifeCycleState state) {
    lock.readLock().lock();
    try {
      return containers.getContainerIDsByState(state);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public ContainerInfo getContainer(final HddsProtos.ContainerID id) {
    lock.readLock().lock();
    try {
      return containers.getContainerInfo(ContainerID.getFromProtobuf(id));
    } finally {
      lock.readLock().unlock();
    }
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

    lock.writeLock().lock();
    try {
      if (!containers.contains(containerID)) {
        ExecutionUtil.create(() -> {
          transactionBuffer.addToBuffer(containerStore, containerID, container);
          containers.addContainer(container);
          if (pipelineManager.containsPipeline(pipelineID)) {
            pipelineManager.addContainerToPipeline(pipelineID, containerID);
          } else if (containerInfo.getState().
              equals(HddsProtos.LifeCycleState.OPEN)) {
            // Pipeline should exist, but not
            throw new PipelineNotFoundException();
          }
          //recon may receive report of closed container,
          // no corresponding Pipeline can be synced for scm.
          // just only add the container.
        }).onException(() -> {
          containers.removeContainer(containerID);
          transactionBuffer.removeFromBuffer(containerStore, containerID);
        }).execute();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean contains(final HddsProtos.ContainerID id) {
    lock.readLock().lock();
    try {
      // TODO: Remove the protobuf conversion after fixing ContainerStateMap.
      return containers.contains(ContainerID.getFromProtobuf(id));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void updateContainerState(final HddsProtos.ContainerID containerID,
                                   final LifeCycleEvent event)
      throws IOException, InvalidStateTransitionException {
    // TODO: Remove the protobuf conversion after fixing ContainerStateMap.
    final ContainerID id = ContainerID.getFromProtobuf(containerID);

    lock.writeLock().lock();
    try {
      if (containers.contains(id)) {
        final ContainerInfo oldInfo = containers.getContainerInfo(id);
        final LifeCycleState oldState = oldInfo.getState();
        final LifeCycleState newState = stateMachine.getNextState(
            oldInfo.getState(), event);
        if (newState.getNumber() > oldState.getNumber()) {
          ExecutionUtil.create(() -> {
            containers.updateState(id, oldState, newState);
            transactionBuffer.addToBuffer(containerStore, id,
                containers.getContainerInfo(id));
          }).onException(() -> {
            transactionBuffer.addToBuffer(containerStore, id, oldInfo);
            containers.updateState(id, newState, oldState);
          }).execute();
          containerStateChangeActions.getOrDefault(event, info -> {
          }).execute(oldInfo);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }


  @Override
  public Set<ContainerReplica> getContainerReplicas(
      final HddsProtos.ContainerID id) {
    lock.readLock().lock();
    try {
      return containers.getContainerReplicas(
          ContainerID.getFromProtobuf(id));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void updateContainerReplica(final HddsProtos.ContainerID id,
                                     final ContainerReplica replica) {
    lock.writeLock().lock();
    try {
      containers.updateContainerReplica(ContainerID.getFromProtobuf(id),
          replica);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeContainerReplica(final HddsProtos.ContainerID id,
                                     final ContainerReplica replica) {
    lock.writeLock().lock();
    try {
      containers.removeContainerReplica(ContainerID.getFromProtobuf(id),
          replica);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void updateDeleteTransactionId(
      final Map<ContainerID, Long> deleteTransactionMap) throws IOException {
    lock.writeLock().lock();
    try {
      // TODO: Refactor this. Error handling is not done.
      for (Map.Entry<ContainerID, Long> transaction :
          deleteTransactionMap.entrySet()) {
        final ContainerInfo info = containers.getContainerInfo(
            transaction.getKey());
        info.updateDeleteTransactionId(transaction.getValue());
        containerStore.put(info.containerID(), info);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ContainerInfo getMatchingContainer(final long size, String owner,
      PipelineID pipelineID, NavigableSet<ContainerID> containerIDs) {
    if (containerIDs.isEmpty()) {
      return null;
    }

    // Get the last used container and find container above the last used
    // container ID.
    final ContainerState key = new ContainerState(owner, pipelineID);
    final ContainerID lastID =
        lastUsedMap.getOrDefault(key, containerIDs.first());

    // There is a small issue here. The first time, we will skip the first
    // container. But in most cases it will not matter.
    NavigableSet<ContainerID> resultSet = containerIDs.tailSet(lastID, false);
    if (resultSet.isEmpty()) {
      resultSet = containerIDs;
    }

    lock.readLock().lock();
    try {
      ContainerInfo selectedContainer = findContainerWithSpace(size, resultSet);
      if (selectedContainer == null) {

        // If we did not find any space in the tailSet, we need to look for
        // space in the headset, we need to pass true to deal with the
        // situation that we have a lone container that has space. That is we
        // ignored the last used container under the assumption we can find
        // other containers with space, but if have a single container that is
        // not true. Hence we need to include the last used container as the
        // last element in the sorted set.

        resultSet = containerIDs.headSet(lastID, true);
        selectedContainer = findContainerWithSpace(size, resultSet);
      }

      // TODO: cleanup entries in lastUsedMap
      if (selectedContainer != null) {
        lastUsedMap.put(key, selectedContainer.containerID());
      }
      return selectedContainer;
    } finally {
      lock.readLock().unlock();
    }
  }

  private ContainerInfo findContainerWithSpace(final long size,
      final NavigableSet<ContainerID> searchSet) {
    // Get the container with space to meet our request.
    for (ContainerID id : searchSet) {
      final ContainerInfo containerInfo = containers.getContainerInfo(id);
      if (containerInfo.getUsedBytes() + size <= this.containerSize) {
        containerInfo.updateLastUsedTime();
        return containerInfo;
      }
    }
    return null;
  }


  public void removeContainer(final HddsProtos.ContainerID id)
      throws IOException {
    lock.writeLock().lock();
    try {
      final ContainerID cid = ContainerID.getFromProtobuf(id);
      final ContainerInfo containerInfo = containers.getContainerInfo(cid);
      ExecutionUtil.create(() -> {
        transactionBuffer.removeFromBuffer(containerStore, cid);
        containers.removeContainer(cid);
      }).onException(() -> containerStore.put(cid, containerInfo)).execute();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void reinitialize(
      Table<ContainerID, ContainerInfo> store) throws IOException {
    lock.writeLock().lock();
    try {
      close();
      this.containerStore = store;
      this.containers = new ContainerStateMap();
      this.lastUsedMap = new ConcurrentHashMap<>();
      initialize();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      containerStore.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
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
    private DBTransactionBuffer transactionBuffer;

    public Builder setSCMDBTransactionBuffer(DBTransactionBuffer buffer) {
      this.transactionBuffer = buffer;
      return this;
    }
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
      Preconditions.checkNotNull(table);

      final ContainerStateManagerV2 csm = new ContainerStateManagerImpl(
          conf, pipelineMgr, table, transactionBuffer);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(RequestType.CONTAINER, csm,
              scmRatisServer);

      return (ContainerStateManagerV2) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{ContainerStateManagerV2.class}, invocationHandler);
    }

  }
}

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

package org.apache.hadoop.hdds.scm.container.states;

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container State Map acts like a unified map for various attributes that are
 * used to select containers when we need allocated blocks.
 * <p>
 * This class provides the ability to query 5 classes of attributes. They are
 * <p>
 * 1. LifeCycleStates - LifeCycle States of container describe in which state
 * a container is. For example, a container needs to be in Open State for a
 * client to able to write to it.
 * <p>
 * 2. Owners - Each instance of Name service, for example, Namenode of HDFS or
 * Ozone Manager (OM) of Ozone or CBlockServer --  is an owner. It is
 * possible to have many OMs for a Ozone cluster and only one SCM. But SCM
 * keeps the data from each OM in separate bucket, never mixing them. To
 * write data, often we have to find all open containers for a specific owner.
 * <p>
 * 3. ReplicationType - The clients are allowed to specify what kind of
 * replication pipeline they want to use. Each Container exists on top of a
 * pipeline, so we need to get ReplicationType that is specified by the user.
 * <p>
 * 4. ReplicationConfig - The replication config represents how many copies
 * of data should be made, right now we support 2 different types, ONE
 * Replica and THREE Replica. User can specify how many copies should be made
 * for a ozone key.
 * <p>
 * The most common access pattern of this class is to select a container based
 * on all these parameters, for example, when allocating a block we will
 * select a container that belongs to user1, with Ratis replication which can
 * make 3 copies of data. The fact that we will look for open containers by
 * default and if we cannot find them we will add new containers.
 * <p>
 * All the calls are idempotent.
 * <p>
 * This class is NOT thread-safe.
 */
public class ContainerStateMap {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerStateMap.class);

  /**
   * Map {@link LifeCycleState} to {@link ContainerInfo}.
   * Note that a {@link ContainerInfo} can only exists in at most one of the {@link LifeCycleState}s.
   */
  private final ContainerAttribute<LifeCycleState> lifeCycleStateMap = new ContainerAttribute<>(LifeCycleState.class);
  /**
   * Map {@link ReplicationType} to {@link ContainerInfo}.
   * Note that a {@link ContainerInfo} can only exists in at most one of the {@link ReplicationType}s.
   */
  private final ContainerAttribute<ReplicationType> typeMap = new ContainerAttribute<>(ReplicationType.class);
  /**
   * Map {@link ContainerID} to ({@link ContainerInfo} and {@link ContainerReplica}).
   * Note that the following sets are exactly the same
   * 1. The {@link ContainerInfo} in this map.
   * 2. The {@link ContainerInfo} in the union of all the states in {@link #lifeCycleStateMap}.
   * 2. The {@link ContainerInfo} in the union of all the types in {@link #typeMap}.
   */
  private final ContainerMap containerMap = new ContainerMap();

  /**
   * Two levels map.
   * Outer container map: {@link ContainerID} -> {@link ContainerEntry} (info and replicas)
   * Inner replica map: {@link DatanodeID} -> {@link ContainerReplica}
   */
  private static class ContainerMap {
    private final NavigableMap<ContainerID, ContainerEntry> map = new TreeMap<>();

    boolean contains(ContainerID id) {
      return map.containsKey(id);
    }

    ContainerInfo getInfo(ContainerID id) {
      final ContainerEntry entry = map.get(id);
      return entry == null ? null : entry.getInfo();
    }

    List<ContainerID> getContainerIDs(ContainerID start, int count) {
      Objects.requireNonNull(start, "start == null");
      Preconditions.assertTrue(count >= 0, "count < 0");
      return map.tailMap(start).keySet().stream()
          .limit(count)
          .collect(Collectors.toList());
    }

    List<ContainerInfo> getInfos(ContainerID start, int count) {
      Objects.requireNonNull(start, "start == null");
      Preconditions.assertTrue(count >= 0, "count < 0");
      return map.tailMap(start).values().stream()
          .map(ContainerEntry::getInfo)
          .limit(count)
          .collect(Collectors.toList());
    }

    Set<ContainerReplica> getReplicas(ContainerID id) {
      Objects.requireNonNull(id, "id == null");
      final ContainerEntry entry = map.get(id);
      return entry == null ? null : entry.getReplicas();
    }

    /**
     * Add if the given info not already in this map.
     *
     * @return true iff the given info is added.
     */
    boolean addIfAbsent(ContainerInfo info) {
      Objects.requireNonNull(info, "info == null");
      final ContainerID id = info.containerID();
      if (map.containsKey(id)) {
        return false; // already exist
      }
      final ContainerEntry previous = map.put(id, new ContainerEntry(info));
      Preconditions.assertNull(previous, "previous");
      return true;
    }

    ContainerReplica put(ContainerReplica replica) {
      Objects.requireNonNull(replica, "replica == null");
      final ContainerEntry entry = map.get(replica.getContainerID());
      return entry == null ? null : entry.put(replica);
    }

    ContainerInfo remove(ContainerID id) {
      Objects.requireNonNull(id, "id == null");
      final ContainerEntry removed = map.remove(id);
      return removed == null ? null : removed.getInfo();
    }

    ContainerReplica removeReplica(ContainerID containerID, DatanodeID datanodeID) {
      Objects.requireNonNull(containerID, "containerID == null");
      Objects.requireNonNull(datanodeID, "datanodeID == null");
      final ContainerEntry entry = map.get(containerID);
      return entry == null ? null : entry.removeReplica(datanodeID);
    }
  }

  /**
   * Create a ContainerStateMap.
   */
  public ContainerStateMap() {
  }

  /**
   * Adds a ContainerInfo Entry in the ContainerStateMap.
   *
   * @param info - container info
   */
  public void addContainer(final ContainerInfo info) {
    Objects.requireNonNull(info, "info == null");
    if (containerMap.addIfAbsent(info)) {
      lifeCycleStateMap.addNonExisting(info.getState(), info);
      typeMap.addNonExisting(info.getReplicationType(), info);
      LOG.trace("Added {}", info);
    }
  }

  public boolean contains(final ContainerID id) {
    return containerMap.contains(id);
  }

  /**
   * Removes a Container Entry from ContainerStateMap.
   *
   * @param id - ContainerID
   */
  public void removeContainer(final ContainerID id) {
    Objects.requireNonNull(id, "id == null");
    final ContainerInfo info = containerMap.remove(id);
    if (info != null) {
      lifeCycleStateMap.removeExisting(info.getState(), info);
      typeMap.removeExisting(info.getReplicationType(), info);
      LOG.trace("Removed {}", info);
    }
  }

  /**
   * Returns the latest state of Container from SCM's Container State Map.
   *
   * @param containerID - ContainerID
   * @return container info, if found else null.
   */
  public ContainerInfo getContainerInfo(final ContainerID containerID) {
    return containerMap.getInfo(containerID);
  }

  /**
   * Returns the latest list of DataNodes where replica for given containerId
   * exist.
   */
  public Set<ContainerReplica> getContainerReplicas(
      final ContainerID containerID) {
    Objects.requireNonNull(containerID, "containerID == null");
    return containerMap.getReplicas(containerID);
  }

  /**
   * Adds given datanodes as nodes where replica for given containerId exist.
   * Logs a debug entry if a datanode is already added as replica for given
   * ContainerId.
   */
  public void updateContainerReplica(ContainerReplica replica) {
    Objects.requireNonNull(replica, "replica == null");
    containerMap.put(replica);
  }

  /**
   * Remove a container Replica for given DataNode.
   */
  public void removeContainerReplica(final ContainerID containerID, DatanodeID datanodeID) {
    Objects.requireNonNull(containerID, "containerID == null");
    Objects.requireNonNull(datanodeID, "datanodeID == null");
    containerMap.removeReplica(containerID, datanodeID);
  }

  /**
   * Update the State of a container.
   *
   * @param containerID - ContainerID
   * @param currentState - CurrentState
   * @param newState - NewState.
   * @throws SCMException - in case of failure.
   */
  public void updateState(ContainerID containerID, LifeCycleState currentState,
      LifeCycleState newState) throws SCMException {
    if (currentState == newState) { // state not changed
      return;
    }
    final ContainerInfo currentInfo = containerMap.getInfo(containerID);
    if (currentInfo == null) { // container not found
      return;
    }
    lifeCycleStateMap.update(currentState, newState, containerID);
    LOG.trace("Updated the container {} from {} to {}", containerID, currentState, newState);
    currentInfo.setState(newState);
  }

  public List<ContainerID> getContainerIDs(ContainerID start, int count) {
    return containerMap.getContainerIDs(start, count);
  }

  /**
   *
   * @param state the state of the containers
   * @param start the start id
   * @param count the maximum size of the returned list
   * @return a list of sorted {@link ContainerID}s
   */
  public List<ContainerID> getContainerIDs(LifeCycleState state, ContainerID start, int count) {
    Preconditions.assertTrue(count >= 0, "count < 0");
    return lifeCycleStateMap.tailMap(state, start).keySet().stream()
        .limit(count)
        .collect(Collectors.toList());
  }

  public List<ContainerInfo> getContainerInfos(ContainerID start, int count) {
    return containerMap.getInfos(start, count);
  }

  /**
   *
   * @param state the state of the {@link ContainerInfo}s
   * @param start the start id
   * @param count the maximum size of the returned list
   * @return a list of {@link ContainerInfo}s sorted by {@link ContainerID}
   */
  public List<ContainerInfo> getContainerInfos(LifeCycleState state, ContainerID start, int count) {
    Preconditions.assertTrue(count >= 0, "count < 0");
    return lifeCycleStateMap.tailMap(state, start).values().stream()
        .limit(count)
        .collect(Collectors.toList());
  }

  public List<ContainerInfo> getContainerInfos(LifeCycleState state) {
    return lifeCycleStateMap.getCollection(state);
  }

  public List<ContainerInfo> getContainerInfos(ReplicationType type) {
    return typeMap.getCollection(type);
  }

  /** @return the number of containers for the given state. */
  public int getContainerCount(LifeCycleState state) {
    return lifeCycleStateMap.count(state);
  }
}

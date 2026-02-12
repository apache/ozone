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

package org.apache.hadoop.hdds.scm.container;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;

/**
 * ContainerManager is responsible for keeping track of all Containers and
 * managing all containers operations like creating, deleting etc.
 */
public interface ContainerManager {
  /**
   * Reinitialize the containerManager with the updated container store.
   * @param containerStore Container Table
   * @throws IOException
   */
  void reinitialize(Table<ContainerID, ContainerInfo> containerStore)
      throws IOException;

  /**
   * Returns the ContainerInfo from the container ID.
   *
   */
  ContainerInfo getContainer(ContainerID containerID)
      throws ContainerNotFoundException;

  default List<ContainerInfo> getContainers() {
    return getContainers(ContainerID.valueOf(0), Integer.MAX_VALUE);
  }

  /**
   * Returns containers under certain conditions.
   * Search container IDs from start ID(exclusive),
   * The max size of the searching range cannot exceed the
   * value of count.
   *
   * @param startID start containerID, &gt;=0,
   * start searching at the head if 0.
   * @param count count must be &gt;= 0
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big.
   *
   * @return a list of container.
   */
  List<ContainerInfo> getContainers(ContainerID startID, int count);

  List<ContainerInfo> getContainers(ReplicationType type);

  /**
   * Returns all the containers which are in the specified state.
   *
   * @return List of ContainerInfo
   */
  List<ContainerInfo> getContainers(LifeCycleState state);


  /**
   * Returns containers under certain conditions.
   * Search container IDs from start ID(exclusive),
   * The max size of the searching range cannot exceed the
   * value of count.
   *
   * @param startID start containerID, &gt;=0,
   * start searching at the head if 0.
   * @param count count must be &gt;= 0
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big.
   * @param state container state
   *
   * @return a list of container.
   */
  List<ContainerInfo> getContainers(ContainerID startID,
                                    int count, LifeCycleState state);

  /**
   * Returns the size of containers which are in the specified state.
   *
   * @return size of containers.
   */
  int getContainerStateCount(LifeCycleState state);

  /**
   * Returns true if the container exist, false otherwise.
   * @param id Container ID
   * @return true if container exist, else false
   */
  boolean containerExist(ContainerID id);

  /**
   * Allocates a new container for a given keyName and replication factor.
   *
   * @throws IOException
   */
  ContainerInfo allocateContainer(ReplicationConfig replicationConfig,
                                  String owner)
      throws IOException;

  /**
   * Update container state.
   * @param containerID - Container ID
   * @param event - container life cycle event
   * @throws IOException
   * @throws InvalidStateTransitionException
   */
  void updateContainerState(ContainerID containerID,
                            LifeCycleEvent event)
      throws IOException, InvalidStateTransitionException;

  /**
   * Bypasses the container state machine to change a container's state from DELETING or DELETED to CLOSED. This API was
   * introduced to fix a bug (HDDS-11136), and should be used with care otherwise.
   *
   * @see <a href="https://issues.apache.org/jira/browse/HDDS-11136">HDDS-11136</a>
   * @param containerID id of the container to transition
   * @throws IOException
   */
  void transitionDeletingOrDeletedToClosedState(ContainerID containerID) throws IOException;

  /**
   * Returns the latest list of replicas for given containerId.
   *
   * @param containerID Container ID
   * @return Set of ContainerReplica
   */
  Set<ContainerReplica> getContainerReplicas(ContainerID containerID)
      throws ContainerNotFoundException;

  /**
   * Adds a container Replica for the given Container.
   *
   * @param containerID Container ID
   * @param replica ContainerReplica
   */
  void updateContainerReplica(ContainerID containerID, ContainerReplica replica)
      throws ContainerNotFoundException;

  /**
   * Remove a container Replica form a given Container.
   *
   * @param containerID Container ID
   * @param replica ContainerReplica
   */
  void removeContainerReplica(ContainerID containerID, ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException;

  /**
   * Update deleteTransactionId according to deleteTransactionMap.
   *
   * @param deleteTransactionMap Maps the containerId to latest delete
   *                             transaction id for the container.
   * @throws IOException
   */
  void updateDeleteTransactionId(Map<ContainerID, Long> deleteTransactionMap)
      throws IOException;

  default ContainerInfo getMatchingContainer(long size, String owner,
                                     Pipeline pipeline) {
    return getMatchingContainer(size, owner, pipeline, Collections.emptySet());
  }

  /**
   * Returns ContainerInfo which matches the requirements.
   * @param size - the amount of space required in the container
   * @param owner - the user which requires space in its owned container
   * @param pipeline - pipeline to which the container should belong.
   * @param excludedContainerIDS - containerIds to be excluded.
   * @return ContainerInfo for the matching container, or null if a container could not be found and could not be
   * allocated
   */
  @Nullable
  ContainerInfo getMatchingContainer(long size, String owner,
                                     Pipeline pipeline,
                                     Set<ContainerID> excludedContainerIDS);

  /**
   * Once after report processor handler completes, call this to notify
   * container manager to increment metrics.
   * @param isFullReport
   * @param success
   */
  // Is it possible to remove this from the Interface?
  void notifyContainerReportProcessing(boolean isFullReport, boolean success);

  /**
   * Deletes a container from SCM.
   *
   * @param containerID - Container ID
   * @throws IOException
   */
  void deleteContainer(ContainerID containerID)
      throws IOException;

  /**
   * Returns containerStateManger.
   * @return containerStateManger
   */
  ContainerStateManager getContainerStateManager();

  /**
   * Update container info in the container manager.
   * This is used for updating container metadata like ackMissing flag.
   *
   * @param containerInfo Updated container info proto
   * @throws IOException
   */
  void updateContainerInfo(ContainerID containerID, ContainerInfoProto containerInfo)
      throws IOException;
}

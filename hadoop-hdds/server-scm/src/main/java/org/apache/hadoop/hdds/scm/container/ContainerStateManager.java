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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;

/**
 * A ContainerStateManager is responsible for keeping track of all the
 * container and its state inside SCM, it also exposes methods to read and
 * modify the container and its state.
 *
 * All the mutation operations are marked with {@link Replicate} annotation so
 * that when SCM-HA is enabled, the mutations are replicated from leader SCM
 * to the followers.
 *
 * When a method is marked with {@link Replicate} annotation it should follow
 * the below rules.
 *
 * 1. The method call should be Idempotent
 * 2. Arguments should be of protobuf objects
 * 3. Return type should be of protobuf object
 * 4. The declaration should throw RaftException
 *
 */
public interface ContainerStateManager {

  /* **********************************************************************
   * Container Life Cycle                                                 *
   *                                                                      *
   * Event and State Transition Mapping:                                  *
   *                                                                      *
   * State: OPEN         ----------------> CLOSING                        *
   * Event:                    FINALIZE                                   *
   *                                                                      *
   * State: CLOSING      ----------------> QUASI_CLOSED                   *
   * Event:                  QUASI_CLOSE                                  *
   *                                                                      *
   * State: CLOSING      ----------------> CLOSED                         *
   * Event:                     CLOSE                                     *
   *                                                                      *
   * State: QUASI_CLOSED ----------------> CLOSED                         *
   * Event:                  FORCE_CLOSE                                  *
   *                                                                      *
   * State: CLOSED       ----------------> DELETING                       *
   * Event:                    DELETE                                     *
   *                                                                      *
   * State: DELETING     ----------------> DELETED                        *
   * Event:                    CLEANUP                                    *
   *                                                                      *
   *                                                                      *
   * Container State Flow:                                                *
   *                                                                      *
   * [OPEN]--------------->[CLOSING]--------------->[QUASI_CLOSED]        *
   *          (FINALIZE)      |      (QUASI_CLOSE)        |               *
   *                          |                           |               *
   *                          |                           |               *
   *                  (CLOSE) |             (FORCE_CLOSE) |               *
   *                          |                           |               *
   *                          |                           |               *
   *                          +--------->[CLOSED]<--------+               *
   *                                        |                             *
   *                                (DELETE)|                             *
   *                                        |                             *
   *                                        |                             *
   *                                   [DELETING]                         *
   *                                        |                             *
   *                              (CLEANUP) |                             *
   *                                        |                             *
   *                                        V                             *
   *                                    [DELETED]                         *
   *                                                                      *
   ************************************************************************/

  /**
   *
   */
  boolean contains(ContainerID containerID);

  /**
   * Get {@link ContainerID}s.
   *
   * @param start the start {@link ContainerID} (inclusive)
   * @param count the size limit
   * @return a list of {@link ContainerID};
   */
  List<ContainerID> getContainerIDs(ContainerID start, int count);

  /**
   * Get {@link ContainerID}s for the given state.
   *
   * @param start the start {@link ContainerID} (inclusive)
   * @param count the size limit
   * @return a list of {@link ContainerID};
   */
  List<ContainerID> getContainerIDs(LifeCycleState state, ContainerID start, int count);

  /**
   * Get {@link ContainerInfo}s.
   *
   * @param start the start {@link ContainerID} (inclusive)
   * @param count the size limit
   * @return a list of {@link ContainerInfo};
   */
  List<ContainerInfo> getContainerInfos(ContainerID start, int count);

  /**
   * Get {@link ContainerInfo}s for the given state.
   *
   * @param start the start {@link ContainerID} (inclusive)
   * @param count the size limit
   * @return a list of {@link ContainerInfo};
   */
  List<ContainerInfo> getContainerInfos(LifeCycleState state, ContainerID start, int count);

  /** @return all {@link ContainerInfo}s for the given state. */
  List<ContainerInfo> getContainerInfos(LifeCycleState state);

  /**
   * @return number of containers for the given state.
   */
  int getContainerCount(LifeCycleState state);

  /** @return all {@link ContainerInfo}s for the given type. */
  List<ContainerInfo> getContainerInfos(ReplicationType type);

  /**
   *
   */
  ContainerInfo getContainer(ContainerID id);

  /**
   *
   */
  Set<ContainerReplica> getContainerReplicas(ContainerID id);

  /**
   *
   */
  void updateContainerReplica(ContainerReplica replica);

  /**
   *
   */
  void removeContainerReplica(ContainerReplica replica);

  /**
   *
   */
  @Replicate
  void addContainer(ContainerInfoProto containerInfo)
      throws IOException;

  /**
   *
   */
  @Replicate
  void updateContainerState(HddsProtos.ContainerID id,
                            HddsProtos.LifeCycleEvent event)
      throws IOException, InvalidStateTransitionException;


  /**
   * Bypasses the container state machine to change a container's state from DELETING or DELETED to CLOSED. This API was
   * introduced to fix a bug (HDDS-11136), and should be used with care otherwise.
   *
   * @see <a href="https://issues.apache.org/jira/browse/HDDS-11136">HDDS-11136</a>
   * @param id id of the container to transition
   * @throws IOException
   */
  @Replicate
  void transitionDeletingOrDeletedToClosedState(HddsProtos.ContainerID id) throws IOException;

  /**
   *
   */
  // Make this as @Replicate
  void updateDeleteTransactionId(Map<ContainerID, Long> deleteTransactionMap)
      throws IOException;

  /**
   *
   */
  ContainerInfo getMatchingContainer(long size, String owner,
                                     PipelineID pipelineID,
                                     NavigableSet<ContainerID> containerIDs);

  /**
   *
   */
  @Replicate
  void removeContainer(HddsProtos.ContainerID containerInfo)
      throws IOException;

  /**
   * Reinitialize the ContainerStateManager with container store.
   * @param containerStore container table.
   * @throws IOException
   */
  void reinitialize(Table<ContainerID, ContainerInfo> containerStore)
      throws IOException;
}

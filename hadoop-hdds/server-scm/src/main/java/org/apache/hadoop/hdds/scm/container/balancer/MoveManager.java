/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * MoveManager is responsible for keeping track of all Containers being moved,
 * and managing all container move operations like replication and deletion.
 */
public interface MoveManager {
  /**
   * This is used for indicating the result of move option and
   * the corresponding reason. this is useful for tracking
   * the result of move option
   */
  enum MoveResult {
    // both replication and deletion are completed
    COMPLETED,
    // RM is not ratis leader
    FAIL_LEADER_NOT_READY,
    // replication fail because the container does not exist in src
    REPLICATION_FAIL_NOT_EXIST_IN_SOURCE,
    // replication fail because the container exists in target
    REPLICATION_FAIL_EXIST_IN_TARGET,
    // replication fail because the container is not cloesed
    REPLICATION_FAIL_CONTAINER_NOT_CLOSED,
    // replication fail because the container is in inflightDeletion
    REPLICATION_FAIL_INFLIGHT_DELETION,
    // replication fail because the container is in inflightReplication
    REPLICATION_FAIL_INFLIGHT_REPLICATION,
    // replication fail because of timeout
    REPLICATION_FAIL_TIME_OUT,
    // replication fail because of node is not in service
    REPLICATION_FAIL_NODE_NOT_IN_SERVICE,
    // replication fail because node is unhealthy
    REPLICATION_FAIL_NODE_UNHEALTHY,
    // replication succeed, but deletion fail because of timeout
    DELETION_FAIL_TIME_OUT,
    // deletion fail because of node is not in service
    DELETION_FAIL_NODE_NOT_IN_SERVICE,
    // replication succeed, but deletion fail because because
    // node is unhealthy
    DELETION_FAIL_NODE_UNHEALTHY,
    // replication succeed, but if we delete the container from
    // the source datanode , the policy(eg, replica num or
    // rack location) will not be satisfied, so we should not delete
    // the container
    DELETE_FAIL_POLICY,
    //  replicas + target - src does not satisfy placement policy
    REPLICATION_NOT_HEALTHY,
    //write DB error
    FAIL_CAN_NOT_RECORD_TO_DB
  }

  /**
  * notify MoveManager that the current scm has become leader.
  */
  void onLeaderReady();

  /**
   * notify MoveManager that the current scm leader step down.
   */
  void onNotLeader();

  /**
   * get all the pending move operations .
   */
  Map<ContainerID, Pair<CompletableFuture<MoveResult>, MoveDataNodePair>>
      getPendingMove();

  /**
   * move a container replica from source datanode to
   * target datanode.
   *
   * @param cid Container to move
   * @param src source datanode
   * @param tgt target datanode
   */
  CompletableFuture<MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws ContainerNotFoundException, NodeNotFoundException,
      TimeoutException, ContainerReplicaNotFoundException;
}

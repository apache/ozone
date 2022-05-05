/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.utils.db.Table;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * MoveScheduler schedule move options.
 */
public interface MoveScheduler {

  /**
   * This is used for indicating the result of move option and
   * the corresponding reason. this is useful for tracking
   * the result of move option
   */
  enum MoveResult {
    // both replication and deletion are completed
    COMPLETED,
    // RM is not running
    FAIL_NOT_RUNNING,
    // RM is not ratis leader
    FAIL_NOT_LEADER,
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
    // deletion fail because of node is not in service
    DELETION_FAIL_NODE_NOT_IN_SERVICE,
    // replication succeed, but deletion fail because of timeout
    DELETION_FAIL_TIME_OUT,
    // replication succeed, but deletion fail because because
    // node is unhealthy
    DELETION_FAIL_NODE_UNHEALTHY,
    // replication succeed, but if we delete the container from
    // the source datanode , the policy(eg, replica num or
    // rack location) will not be satisfied, so we should not delete
    // the container
    DELETE_FAIL_POLICY,
    //  replicas + target - src does not satisfy placement policy
    PLACEMENT_POLICY_NOT_SATISFIED,
    //unexpected action, remove src at inflightReplication
    UNEXPECTED_REMOVE_SOURCE_AT_INFLIGHT_REPLICATION,
    //unexpected action, remove target at inflightDeletion
    UNEXPECTED_REMOVE_TARGET_AT_INFLIGHT_DELETION,
    //write DB error
    FAIL_CAN_NOT_RECORD_TO_DB
  }
  /**
   * completeMove a move action for a given container.
   *
   * @param contianerIDProto Container to which the move option is finished
   */
  @Replicate
  void completeMove(HddsProtos.ContainerID contianerIDProto);

  /**
   * start a move action for a given container.
   *
   * @param contianerIDProto Container to move
   * @param mp encapsulates the source and target datanode infos
   */
  @Replicate
  void startMove(HddsProtos.ContainerID contianerIDProto,
                 HddsProtos.MoveDataNodePairProto mp) throws IOException;

  /**
   * get the MoveDataNodePair of the giver container.
   *
   * @param cid Container to move
   * @return null if cid is not found in MoveScheduler,
   *          or the corresponding MoveDataNodePair
   */
  MoveDataNodePair getMoveDataNodePair(ContainerID cid);

  /**
   * add a completeFuture for tracking and replying move.
   */
  void addMoveCompleteFuture(
      ContainerID cid, CompletableFuture<MoveResult> moveFuture);

  /**
   * complete the CompletableFuture of the container in the given Map with
   * a given MoveResult.
   */
  void compleleteMoveFutureWithResult(ContainerID cid, MoveResult mr);

  /**
   * Reinitialize the MoveScheduler with DB if become leader.
   */
  void reinitialize(Table<ContainerID,
      MoveDataNodePair> moveTable) throws IOException;

  /**
   * get all the inflight move info.
   */
  Map<ContainerID, MoveDataNodePair> getInflightMove();

}

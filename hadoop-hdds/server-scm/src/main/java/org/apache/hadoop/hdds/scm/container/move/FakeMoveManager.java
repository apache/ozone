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
package org.apache.hadoop.hdds.scm.container.move;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * this is a fake move manager used for recon and tests.
 * */
public class FakeMoveManager implements MoveManager {

  public FakeMoveManager() {

  }
  /**
   * notify move manager that a container op has been completed.
   *
   * @param cop ContainerReplicaOp
   * @param containerID ContainerID for which to complete
   */
  @Override
  public void notifyContainerOpCompleted(ContainerReplicaOp cop,
                                  ContainerID containerID) {
  }


  /**
   * notify move manager that a container op has been Expired.
   *
   * @param cop ContainerReplicaOp
   * @param containerID ContainerID for which to complete
   */
  @Override
  public void notifyContainerOpExpired(ContainerReplicaOp cop,
                                ContainerID containerID) {

  }

  /**
   * notify MoveManager that the current scm has become leader.
   */
  @Override
  public void onLeaderReady() {

  }

  /**
   * notify MoveManager that the current scm leader step down.
   */
  @Override
  public void onNotLeader() {

  }

  /**
   * get all the pending move operations .
   */
  public Map<ContainerID, MoveDataNodePair> getPendingMove() {
    return Collections.emptyMap();
  }

  /**
   * move a container replica from source datanode to
   * target datanode.
   *
   * @param cid Container to move
   * @param src source datanode
   * @param tgt target datanode
   */
  @Override
  public CompletableFuture<MoveManager.MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws ContainerNotFoundException, NodeNotFoundException,
      TimeoutException, ContainerReplicaNotFoundException {
    CompletableFuture<MoveResult> ret = new CompletableFuture<>();
    ret.complete(MoveResult.FAIL_LEADER_NOT_READY);
    return ret;
  }
}

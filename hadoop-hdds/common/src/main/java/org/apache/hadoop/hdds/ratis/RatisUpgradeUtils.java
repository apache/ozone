/**
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

package org.apache.hadoop.hdds.ratis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis utility functions.
 */
public final class RatisUpgradeUtils {

  private RatisUpgradeUtils() {
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisUpgradeUtils.class);

  /**
   * Flush all committed transactions in a given Raft Server for a given group.
   * @param stateMachine state machine to use
   * @param raftGroup raft group
   * @param server Raft server proxy instance.
   * @param maxTimeToWaitSeconds Max time to wait before declaring failure.
   * @throws InterruptedException when interrupted
   * @throws IOException on error while waiting
   */
  public static void waitForAllTxnsApplied(
      StateMachine stateMachine,
      RaftGroup raftGroup,
      RaftServerProxy server,
      long maxTimeToWaitSeconds,
      long timeBetweenRetryInSeconds)
      throws InterruptedException, IOException {

    long intervalTime = TimeUnit.SECONDS.toMillis(timeBetweenRetryInSeconds);
    long endTime = System.currentTimeMillis() +
        TimeUnit.SECONDS.toMillis(maxTimeToWaitSeconds);
    boolean success = false;
    while (System.currentTimeMillis() < endTime) {
      success = checkIfAllTransactionsApplied(stateMachine, server, raftGroup);
      if (success) {
        break;
      }
      Thread.sleep(intervalTime);
    }

    if (!success) {
      throw new IOException(String.format("After waiting for %d seconds, " +
          "State Machine has not applied  all the transactions.",
          maxTimeToWaitSeconds));
    }

    long snapshotIndex = stateMachine.takeSnapshot();
    if (snapshotIndex != stateMachine.getLastAppliedTermIndex().getIndex()) {
      throw new IOException("Index from Snapshot does not match last applied " +
          "Index");
    }
  }

  private static boolean checkIfAllTransactionsApplied(
      StateMachine stateMachine,
      RaftServerProxy serverProxy,
      RaftGroup raftGroup) throws IOException {
    LOG.info("Checking for pending transactions to be applied.");
    RaftServerImpl impl = serverProxy.getImpl(raftGroup.getGroupId());
    long lastCommittedIndex = impl.getState().getLog().getLastCommittedIndex();
    long appliedIndex = stateMachine.getLastAppliedTermIndex().getIndex();
    LOG.info("lastCommittedIndex = {}, appliedIndex = {}",
        lastCommittedIndex, appliedIndex);
    return (lastCommittedIndex == appliedIndex);
  }

}

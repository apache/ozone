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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.raftlog.RaftLog;
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
   * @param impl RaftServerImpl instance
   * @param maxTimeToWaitSeconds Max time to wait before declaring failure.
   * @param timeBetweenRetryInSeconds interval time to retry.
   * @throws InterruptedException when interrupted
   * @throws IOException on error while waiting
   */
  public static void waitForAllTxnsApplied(
      StateMachine stateMachine,
      RaftServerImpl impl,
      long maxTimeToWaitSeconds,
      long timeBetweenRetryInSeconds)
      throws InterruptedException, IOException {

    long intervalTime = TimeUnit.SECONDS.toMillis(timeBetweenRetryInSeconds);
    long endTime = System.currentTimeMillis() +
        TimeUnit.SECONDS.toMillis(maxTimeToWaitSeconds);
    boolean success = false;
    while (System.currentTimeMillis() < endTime) {
      success = checkIfAllTransactionsApplied(stateMachine, impl);
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
  }

  /**
   * Take a snapshot of the state machine at the last index, and purge ALL logs.
   * @param impl RaftServerImpl instance
   * @param stateMachine state machine to use
   * @throws IOException on Error.
   */
  public static long takeSnapshotAndPurgeLogs(RaftServerImpl impl,
                                              StateMachine stateMachine)
      throws IOException {

    long snapshotIndex = stateMachine.takeSnapshot();
    if (snapshotIndex != stateMachine.getLastAppliedTermIndex().getIndex()) {
      throw new IOException("Index from Snapshot does not match last applied " +
          "Index");
    }

    RaftLog raftLog = impl.getState().getLog();
    // In order to get rid of all logs, make sure we also account for
    // intermediate Ratis entries that do not pertain to OM.
    long lastIndex = Math.max(snapshotIndex,
        raftLog.getLastEntryTermIndex().getIndex());

    CompletableFuture<Long> purgeFuture =
        raftLog.syncWithSnapshot(lastIndex);
    try {
      Long purgeIndex = purgeFuture.get();
      if (purgeIndex != lastIndex) {
        throw new IOException("Purge index " + purgeIndex +
            " does not match last index " + lastIndex);
      }
    } catch (Exception e) {
      throw new IOException("Unable to purge logs.", e);
    }
    return lastIndex;
  }

  private static boolean checkIfAllTransactionsApplied(
      StateMachine stateMachine,
      RaftServerImpl impl) {
    LOG.info("Checking for pending transactions to be applied.");
    long lastCommittedIndex = impl.getState().getLog().getLastCommittedIndex();
    long appliedIndex = stateMachine.getLastAppliedTermIndex().getIndex();
    LOG.info("committedIndex = {}, appliedIndex = {}",
        lastCommittedIndex, appliedIndex);
    return (lastCommittedIndex == appliedIndex);
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server.ratis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Class for SCM StateMachine.
 */
public class SCMStateMachine extends BaseStateMachine {
  static final Logger LOG =
      LoggerFactory.getLogger(SCMStateMachine.class);
  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();
  private final SCMRatisServer scmRatisServer;
  private final StorageContainerManager scm;
  private RaftGroupId raftGroupId;
  private final SCMRatisSnapshotInfo snapshotInfo;
  private final ExecutorService executorService;
  private final ExecutorService installSnapshotExecutor;

  // Map which contains index and term for the ratis transactions which are
  // stateMachine entries which are recived through applyTransaction.
  private ConcurrentMap<Long, Long> applyTransactionMap =
      new ConcurrentSkipListMap<>();

  /**
   * Create a SCM state machine.
   */
  public SCMStateMachine(SCMRatisServer ratisServer) {
    this.scmRatisServer = ratisServer;
    this.scm = ratisServer.getSCM();

    // TODO: remove the whole file later
    this.snapshotInfo = null;
    updateLastAppliedIndexWithSnaphsotIndex();

    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("SCM StateMachine ApplyTransaction Thread - %d").build();
    this.executorService = HadoopExecutors.newSingleThreadExecutor(build);
    this.installSnapshotExecutor = HadoopExecutors.newSingleThreadExecutor();
  }

  /**
   * Initializes the State Machine with the given server, group and storage.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId id,
                         RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(server, id, raftStorage);
      this.raftGroupId = id;
      storage.init(raftStorage);
    });
  }

  /**
   * Pre-execute the update request into state machine.
   */
  @Override
  public TransactionContext startTransaction(
      RaftClientRequest raftClientRequest) {
    return TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(raftClientRequest.getMessage().getContent())
        .build();
  }

  /**
   * Apply a committed log entry to state machine.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    CompletableFuture<Message> ratisFuture =
        new CompletableFuture<>();
    //TODO execute SCMRequest and process SCMResponse
    return ratisFuture;
  }

  /**
   * Query state machine.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    //TODO make handler respond to the query request.
    return CompletableFuture.completedFuture(request);
  }

  /**
   * Pause state machine.
   */
  @Override
  public void pause() {
    getLifeCycle().transition(LifeCycle.State.PAUSING);
    getLifeCycle().transition(LifeCycle.State.PAUSED);
  }

  /**
   * Unpause state machine and update the lastAppliedIndex.
   * Following after uploading new state to state machine.
   */
  public void unpause(long newLastAppliedSnaphsotIndex,
                      long newLastAppliedSnapShotTermIndex) {
    getLifeCycle().startAndTransition(() -> {
      this.setLastAppliedTermIndex(TermIndex.valueOf(
          newLastAppliedSnapShotTermIndex, newLastAppliedSnaphsotIndex));
    });
  }

  /**
   * Take SCM snapshot and write index to file.
   * @return actual index or 0 if error.
   */
  @Override
  public long takeSnapshot() throws IOException {
    LOG.info("Saving Ratis snapshot on the SCM.");
    if (scm != null) {
      // TODO: remove the whole file later
      return 0;
    }
    return 0;
  }

  /**
   * Get latest SCM snapshot.
   */
  @Override
  public SnapshotInfo getLatestSnapshot() {
    return snapshotInfo;
  }

  private synchronized void updateLastApplied() {
    Long appliedTerm = null;
    long appliedIndex = -1;
    for(long i = getLastAppliedTermIndex().getIndex() + 1;; i++) {
      final Long removed = applyTransactionMap.remove(i);
      if (removed == null) {
        break;
      }
      appliedTerm = removed;
      appliedIndex = i;
    }
    if (appliedTerm != null) {
      updateLastAppliedTermIndex(appliedTerm, appliedIndex);
    }
  }

  /**
   * Called to notify state machine about indexes which are processed
   * internally by Raft Server, this currently happens when conf entries are
   * processed in raft Server. This keep state machine to keep a track of index
   * updates.
   */
  public void notifyIndexUpdate(long currentTerm, long index) {
    applyTransactionMap.put(index, currentTerm);
    updateLastApplied();
    snapshotInfo.updateTerm(currentTerm);
  }

  /**
   * Notifies the state machine that the raft peer is no longer leader.
   */
  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    scmRatisServer.updateServerRole();
  }

  /**
   * Transfer from log entry to string.
   */
  @Override
  public String toStateMachineLogEntryString(
      RaftProtos.StateMachineLogEntryProto proto) {
    //TODO implement transfer from proto to SCMRequest body.
    return null;
  }

  /**
   * Update lastAppliedIndex term in snapshot info.
   */
  public void updateLastAppliedIndexWithSnaphsotIndex() {
    setLastAppliedTermIndex(TermIndex.valueOf(snapshotInfo.getTerm(),
        snapshotInfo.getIndex()));
    LOG.info("LastAppliedIndex set from SnapShotInfo {}",
        getLastAppliedTermIndex());
  }

  @VisibleForTesting
  void addApplyTransactionTermIndex(long term, long index) {
    applyTransactionMap.put(index, term);
  }

  public void stop() {
    HadoopExecutors.shutdown(executorService, LOG, 5, TimeUnit.SECONDS);
    HadoopExecutors.shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
  }
}

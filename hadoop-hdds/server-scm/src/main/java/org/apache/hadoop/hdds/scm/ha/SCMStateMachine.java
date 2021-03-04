/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImplV2;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.SCM_NOT_INITIALIZED;

/**
 * TODO.
 */
public class SCMStateMachine extends BaseStateMachine {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMStateMachine.class);

  private StorageContainerManager scm;
  private SCMRatisServer ratisServer;
  private Map<RequestType, Object> handlers;
  private DBTransactionBuffer transactionBuffer;
  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();
  private final AtomicBoolean isInitialized;
  private ExecutorService installSnapshotExecutor;

  public SCMStateMachine(final StorageContainerManager scm,
      final SCMRatisServer ratisServer, DBTransactionBuffer buffer)
      throws SCMException {
    this.scm = scm;
    this.ratisServer = ratisServer;
    this.handlers = new EnumMap<>(RequestType.class);
    this.transactionBuffer = buffer;
    TransactionInfo latestTrxInfo =
        this.transactionBuffer.getLatestTrxInfo();
    if (!latestTrxInfo.isInitialized()) {
      if (!updateLastAppliedTermIndex(latestTrxInfo.getTerm(),
          latestTrxInfo.getTransactionIndex())) {
        throw new SCMException(
            String.format("Failed to update LastAppliedTermIndex " +
                    "in StateMachine to term:{} index:{}",
                latestTrxInfo.getTerm(), latestTrxInfo.getTransactionIndex()
            ), SCM_NOT_INITIALIZED);
      }
    }
    this.installSnapshotExecutor = HadoopExecutors.newSingleThreadExecutor();
    isInitialized = new AtomicBoolean(true);
  }

  public SCMStateMachine(boolean init) {
    isInitialized = new AtomicBoolean(init);
  }
  public void registerHandler(RequestType type, Object handler) {
    handlers.put(type, handler);
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    // Transaction buffer will be null during scm initlialization phase
    return transactionBuffer == null ?
        null :
        transactionBuffer.getLatestSnapshot();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(
      final TransactionContext trx) {
    final CompletableFuture<Message> applyTransactionFuture =
        new CompletableFuture<>();
    try {
      final SCMRatisRequest request = SCMRatisRequest.decode(
          Message.valueOf(trx.getStateMachineLogEntry().getLogData()));
      applyTransactionFuture.complete(process(request));
      transactionBuffer.updateLatestTrxInfo(TransactionInfo.builder()
          .setCurrentTerm(trx.getLogEntry().getTerm())
          .setTransactionIndex(trx.getLogEntry().getIndex())
          .build());
    } catch (Exception ex) {
      applyTransactionFuture.completeExceptionally(ex);
    }
    return applyTransactionFuture;
  }

  private Message process(final SCMRatisRequest request) throws Exception {
    try {
      final Object handler = handlers.get(request.getType());

      if (handler == null) {
        throw new IOException("No handler found for request type " +
            request.getType());
      }

      final List<Class<?>> argumentTypes = new ArrayList<>();
      for(Object args : request.getArguments()) {
        argumentTypes.add(args.getClass());
      }
      final Object result = handler.getClass().getMethod(
          request.getOperation(), argumentTypes.toArray(new Class<?>[0]))
          .invoke(handler, request.getArguments());
      return SCMRatisResponse.encode(result);
    } catch (NoSuchMethodException | SecurityException ex) {
      throw new InvalidProtocolBufferException(ex.getMessage());
    } catch (InvocationTargetException e) {
      final Exception targetEx = (Exception) e.getTargetException();
      throw targetEx != null ? targetEx : e;
    }
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    LOG.info("current leader SCM steps down.");

    if (!isInitialized.get()) {
      return;
    }
    scm.getScmContext().updateLeaderAndTerm(false, 0);
    scm.getSCMServiceManager().notifyStatusChanged();
  }

  /**
   * Leader SCM has purged entries from its log. To catch up, SCM must download
   * the latest checkpoint from the leader SCM and install it.
   * @param roleInfoProto the leader node information
   * @param firstTermIndexInLog TermIndex of the first append entry available
   *                           in the Leader's log.
   * @return the last term index included in the installed snapshot.
   */
  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {

    String leaderNodeId = RaftPeerId.valueOf(roleInfoProto.getFollowerInfo()
        .getLeaderInfo().getId().getId()).toString();
    LOG.info("Received install snapshot notification from SCM leader: {} with "
        + "term index: {}", leaderNodeId, firstTermIndexInLog);

    CompletableFuture<TermIndex> future = CompletableFuture.supplyAsync(
        () -> scm.getScmHAManager().installSnapshotFromLeader(leaderNodeId),
        installSnapshotExecutor);
    return future;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId,
                                  RaftPeerId newLeaderId) {
    if (!groupMemberId.getPeerId().equals(newLeaderId)) {
      LOG.info("leader changed, yet current SCM is still follower.");
      return;
    }

    if (!isInitialized.get()) {
      return;
    }

    long term = scm.getScmHAManager()
        .getRatisServer()
        .getDivision()
        .getInfo()
        .getCurrentTerm();

    LOG.info("current SCM becomes leader of term {}.", term);

    scm.getScmContext().updateLeaderAndTerm(true, term);
    scm.getSCMServiceManager().notifyStatusChanged();

    DeletedBlockLog deletedBlockLog = scm.getScmBlockManager()
        .getDeletedBlockLog();
    Preconditions.checkArgument(
        deletedBlockLog instanceof DeletedBlockLogImplV2);
    ((DeletedBlockLogImplV2) deletedBlockLog)
          .onBecomeLeader();
  }

  @Override
  public long takeSnapshot() throws IOException {
    long startTime = Time.monotonicNow();
    TermIndex lastTermIndex = getLastAppliedTermIndex();
    long lastAppliedIndex = lastTermIndex.getIndex();
    if (isInitialized.get()) {
      TransactionInfo lastAppliedTrxInfo =
          TransactionInfo.fromTermIndex(lastTermIndex);
      if (transactionBuffer.getLatestTrxInfo().compareTo(lastAppliedTrxInfo)
          < 0) {
        transactionBuffer.updateLatestTrxInfo(
            TransactionInfo.builder().setCurrentTerm(lastTermIndex.getTerm())
                .setTransactionIndex(lastTermIndex.getIndex()).build());
        transactionBuffer.setLatestSnapshot(
            transactionBuffer.getLatestTrxInfo().toSnapshotInfo());
      } else {
        lastAppliedIndex =
            transactionBuffer.getLatestTrxInfo().getTransactionIndex();
      }

      transactionBuffer.flush();
      LOG.info("Current Snapshot Index {}, takeSnapshot took {} ms",
          lastAppliedIndex, Time.monotonicNow() - startTime);
    }
    super.takeSnapshot();
    return lastAppliedIndex;
  }

  @Override
  public void notifyTermIndexUpdated(long term, long index) {
    if (transactionBuffer != null) {
      transactionBuffer.updateLatestTrxInfo(
          TransactionInfo.builder().setCurrentTerm(term)
              .setTransactionIndex(index).build());
    }
    // We need to call updateLastApplied here because now in ratis when a
    // node becomes leader, it is checking stateMachineIndex >=
    // placeHolderIndex (when a node becomes leader, it writes a conf entry
    // with some information like its peers and termIndex). So, calling
    // updateLastApplied updates lastAppliedTermIndex.
    updateLastAppliedTermIndex(term, index);
  }


  @Override
  public void notifyConfigurationChanged(long term, long index,
      RaftProtos.RaftConfigurationProto newRaftConfiguration) {
  }

  @Override
  public void pause() {
    getLifeCycle().transition(LifeCycle.State.PAUSING);
    getLifeCycle().transition(LifeCycle.State.PAUSED);
  }

  public void stop() {
    HadoopExecutors.
        shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
  }
  /**
   * Unpause the StateMachine, re-initialize the DoubleBuffer and update the
   * lastAppliedIndex. This should be done after uploading new state to the
   * StateMachine.
   */
  public void unpause(long newLastAppliedSnaphsotIndex,
      long newLastAppliedSnapShotTermIndex) {
    getLifeCycle().startAndTransition(() -> {
      try {
        transactionBuffer.init();
        this.setLastAppliedTermIndex(TermIndex
            .valueOf(newLastAppliedSnapShotTermIndex,
                newLastAppliedSnaphsotIndex));
      } catch (IOException ioe) {
        LOG.error("Failed to unpause ", ioe);
      }
    });
  }
}

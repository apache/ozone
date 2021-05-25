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
import java.util.EnumMap;
import java.util.Map;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImplV2;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SCMStateMachine is the state machine for SCMRatisServer. It is
 * responsible for applying ratis committed transactions to
 * {@link StorageContainerManager}.
 */
public class SCMStateMachine extends BaseStateMachine {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMStateMachine.class);

  private StorageContainerManager scm;
  private Map<RequestType, Object> handlers;
  private SCMHADBTransactionBuffer transactionBuffer;
  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();
  private final boolean isInitialized;
  private ExecutorService installSnapshotExecutor;

  // The atomic variable RaftServerImpl#inProgressInstallSnapshotRequest
  // ensures serializable between notifyInstallSnapshotFromLeader()
  // and reinitialize().
  private DBCheckpoint installingDBCheckpoint = null;

  public SCMStateMachine(final StorageContainerManager scm,
      SCMHADBTransactionBuffer buffer) {
    this.scm = scm;
    this.handlers = new EnumMap<>(RequestType.class);
    this.transactionBuffer = buffer;
    TransactionInfo latestTrxInfo = this.transactionBuffer.getLatestTrxInfo();
    if (!latestTrxInfo.isDefault()) {
      updateLastAppliedTermIndex(latestTrxInfo.getTerm(),
          latestTrxInfo.getTransactionIndex());
      LOG.info("Updated lastAppliedTermIndex {} with transactionInfo term and" +
          "Index", latestTrxInfo);
    }
    this.installSnapshotExecutor = HadoopExecutors.newSingleThreadExecutor();
    isInitialized = true;
  }

  public SCMStateMachine() {
    isInitialized = false;
  }

  public void registerHandler(RequestType type, Object handler) {
    handlers.put(type, handler);
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    // Transaction buffer will be null during scm initlialization phase
    return transactionBuffer == null
        ? null : transactionBuffer.getLatestSnapshot();
  }

  /**
   * Initializes the State Machine with the given server, group and storage.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId id,
      RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(server, id, raftStorage);
      storage.init(raftStorage);
    });
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
      ExitUtils.terminate(1, ex.getMessage(), ex, StateMachine.LOG);
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

      final Object result = handler.getClass().getMethod(
          request.getOperation(), request.getParameterTypes())
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
    if (!isInitialized) {
      return;
    }
    LOG.info("current leader SCM steps down.");

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
    if (!roleInfoProto.getFollowerInfo().hasLeaderInfo()) {
      return JavaUtils.completeExceptionally(new IOException("Failed to " +
          "notifyInstallSnapshotFromLeader due to missing leader info"));
    }
    String leaderAddress = roleInfoProto.getFollowerInfo()
        .getLeaderInfo().getId().getAddress();
    Optional<SCMNodeDetails> leaderDetails =
        scm.getSCMHANodeDetails().getPeerNodeDetails().stream().filter(
            p -> p.getRatisHostPortStr().equals(leaderAddress))
            .findFirst();
    Preconditions.checkState(leaderDetails.isPresent());
    final String leaderNodeId = leaderDetails.get().getNodeId();
    LOG.info("Received install snapshot notification from SCM leader: {} with "
        + "term index: {}", leaderAddress, firstTermIndexInLog);

    CompletableFuture<TermIndex> future = CompletableFuture.supplyAsync(
        () -> {
          DBCheckpoint checkpoint =
              scm.getScmHAManager().downloadCheckpointFromLeader(leaderNodeId);

          if (checkpoint == null) {
            return null;
          }

          TermIndex termIndex =
              scm.getScmHAManager().verifyCheckpointFromLeader(
                  leaderNodeId, checkpoint);

          if (termIndex != null) {
            setInstallingDBCheckpoint(checkpoint);
          }
          return termIndex;
        },
        installSnapshotExecutor);
    return future;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId,
                                  RaftPeerId newLeaderId) {
    if (!isInitialized) {
      return;
    }
    if (!groupMemberId.getPeerId().equals(newLeaderId)) {
      LOG.info("leader changed, yet current SCM is still follower.");
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
    scm.getSequenceIdGen().invalidateBatch();

    DeletedBlockLog deletedBlockLog = scm.getScmBlockManager()
        .getDeletedBlockLog();
    Preconditions.checkArgument(
        deletedBlockLog instanceof DeletedBlockLogImplV2);
    ((DeletedBlockLogImplV2) deletedBlockLog).onBecomeLeader();

    scm.getScmDecommissionManager().onBecomeLeader();
  }

  @Override
  public long takeSnapshot() throws IOException {
    TermIndex lastTermIndex = getLastAppliedTermIndex();
    long lastAppliedIndex = lastTermIndex.getIndex();

    if (!isInitialized) {
      return lastAppliedIndex;
    }

    long startTime = Time.monotonicNow();

    TransactionInfo latestTrxInfo = transactionBuffer.getLatestTrxInfo();
    TransactionInfo lastAppliedTrxInfo =
        TransactionInfo.fromTermIndex(lastTermIndex);

    if (latestTrxInfo.compareTo(lastAppliedTrxInfo) < 0) {
      transactionBuffer.updateLatestTrxInfo(lastAppliedTrxInfo);
      transactionBuffer.setLatestSnapshot(lastAppliedTrxInfo.toSnapshotInfo());
    } else {
      lastAppliedIndex = latestTrxInfo.getTransactionIndex();
    }

    transactionBuffer.flush();

    LOG.info("Current Snapshot Index {}, takeSnapshot took {} ms",
        lastAppliedIndex, Time.monotonicNow() - startTime);
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

  @Override
  public void reinitialize() {
    Preconditions.checkNotNull(installingDBCheckpoint);
    DBCheckpoint checkpoint = installingDBCheckpoint;

    // explicitly set installingDBCheckpoint to be null
    installingDBCheckpoint = null;

    TermIndex termIndex = null;
    try {
      termIndex =
          scm.getScmHAManager().installCheckpoint(checkpoint);
    } catch (Exception e) {
      LOG.error("Failed to reinitialize SCMStateMachine.");
      return;
    }

    // re-initialize the DBTransactionBuffer and update the lastAppliedIndex.
    try {
      transactionBuffer.init();
      this.setLastAppliedTermIndex(termIndex);
    } catch (IOException ioe) {
      LOG.error("Failed to unpause ", ioe);
    }

    getLifeCycle().transition(LifeCycle.State.STARTING);
    getLifeCycle().transition(LifeCycle.State.RUNNING);
  }

  @Override
  public void close() throws IOException {
    if (!isInitialized) {
      return;
    }
    super.close();
    transactionBuffer.close();
    HadoopExecutors.
        shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  public void setInstallingDBCheckpoint(DBCheckpoint checkpoint) {
    Preconditions.checkArgument(installingDBCheckpoint == null);
    installingDBCheckpoint = checkpoint;
  }
}

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

package org.apache.hadoop.hdds.scm.ha;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
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

  private DBCheckpoint installingDBCheckpoint = null;
  private List<ManagedSecretKey> installingSecretKeys = null;

  private AtomicLong currentLeaderTerm = new AtomicLong(-1L);
  private AtomicBoolean refreshedAfterLeaderReady = new AtomicBoolean();

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
    this.installSnapshotExecutor = HadoopExecutors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat(scm.threadNamePrefix() + "SCMInstallSnapshot-%d")
            .build()
    );
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
      LOG.info("{}: initialize {}", server.getId(), id);
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

      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: applyTransaction {}", getId(), TermIndex.valueOf(trx.getLogEntry()));
      }
      try {
        applyTransactionFuture.complete(process(request));
      } catch (SCMException ex) {
        // For SCM exceptions while applying a transaction, if the error
        // code indicate a FATAL issue, let it crash SCM.
        if (ex.getResult() == ResultCodes.INTERNAL_ERROR
            || ex.getResult() == ResultCodes.IO_EXCEPTION) {
          throw ex;
        }
        // Otherwise, it's considered as a logical rejection and is returned to
        // Ratis client, leaving SCM intact.
        applyTransactionFuture.completeExceptionally(ex);
      }

      // After previous term transactions are applied, still in safe mode,
      // perform refreshAndValidate to update the safemode rule state.
      if (scm.isInSafeMode() && refreshedAfterLeaderReady.get()) {
        scm.getScmSafeModeManager().refreshAndValidate();
      }
      final TermIndex appliedTermIndex = TermIndex.valueOf(trx.getLogEntry());
      transactionBuffer.updateLatestTrxInfo(TransactionInfo.valueOf(appliedTermIndex));
      updateLastAppliedTermIndex(appliedTermIndex);
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
  public void notifyLogFailed(Throwable ex,
      RaftProtos.LogEntryProto failedEntry) {
    LOG.error("SCM statemachine appendLog failed, entry: {}", failedEntry);
    ExitUtils.terminate(1, ex.getMessage(), ex, StateMachine.LOG);
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
        scm.getSCMHANodeDetails().getPeerNodeDetails().stream()
            .filter(p -> p.getRatisHostPortStr().equals(leaderAddress))
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

          List<ManagedSecretKey> secretKeys;
          try {
            secretKeys =
                scm.getScmHAManager().getSecretKeysFromLeader(leaderNodeId);
            LOG.info("Got secret keys from leaders {}", secretKeys);
          } catch (IOException ex) {
            LOG.error("Failed to get secret keys from SCM leader {}",
                leaderNodeId, ex);
            return null;
          }

          TermIndex termIndex =
              scm.getScmHAManager().verifyCheckpointFromLeader(
                  leaderNodeId, checkpoint);

          if (termIndex != null) {
            setInstallingSnapshotData(checkpoint, secretKeys);
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

    currentLeaderTerm.set(scm.getScmHAManager().getRatisServer().getDivision()
        .getInfo().getCurrentTerm());

    if (!groupMemberId.getPeerId().equals(newLeaderId)) {
      LOG.info("leader changed, yet current SCM is still follower.");
      return;
    }

    LOG.info("current SCM becomes leader of term {}.", currentLeaderTerm);

    scm.getScmContext().updateLeaderAndTerm(true,
        currentLeaderTerm.get());
    scm.getSequenceIdGen().invalidateBatch();

    try {
      transactionBuffer.flush();
    } catch (Exception ex) {
      ExitUtils.terminate(1, "Failed to flush transactionBuffer", ex, StateMachine.LOG);
    }

    DeletedBlockLog deletedBlockLog = scm.getScmBlockManager()
        .getDeletedBlockLog();
    Preconditions.checkArgument(
        deletedBlockLog instanceof DeletedBlockLogImpl);
    ((DeletedBlockLogImpl) deletedBlockLog).onBecomeLeader();
    scm.getScmDecommissionManager().onBecomeLeader();

    scm.scmHAMetricsUpdate(newLeaderId.toString());
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
    final TransactionInfo lastAppliedTrxInfo = TransactionInfo.valueOf(lastTermIndex);

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

    // We need to call updateLastApplied here because now in ratis when a
    // node becomes leader, it is checking stateMachineIndex >=
    // placeHolderIndex (when a node becomes leader, it writes a conf entry
    // with some information like its peers and termIndex). So, calling
    // updateLastApplied updates lastAppliedTermIndex.
    updateLastAppliedTermIndex(term, index);

    // Skip below part if state machine is not initialized.

    if (!isInitialized) {
      return;
    }

    if (transactionBuffer != null) {
      transactionBuffer.updateLatestTrxInfo(TransactionInfo.valueOf(term, index));
    }

    if (currentLeaderTerm.get() == term) {
      // Means all transactions before this term have been applied.
      // This means after a restart, all pending transactions have been applied.
      // Perform
      // 1. Refresh Safemode rules state.
      // 2. Start DN Rpc server.
      if (!refreshedAfterLeaderReady.get()) {
        refreshedAfterLeaderReady.set(true);
        scm.getScmSafeModeManager().refreshAndValidate();
      }
      currentLeaderTerm.set(-1L);
    }
  }

  public boolean isRefreshedAfterLeaderReady() {
    return refreshedAfterLeaderReady.get();
  }

  @Override
  public void notifyLeaderReady() {
    if (!isInitialized) {
      return;
    }
    // On leader SCM once after it is ready, notify SCM services and also set
    // leader ready  in SCMContext.
    scm.getScmContext().setLeaderReady();
    scm.getSCMServiceManager().notifyStatusChanged();
    scm.getFinalizationManager().onLeaderReady();
  }

  @Override
  public void notifyConfigurationChanged(long term, long index,
      RaftProtos.RaftConfigurationProto newRaftConfiguration) {
  }

  @Override
  public void pause() {
    final LifeCycle lc = getLifeCycle();
    LOG.info("{}: Try to pause from current LifeCycle state {}", getId(), lc);
    if (lc.getCurrentState() != LifeCycle.State.NEW) {
      lc.transition(LifeCycle.State.PAUSING);
      lc.transition(LifeCycle.State.PAUSED);
    }
  }

  @Override
  public void reinitialize() throws IOException {
    requireNonNull(installingDBCheckpoint, "installingDBCheckpoint == null");
    DBCheckpoint checkpoint = installingDBCheckpoint;
    List<ManagedSecretKey> secretKeys = installingSecretKeys;

    // explicitly set installingDBCheckpoint to be null
    installingDBCheckpoint = null;
    installingSecretKeys = null;

    TermIndex termIndex = null;
    try {
      termIndex =
          scm.getScmHAManager().installCheckpoint(checkpoint);
    } catch (Exception e) {
      LOG.error("Failed to reinitialize SCMStateMachine.", e);
      throw new IOException(e);
    }

    LOG.info("{}: SCMStateMachine is reinitializing. newTermIndex = {}", getId(), termIndex);

    // re-initialize the DBTransactionBuffer and update the lastAppliedIndex.
    try {
      transactionBuffer.init();
      this.setLastAppliedTermIndex(termIndex);
    } catch (IOException ioe) {
      LOG.error("Failed to unpause ", ioe);
    }

    if (secretKeys != null) {
      requireNonNull(scm.getSecretKeyManager()).reinitialize(secretKeys);
    }

    getLifeCycle().transition(LifeCycle.State.STARTING);
    getLifeCycle().transition(LifeCycle.State.RUNNING);
  }

  @Override
  public void close() throws IOException {
    if (!isInitialized) {
      return;
    }
    //if ratis server is stopped , it indicates this `close` originates
    // from `scm.stop()`, otherwise, it indicates this `close` originates
    // from ratis.
    if (scm.getScmHAManager().getRatisServer().isStopped()) {
      super.close();
      transactionBuffer.close();
      HadoopExecutors.
          shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
    } else if (!scm.isStopped()) {
      scm.shutDown("scm statemachine is closed by ratis, terminate SCM");
    }
  }

  @VisibleForTesting
  public void setInstallingSnapshotData(DBCheckpoint checkpoint,
      List<ManagedSecretKey> secretKeys) {
    Preconditions.checkArgument(installingDBCheckpoint == null);
    installingDBCheckpoint = checkpoint;
    installingSecretKeys = secretKeys;
  }
}

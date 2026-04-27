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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.METADATA_ERROR;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.protocolPB.RequestHandler;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The StateMachine for group of buckets . It is
 * responsible for applying Ratis committed bucket write transactions to
 * {@link OzoneManager}.
 */
public class BucketStateMachine extends BaseStateMachine {

  private final OzoneManager ozoneManager;

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private volatile OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;

  private final String threadNamePrefix;

  private final ExecutorService executorService;

  private final RaftGroupId currentRaftGroupId;

  private RequestHandler handler;

  private final AtomicInteger statePausedCount = new AtomicInteger(0);

  private final ExecutorService installSnapshotExecutor;

  private final long crossGroupSyncTimeoutMs;

  private ConcurrentMap<Long, Long> applyTransactionMap =
      new ConcurrentSkipListMap<>();

  private ConcurrentMap<Long, Long> ratisTransactionMap =
      new ConcurrentSkipListMap<>();

  public static final Logger LOG =
          LoggerFactory.getLogger(BucketStateMachine.class);

  public static final Logger LOG_MULTI_RAFT = LoggerFactory.getLogger("multiraft");

  public BucketStateMachine(RaftGroupId raftGroupId, OzoneManager om) throws IOException {
    this.ozoneManager = om;
    this.ozoneManagerDoubleBuffer =  buildDoubleBufferForRatis();
    this.threadNamePrefix = om.getThreadNamePrefix() + "-" + raftGroupId;
    this.currentRaftGroupId = raftGroupId;

    loadSnapshotInfoFromDB();
    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadNamePrefix +
            "OMBucketStateMachineApplyTransactionThread - %d").build();
    this.executorService = HadoopExecutors.newSingleThreadExecutor(build);
    this.handler = new OzoneManagerRequestHandler(ozoneManager);
    ThreadFactory installSnapshotThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(threadNamePrefix + "-OmBucketInstallSnapshotThread").build();
    this.installSnapshotExecutor =
        HadoopExecutors.newSingleThreadExecutor(installSnapshotThreadFactory);
    this.crossGroupSyncTimeoutMs = om.getConfiguration().getLong(
        OMConfigKeys.OZONE_OM_MULTI_RAFT_CROSS_GROUP_SYNC_TIMEOUT,
        OMConfigKeys.OZONE_OM_MULTI_RAFT_CROSS_GROUP_SYNC_TIMEOUT_DEFAULT);
  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {

    String leaderNodeId = RaftPeerId.valueOf(roleInfoProto.getFollowerInfo()
        .getLeaderInfo().getId().getId()).toString();
    LOG.info("Received install snapshot notification from OM leader: {} with " +
        "term index: {}", leaderNodeId, firstTermIndexInLog);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return ozoneManager.installSnapshotFromLeader(currentRaftGroupId, leaderNodeId);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        installSnapshotExecutor);
  }

  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(raftServer, raftGroupId, raftStorage);
      storage.init(raftStorage);
      LOG.info("{}: initialize {} with {}", getId(), raftGroupId, getLastAppliedTermIndex());
    });
  }

  /**
   * Serve a linearizable read on this state machine. Ratis runs ReadIndex
   * before invoking this method, guaranteeing the local state machine has
   * applied everything committed before the read started — exactly the
   * read-after-write barrier needed for multi-raft bucket reads.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    try {
      OzoneManagerProtocolProtos.OMRequest omRequest = OMRatisHelper.convertByteStringToOMRequest(request.getContent());
      OzoneManagerProtocolProtos.OMResponse response = handler.handleReadRequest(omRequest);
      return CompletableFuture.completedFuture(OMRatisHelper.convertResponseToMessage(response));
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

/*
 * Apply a committed log entry to the state machine.
 */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    LOG.trace("Apply transaction {} {}", currentRaftGroupId, trx.getLogEntry().getIndex());
    try {
      // For the Leader, the OMRequest is set in trx in startTransaction.
      // For Followers, the OMRequest hast to be converted from the log entry.
      final Object context = trx.getStateMachineContext();
      final OzoneManagerProtocolProtos.OMRequest request =
          context != null ? (OzoneManagerProtocolProtos.OMRequest) context
              : OMRatisHelper.convertByteStringToOMRequest(
              trx.getStateMachineLogEntry().getLogData());
      long trxLogIndex = trx.getLogEntry().getIndex();
      final TermIndex termIndex = TermIndex.valueOf(trx.getLogEntry());
      // A cluster can contain several Raft groups. These groups process
      // write request for particular buckets
      // There are can be In the current approach we have one single
      // thread executor per Ratis group with single thread.
      // Right now this is being done for correctness, as
      // applyTransaction will be run on multiple OM's we want to execute the
      // transactions in the same order on all OM's, otherwise there is a
      // chance that OM replica's can be out of sync.

      // We have 300 transactions, And for each volume we have transactions
      // of 150. Volume1 transactions 0 - 149 and Volume2 transactions 150 -
      // 299.
      // Example: Executor1 - Volume1 - 100 (current completed transaction)
      // Example: Executor2 - Volume2 - 299 (current completed transaction)

      // Now we have applied transactions of 0 - 100 and 149 - 299. We
      // cannot update lastAppliedIndex to 299. We need to update it to 100,
      // since 101 - 149 are not applied. When OM restarts it will
      // applyTransactions from lastAppliedIndex.
      // We can update the lastAppliedIndex to 100, and update it to 299,
      // only after completing 101 - 149. In initial stage, we are starting
      // with single global executor. Will revisit this when needed.

      // Add the term index and transaction log index to applyTransaction map.
      // This map will be used to update lastAppliedIndex.

      CompletableFuture<Message> ratisFuture =
          new CompletableFuture<>();
      applyTransactionMap.put(trxLogIndex, trx.getLogEntry().getTerm());

      //if there are too many pending requests, wait for doubleBuffer flushing
      ozoneManagerDoubleBuffer.acquireUnFlushedTransactions(1);

      CompletableFuture<OzoneManagerProtocolProtos.OMResponse> future = CompletableFuture.supplyAsync(
          () -> runCommand(request, termIndex), executorService);
      future.thenApply(omResponse -> {
        if (!omResponse.getSuccess()) {
          // When INTERNAL_ERROR or METADATA_ERROR it is considered as
          // critical error and terminate the OM. Considering INTERNAL_ERROR
          // also for now because INTERNAL_ERROR is thrown for any error
          // which is not type OMException.

          // Not done future with completeExceptionally because if we do
          // that OM will still continue applying transaction until next
          // snapshot. So in OM case if a transaction failed with un
          // recoverable error and if we wait till snapshot to terminate
          // OM, then if some client requested the read transaction of the
          // failed request, there is a chance we shall give wrong result.
          // So, to avoid these kind of issue, we should terminate OM here.
          if (omResponse.getStatus() == INTERNAL_ERROR) {
            terminate(omResponse, OMException.ResultCodes.INTERNAL_ERROR);
            terminate(omResponse, OMException.ResultCodes.INTERNAL_ERROR);
          } else if (omResponse.getStatus() == METADATA_ERROR) {
            terminate(omResponse, OMException.ResultCodes.METADATA_ERROR);
          }
        }

        // For successful response and for all other errors which are not
        // critical, we can complete future normally.
        ratisFuture.complete(OMRatisHelper.convertResponseToMessage(
            omResponse));
        return ratisFuture;
      });
      return ratisFuture;
    } catch (Exception e) {
      return completeExceptionally(e);
    } finally {
      ozoneManager.getOmRaftGroupManager().incrRaftGroupUsageCounter(currentRaftGroupId);
    }
  }

  /**
   * Submits write request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   */
  private OzoneManagerProtocolProtos.OMResponse runCommand(
      OzoneManagerProtocolProtos.OMRequest request,
      TermIndex termIndex
  ) {
    try {
      waitForMainStateMachineCatchUp();
      ExecutionContext context = ExecutionContext.of(termIndex.getIndex(), termIndex);
      final OMClientResponse omClientResponse = handler.handleWriteRequest(
          request, context, getGroupId(), ozoneManagerDoubleBuffer);
      OMLockDetails omLockDetails = omClientResponse.getOmLockDetails();
      OzoneManagerProtocolProtos.OMResponse omResponse = omClientResponse.getOMResponse();
      if (omLockDetails != null) {
        return omResponse.toBuilder()
            .setOmLockDetails(omLockDetails.toProtobufBuilder()).build();
      } else {
        return omResponse;
      }
    } catch (IOException e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      return createErrorResponse(request, e, termIndex);
    } catch (Throwable e) {
      // For any Runtime exceptions, terminate OM.
      String errorMessage = "Request " + request + " failed with exception";
      ExitUtils.terminate(1, errorMessage, e, LOG);
    }
    return null;
  }

  /**
   * Ensures the main OM state machine has applied all its committed entries
   * before the bucket raft group processes a request. This prevents
   * cross-raft-group consistency issues where metadata created through the
   * main raft group (e.g. buckets, volumes) is not yet visible on this OM
   * because the follower hasn't applied the entry yet.
   *
   * <p>Compares {@code lastApplyTransactionIndex} (the highest log index
   * received by the main SM's {@code applyTransaction} — set immediately
   * when Ratis delivers the entry) with {@code lastAppliedTermIndex}
   * (updated only after the double buffer flushes the entry to RocksDB).
   * A gap means there are entries queued in the main SM's executor that
   * haven't been written to the shared metadata store yet.
   */
  private void waitForMainStateMachineCatchUp() throws IOException {
    OzoneManagerRatisServer ratisServer = ozoneManager.getOmRatisServer();
    if (ratisServer == null) {
      return;
    }
    OzoneManagerStateMachine mainSM = ratisServer.getOmStateMachine();
    long received = mainSM.getLastApplyTransactionIndex();
    long applied = mainSM.getLastAppliedTermIndex().getIndex();
    if (applied >= received) {
      return;
    }

    LOG.debug("Bucket raft group {} waiting for main SM to catch up:"
        + " applied={}, received={}", currentRaftGroupId, applied, received);
    long deadline = Time.monotonicNow() + crossGroupSyncTimeoutMs;
    while (applied < received) {
      if (Time.monotonicNow() >= deadline) {
        LOG.warn("Timed out ({}ms) waiting for main OM state machine"
                + " to catch up. applied={}, received={}, raftGroup={}",
            crossGroupSyncTimeoutMs, applied, received, currentRaftGroupId);
        return;
      }
      try {
        mainSM.awaitDoubleBufferFlush();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            "Interrupted waiting for main OM state machine catch-up", e);
      }
      applied = mainSM.getLastAppliedTermIndex().getIndex();
      received = mainSM.getLastApplyTransactionIndex();
    }
  }

  private OzoneManagerProtocolProtos.OMResponse createErrorResponse(
      OzoneManagerProtocolProtos.OMRequest omRequest, IOException exception, TermIndex termIndex) {
    OzoneManagerProtocolProtos.OMResponse.Builder omResponseBuilder = OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponseBuilder.setMessage(exception.getMessage());
    }
    OzoneManagerProtocolProtos.OMResponse omResponse = omResponseBuilder.build();
    OMClientResponse omClientResponse = new DummyOMClientResponse(omResponse);
    ozoneManagerDoubleBuffer.add(omClientResponse, termIndex);
    return omResponse;
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  private static void terminate(OzoneManagerProtocolProtos.OMResponse omResponse, OMException.ResultCodes resultCode) {
    OMException exception = new OMException(omResponse.getMessage(),
        resultCode);
    String errorMessage = "OM Ratis Server has received unrecoverable " +
        "error, to avoid further DB corruption, terminating OM. Error " +
        "Response received is:" + omResponse;
    ExitUtils.terminate(1, errorMessage, exception, LOG);
  }

  public OzoneManagerDoubleBuffer buildDoubleBufferForRatis() {
    final int maxUnFlushedTransactionCount = ozoneManager.getConfiguration()
        .getInt(OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT,
            OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT_DEFAULT);
    return OzoneManagerDoubleBuffer.newBuilder()
        .setOmMetadataManager(ozoneManager.getMetadataManager())
        .setUpdateLastAppliedIndex(this::updateLastAppliedTermIndex)
        .setMaxUnFlushedTransactionCount(maxUnFlushedTransactionCount)
        .setThreadPrefix(threadNamePrefix)
        .setS3SecretManager(ozoneManager.getS3SecretManager())
        .setThreadPrefix(threadNamePrefix)
        .enableTracing(TracingUtil.isTracingEnabled(ozoneManager.getConfiguration()))
        .build()
        .start();
  }

  /**
   * Update lastAppliedIndex term and it's corresponding term in the
   * stateMachine.
   * @param flushedEpochs
   */
  public void updateLastAppliedIndex(List<Long> flushedEpochs) {
    Preconditions.checkArgument(!flushedEpochs.isEmpty());
    computeAndUpdateLastAppliedIndex(
        flushedEpochs.get(flushedEpochs.size() - 1), -1L, flushedEpochs, true);
  }

  /**
   * Update State machine lastAppliedTermIndex.
   * @param lastFlushedIndex
   * @param currentTerm
   * @param flushedEpochs - list of ratis transactions flushed to DB. If it
   * is just one index and term, this can be set to null.
   * @param checkMap - if true check applyTransactionMap, ratisTransaction
   * Map and update lastAppliedTermIndex accordingly, else check
   * lastAppliedTermIndex and update it.
   */
  private synchronized void computeAndUpdateLastAppliedIndex(
      long lastFlushedIndex, long currentTerm, List<Long> flushedEpochs,
      boolean checkMap) {
    if (checkMap) {
      List<Long> flushedTrans = new ArrayList<>(flushedEpochs);
      Long appliedTerm = null;
      long appliedIndex = -1;
      for (long i = getLastAppliedTermIndex().getIndex() + 1; ; i++) {
        if (flushedTrans.contains(i)) {
          appliedIndex = i;
          final Long removed = applyTransactionMap.remove(i);
          appliedTerm = removed;
          flushedTrans.remove(i);
        } else if (ratisTransactionMap.containsKey(i)) {
          final Long removed = ratisTransactionMap.remove(i);
          appliedTerm = removed;
          appliedIndex = i;
        } else {
          // Add remaining which are left in flushedEpochs to
          // ratisTransactionMap to be considered further.
          for (long epoch : flushedTrans) {
            ratisTransactionMap.put(epoch, applyTransactionMap.remove(epoch));
          }
          if (LOG.isDebugEnabled()) {
            if (!flushedTrans.isEmpty()) {
              LOG.debug("ComputeAndUpdateLastAppliedIndex due to SM added " +
                        "to map remaining {}", flushedTrans);
            }
          }
          break;
        }
      }
      if (appliedTerm != null) {
        updateLastAppliedTermIndex(appliedTerm, appliedIndex);
        if (LOG.isDebugEnabled()) {
          LOG.debug("ComputeAndUpdateLastAppliedIndex due to SM is {}",
              getLastAppliedTermIndex());
        }
      }
    } else {
      if (getLastAppliedTermIndex().getIndex() + 1 == lastFlushedIndex) {
        updateLastAppliedTermIndex(currentTerm, lastFlushedIndex);
        if (LOG.isDebugEnabled()) {
          LOG.debug("ComputeAndUpdateLastAppliedIndex due to notifyIndex {}",
              getLastAppliedTermIndex());
        }
      } else {
        ratisTransactionMap.put(lastFlushedIndex, currentTerm);
        if (LOG.isDebugEnabled()) {
          LOG.debug("ComputeAndUpdateLastAppliedIndex due to notifyIndex " +
                    "added to map. Passed Term {} index {}, where as lastApplied " +
                    "Index {}", currentTerm, lastFlushedIndex,
              getLastAppliedTermIndex());
        }
      }
    }
  }

  @Override
  public long takeSnapshot() throws IOException {
    LOG.info("Current Snapshot Index {}", getLastAppliedTermIndex());
    TermIndex lastTermIndex = getLastAppliedTermIndex();
    long lastAppliedIndex = lastTermIndex.getIndex();
    TransactionInfo transactionInfo = TransactionInfo.valueOf(lastTermIndex.getIndex(), lastAppliedIndex);
    ozoneManager.setTransactionInfo(currentRaftGroupId, transactionInfo);
    Table<String, TransactionInfo> txnInfoTable =
            ozoneManager.getMetadataManager().getTransactionInfoTable();
    txnInfoTable.put(TRANSACTION_INFO_KEY + currentRaftGroupId.toString(), transactionInfo);
    ozoneManager.getMetadataManager().getStore().flushDB();
    return lastAppliedIndex;
  }

  @Override
  public synchronized void reinitialize() throws IOException {
    loadSnapshotInfoFromDB();
    if (getLifeCycleState() == LifeCycle.State.PAUSED) {
      final TermIndex lastApplied = getLastAppliedTermIndex();
      unpause(lastApplied.getIndex(), lastApplied.getTerm());
    }
  }

  @Override
  public synchronized void pause() {
    LOG.info("BucketStateMachine is pausing");
    statePausedCount.incrementAndGet();
    final LifeCycle.State state = getLifeCycleState();
    if (state == LifeCycle.State.PAUSED) {
      return;
    }
    if (state != LifeCycle.State.NEW) {
      getLifeCycle().transition(LifeCycle.State.PAUSING);
      getLifeCycle().transition(LifeCycle.State.PAUSED);
    }

    ozoneManagerDoubleBuffer.stop();
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    final SnapshotInfo snapshotInfo = ozoneManager.getTransactionInfo(currentRaftGroupId).toSnapshotInfo();
    LOG.debug("Latest Snapshot Info {} - {}", currentRaftGroupId, snapshotInfo);
    return snapshotInfo;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId,
                                  RaftPeerId newLeaderId) {
    LOG_MULTI_RAFT.info("Change leader in group {}. New leader {}", groupMemberId.getGroupId(), newLeaderId);
    // Initialize OMHAMetrics
    if (ozoneManager.getOmhaMetrics() == null) {
      LOG_MULTI_RAFT.info("OM ha metrics are not ready, put tmp leader {} {}", groupMemberId.getGroupId(),
          newLeaderId.toString());
      ozoneManager.getTmpLeadersMap().put(groupMemberId.getGroupId(), newLeaderId.toString());
    } else {
      LOG_MULTI_RAFT.info("OM ha metrics are ready, put leader to metrics {} {}", groupMemberId.getGroupId(),
          newLeaderId.toString());
      ozoneManager.getOmhaMetrics().defineRaftGroupLeader(groupMemberId.getGroupId(), newLeaderId.toString(), false);
    }
  }

  /**
   * Called to notify state machine about indexes which are processed
   * internally by Raft Server, this currently happens when conf entries are
   * processed in raft Server. This keep state machine to keep a track of index
   * updates.
   * @param currentTerm term of the current log entry
   * @param index index which is being updated
   */
  @Override
  public void notifyTermIndexUpdated(long currentTerm, long index) {
    LOG.trace("Notify term index updated {} {} - {}", currentRaftGroupId, index, currentTerm);
    // SnapshotInfo should be updated when the term changes.
    // The index here refers to the log entry index and the index in
    // SnapshotInfo represents the snapshotIndex i.e. the index of the last
    // transaction included in the snapshot. Hence, snaphsotInfo#index is not
    // updated here.

    // We need to call updateLastApplied here because now in ratis when a
    // node becomes leader, it is checking stateMachineIndex >=
    // placeHolderIndex (when a node becomes leader, it writes a conf entry
    // with some information like its peers and termIndex). So, calling
    // updateLastApplied updates lastAppliedTermIndex.
    computeAndUpdateLastAppliedIndex(index, currentTerm, null, false);
  }

  @Override
  public void notifySnapshotInstalled(RaftProtos.InstallSnapshotResult result,
                                      long snapshotIndex, RaftPeer peer) {
    LOG.trace("Receive notifySnapshotInstalled event {} for the peer: {}" +
        " snapshotIndex: {}.", result, peer.getId(), snapshotIndex);
    switch (result) {
    case SUCCESS:
    case SNAPSHOT_UNAVAILABLE:
      // Currently, only trigger for the one who installed snapshot
      if (ozoneManager.getOmRatisServer().getServerDivision(currentRaftGroupId).getPeer().equals(peer)) {
        ozoneManager.getOmSnapshotProvider().init();
      }
      break;
    default:
      break;
    }
  }

  /** Assert if the given {@link TermIndex} is updated increasingly. */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private TermIndex assertUpdateIncreasingly(String name, TermIndex oldTermIndex, TermIndex newTermIndex) {
    Preconditions.checkArgument(newTermIndex.compareTo(oldTermIndex) >= 0,
        "%s: newTermIndex = %s < oldTermIndex = %s", name, newTermIndex, oldTermIndex);
    return newTermIndex;
  }

  public void loadSnapshotInfoFromDB() throws IOException {
    // This is done, as we have a check in Ratis for not throwing
    // LeaderNotReadyException, it checks stateMachineIndex >= raftLog
    // nextIndex (placeHolderIndex).
    LOG.trace("Load snapshot info from db");
    TransactionInfo transactionInfo = TransactionInfo.readTransactionInfo(
            ozoneManager.getMetadataManager(), currentRaftGroupId.toString()
    );
    LOG.trace("Found transaction info {} for group {}", transactionInfo, currentRaftGroupId);
    if (transactionInfo != null) {
      final TermIndex ti = transactionInfo.getTermIndex();
      setLastAppliedTermIndex(ti);
      ozoneManager.setTransactionInfo(currentRaftGroupId, transactionInfo);
    }
    LOG.trace("LastAppliedIndex is set from TransactionInfo from OM DB as {}",
            getLastAppliedTermIndex());
  }

  public synchronized void unpause(long newLastAppliedSnaphsotIndex,
                                   long newLastAppliedSnapShotTermIndex) {
    LOG.info("BucketStateMachine is un-pausing");
    if (statePausedCount.decrementAndGet() == 0) {
      getLifeCycle().startAndTransition(() -> {
        this.ozoneManagerDoubleBuffer = buildDoubleBufferForRatis();
        this.setLastAppliedTermIndex(TermIndex.valueOf(
            newLastAppliedSnapShotTermIndex, newLastAppliedSnaphsotIndex));
        LOG.info("{}: OzoneManagerStateMachine un-pause completed. " +
                "newLastAppliedSnaphsotIndex: {}, newLastAppliedSnapShotTermIndex: {}",
            getId(), newLastAppliedSnaphsotIndex, newLastAppliedSnapShotTermIndex);
      });
    }
  }

  public long getTermForIndex(long transactionIndex) {
    return applyTransactionMap.get(transactionIndex);
  }

  /**
   * Wait until both buffers are flushed.  This is used in cases like
   * "follower bootstrap tarball creation" where the rocksDb for the active
   * fs needs to synchronized with the rocksdb's for the snapshots.
   */
  public void awaitDoubleBufferFlush() throws InterruptedException {
    ozoneManagerDoubleBuffer.awaitFlush();
  }

  public OzoneManagerDoubleBuffer getOzoneManagerDoubleBuffer() {
    return ozoneManagerDoubleBuffer;
  }

  @Override
  public void notifyGroupRemove() {
    LOG.trace("Start removing group {}", currentRaftGroupId);
    ozoneManager.getStateMachines().remove(currentRaftGroupId);
    ozoneManager.getOmRaftGroups().remove(currentRaftGroupId);
    ozoneManager.getOmhaMetrics().deleteRaftGroup(currentRaftGroupId);
    ozoneManager.getOmRaftGroupManager().removeGroup(currentRaftGroupId);
    try {
      LOG.trace("Deleting transaction for group {}", currentRaftGroupId);
      TransactionInfo.deleteTransactionInfo(
              ozoneManager.getMetadataManager(), currentRaftGroupId.toString()
      );
    } catch (IOException e) {
      LOG.error("Error deleting transaction for group {}", currentRaftGroupId);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("BucketStateMachine has shutdown.");
    stop();
  }

  public void stop() {
    ozoneManagerDoubleBuffer.stop();
    HadoopExecutors.shutdown(executorService, LOG, 5, TimeUnit.SECONDS);
    HadoopExecutors.shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
    LOG.info("applyTransactionMap size {} ", applyTransactionMap.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug("applyTransactionMap {}",
              applyTransactionMap.keySet().stream().map(Object::toString)
                      .collect(Collectors.joining(",")));
    }
    LOG.info("ratisTransactionMap size {}", ratisTransactionMap.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug("ratisTransactionMap {}",
              ratisTransactionMap.keySet().stream().map(Object::toString)
                      .collect(Collectors.joining(",")));
    }
  }

  /**
   * Notifies the state machine that the raft peer is no longer leader.
   */
  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries)
          throws IOException {
  }

  @Override
  public String toStateMachineLogEntryString(RaftProtos.StateMachineLogEntryProto proto) {
    return OMRatisHelper.smProtoToString(proto);
  }

}

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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.NettyMetrics;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.protocolPB.RequestHandler;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.METADATA_ERROR;

/**
 * The OM StateMachine is the state machine for OM Ratis server. It is
 * responsible for applying ratis committed transactions to
 * {@link OzoneManager}.
 */
public class OMBasicStateMachine extends BaseStateMachine {

  public static final Logger LOG = LoggerFactory.getLogger(OMBasicStateMachine.class);
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final OzoneManager ozoneManager;
  private final boolean isTracingEnabled;
  private RequestHandler handler;
  private RaftGroupId raftGroupId;
  private final ExecutorService installSnapshotExecutor;
  private final AtomicInteger statePausedCount = new AtomicInteger(0);
  private final String threadPrefix;
  private final NettyMetrics nettyMetrics;
  private final List<Consumer<String>> notifiers = new CopyOnWriteArrayList<>();

  public OMBasicStateMachine(OzoneManagerRatisServer ratisServer, boolean isTracingEnabled) throws IOException {
    this.isTracingEnabled = isTracingEnabled;
    this.ozoneManager = ratisServer.getOzoneManager();

    loadSnapshotInfoFromDB();
    this.threadPrefix = ozoneManager.getThreadNamePrefix();

    this.handler = new OzoneManagerRequestHandler(ozoneManager);

    ThreadFactory installSnapshotThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(threadPrefix + "InstallSnapshotThread").build();
    this.installSnapshotExecutor = HadoopExecutors.newSingleThreadExecutor(installSnapshotThreadFactory);
    this.nettyMetrics = NettyMetrics.create();
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

  @Override
  public synchronized void reinitialize() throws IOException {
    loadSnapshotInfoFromDB();
    if (getLifeCycleState() == LifeCycle.State.PAUSED) {
      unpause(getLastAppliedTermIndex().getIndex(),
          getLastAppliedTermIndex().getTerm());
    }
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    final SnapshotInfo snapshotInfo = ozoneManager.getTransactionInfo().toSnapshotInfo();
    LOG.debug("Latest Snapshot Info {}", snapshotInfo);
    return snapshotInfo;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
    // Initialize OMHAMetrics
    ozoneManager.omHAMetricsInit(newLeaderId.toString());
    for (Consumer<String> notifier : notifiers) {
      notifier.accept(newLeaderId.toString());
    }
  }

  public void registerLeaderNotifier(Consumer<String> notifier) {
    notifiers.add(notifier);
  }
  /** Notified by Ratis for non-StateMachine term-index update. */
  @Override
  public synchronized void notifyTermIndexUpdated(long currentTerm, long newIndex) {
    updateLastAppliedTermIndex(TermIndex.valueOf(currentTerm, newIndex));
  }

  @Override
  protected synchronized boolean updateLastAppliedTermIndex(TermIndex newTermIndex) {
    if (getLastAppliedTermIndex().getIndex() < newTermIndex.getIndex()) {
      return super.updateLastAppliedTermIndex(newTermIndex);
    }
    return true;
  }

  /**
   * Called to notify state machine about configuration changes.
   * Configurations changes include addition of newly bootstrapped OM.
   */
  @Override
  public void notifyConfigurationChanged(long term, long index,
      RaftProtos.RaftConfigurationProto newRaftConfiguration) {
    List<RaftProtos.RaftPeerProto> newPeers = newRaftConfiguration.getPeersList();
    LOG.info("Received Configuration change notification from Ratis. New Peer" +
        " list:\n{}", newPeers);

    List<String> newPeerIds = new ArrayList<>();
    for (RaftProtos.RaftPeerProto raftPeerProto : newPeers) {
      newPeerIds.add(RaftPeerId.valueOf(raftPeerProto.getId()).toString());
    }
    // Check and update the peer list in OzoneManager
    ozoneManager.updatePeerList(newPeerIds);
  }

  /**
   * Called to notify state machine about the snapshot install result.
   * Trigger the cleanup of candidate DB dir.
   * @param result InstallSnapshotResult
   * @param snapshotIndex the index of installed snapshot
   * @param peer the peer which fini
   */
  @Override
  public void notifySnapshotInstalled(RaftProtos.InstallSnapshotResult result,
                                      long snapshotIndex, RaftPeer peer) {
    LOG.info("Receive notifySnapshotInstalled event {} for the peer: {}" +
        " snapshotIndex: {}.", result, peer.getId(), snapshotIndex);
    switch (result) {
    case SUCCESS:
    case SNAPSHOT_UNAVAILABLE:
      // Currently, only trigger for the one who installed snapshot
      if (ozoneManager.getOmRatisServer().getServerDivision().getPeer().equals(peer)) {
        ozoneManager.getOmSnapshotProvider().init();
      }
      break;
    default:
      break;
    }
  }

  /**
   * Validate/pre-process the incoming update request in the state machine.
   * @return the content to be written to the log entry. Null means the request
   * should be rejected.
   * @throws IOException thrown by the state machine while validating
   */
  @Override
  public TransactionContext startTransaction(
      RaftClientRequest raftClientRequest) throws IOException {
    ByteString messageContent = raftClientRequest.getMessage().getContent();
    OMRequest omRequest = OMRatisHelper.convertByteStringToOMRequest(messageContent);
    return TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(raftClientRequest.getMessage().getContent())
        .setStateMachineContext(omRequest)
        .build();
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
    return trx;
  }

  /*
   * Apply a committed log entry to the state machine.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final Object context = trx.getStateMachineContext();
    final TermIndex termIndex = TermIndex.valueOf(trx.getLogEntry());
    try {
      // For the Leader, the OMRequest is set in trx in startTransaction.
      // For Followers, the OMRequest hast to be converted from the log entry.
      final OMRequest request = context != null ? (OMRequest) context
          : OMRatisHelper.convertByteStringToOMRequest(
          trx.getStateMachineLogEntry().getLogData());
      // Prepare have special handling for execution, as snapshot is taken during this operation
      // and its required that applyTransaction should be finished for this to handle
      // And from leader, its guranteed that no other execution is allowed
      if (request.getCmdType() == OzoneManagerProtocolProtos.Type.Prepare) {
        return CompletableFuture.supplyAsync(() ->
            OMRatisHelper.convertResponseToMessage(runCommand(request, termIndex, handler, ozoneManager)),
            installSnapshotExecutor);
      }
      OMResponse response = runCommand(request, termIndex, handler, ozoneManager);
      CompletableFuture<Message> future = new CompletableFuture<>();
      future.complete(OMRatisHelper.convertResponseToMessage(response));
      return future;
    } catch (Exception e) {
      return completeExceptionally(e);
    } finally {
      updateLastAppliedTermIndex(termIndex);
    }
  }

  private static void terminate(OMResponse omResponse, OMException.ResultCodes resultCode) {
    OMException exception = new OMException(omResponse.getMessage(), resultCode);
    String errorMessage = "OM Ratis Server has received unrecoverable " +
        "error, to avoid further DB corruption, terminating OM. Error " +
        "Response received is:" + omResponse;
    ExitUtils.terminate(1, errorMessage, exception, LOG);
  }

  /**
   * Query the state machine. The request must be read-only.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    try {
      OMRequest omRequest = OMRatisHelper.convertByteStringToOMRequest(
          request.getContent());
      return CompletableFuture.completedFuture(OMRatisHelper.convertResponseToMessage(
          handler.handleReadRequest(omRequest)));
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  @Override
  public synchronized void pause() {
    LOG.info("OzoneManagerStateMachine is pausing");
    statePausedCount.incrementAndGet();
    final LifeCycle.State state = getLifeCycleState();
    if (state == LifeCycle.State.PAUSED) {
      return;
    }
    if (state != LifeCycle.State.NEW) {
      getLifeCycle().transition(LifeCycle.State.PAUSING);
      getLifeCycle().transition(LifeCycle.State.PAUSED);
    }
  }

  /**
   * Unpause the StateMachine, re-initialize the DoubleBuffer and update the
   * lastAppliedIndex. This should be done after uploading new state to the
   * StateMachine.
   */
  public synchronized void unpause(long newLastAppliedSnaphsotIndex,
      long newLastAppliedSnapShotTermIndex) {
    LOG.info("OzoneManagerStateMachine is un-pausing");
    if (statePausedCount.decrementAndGet() == 0) {
      getLifeCycle().startAndTransition(() -> {
        this.setLastAppliedTermIndex(TermIndex.valueOf(
            newLastAppliedSnapShotTermIndex, newLastAppliedSnaphsotIndex));
      });
    }
  }

  /**
   * Take OM Ratis snapshot is a dummy operation as when double buffer
   * flushes the lastAppliedIndex is flushed to DB and that is used as
   * snapshot index.
   *
   * @return the last applied index on the state machine which has been
   * stored in the snapshot file.
   */
  @Override
  public long takeSnapshot() throws IOException {
    // wait until applied == skipped
    if (ozoneManager.isStopped()) {
      throw new IOException("OzoneManager is already stopped: " + ozoneManager.getNodeDetails());
    }
    final TermIndex applied = getLastAppliedTermIndex();
    Long index = TransactionInfo.readTransactionInfo(ozoneManager.getMetadataManager()).getIndex();
    final TransactionInfo transactionInfo = TransactionInfo.valueOf(applied, index);
    ozoneManager.setTransactionInfo(transactionInfo);
    ozoneManager.getMetadataManager().getTransactionInfoTable().put(TRANSACTION_INFO_KEY, transactionInfo);
    ozoneManager.getMetadataManager().getStore().flushDB();
    return applied.getIndex();
  }

  /**
   * Leader OM has purged entries from its log. To catch up, OM must download
   * the latest checkpoint from the leader OM and install it.
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
    LOG.info("Received install snapshot notification from OM leader: {} with " +
            "term index: {}", leaderNodeId, firstTermIndexInLog);
    return CompletableFuture.supplyAsync(
        () -> ozoneManager.installSnapshotFromLeader(leaderNodeId), installSnapshotExecutor);
  }

  @Override
  public String toStateMachineLogEntryString(StateMachineLogEntryProto proto) {
    return OMRatisHelper.smProtoToString(proto);
  }

  @Override
  public void close() {
    // OM should be shutdown as the StateMachine has shutdown.
    if (!ozoneManager.isStopped()) {
      LOG.info("Stopping {}. Shutdown also OzoneManager {}.", this, ozoneManager);
      ozoneManager.shutDown("OM state machine is shutdown by Ratis server");
    } else {
      LOG.info("Stopping {}.", this);
      stop();
    }
  }

  /**
   * Submits write request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   */
  public static OMResponse runCommand(
      OMRequest request, TermIndex termIndex, RequestHandler handler, OzoneManager om) {
    OMClientResponse omClientResponse = null;
    try {
      long index = 0;
      TransactionInfo transactionInfo = TransactionInfo.readTransactionInfo(om.getMetadataManager());
      if (null != transactionInfo && null != transactionInfo.getIndex()) {
        index = transactionInfo.getIndex();
      }
      try {
        // leader index shared to follower for follower execution
        if (request.hasPersistDbRequest() && request.getPersistDbRequest().getIndexCount() > 0) {
          index = Math.max(Collections.max(request.getPersistDbRequest().getIndexList()).longValue(), index);
        }
        // TODO temp way to pass index used for object ID creation for certain follower execution
        TermIndex objectIndex = termIndex;
        if (request.getCmdType() != OzoneManagerProtocolProtos.Type.PersistDb
            && request.getCmdType() != OzoneManagerProtocolProtos.Type.Prepare) {
          objectIndex = TermIndex.valueOf(termIndex.getTerm(), index);
        }
        omClientResponse = handler.handleWriteRequestImpl(request, objectIndex);
        validateResponseError(omClientResponse.getOMResponse());
      } catch (IOException e) {
        LOG.warn("Failed to apply command, Exception occurred ", e);
        omClientResponse = new DummyOMClientResponse(createErrorResponse(request, e, termIndex));
        validateResponseError(omClientResponse.getOMResponse());
        om.getMetadataManager().getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
            TransactionInfo.valueOf(termIndex, index));
      }

      if (!(omClientResponse instanceof DummyOMClientResponse)) {
        // need perform db operation for other request (not for PersistDB request)
        try (BatchOperation batchOperation = om.getMetadataManager().getStore().initBatchOperation()) {
          omClientResponse.checkAndUpdateDB(om.getMetadataManager(), batchOperation);
          om.getMetadataManager().getTransactionInfoTable().putWithBatch(
              batchOperation, TRANSACTION_INFO_KEY, TransactionInfo.valueOf(termIndex, index));
          om.getMetadataManager().getStore().commitBatchOperation(batchOperation);
        }
      }
      return omClientResponse.getOMResponse();
    } catch (Throwable e) {
      // For any further exceptions, terminate OM as db update fails
      String errorMessage = "Request " + request + " failed with exception";
      ExitUtils.terminate(1, errorMessage, e, LOG);
    }
    return null;
  }

  private static void validateResponseError(OMResponse omResponse) {
    if (omResponse.getStatus() == INTERNAL_ERROR) {
      terminate(omResponse, OMException.ResultCodes.INTERNAL_ERROR);
    } else if (omResponse.getStatus() == METADATA_ERROR) {
      terminate(omResponse, OMException.ResultCodes.METADATA_ERROR);
    }
  }

  private static OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception, TermIndex termIndex) {
    OMResponse.Builder omResponseBuilder = OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponseBuilder.setMessage(exception.getMessage());
    }
    OMResponse omResponse = omResponseBuilder.build();
    return omResponse;
  }

  public void loadSnapshotInfoFromDB() throws IOException {
    // This is done, as we have a check in Ratis for not throwing LeaderNotReadyException,
    // it checks stateMachineIndex >= raftLog nextIndex (placeHolderIndex).
    TransactionInfo transactionInfo = TransactionInfo.readTransactionInfo(ozoneManager.getMetadataManager());
    if (transactionInfo != null) {
      final TermIndex ti =  transactionInfo.getTermIndex();
      setLastAppliedTermIndex(ti);
      ozoneManager.setTransactionInfo(transactionInfo);
      LOG.info("LastAppliedIndex is set from TransactionInfo from OM DB as {}", ti);
    } else {
      LOG.info("TransactionInfo not found in OM DB.");
    }
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @VisibleForTesting
  public void setHandler(OzoneManagerRequestHandler handler) {
    this.handler = handler;
  }

  @VisibleForTesting
  public OzoneManagerRequestHandler getHandler() {
    return (OzoneManagerRequestHandler) this.handler;
  }

  public void stop() {
    HadoopExecutors.shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
    if (this.nettyMetrics != null) {
      this.nettyMetrics.unregister();
    }
  }
}

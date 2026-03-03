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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.utils.NettyMetrics;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.protocolPB.RequestHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OM StateMachine is the state machine for OM Ratis server. It is
 * responsible for applying ratis committed transactions to
 * {@link OzoneManager}.
 */
public class OzoneManagerStateMachine extends BaseStateMachine {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerStateMachine.class);
  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_PREVIOUS_LEADER = "previousLeader";
  private static final String AUDIT_PARAM_NEW_LEADER = "newLeader";
  private RaftPeerId previousLeaderId = null;
  private final OzoneManager ozoneManager;
  private RequestHandler handler;
  private volatile OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;
  private final ExecutorService executorService;
  private final ExecutorService installSnapshotExecutor;
  private final boolean isTracingEnabled;
  private final AtomicInteger statePausedCount = new AtomicInteger(0);
  private final String threadPrefix;

  /** The last {@link TermIndex} received from {@link #notifyTermIndexUpdated(long, long)}. */
  private volatile TermIndex lastNotifiedTermIndex = TermIndex.valueOf(0, RaftLog.INVALID_LOG_INDEX);
  /** The last index skipped by {@link #notifyTermIndexUpdated(long, long)}. */
  private volatile long lastSkippedIndex = RaftLog.INVALID_LOG_INDEX;

  private final NettyMetrics nettyMetrics;

  public OzoneManagerStateMachine(OzoneManagerRatisServer ratisServer,
      boolean isTracingEnabled) throws IOException {
    this.isTracingEnabled = isTracingEnabled;
    this.ozoneManager = ratisServer.getOzoneManager();

    loadSnapshotInfoFromDB();
    this.threadPrefix = ozoneManager.getThreadNamePrefix();

    this.ozoneManagerDoubleBuffer = buildDoubleBufferForRatis();
    this.handler = new OzoneManagerRequestHandler(ozoneManager);

    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadPrefix +
            "OMStateMachineApplyTransactionThread - %d").build();
    this.executorService = HadoopExecutors.newSingleThreadExecutor(build);

    ThreadFactory installSnapshotThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(threadPrefix + "InstallSnapshotThread").build();
    this.installSnapshotExecutor =
        HadoopExecutors.newSingleThreadExecutor(installSnapshotThreadFactory);
    this.nettyMetrics = NettyMetrics.create();
  }

  @VisibleForTesting
  OzoneManagerStateMachine(OzoneManager ozoneManager,
      OzoneManagerDoubleBuffer doubleBuffer,
      RequestHandler handler,
      ExecutorService executorService,
      NettyMetrics nettyMetrics) {
    this.isTracingEnabled = false;
    this.ozoneManager = ozoneManager;
    this.threadPrefix = "";
    this.ozoneManagerDoubleBuffer = doubleBuffer;
    this.handler = handler;
    this.executorService = executorService;
    ThreadFactory installSnapshotThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("TestInstallSnapshotThread").build();
    this.installSnapshotExecutor =
        HadoopExecutors.newSingleThreadExecutor(installSnapshotThreadFactory);
    this.nettyMetrics = nettyMetrics;
  }

  /**
   * Initializes the State Machine with the given server, group and storage.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId id, RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(server, id, raftStorage);
      LOG.info("{}: initialize {} with {}", getId(), id, getLastAppliedTermIndex());
    });
  }

  @Override
  public synchronized void reinitialize() throws IOException {
    loadSnapshotInfoFromDB();
    if (getLifeCycleState() == LifeCycle.State.PAUSED) {
      final TermIndex lastApplied = getLastAppliedTermIndex();
      unpause(lastApplied.getIndex(), lastApplied.getTerm());
      LOG.info("{}: reinitialize {} with {}", getId(), getGroupId(), lastApplied);
    }
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    final SnapshotInfo snapshotInfo = ozoneManager.getTransactionInfo().toSnapshotInfo();
    LOG.debug("Latest Snapshot Info {}", snapshotInfo);
    return snapshotInfo;
  }

  @Override
  public void notifyLeaderReady() {
    ozoneManager.getOmSnapshotManager().resetInFlightSnapshotCount();
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId,
                                  RaftPeerId newLeaderId) {
    RaftPeerId currentPeerId = groupMemberId.getPeerId();
    if (newLeaderId.equals(currentPeerId)) {
      // warmup cache
      ozoneManager.initializeEdekCache(ozoneManager.getConfiguration());
    }
    // Store the previous leader before updating
    RaftPeerId actualPreviousLeader = previousLeaderId;

    // Update the previous leader for next time
    previousLeaderId = newLeaderId;
    // Initialize OMHAMetrics
    ozoneManager.omHAMetricsInit(newLeaderId.toString());

    Map<String, String> auditParams = new LinkedHashMap<>();
    auditParams.put(AUDIT_PARAM_PREVIOUS_LEADER,
        actualPreviousLeader != null ? String.valueOf(actualPreviousLeader) : "NONE");
    auditParams.put(AUDIT_PARAM_NEW_LEADER, String.valueOf(newLeaderId));
    AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.LEADER_CHANGE, auditParams));

    LOG.info("{}: leader changed to {}", groupMemberId, newLeaderId);
  }

  /** Notified by Ratis for non-StateMachine term-index update. */
  @Override
  public synchronized void notifyTermIndexUpdated(long currentTerm, long newIndex) {
    // lastSkippedIndex is start of sequence (one less) of continuous notification from ratis
    // if there is any applyTransaction (double buffer index), then this gap is handled during double buffer
    // notification and lastSkippedIndex will be the start of last continuous sequence.
    final long oldIndex = lastNotifiedTermIndex.getIndex();
    if (newIndex - oldIndex > 1) {
      lastSkippedIndex = newIndex - 1;
    }
    final TermIndex newTermIndex = TermIndex.valueOf(currentTerm, newIndex);
    lastNotifiedTermIndex = assertUpdateIncreasingly("lastNotified", lastNotifiedTermIndex, newTermIndex);
    if (lastNotifiedTermIndex.getIndex() - getLastAppliedTermIndex().getIndex() == 1) {
      updateLastAppliedTermIndex(lastNotifiedTermIndex);
    }
  }

  public TermIndex getLastNotifiedTermIndex() {
    return lastNotifiedTermIndex;
  }

  @Override
  protected synchronized boolean updateLastAppliedTermIndex(TermIndex newTermIndex) {
    TermIndex lastApplied = getLastAppliedTermIndex();
    assertUpdateIncreasingly("lastApplied", lastApplied, newTermIndex);
    // if newTermIndex getting updated is within sequence of notifiedTermIndex (i.e. from lastSkippedIndex and
    // notifiedTermIndex), then can update directly to lastNotifiedTermIndex as it ensure previous double buffer's
    // Index is notified or getting notified matching lastSkippedIndex
    if (newTermIndex.getIndex() < getLastNotifiedTermIndex().getIndex()
        && newTermIndex.getIndex() >= lastSkippedIndex) {
      newTermIndex = getLastNotifiedTermIndex();
    }
    return super.updateLastAppliedTermIndex(newTermIndex);
  }

  /** Assert if the given {@link TermIndex} is updated increasingly. */
  private TermIndex assertUpdateIncreasingly(String name, TermIndex oldTermIndex, TermIndex newTermIndex) {
    Preconditions.checkArgument(newTermIndex.compareTo(oldTermIndex) >= 0,
        "%s: newTermIndex = %s < oldTermIndex = %s", name, newTermIndex, oldTermIndex);
    return newTermIndex;
  }

  /**
   * Called to notify state machine about configuration changes.
   * Configurations changes include addition of newly bootstrapped OM.
   */
  @Override
  public void notifyConfigurationChanged(long term, long index,
      RaftProtos.RaftConfigurationProto newRaftConfiguration) {
    List<RaftProtos.RaftPeerProto> newPeers =
        newRaftConfiguration.getPeersList();
    List<RaftProtos.RaftPeerProto> newListeners =
        newRaftConfiguration.getListenersList();
    final StringBuilder logBuilder = new StringBuilder(1024)
        .append("notifyConfigurationChanged from Ratis: term=").append(term)
        .append(", index=").append(index)
        .append(", New Peer list: ");
    newPeers.forEach(peer -> logBuilder.append(peer.getId().toStringUtf8())
        .append('(')
        .append(peer.getAddress())
        .append("), "));
    logBuilder.append("New Listener list: ");
    newListeners.forEach(peer -> logBuilder.append(peer.getId().toStringUtf8())
        .append('(')
        .append(peer.getAddress())
        .append("), "));
    LOG.info(logBuilder.substring(0, logBuilder.length() - 2));

    List<String> newPeerIds = new ArrayList<>();
    for (RaftProtos.RaftPeerProto raftPeerProto : newPeers) {
      newPeerIds.add(RaftPeerId.valueOf(raftPeerProto.getId()).toString());
    }
    for (RaftProtos.RaftPeerProto raftPeerProto : newListeners) {
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
    OMRequest omRequest = OMRatisHelper.convertByteStringToOMRequest(
        messageContent);

    Preconditions.checkArgument(raftClientRequest.getRaftGroupId().equals(
        getGroupId()));
    try {
      handler.validateRequest(omRequest);
    } catch (IOException ioe) {
      TransactionContext ctxt = TransactionContext.newBuilder()
          .setClientRequest(raftClientRequest)
          .setStateMachine(this)
          .setServerRole(RaftProtos.RaftPeerRole.LEADER)
          .build();
      ctxt.setException(ioe);
      return ctxt;
    }

    return TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(raftClientRequest.getMessage().getContent())
        .setStateMachineContext(omRequest)
        .build();
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext trx)
      throws IOException {
    final OMRequest request = (OMRequest) trx.getStateMachineContext();
    OzoneManagerProtocolProtos.Type cmdType = request.getCmdType();

    OzoneManagerPrepareState prepareState = ozoneManager.getPrepareState();

    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: preAppendTransaction {}", getId(), TermIndex.valueOf(trx.getLogEntry()));
    }

    if (cmdType == OzoneManagerProtocolProtos.Type.Prepare) {
      // Must authenticate prepare requests here, since we must determine
      // whether or not to apply the prepare gate before proceeding with the
      // prepare request.
      UserGroupInformation userGroupInformation =
          UserGroupInformation.createRemoteUser(
          request.getUserInfo().getUserName());
      if (ozoneManager.getAclsEnabled()
          && !ozoneManager.isAdmin(userGroupInformation)) {
        String message = "Access denied for user " + userGroupInformation
            + ". Superuser privilege is required to prepare upgrade/downgrade.";
        OMException cause =
            new OMException(message, OMException.ResultCodes.ACCESS_DENIED);
        // Leader should not step down because of this failure.
        throw new StateMachineException(message, cause, false);
      } else {
        prepareState.enablePrepareGate();
      }
    }

    // In prepare mode, only prepare and cancel requests are allowed to go
    // through.
    if (prepareState.requestAllowed(cmdType)) {
      return trx;
    } else {
      String message = "Cannot apply write request " +
          request.getCmdType().name() + " when OM is in prepare mode.";
      OMException cause = new OMException(message,
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED);
      // Indicate that the leader should not step down because of this failure.
      throw new StateMachineException(message, cause, false);
    }
  }

  /*
   * Apply a committed log entry to the state machine.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      // For the Leader, the OMRequest is set in trx in startTransaction.
      // For Followers, the OMRequest hast to be converted from the log entry.
      final Object context = trx.getStateMachineContext();
      final OMRequest request = context != null ? (OMRequest) context
          : OMRatisHelper.convertByteStringToOMRequest(
          trx.getStateMachineLogEntry().getLogData());
      final TermIndex termIndex = TermIndex.valueOf(trx.getLogEntry());
      LOG.debug("{}: applyTransaction {}", getId(), termIndex);
      // In the current approach we have one single global thread executor.
      // with single thread. Right now this is being done for correctness, as
      // applyTransaction will be run on multiple OM's we want to execute the
      // transactions in the same order on all OM's, otherwise there is a
      // chance that OM replica's can be out of sync.
      // TODO: In this way we are making all applyTransactions in
      // OM serial order. Revisit this in future to use multiple executors for
      // volume/bucket.

      // Reason for not immediately implementing executor per volume is, if
      // one executor operations are slow, we cannot update the
      // lastAppliedIndex in OzoneManager StateMachine, even if other
      // executor has completed the transactions with id more.

      //if there are too many pending requests, wait for doubleBuffer flushing
      ozoneManagerDoubleBuffer.acquireUnFlushedTransactions(1);

      return CompletableFuture.supplyAsync(() -> runCommand(request, termIndex), executorService)
          .thenApply(this::processResponse);
    } catch (Exception e) {
      return completeExceptionally(e);
    }
  }

  @VisibleForTesting
  Message processResponse(OMResponse omResponse) {
    if (!omResponse.getSuccess()) {
      // INTERNAL_ERROR or METADATA_ERROR are considered as critical errors.
      // In such cases, OM must be terminated instead of completing the future exceptionally,
      // Otherwise, OM may continue applying transactions which leads to an inconsistent state.
      if (omResponse.getStatus() == INTERNAL_ERROR) {
        terminate(omResponse, OMException.ResultCodes.INTERNAL_ERROR);
      } else if (omResponse.getStatus() == METADATA_ERROR) {
        terminate(omResponse, OMException.ResultCodes.METADATA_ERROR);
      }
    }

    // For successful response and non-critical errors, convert the response.
    return OMRatisHelper.convertResponseToMessage(omResponse);
  }

  private static void terminate(OMResponse omResponse, OMException.ResultCodes resultCode) {
    OMException exception = new OMException(omResponse.getMessage(),
        resultCode);
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
      return CompletableFuture.completedFuture(queryCommand(omRequest));
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

    ozoneManagerDoubleBuffer.stop();
  }

  /**
   * Unpause the StateMachine, re-initialize the DoubleBuffer and update the
   * lastAppliedIndex. This should be done after uploading new state to the
   * StateMachine.
   */
  public synchronized void unpause(long newLastAppliedSnaphsotIndex,
      long newLastAppliedSnapShotTermIndex) {
    if (statePausedCount.decrementAndGet() == 0) {
      getLifeCycle().startAndTransition(() -> {
        this.ozoneManagerDoubleBuffer = buildDoubleBufferForRatis();
        this.setLastAppliedTermIndex(TermIndex.valueOf(
            newLastAppliedSnapShotTermIndex, newLastAppliedSnaphsotIndex));
        LOG.info("{}: OzoneManagerStateMachine un-pause completed. " +
            "newLastAppliedSnapshotIndex: {}, newLastAppliedSnapShotTermIndex: {}",
                getId(), newLastAppliedSnaphsotIndex, newLastAppliedSnapShotTermIndex);
      });
    }
  }

  public OzoneManagerDoubleBuffer buildDoubleBufferForRatis() {
    final int maxUnFlushedTransactionCount = ozoneManager.getConfiguration()
        .getInt(OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT,
            OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT_DEFAULT);
    return OzoneManagerDoubleBuffer.newBuilder()
        .setOmMetadataManager(ozoneManager.getMetadataManager())
        .setUpdateLastAppliedIndex(this::updateLastAppliedTermIndex)
        .setMaxUnFlushedTransactionCount(maxUnFlushedTransactionCount)
        .setThreadPrefix(threadPrefix)
        .setS3SecretManager(ozoneManager.getS3SecretManager())
        .enableTracing(isTracingEnabled)
        .build()
        .start();
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
    while (getLastAppliedTermIndex().getIndex() < lastSkippedIndex) {
      if (ozoneManager.isStopped()) {
        throw new IOException("OzoneManager is already stopped: " + ozoneManager.getNodeDetails());
      }
      try {
        ozoneManagerDoubleBuffer.awaitFlush();
      } catch (InterruptedException e) {
        throw IOUtils.toInterruptedIOException("Interrupted ozoneManagerDoubleBuffer.awaitFlush", e);
      }
    }

    return takeSnapshotImpl();
  }

  private synchronized long takeSnapshotImpl() throws IOException {
    final TermIndex applied = getLastAppliedTermIndex();
    final TermIndex notified = getLastNotifiedTermIndex();
    final TermIndex snapshot = applied.compareTo(notified) > 0 ? applied : notified;

    long startTime = Time.monotonicNow();
    final TransactionInfo transactionInfo = TransactionInfo.valueOf(snapshot);
    ozoneManager.setTransactionInfo(transactionInfo);
    ozoneManager.getMetadataManager().getTransactionInfoTable().put(TRANSACTION_INFO_KEY, transactionInfo);
    ozoneManager.getMetadataManager().getStore().flushDB();
    LOG.info("{}: taking snapshot. applied = {}, skipped = {}, " +
        "notified = {}, current snapshot index = {}, took {} ms",
            getId(), applied, lastSkippedIndex, notified, snapshot, Time.monotonicNow() - startTime);
    return snapshot.getIndex();
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
        () -> {
          try {
            return ozoneManager.installSnapshotFromLeader(leaderNodeId);
          } catch (IOException e) {
            throw new CompletionException(e);
          }
        },
        installSnapshotExecutor);
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
  @VisibleForTesting
  OMResponse runCommand(OMRequest request, TermIndex termIndex) {
    try {
      ExecutionContext context = ExecutionContext.of(termIndex.getIndex(), termIndex);
      final OMClientResponse omClientResponse = handler.handleWriteRequest(
          request, context, ozoneManagerDoubleBuffer);
      OMLockDetails omLockDetails = omClientResponse.getOmLockDetails();
      OMResponse omResponse = omClientResponse.getOMResponse();
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

  @VisibleForTesting
  OMResponse createErrorResponse(
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
    OMClientResponse omClientResponse = new DummyOMClientResponse(omResponse);
    ozoneManagerDoubleBuffer.add(omClientResponse, termIndex);
    return omResponse;
  }

  public void loadSnapshotInfoFromDB() throws IOException {
    // This is done, as we have a check in Ratis for not throwing
    // LeaderNotReadyException, it checks stateMachineIndex >= raftLog
    // nextIndex (placeHolderIndex).
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(
            ozoneManager.getMetadataManager());
    if (transactionInfo != null) {
      final TermIndex ti =  transactionInfo.getTermIndex();
      setLastAppliedTermIndex(ti);
      ozoneManager.setTransactionInfo(transactionInfo);
      LOG.info("LastAppliedIndex is set from TransactionInfo from OM DB as {}", ti);
    } else {
      LOG.info("TransactionInfo not found in OM DB.");
    }
  }

  /**
   * Submits read request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   */
  private Message queryCommand(OMRequest request) {
    OMResponse response = handler.handleReadRequest(request);
    return OMRatisHelper.convertResponseToMessage(response);
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
    ozoneManagerDoubleBuffer.stop();
    HadoopExecutors.shutdown(executorService, LOG, 5, TimeUnit.SECONDS);
    HadoopExecutors.shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
    if (this.nettyMetrics != null) {
      this.nettyMetrics.unregister();
    }
  }

  /**
   * Wait until both buffers are flushed.  This is used in cases like
   * "follower bootstrap tarball creation" where the rocksDb for the active
   * fs needs to synchronized with the rocksdb's for the snapshots.
   */
  public void awaitDoubleBufferFlush() throws InterruptedException {
    ozoneManagerDoubleBuffer.awaitFlush();
  }

  @VisibleForTesting
  public OzoneManagerDoubleBuffer getOzoneManagerDoubleBuffer() {
    return ozoneManagerDoubleBuffer;
  }
}

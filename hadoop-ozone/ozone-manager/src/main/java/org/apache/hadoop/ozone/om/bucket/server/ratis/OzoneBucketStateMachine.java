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
package org.apache.hadoop.ozone.om.bucket.server.ratis;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.Cache;
import org.apache.hadoop.hdds.utils.ResourceCache;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.protocolPB.RequestHandler;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.METADATA_ERROR;

/**
 *
 */
public class OzoneBucketStateMachine extends BaseStateMachine {

  private final XbeiverServerRatis ratisServer;

  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();

  private final AtomicBoolean stateMachineHealthy;
  private volatile OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;
  private volatile long lastSkippedIndex = RaftLog.INVALID_LOG_INDEX;
  private final OzoneManager ozoneManager;
  private RequestHandler handler;
  /** The last {@link TermIndex} received from {@link #notifyTermIndexUpdated(long, long)}. */
  private volatile TermIndex lastNotifiedTermIndex = TermIndex.valueOf(0, RaftLog.INVALID_LOG_INDEX);
  private final ExecutorService executorService;
  private final String threadPrefix;
  private final Cache<Long, ByteString> stateMachineDataCache;
  private final boolean isTracingEnabled;

  public OzoneBucketStateMachine(XbeiverServerRatis ratisServer, OzoneManager ozoneManager) {
    this.isTracingEnabled = false;
    this.ozoneManager = ozoneManager;
    this.ozoneManagerDoubleBuffer = buildDoubleBufferForRatis();
    this.ratisServer = ratisServer;
    stateMachineHealthy = new AtomicBoolean(true);
    this.handler = new OzoneManagerRequestHandler(ozoneManager);

    this.threadPrefix = ozoneManager.getThreadNamePrefix();

    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadPrefix +
                       "OMStateMachineApplyTransactionThread - %d").build();
    this.executorService = HadoopExecutors.newSingleThreadExecutor(build);

    long pendingRequestsBytesLimit = (long) ozoneManager.getConfiguration().getStorageSize(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // cache with FIFO eviction, and if element not found, this needs
    // to be obtained from disk for slow follower
    stateMachineDataCache = new ResourceCache<>(
        (index, data) -> data.size(),
        pendingRequestsBytesLimit,
        (p) -> {
//          if (p.wasEvicted()) {
//            metrics.incNumEvictedCacheCount();
//          }
        });
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void initialize(
      RaftServer server, RaftGroupId id, RaftStorage raftStorage)
      throws IOException {
    super.initialize(server, id, raftStorage);
    storage.init(raftStorage);
//    ratisServer.notifyGroupAdd(id);

    LOG.info("{}: initialize {}", server.getId(), id);
    loadSnapshot(storage.getLatestSnapshot());
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

  public TermIndex getLastNotifiedTermIndex() {
    return lastNotifiedTermIndex;
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

  /**
   * Submits read request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   */
  private Message queryCommand(OMRequest request) {
    OzoneManagerProtocolProtos.OMResponse response = handler.handleReadRequest(request);
    return OMRatisHelper.convertResponseToMessage(response);
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
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

  /** Assert if the given {@link TermIndex} is updated increasingly. */
  private TermIndex assertUpdateIncreasingly(String name, TermIndex oldTermIndex, TermIndex newTermIndex) {
    Preconditions.checkArgument(newTermIndex.compareTo(oldTermIndex) >= 0,
        "%s: newTermIndex = %s < oldTermIndex = %s", name, newTermIndex, oldTermIndex);
    return newTermIndex;
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

  private Message processResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
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

  private static void terminate(OzoneManagerProtocolProtos.OMResponse omResponse, OMException.ResultCodes resultCode) {
    OMException exception = new OMException(omResponse.getMessage(),
        resultCode);
    String errorMessage = "OM Ratis Server has received unrecoverable " +
                          "error, to avoid further DB corruption, terminating OM. Error " +
                          "Response received is:" + omResponse;
    ExitUtils.terminate(1, errorMessage, exception, LOG);
  }

  /**
   * Submits write request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   */
  private OzoneManagerProtocolProtos.OMResponse runCommand(OMRequest request, TermIndex termIndex) {
    try {
      ExecutionContext context = ExecutionContext.of(termIndex.getIndex(), termIndex);
      final OMClientResponse omClientResponse = handler.handleWriteRequest(
          request, context, ozoneManagerDoubleBuffer);
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

  private OzoneManagerProtocolProtos.OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception, TermIndex termIndex) {
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

//  @Override
//  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
//    // once the leader steps down , clear the cache
//    evictStateMachineCache();
//  }

//  @Override
//  public CompletableFuture<Void> truncate(long index) {
//    stateMachineDataCache.removeIf(k -> k > index);
//    return CompletableFuture.completedFuture(null);
//  }
//
//  @VisibleForTesting
//  public void evictStateMachineCache() {
//    stateMachineDataCache.clear();
//  }
//
//  @Override
//  public void notifyFollowerSlowness(RaftProtos.RoleInfoProto roleInfoProto, RaftPeer follower) {
//    ratisServer.handleFollowerSlowness(getGroupId(), roleInfoProto, follower);
//  }

//  @Override
//  public void notifyExtendedNoLeader(RaftProtos.RoleInfoProto roleInfoProto) {
//    ratisServer.handleNoLeader(getGroupId(), roleInfoProto);
//  }
//
//  @Override
//  public void notifyLogFailed(Throwable t, RaftProtos.LogEntryProto failedEntry) {
//    LOG.error("{}: {} {}", getGroupId(), TermIndex.valueOf(failedEntry),
//        toStateMachineLogEntryString(failedEntry.getStateMachineLogEntry()), t);
//    ratisServer.handleNodeLogFailure(getGroupId(), t);
//  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    ratisServer.handleInstallSnapshotFromLeader(getGroupId(), roleInfoProto,
        firstTermIndexInLog);
    final CompletableFuture<TermIndex> future = new CompletableFuture<>();
    future.complete(firstTermIndexInLog);
    return future;
  }

  @Override
  public void notifyGroupRemove() {
    ratisServer.notifyGroupRemove(getGroupId());
    // Make best effort to quasi-close all the containers on group removal.
    // Containers already in terminal state like CLOSED or UNHEALTHY will not
    // be affected.
//    for (Long cid : container2BCSIDMap.keySet()) {
//      try {
//        containerController.markContainerForClose(cid);
//        containerController.quasiCloseContainer(cid,
//            "Ratis group removed. Group id: " + getGroupId());
//      } catch (IOException e) {
//        LOG.debug("Failed to quasi-close container {}", cid);
//      }
//    }
  }

  @Override
  public void close() {
    evictStateMachineCache();
//    executor.shutdown();
//    metrics.unRegister();
  }

  public void evictStateMachineCache() {
    stateMachineDataCache.clear();
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId,
                                  RaftPeerId raftPeerId) {
    ratisServer.handleLeaderChangedNotification(groupMemberId, raftPeerId);
  }

//  @Override
//  public String toStateMachineLogEntryString(RaftProtos.StateMachineLogEntryProto proto) {
//    return smProtoToString(getGroupId(), containerController, proto);
//  }

  @Override
  public String toStateMachineLogEntryString(RaftProtos.StateMachineLogEntryProto proto) {
    return OMRatisHelper.smProtoToString(proto);
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot)
      throws IOException {
    if (snapshot == null) {
      TermIndex empty = TermIndex.valueOf(0, RaftLog.INVALID_LOG_INDEX);
      LOG.info("{}: The snapshot info is null. Setting the last applied index " +
               "to:{}", getGroupId(), empty);
      setLastAppliedTermIndex(empty);
      return empty.getIndex();
    }

    final File snapshotFile = snapshot.getFile().getPath().toFile();
    final TermIndex last =
        SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    LOG.info("{}: Setting the last applied index to {}", getGroupId(), last);
    setLastAppliedTermIndex(last);

    // initialize the dispatcher with snapshot so that it build the missing
    // container list
//    buildMissingContainerSet(snapshotFile);
    return last.getIndex();
  }

//  @VisibleForTesting
//  public void buildMissingContainerSet(File snapshotFile) throws IOException {
//    // initialize the dispatcher with snapshot so that it build the missing
//    // container list
//    try (FileInputStream fin = new FileInputStream(snapshotFile)) {
//      ContainerProtos.Container2BCSIDMapProto proto =
//          ContainerProtos.Container2BCSIDMapProto
//              .parseFrom(fin);
//      // read the created containers list from the snapshot file and add it to
//      // the container2BCSIDMap here.
//      // container2BCSIDMap will further grow as and when containers get created
//      container2BCSIDMap.putAll(proto.getContainer2BCSIDMap());
//      dispatcher.buildMissingContainerSetAndValidate(container2BCSIDMap);
//    }
//  }

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
        .enableRatis(true)
        .enableTracing(isTracingEnabled)
        .build()
        .start();
  }

  public boolean isStateMachineHealthy() {
    return stateMachineHealthy.get();
  }
}

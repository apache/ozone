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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Container2BCSIDMapProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.Cache;
import org.apache.hadoop.hdds.utils.ResourceCache;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.impl.KeyValueStreamDataChannel;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.util.Time;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.StateMachineEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.TaskQueue;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StateMachine} for containers,
 * which is responsible for handling different types of container requests.
 * <p>
 * The container requests can be divided into readonly request, WriteChunk request and other write requests.
 * - Read only requests (see {@link HddsUtils#isReadOnly}) are handled by {@link #query(Message)}.
 * - WriteChunk request contains user data
 * - Other write request does not contain user data.
 * <p>
 * In order to optimize the write throughput, a WriteChunk request is processed :
 * (1) {@link #startTransaction(RaftClientRequest)} separate user data from the client request
 * (2) the user data is written directly into the state machine via {@link #write}
 * (3) transaction is committed via {@link #applyTransaction(TransactionContext)}
 * <p>
 * For the other write requests,
 * the transaction is directly committed via {@link #applyTransaction(TransactionContext)}.
 * <p>
 * There are 2 ordering operation which are enforced right now in the code,
 * 1) WriteChunk must be executed after the CreateContainer;
 *    otherwise, WriteChunk will fail with container not found.
 * 2) WriteChunk commit is executed after WriteChunk write.
 *    Then, WriteChunk commit and CreateContainer will be executed in the same order.
 */
public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(ContainerStateMachine.class);

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final ContainerDispatcher dispatcher;
  private final ContainerController containerController;
  private final XceiverServerRatis ratisServer;
  private final NavigableMap<Long, WriteFutures> writeChunkFutureMap;

  private final long writeChunkWaitMaxNs;

  // keeps track of the containers created per pipeline
  private final Map<Long, Long> container2BCSIDMap;
  private final TaskQueueMap containerTaskQueues = new TaskQueueMap();
  private final ExecutorService executor;
  private final List<ThreadPoolExecutor> chunkExecutors;
  private final Map<Long, Long> applyTransactionCompletionMap;
  private final Set<Long> unhealthyContainers;
  private final Cache<Long, ByteString> stateMachineDataCache;
  private final AtomicBoolean stateMachineHealthy;

  private final Semaphore applyTransactionSemaphore;
  private final boolean waitOnBothFollowers;
  private final HddsDatanodeService datanodeService;
  private static Semaphore semaphore = new Semaphore(1);
  private final AtomicBoolean peersValidated;

  /**
   * CSM metrics.
   */
  private final CSMMetrics metrics;

  static class TaskQueueMap {
    private final Map<Long, TaskQueue> map = new HashMap<>();

    synchronized CompletableFuture<ContainerCommandResponseProto> submit(
        long containerId,
        CheckedSupplier<ContainerCommandResponseProto, Exception> task,
        ExecutorService executor) {
      final TaskQueue queue = map.computeIfAbsent(
          containerId, id -> new TaskQueue("container" + id));
      final CompletableFuture<ContainerCommandResponseProto> f
          = queue.submit(task, executor);
      // after the task is completed, remove the queue if the queue is empty.
      f.thenAccept(dummy -> removeIfEmpty(containerId));
      return f;
    }

    synchronized void removeIfEmpty(long containerId) {
      map.computeIfPresent(containerId,
          (id, q) -> q.isEmpty() ? null : q);
    }
  }

  /**
   * {@link StateMachine} context.
   *
   * @see TransactionContext#setStateMachineContext(Object)
   * @see TransactionContext#getStateMachineContext()
   */
  static class Context {
    private final ContainerCommandRequestProto requestProto;
    private final ContainerCommandRequestProto logProto;
    private final long startTime = Time.monotonicNowNanos();

    Context(ContainerCommandRequestProto requestProto, ContainerCommandRequestProto logProto) {
      this.requestProto = requestProto;
      this.logProto = logProto;
    }

    ContainerCommandRequestProto getRequestProto() {
      return requestProto;
    }

    ContainerCommandRequestProto getLogProto() {
      return logProto;
    }

    long getStartTime() {
      return startTime;
    }
  }

  static class WriteFutures {
    private final Future<ContainerCommandResponseProto> writeChunkFuture;
    private final CompletableFuture<Message> raftFuture;
    private final long startTime;

    WriteFutures(Future<ContainerCommandResponseProto> writeChunkFuture,
                 CompletableFuture<Message> raftFuture, long startTime) {
      this.writeChunkFuture = writeChunkFuture;
      this.raftFuture = raftFuture;
      this.startTime = startTime;
    }

    public Future<ContainerCommandResponseProto> getWriteChunkFuture() {
      return writeChunkFuture;
    }

    public CompletableFuture<Message> getRaftFuture() {
      return raftFuture;
    }

    long getStartTime() {
      return startTime;
    }
  }

  @SuppressWarnings("parameternumber")
  public ContainerStateMachine(HddsDatanodeService hddsDatanodeService, RaftGroupId gid,
      ContainerDispatcher dispatcher,
      ContainerController containerController,
      List<ThreadPoolExecutor> chunkExecutors,
      XceiverServerRatis ratisServer,
      ConfigurationSource conf,
      String threadNamePrefix) {
    this.datanodeService = hddsDatanodeService;
    this.dispatcher = dispatcher;
    this.containerController = containerController;
    this.ratisServer = ratisServer;
    metrics = CSMMetrics.create(gid);
    this.writeChunkFutureMap = new ConcurrentSkipListMap<>();
    applyTransactionCompletionMap = new ConcurrentHashMap<>();
    this.unhealthyContainers = ConcurrentHashMap.newKeySet();
    long pendingRequestsBytesLimit = (long)conf.getStorageSize(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // cache with FIFO eviction, and if element not found, this needs
    // to be obtained from disk for slow follower
    stateMachineDataCache = new ResourceCache<>(
        (index, data) -> data.size(),
        pendingRequestsBytesLimit,
        (p) -> {
          if (p.wasEvicted()) {
            metrics.incNumEvictedCacheCount();
          }
        });

    this.chunkExecutors = chunkExecutors;

    this.container2BCSIDMap = new ConcurrentHashMap<>();

    final int numContainerOpExecutors = conf.getInt(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT);
    int maxPendingApplyTransactions = conf.getInt(
        ScmConfigKeys.
            HDDS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS,
        ScmConfigKeys.
            HDDS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS_DEFAULT);
    applyTransactionSemaphore = new Semaphore(maxPendingApplyTransactions);
    stateMachineHealthy = new AtomicBoolean(true);
    this.peersValidated = new AtomicBoolean(false);

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(
            threadNamePrefix + "ContainerOp-" + gid.getUuid() + "-%d")
        .build();
    this.executor = Executors.newFixedThreadPool(numContainerOpExecutors,
        threadFactory);

    this.waitOnBothFollowers = conf.getObject(
        DatanodeConfiguration.class).waitOnAllFollowers();

    this.writeChunkWaitMaxNs = conf.getTimeDuration(ScmConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINE_WRITE_WAIT_INTERVAL,
        ScmConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINE_WRITE_WAIT_INTERVAL_NS_DEFAULT, TimeUnit.NANOSECONDS);
  }

  private void validatePeers() throws IOException {
    if (this.peersValidated.get()) {
      return;
    }
    final RaftGroup group = ratisServer.getServerDivision(getGroupId()).getGroup();
    final RaftPeerId selfId = ratisServer.getServer().getId();
    if (group.getPeer(selfId) == null) {
      throw new StorageContainerException("Current datanode " + selfId + " is not a member of " + group,
          ContainerProtos.Result.INVALID_CONFIG);
    }
    peersValidated.set(true);
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  public CSMMetrics getMetrics() {
    return metrics;
  }

  @Override
  public void initialize(
      RaftServer server, RaftGroupId id, RaftStorage raftStorage)
      throws IOException {
    super.initialize(server, id, raftStorage);
    storage.init(raftStorage);
    ratisServer.notifyGroupAdd(id);

    LOG.info("{}: initialize {}", server.getId(), id);
    loadSnapshot(storage.getLatestSnapshot());
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
    buildMissingContainerSet(snapshotFile);
    return last.getIndex();
  }

  @VisibleForTesting
  public void buildMissingContainerSet(File snapshotFile) throws IOException {
    // initialize the dispatcher with snapshot so that it build the missing
    // container list
    try (InputStream fin = Files.newInputStream(snapshotFile.toPath())) {
      ContainerProtos.Container2BCSIDMapProto proto =
              ContainerProtos.Container2BCSIDMapProto
                      .parseFrom(fin);
      // read the created containers list from the snapshot file and add it to
      // the container2BCSIDMap here.
      // container2BCSIDMap will further grow as and when containers get created
      container2BCSIDMap.putAll(proto.getContainer2BCSIDMap());
      dispatcher.buildMissingContainerSetAndValidate(container2BCSIDMap);
    }
  }

  /**
   * As a part of taking snapshot with Ratis StateMachine, it will persist
   * the existing container set in the snapshotFile.
   * @param out OutputStream mapped to the Ratis snapshot file
   * @throws IOException
   */
  public void persistContainerSet(OutputStream out) throws IOException {
    Container2BCSIDMapProto.Builder builder =
        Container2BCSIDMapProto.newBuilder();
    builder.putAllContainer2BCSID(container2BCSIDMap);
    // TODO : while snapshot is being taken, deleteContainer call should not
    // should not happen. Lock protection will be required if delete
    // container happens outside of Ratis.
    builder.build().writeTo(out);
  }

  public boolean isStateMachineHealthy() {
    return stateMachineHealthy.get();
  }

  private void checkContainerHealthy(long containerId, boolean skipContainerUnhealthyCheck)
      throws StorageContainerException {
    if (!isStateMachineHealthy() && unhealthyContainers.contains(containerId)) {
      throw new StorageContainerException(String.format("Prev writes to container %d failed, stopping all writes to " +
              "container", containerId), ContainerProtos.Result.CONTAINER_UNHEALTHY);
    } else if (!isStateMachineHealthy() && skipContainerUnhealthyCheck) {
      throw new StorageContainerException(String.format("Prev writes to containers %s failed, stopping all writes to " +
          "container", unhealthyContainers.toString()), ContainerProtos.Result.CONTAINER_UNHEALTHY);
    }
  }

  @Override
  public long takeSnapshot() throws IOException {
    TermIndex ti = getLastAppliedTermIndex();
    long startTime = Time.monotonicNow();
    if (!isStateMachineHealthy()) {
      String msg =
          "Failed to take snapshot " + " for " + getGroupId() + " as the stateMachine"
              + " is unhealthy. The last applied index is at " + ti;
      StateMachineException sme = new StateMachineException(msg);
      LOG.error(msg);
      throw sme;
    }
    if (ti != null && ti.getIndex() != RaftLog.INVALID_LOG_INDEX) {
      final File snapshotFile =
          storage.getSnapshotFile(ti.getTerm(), ti.getIndex());
      LOG.info("{}: Taking a snapshot at:{} file {}", getGroupId(), ti, snapshotFile);
      try (OutputStream fos = FileUtils.newOutputStreamForceAtClose(snapshotFile, CREATE, TRUNCATE_EXISTING, WRITE)) {
        persistContainerSet(fos);
        fos.flush();
      } catch (IOException ioe) {
        LOG.error("{}: Failed to write snapshot at:{} file {}", getGroupId(), ti,
            snapshotFile);
        throw ioe;
      }
      LOG.info("{}: Finished taking a snapshot at:{} file:{} took: {} ms",
              getGroupId(), ti, snapshotFile, (Time.monotonicNow() - startTime));
      return ti.getIndex();
    }
    return -1;
  }

  /** For applying log entry. */
  @Override
  public TransactionContext startTransaction(LogEntryProto entry, RaftPeerRole role) {
    final TransactionContext trx = super.startTransaction(entry, role);

    final StateMachineLogEntryProto stateMachineLogEntry = entry.getStateMachineLogEntry();
    final ContainerCommandRequestProto logProto;
    try {
      logProto = getContainerCommandRequestProto(getGroupId(), stateMachineLogEntry.getLogData());
    } catch (InvalidProtocolBufferException e) {
      trx.setException(e);
      return trx;
    }

    final ContainerCommandRequestProto requestProto;
    if (logProto.getCmdType() == Type.WriteChunk) {
      // combine state machine data
      requestProto = ContainerCommandRequestProto.newBuilder(logProto)
          .setWriteChunk(WriteChunkRequestProto.newBuilder(logProto.getWriteChunk())
          .setData(stateMachineLogEntry.getStateMachineEntry().getStateMachineData()))
          .build();
    } else {
      // request and log are the same when there is no state machine data,
      requestProto = logProto;
    }
    return trx.setStateMachineContext(new Context(requestProto, logProto));
  }

  /** For the Leader to serve the given client request. */
  @Override
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    long startTime = Time.monotonicNowNanos();
    final ContainerCommandRequestProto proto =
        message2ContainerCommandRequestProto(request.getMessage());
    Preconditions.checkArgument(request.getRaftGroupId().equals(getGroupId()));

    final TransactionContext.Builder builder = TransactionContext.newBuilder()
        .setClientRequest(request)
        .setStateMachine(this)
        .setServerRole(RaftPeerRole.LEADER);

    metrics.incPendingApplyTransactions();

    try {
      dispatcher.validateContainerCommand(proto);
    } catch (IOException ioe) {
      if (ioe instanceof ContainerNotOpenException) {
        metrics.incNumContainerNotOpenVerifyFailures();
      } else {
        metrics.incNumStartTransactionVerifyFailures();
        LOG.error("startTransaction validation failed on leader", ioe);
      }
      return builder.build().setException(ioe);
    }

    // once the token is verified, clear it from the proto
    final ContainerCommandRequestProto.Builder protoBuilder = ContainerCommandRequestProto.newBuilder(proto)
        .clearEncodedToken();
    boolean blockAlreadyFinalized = false;
    if (proto.getCmdType() == Type.PutBlock) {
      blockAlreadyFinalized = shouldRejectRequest(proto.getPutBlock().getBlockData().getBlockID());
    } else if (proto.getCmdType() == Type.WriteChunk) {
      final WriteChunkRequestProto write = proto.getWriteChunk();
      blockAlreadyFinalized = shouldRejectRequest(write.getBlockID());
      if (!blockAlreadyFinalized) {
        Preconditions.checkArgument(write.hasData());
        Preconditions.checkArgument(!write.getData().isEmpty());
        final WriteChunkRequestProto.Builder commitWriteChunkProto = WriteChunkRequestProto.newBuilder(write)
            .clearData();
        protoBuilder.setWriteChunk(commitWriteChunkProto)
            .setPipelineID(getGroupId().getUuid().toString())
            .setTraceID(proto.getTraceID());

        builder.setStateMachineData(write.getData());
      }
    } else if (proto.getCmdType() == Type.FinalizeBlock) {
      containerController.addFinalizedBlock(proto.getContainerID(),
          proto.getFinalizeBlock().getBlockID().getLocalID());
    }

    if (blockAlreadyFinalized) {
      TransactionContext transactionContext = builder.build();
      transactionContext.setException(new StorageContainerException("Block already finalized",
          ContainerProtos.Result.BLOCK_ALREADY_FINALIZED));
      return transactionContext;
    } else {
      final ContainerCommandRequestProto containerCommandRequestProto = protoBuilder.build();
      TransactionContext txnContext = builder.setStateMachineContext(new Context(proto, containerCommandRequestProto))
          .setLogData(containerCommandRequestProto.toByteString())
          .build();
      metrics.recordStartTransactionCompleteNs(Time.monotonicNowNanos() - startTime);
      return txnContext;
    }
  }

  private boolean shouldRejectRequest(ContainerProtos.DatanodeBlockID blockID) {
    return containerController.isFinalizedBlockExist(blockID.getContainerID(), blockID.getLocalID());
  }

  private static ContainerCommandRequestProto getContainerCommandRequestProto(
      RaftGroupId id, ByteString request)
      throws InvalidProtocolBufferException {
    // TODO: We can avoid creating new builder and set pipeline Id if
    // the client is already sending the pipeline id, then we just have to
    // validate the pipeline Id.
    return ContainerCommandRequestProto.newBuilder(
        ContainerCommandRequestProto.parseFrom(request))
        .setPipelineID(id.getUuid().toString()).build();
  }

  private ContainerCommandRequestProto message2ContainerCommandRequestProto(
      Message message) throws InvalidProtocolBufferException {
    return ContainerCommandRequestMessage.toProto(message.getContent(), getGroupId());
  }

  private ContainerCommandResponseProto dispatchCommand(
      ContainerCommandRequestProto requestProto, DispatcherContext context) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: dispatch {} containerID={} pipelineID={} traceID={}", getGroupId(),
          requestProto.getCmdType(), requestProto.getContainerID(),
          requestProto.getPipelineID(), requestProto.getTraceID());
    }
    ContainerCommandResponseProto response =
        dispatcher.dispatch(requestProto, context);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: response {}", getGroupId(), response);
    }
    return response;
  }

  private CompletableFuture<Message> writeStateMachineData(
      ContainerCommandRequestProto requestProto, long entryIndex, long term,
      long startTime) {
    final WriteFutures previous = writeChunkFutureMap.get(entryIndex);
    if (previous != null) {
      // generally state machine will wait forever, for precaution, a check is added if retry happens.
      return previous.getRaftFuture();
    }
    try {
      validateLongRunningWrite();
    } catch (StorageContainerException e) {
      ContainerCommandResponseProto result = ContainerUtils.logAndReturnError(LOG, e, requestProto);
      return CompletableFuture.completedFuture(result::toByteString);
    }
    final WriteChunkRequestProto write = requestProto.getWriteChunk();
    RaftServer server = ratisServer.getServer();
    Preconditions.checkArgument(!write.getData().isEmpty());
    try {
      if (server.getDivision(getGroupId()).getInfo().isLeader()) {
        stateMachineDataCache.put(entryIndex, write.getData());
      }
    } catch (InterruptedException ioe) {
      Thread.currentThread().interrupt();
      return completeExceptionally(ioe);
    } catch (IOException ioe) {
      return completeExceptionally(ioe);
    }
    final DispatcherContext context =
        DispatcherContext
            .newBuilder(DispatcherContext.Op.WRITE_STATE_MACHINE_DATA)
            .setTerm(term)
            .setLogIndex(entryIndex)
            .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
            .setContainer2BCSIDMap(container2BCSIDMap)
            .build();
    CompletableFuture<Message> raftFuture = new CompletableFuture<>();
    // ensure the write chunk happens asynchronously in writeChunkExecutor pool thread.
    Future<ContainerCommandResponseProto> future = getChunkExecutor(
        requestProto.getWriteChunk()).submit(() -> {
          try {
            try {
              checkContainerHealthy(write.getBlockID().getContainerID(), true);
            } catch (StorageContainerException e) {
              ContainerCommandResponseProto result = ContainerUtils.logAndReturnError(LOG, e, requestProto);
              handleCommandResult(requestProto, entryIndex, startTime, result, write, raftFuture);
              return result;
            }
            metrics.recordWriteStateMachineQueueingLatencyNs(
                Time.monotonicNowNanos() - startTime);
            ContainerCommandResponseProto result = dispatchCommand(requestProto, context);
            handleCommandResult(requestProto, entryIndex, startTime, result, write, raftFuture);
            return result;
          } catch (Exception e) {
            LOG.error("{}: writeChunk writeStateMachineData failed: blockId" +
                "{} logIndex {} chunkName {}", getGroupId(), write.getBlockID(),
                entryIndex, write.getChunkData().getChunkName(), e);
            metrics.incNumWriteDataFails();
            // write chunks go in parallel. It's possible that one write chunk
            // see the stateMachine is marked unhealthy by other parallel thread
            unhealthyContainers.add(write.getBlockID().getContainerID());
            stateMachineHealthy.set(false);
            StorageContainerException sce = new StorageContainerException("Failed to write chunk data",
                e, ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
            ContainerCommandResponseProto result = ContainerUtils.logAndReturnError(LOG, sce, requestProto);
            raftFuture.complete(result::toByteString);
            return result;
          } finally {
            // Remove the future once it finishes execution from the
            writeChunkFutureMap.remove(entryIndex);
          }
        });

    writeChunkFutureMap.put(entryIndex, new WriteFutures(future, raftFuture, startTime));
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: writeChunk writeStateMachineData : blockId" +
              "{} logIndex {} chunkName {}", getGroupId(), write.getBlockID(),
          entryIndex, write.getChunkData().getChunkName());
    }
    return raftFuture;
  }

  private void handleCommandResult(ContainerCommandRequestProto requestProto, long entryIndex, long startTime,
                                   ContainerCommandResponseProto r, WriteChunkRequestProto write,
                                   CompletableFuture<Message> raftFuture) {
    if (r.getResult() != ContainerProtos.Result.SUCCESS
        && r.getResult() != ContainerProtos.Result.CONTAINER_NOT_OPEN
        && r.getResult() != ContainerProtos.Result.CLOSED_CONTAINER_IO
        // After concurrent flushes are allowed on the same key, chunk file inconsistencies can happen and
        // that should not crash the pipeline.
        && r.getResult() != ContainerProtos.Result.CHUNK_FILE_INCONSISTENCY) {
      LOG.error(getGroupId() + ": writeChunk writeStateMachineData failed: blockId" +
          write.getBlockID() + " logIndex " + entryIndex + " chunkName " +
          write.getChunkData().getChunkName() + " Error message: " +
          r.getMessage() + " Container Result: " + r.getResult());
      metrics.incNumWriteDataFails();
      // If the write fails currently we mark the stateMachine as unhealthy.
      // This leads to pipeline close. Any change in that behavior requires
      // handling the entry for the write chunk in cache.
      stateMachineHealthy.set(false);
      unhealthyContainers.add(write.getBlockID().getContainerID());
      raftFuture.complete(r::toByteString);
    } else {
      metrics.incNumBytesWrittenCount(
          requestProto.getWriteChunk().getChunkData().getLen());
      if (LOG.isDebugEnabled()) {
        LOG.debug(getGroupId() +
            ": writeChunk writeStateMachineData  completed: blockId" +
            write.getBlockID() + " logIndex " + entryIndex + " chunkName " +
            write.getChunkData().getChunkName());
      }
      raftFuture.complete(r::toByteString);
      metrics.recordWriteStateMachineCompletionNs(
          Time.monotonicNowNanos() - startTime);
    }
  }

  private void validateLongRunningWrite() throws StorageContainerException {
    // get min valid write chunk operation's future context
    Map.Entry<Long, WriteFutures> writeFutureContextEntry = null;
    for (boolean found = false; !found;) {
      writeFutureContextEntry = writeChunkFutureMap.firstEntry();
      if (null == writeFutureContextEntry) {
        return;
      }
      if (writeFutureContextEntry.getValue().getWriteChunkFuture().isDone()) {
        // there is a possibility that writeChunkFutureMap may have dangling entry, as remove is done before add future
        writeChunkFutureMap.remove(writeFutureContextEntry.getKey());
      } else {
        found = true;
      }
    }
    // validate for timeout in milli second
    long waitTime = Time.monotonicNowNanos() - writeFutureContextEntry.getValue().getStartTime();
    if (waitTime > writeChunkWaitMaxNs) {
      LOG.error("Write chunk has taken {}ns crossing threshold {}ns for index {} groupId {}, " +
              "cancelling pending write chunk for this group", waitTime, writeChunkWaitMaxNs,
          writeFutureContextEntry.getKey(), getGroupId());
      stateMachineHealthy.set(false);
      writeChunkFutureMap.forEach((key, value) -> {
        value.getWriteChunkFuture().cancel(true);
      });
      throw new StorageContainerException("Write chunk has taken " + waitTime + "ns crossing threshold "
          + writeChunkWaitMaxNs + "ns for index " + writeFutureContextEntry.getKey() + " groupId " + getGroupId(),
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
    }
  }

  private StateMachine.DataChannel getStreamDataChannel(
          ContainerCommandRequestProto requestProto,
          DispatcherContext context) throws StorageContainerException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: getStreamDataChannel {} containerID={} pipelineID={} " +
                      "traceID={}", getGroupId(), requestProto.getCmdType(),
              requestProto.getContainerID(), requestProto.getPipelineID(),
              requestProto.getTraceID());
    }
    dispatchCommand(requestProto, context);  // stream init
    return dispatcher.getStreamDataChannel(requestProto);
  }

  @Override
  public CompletableFuture<DataStream> stream(RaftClientRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        ContainerCommandRequestProto requestProto =
            message2ContainerCommandRequestProto(request.getMessage());
        DispatcherContext context =
            DispatcherContext
                .newBuilder(DispatcherContext.Op.STREAM_INIT)
                .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
                .setContainer2BCSIDMap(container2BCSIDMap)
                .build();
        DataChannel channel = getStreamDataChannel(requestProto, context);
        final ExecutorService chunkExecutor = requestProto.hasWriteChunk() ?
            getChunkExecutor(requestProto.getWriteChunk()) : null;
        return new LocalStream(channel, chunkExecutor);
      } catch (IOException e) {
        throw new CompletionException("Failed to create data stream", e);
      }
    }, executor);
  }

  @Override
  public CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
    if (stream == null) {
      return JavaUtils.completeExceptionally(new IllegalStateException(
          "DataStream is null"));
    } else if (!(stream instanceof LocalStream)) {
      return JavaUtils.completeExceptionally(new IllegalStateException(
          "Unexpected DataStream " + stream.getClass()));
    }
    final DataChannel dataChannel = stream.getDataChannel();
    if (dataChannel.isOpen()) {
      return JavaUtils.completeExceptionally(new IllegalStateException(
          "DataStream: " + stream + " is not closed properly"));
    }

    if (!(dataChannel instanceof KeyValueStreamDataChannel)) {
      return JavaUtils.completeExceptionally(new IllegalStateException(
          "Unexpected DataChannel " + dataChannel.getClass()));
    }

    final KeyValueStreamDataChannel kvStreamDataChannel =
        (KeyValueStreamDataChannel) dataChannel;
    kvStreamDataChannel.setLinked();
    return CompletableFuture.completedFuture(null);
  }

  private ExecutorService getChunkExecutor(WriteChunkRequestProto req) {
    int i = (int)(req.getBlockID().getLocalID() % chunkExecutors.size());
    return chunkExecutors.get(i);
  }

  /**
   * {@link #writeStateMachineData}
   * calls are not synchronized with each other
   * and also with {@code applyTransaction(TransactionContext)}.
   */
  @Override
  public CompletableFuture<Message> write(LogEntryProto entry, TransactionContext trx) {
    try {
      metrics.incNumWriteStateMachineOps();
      long writeStateMachineStartTime = Time.monotonicNowNanos();
      final Context context = (Context) trx.getStateMachineContext();
      Objects.requireNonNull(context, "context == null");
      final ContainerCommandRequestProto requestProto = context.getRequestProto();
      final Type cmdType = requestProto.getCmdType();

      // For only writeChunk, there will be writeStateMachineData call.
      // CreateContainer will happen as a part of writeChunk only.
      switch (cmdType) {
      case WriteChunk:
        return writeStateMachineData(requestProto, entry.getIndex(),
            entry.getTerm(), writeStateMachineStartTime);
      default:
        throw new IllegalStateException("Cmd Type:" + cmdType
            + " should not have state machine data");
      }
    } catch (Exception e) {
      metrics.incNumWriteStateMachineFails();
      return completeExceptionally(e);
    }
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    try {
      metrics.incNumQueryStateMachineOps();
      final ContainerCommandRequestProto requestProto =
          message2ContainerCommandRequestProto(request);
      return CompletableFuture.completedFuture(
          dispatchCommand(requestProto, null)::toByteString);
    } catch (IOException e) {
      metrics.incNumQueryStateMachineFails();
      return completeExceptionally(e);
    }
  }

  private ByteString readStateMachineData(
      ContainerCommandRequestProto requestProto, long term, long index)
      throws IOException {
    // the stateMachine data is not present in the stateMachine cache,
    // increment the stateMachine cache miss count
    metrics.incNumReadStateMachineMissCount();
    WriteChunkRequestProto writeChunkRequestProto =
        requestProto.getWriteChunk();
    ContainerProtos.ChunkInfo chunkInfo = writeChunkRequestProto.getChunkData();
    // prepare the chunk to be read
    ReadChunkRequestProto.Builder readChunkRequestProto =
        ReadChunkRequestProto.newBuilder()
            .setBlockID(writeChunkRequestProto.getBlockID())
            .setChunkData(chunkInfo)
            .setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1);
    ContainerCommandRequestProto dataContainerCommandProto =
        ContainerCommandRequestProto.newBuilder(requestProto)
            .setCmdType(Type.ReadChunk).setReadChunk(readChunkRequestProto)
            .build();
    final DispatcherContext context = DispatcherContext
        .newBuilder(DispatcherContext.Op.READ_STATE_MACHINE_DATA)
        .setTerm(term)
        .setLogIndex(index)
        .build();
    // read the chunk
    ContainerCommandResponseProto response =
        dispatchCommand(dataContainerCommandProto, context);
    if (response.getResult() != ContainerProtos.Result.SUCCESS) {
      StorageContainerException sce =
          new StorageContainerException(response.getMessage(),
              response.getResult());
      LOG.error("gid {} : ReadStateMachine failed. cmd {} logIndex {} msg : "
              + "{} Container Result: {}", getGroupId(), response.getCmdType(), index,
          response.getMessage(), response.getResult());
      stateMachineHealthy.set(false);
      unhealthyContainers.add(requestProto.getContainerID());
      throw sce;
    }

    ReadChunkResponseProto responseProto = response.getReadChunk();
    ByteString data;
    if (responseProto.hasData()) {
      data = responseProto.getData();
    } else {
      data = BufferUtils.concatByteStrings(
          responseProto.getDataBuffers().getBuffersList());
    }

    // assert that the response has data in it.
    Objects.requireNonNull(data, () -> "data == null for " + TextFormat.shortDebugString(chunkInfo));
    Preconditions.checkState(data.size() == chunkInfo.getLen(),
        "read chunk len=%s does not match chunk expected len=%s for chunk:%s",
        data.size(), chunkInfo.getLen(), chunkInfo);

    return data;
  }

  /**
   * Returns the combined future of all the writeChunks till the given log
   * index. The Raft log worker will wait for the stateMachineData to complete
   * flush as well.
   *
   * @param index log index till which the stateMachine data needs to be flushed
   * @return Combined future of all writeChunks till the log index given.
   */
  @Override
  public CompletableFuture<Void> flush(long index) {
    final SortedMap<Long, WriteFutures> head = writeChunkFutureMap.headMap(index, true);
    if (head.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.allOf(head.values().stream()
        .map(WriteFutures::getRaftFuture)
        .toArray(CompletableFuture[]::new));
  }

  /**
   * This method is used by the Leader to read state machine data for sending appendEntries to followers.
   * It will first get the data from {@link #stateMachineDataCache}.
   * If the data is not in the cache, it will read from the file by dispatching a command
   *
   * @param trx the transaction context,
   *           which can be null if this method is invoked after {@link #applyTransaction(TransactionContext)}.
   */
  @Override
  public CompletableFuture<ByteString> read(LogEntryProto entry, TransactionContext trx) {
    metrics.incNumReadStateMachineOps();
    final ByteString dataInContext = Optional.ofNullable(trx)
        .map(TransactionContext::getStateMachineLogEntry)
        .map(StateMachineLogEntryProto::getStateMachineEntry)
        .map(StateMachineEntryProto::getStateMachineData)
        .orElse(null);
    if (dataInContext != null && !dataInContext.isEmpty()) {
      return CompletableFuture.completedFuture(dataInContext);
    }

    final ByteString dataInCache = stateMachineDataCache.get(entry.getIndex());
    if (dataInCache != null) {
      Preconditions.checkArgument(!dataInCache.isEmpty());
      metrics.incNumDataCacheHit();
      return CompletableFuture.completedFuture(dataInCache);
    } else {
      metrics.incNumDataCacheMiss();
    }

    try {
      final Context context = (Context) Optional.ofNullable(trx)
          .map(TransactionContext::getStateMachineContext)
          .orElse(null);
      final ContainerCommandRequestProto requestProto = context != null ? context.getLogProto()
          : getContainerCommandRequestProto(getGroupId(), entry.getStateMachineLogEntry().getLogData());

      if (requestProto.getCmdType() != Type.WriteChunk) {
        throw new IllegalStateException("Cmd type:" + requestProto.getCmdType()
            + " cannot have state machine data");
      }
      final CompletableFuture<ByteString> future = new CompletableFuture<>();
      CompletableFuture.runAsync(() -> {
        try {
          future.complete(readStateMachineData(requestProto, entry.getTerm(), entry.getIndex()));
        } catch (IOException e) {
          metrics.incNumReadStateMachineFails();
          future.completeExceptionally(e);
        }
      }, getChunkExecutor(requestProto.getWriteChunk()));
      return future;
    } catch (Exception e) {
      metrics.incNumReadStateMachineFails();
      LOG.error("{} unable to read stateMachineData:", getGroupId(), e);
      return completeExceptionally(e);
    }
  }

  private synchronized void updateLastApplied() {
    Long appliedTerm = null;
    long appliedIndex = -1;
    for (long i = getLastAppliedTermIndex().getIndex() + 1;; i++) {
      final Long removed = applyTransactionCompletionMap.remove(i);
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
   * Notifies the state machine about index updates because of entries
   * which do not cause state machine update, i.e. conf entries, metadata
   * entries
   * @param term term of the log entry
   * @param index index of the log entry
   */
  @Override
  public void notifyTermIndexUpdated(long term, long index) {
    applyTransactionCompletionMap.put(index, term);
    // We need to call updateLastApplied here because now in ratis when a
    // node becomes leader, it is checking stateMachineIndex >=
    // placeHolderIndex (when a node becomes leader, it writes a conf entry
    // with some information like its peers and termIndex). So, calling
    // updateLastApplied updates lastAppliedTermIndex.
    updateLastApplied();
    removeStateMachineDataIfNeeded(index);
  }

  @Override
  public void notifyServerShutdown(RaftProtos.RoleInfoProto roleInfo, boolean allServer) {
    // if datanodeService is stopped , it indicates this `close` originates
    // from `HddsDatanodeService.stop()`, otherwise, it indicates this `close` originates from ratis.
    if (allServer) {
      if (datanodeService != null && !datanodeService.isStopped()) {
        LOG.info("{} is closed by ratis", getGroupId());
        if (semaphore.tryAcquire()) {
          // run with a different thread, so this raft group can be closed
          Runnable runnable = () -> {
            try {
              int closed = 0, total = 0;
              try {
                Thread.sleep(5000); // sleep 5s
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              Iterator<RaftGroupId> iterator = ratisServer.getServer().getGroupIds().iterator();
              while (iterator.hasNext()) {
                RaftGroupId id = iterator.next();
                RaftServer.Division division = ratisServer.getServer().getDivision(id);
                if (division.getRaftServer().getLifeCycleState() == LifeCycle.State.CLOSED) {
                  closed++;
                }
                total++;
              }
              LOG.error("Container statemachine is closed by ratis, terminating HddsDatanodeService. " +
                  "closed({})/total({})", closed, total);
              datanodeService.terminateDatanode();
            } catch (IOException e) {
              LOG.warn("Failed to get division for raft groups", e);
              LOG.error("Container statemachine is closed by ratis, terminating HddsDatanodeService");
              datanodeService.terminateDatanode();
            }
          };
          CompletableFuture.runAsync(runnable);
        }
      } else {
        LOG.info("{} is closed by HddsDatanodeService", getGroupId());
      }
    }
  }

  private CompletableFuture<ContainerCommandResponseProto> applyTransaction(
      ContainerCommandRequestProto request, DispatcherContext context,
      Consumer<Throwable> exceptionHandler) {
    final long containerId = request.getContainerID();
    final CheckedSupplier<ContainerCommandResponseProto, Exception> task
        = () -> {
          try {
            try {
              this.validatePeers();
              this.checkContainerHealthy(containerId, false);
            } catch (StorageContainerException e) {
              return ContainerUtils.logAndReturnError(LOG, e, request);
            }
            long timeNow = Time.monotonicNowNanos();
            long queueingDelay = timeNow - context.getStartTime();
            metrics.recordQueueingDelay(request.getCmdType(), queueingDelay);
            // TODO: add a counter to track number of executing applyTransaction
            // and queue size
            return dispatchCommand(request, context);
          } catch (Exception e) {
            exceptionHandler.accept(e);
            throw e;
          }
        };
    return containerTaskQueues.submit(containerId, task, executor);
  }

  // Removes the stateMachine data from cache once both followers catch up
  // to the particular index.
  private void removeStateMachineDataIfNeeded(long index) {
    if (waitOnBothFollowers) {
      try {
        RaftServer.Division division = ratisServer.getServer().getDivision(getGroupId());
        if (division.getInfo().isLeader()) {
          Arrays.stream(division.getInfo()
              .getFollowerNextIndices()).min().ifPresent(minIndex -> {
                removeCacheDataUpTo(Math.min(minIndex, index));
                LOG.debug("Removing data corresponding to log index {} min index {} "
                    + "from cache", index, minIndex);
              });
        }
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /*
   * ApplyTransaction calls in Ratis are sequential.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    long index = trx.getLogEntry().getIndex();
    try {
      // Remove the stateMachine data once both followers have caught up. If any
      // one of the follower is behind, the pending queue will max out as
      // configurable limit on pending request size and count and then will
      // block and client will backoff as a result of that.
      removeStateMachineDataIfNeeded(index);
      // if waitOnBothFollower is false, remove the entry from the cache
      // as soon as its applied and such entry exists in the cache.
      removeStateMachineDataIfMajorityFollowSync(index);
      final DispatcherContext.Builder builder = DispatcherContext
          .newBuilder(DispatcherContext.Op.APPLY_TRANSACTION)
          .setTerm(trx.getLogEntry().getTerm())
          .setLogIndex(index);

      final Context context = (Context) trx.getStateMachineContext();
      long applyTxnStartTime = Time.monotonicNowNanos();
      metrics.recordUntilApplyTransactionNs(applyTxnStartTime - context.getStartTime());
      applyTransactionSemaphore.acquire();
      metrics.incNumApplyTransactionsOps();


      Objects.requireNonNull(context, "context == null");
      final ContainerCommandRequestProto requestProto = context.getLogProto();
      final Type cmdType = requestProto.getCmdType();
      // Make sure that in write chunk, the user data is not set
      if (cmdType == Type.WriteChunk) {
        Preconditions
            .checkArgument(requestProto.getWriteChunk().getData().isEmpty());
        builder.setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA);
      }
      if (cmdType == Type.WriteChunk || cmdType == Type.PutSmallFile
          || cmdType == Type.PutBlock || cmdType == Type.CreateContainer
          || cmdType == Type.StreamInit) {
        builder.setContainer2BCSIDMap(container2BCSIDMap);
      }
      CompletableFuture<Message> applyTransactionFuture =
          new CompletableFuture<>();
      final Consumer<Throwable> exceptionHandler = e -> {
        LOG.error(getGroupId() + ": failed to applyTransaction at logIndex " + index
            + " for " + requestProto.getCmdType(), e);
        stateMachineHealthy.compareAndSet(true, false);
        unhealthyContainers.add(requestProto.getContainerID());
        metrics.incNumApplyTransactionsFails();
        applyTransactionFuture.completeExceptionally(e);
      };

      // Ensure the command gets executed in a separate thread than
      // stateMachineUpdater thread which is calling applyTransaction here.
      final CompletableFuture<ContainerCommandResponseProto> future =
          applyTransaction(requestProto, builder.build(), exceptionHandler);
      future.thenApply(r -> {
        // TODO: add metrics for non-leader case
        if (trx.getServerRole() == RaftPeerRole.LEADER) {
          final long startTime = context.getStartTime();
          metrics.incPipelineLatencyMs(cmdType,
              (Time.monotonicNowNanos() - startTime) / 1000000L);
        }
        // ignore close container exception while marking the stateMachine
        // unhealthy
        if (r.getResult() != ContainerProtos.Result.SUCCESS
            && r.getResult() != ContainerProtos.Result.CONTAINER_NOT_OPEN
            && r.getResult() != ContainerProtos.Result.CLOSED_CONTAINER_IO
            && r.getResult() != ContainerProtos.Result.CHUNK_FILE_INCONSISTENCY) {
          StorageContainerException sce =
              new StorageContainerException(r.getMessage(), r.getResult());
          LOG.error(
              "gid {} : ApplyTransaction failed. cmd {} logIndex {} msg : "
                  + "{} Container Result: {}", getGroupId(), r.getCmdType(), index,
              r.getMessage(), r.getResult());
          metrics.incNumApplyTransactionsFails();
          // Since the applyTransaction now is completed exceptionally,
          // before any further snapshot is taken , the exception will be
          // caught in stateMachineUpdater in Ratis and ratis server will
          // shutdown.
          applyTransactionFuture.completeExceptionally(sce);
          stateMachineHealthy.compareAndSet(true, false);
          unhealthyContainers.add(requestProto.getContainerID());
          ratisServer.handleApplyTransactionFailure(getGroupId(), trx.getServerRole());
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "gid {} : ApplyTransaction completed. cmd {} logIndex {} msg : "
                    + "{} Container Result: {}", getGroupId(), r.getCmdType(), index,
                r.getMessage(), r.getResult());
          }
          if (cmdType == Type.WriteChunk || cmdType == Type.PutSmallFile) {
            metrics.incNumBytesCommittedCount(
                requestProto.getWriteChunk().getChunkData().getLen());
          }
          applyTransactionFuture.complete(r::toByteString);
          // add the entry to the applyTransactionCompletionMap only if the
          // stateMachine is healthy i.e, there has been no applyTransaction
          // failures before.
          if (isStateMachineHealthy()) {
            final Long previous = applyTransactionCompletionMap
                .put(index, trx.getLogEntry().getTerm());
            Preconditions.checkState(previous == null);
            updateLastApplied();
          }
        }
        return applyTransactionFuture;
      }).whenComplete((r, t) -> {
        if (t != null) {
          exceptionHandler.accept(t);
        }
        applyTransactionSemaphore.release();
        metrics.recordApplyTransactionCompletionNs(
            Time.monotonicNowNanos() - applyTxnStartTime);
        if (trx.getServerRole() == RaftPeerRole.LEADER) {
          metrics.decPendingApplyTransactions();
        }
      });
      return applyTransactionFuture;
    } catch (InterruptedException e) {
      metrics.incNumApplyTransactionsFails();
      Thread.currentThread().interrupt();
      return completeExceptionally(e);
    } catch (Exception e) {
      metrics.incNumApplyTransactionsFails();
      return completeExceptionally(e);
    }
  }

  private void removeStateMachineDataIfMajorityFollowSync(long index) {
    if (!waitOnBothFollowers) {
      // if majority follow in sync, remove all cache previous to current index
      // including current index
      removeCacheDataUpTo(index);
    }
  }

  private void removeCacheDataUpTo(long index) {
    stateMachineDataCache.removeIf(k -> k <= index);
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    // once the leader steps down , clear the cache
    evictStateMachineCache();
  }

  @Override
  public CompletableFuture<Void> truncate(long index) {
    stateMachineDataCache.removeIf(k -> k > index);
    return CompletableFuture.completedFuture(null);
  }

  @VisibleForTesting
  public void evictStateMachineCache() {
    stateMachineDataCache.clear();
  }

  @Override
  public void notifyFollowerSlowness(RoleInfoProto roleInfoProto, RaftPeer follower) {
    ratisServer.handleFollowerSlowness(getGroupId(), roleInfoProto, follower);
  }

  @Override
  public void notifyExtendedNoLeader(RoleInfoProto roleInfoProto) {
    ratisServer.handleNoLeader(getGroupId(), roleInfoProto);
  }

  @Override
  public void notifyLogFailed(Throwable t, LogEntryProto failedEntry) {
    String stateMachineLogEntry = failedEntry == null
        ? "null"
        : toStateMachineLogEntryString(failedEntry.getStateMachineLogEntry());
    LOG.error("{}: {} {}", getGroupId(), TermIndex.valueOf(failedEntry), stateMachineLogEntry, t);
    ratisServer.handleNodeLogFailure(getGroupId(), t);
  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
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
    for (Long cid : container2BCSIDMap.keySet()) {
      try {
        containerController.markContainerForClose(cid);
        containerController.quasiCloseContainer(cid,
            "Ratis group removed. Group id: " + getGroupId());
      } catch (IOException e) {
        LOG.debug("Failed to quasi-close container {}", cid);
      }
    }
  }

  @Override
  public void close() {
    evictStateMachineCache();
    executor.shutdown();
    metrics.unRegister();
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId,
                                  RaftPeerId raftPeerId) {
    ratisServer.handleLeaderChangedNotification(groupMemberId, raftPeerId);
  }

  @Override
  public String toStateMachineLogEntryString(StateMachineLogEntryProto proto) {
    return smProtoToString(getGroupId(), containerController, proto);
  }

  public static String smProtoToString(RaftGroupId gid,
                                   ContainerController containerController,
                                   StateMachineLogEntryProto proto) {
    StringBuilder builder = new StringBuilder();
    try {
      ContainerCommandRequestProto requestProto =
          getContainerCommandRequestProto(gid, proto.getLogData());
      long contId = requestProto.getContainerID();

      builder.append(TextFormat.shortDebugString(requestProto));

      if (containerController != null) {
        String location = containerController.getContainerLocation(contId);
        builder.append(", container path=");
        builder.append(location);
      }
    } catch (Exception t) {
      LOG.info("smProtoToString failed", t);
      builder.append("smProtoToString failed with ");
      builder.append(t.getMessage());
    }
    return builder.toString();
  }
}

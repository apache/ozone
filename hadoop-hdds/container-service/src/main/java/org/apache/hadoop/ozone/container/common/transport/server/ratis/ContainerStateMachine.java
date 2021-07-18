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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
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
import org.apache.hadoop.hdds.utils.ResourceLimitCache;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
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
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.apache.ratis.util.TaskQueue;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link org.apache.ratis.statemachine.StateMachine} for containers.
 *
 * The stateMachine is responsible for handling different types of container
 * requests. The container requests can be divided into readonly and write
 * requests.
 *
 * Read only requests are classified in
 * {@link org.apache.hadoop.hdds.HddsUtils#isReadOnly}
 * and these readonly requests are replied from the {@link #query(Message)}.
 *
 * The write requests can be divided into requests with user data
 * (WriteChunkRequest) and other request without user data.
 *
 * In order to optimize the write throughput, the writeChunk request is
 * processed in 2 phases. The 2 phases are divided in
 * {@link #startTransaction(RaftClientRequest)}, in the first phase the user
 * data is written directly into the state machine via
 * {@link #write} and in the second phase the
 * transaction is committed via {@link #applyTransaction(TransactionContext)}
 *
 * For the requests with no stateMachine data, the transaction is directly
 * committed through
 * {@link #applyTransaction(TransactionContext)}
 *
 * There are 2 ordering operation which are enforced right now in the code,
 * 1) Write chunk operation are executed after the create container operation,
 * the write chunk operation will fail otherwise as the container still hasn't
 * been created. Hence the create container operation has been split in the
 * {@link #startTransaction(RaftClientRequest)}, this will help in synchronizing
 * the calls in {@link #write}
 *
 * 2) Write chunk commit operation is executed after write chunk state machine
 * operation. This will ensure that commit operation is sync'd with the state
 * machine operation. For example, synchronization between writeChunk and
 * createContainer in {@link ContainerStateMachine}.
 **/

public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG =
      LoggerFactory.getLogger(ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();
  private final RaftGroupId gid;
  private final ContainerDispatcher dispatcher;
  private final ContainerController containerController;
  private final XceiverServerRatis ratisServer;
  private final ConcurrentHashMap<Long,
      CompletableFuture<ContainerCommandResponseProto>> writeChunkFutureMap;

  // keeps track of the containers created per pipeline
  private final Map<Long, Long> container2BCSIDMap;
  private final ConcurrentMap<Long, TaskQueue> containerTaskQueues;
  private final ExecutorService executor;
  private final List<ThreadPoolExecutor> chunkExecutors;
  private final Map<Long, Long> applyTransactionCompletionMap;
  private final Cache<Long, ByteString> stateMachineDataCache;
  private final AtomicBoolean stateMachineHealthy;

  private final Semaphore applyTransactionSemaphore;
  /**
   * CSM metrics.
   */
  private final CSMMetrics metrics;

  @SuppressWarnings("parameternumber")
  public ContainerStateMachine(RaftGroupId gid, ContainerDispatcher dispatcher,
      ContainerController containerController,
      List<ThreadPoolExecutor> chunkExecutors,
      XceiverServerRatis ratisServer, ConfigurationSource conf) {
    this.gid = gid;
    this.dispatcher = dispatcher;
    this.containerController = containerController;
    this.ratisServer = ratisServer;
    metrics = CSMMetrics.create(gid);
    this.writeChunkFutureMap = new ConcurrentHashMap<>();
    applyTransactionCompletionMap = new ConcurrentHashMap<>();
    int numPendingRequests = conf
        .getObject(DatanodeRatisServerConfig.class)
        .getLeaderNumPendingRequests();
    int pendingRequestsByteLimit = (int) conf.getStorageSize(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    stateMachineDataCache = new ResourceLimitCache<>(new ConcurrentHashMap<>(),
        (index, data) -> new int[] {1, data.size()}, numPendingRequests,
        pendingRequestsByteLimit);

    this.chunkExecutors = chunkExecutors;

    this.container2BCSIDMap = new ConcurrentHashMap<>();

    final int numContainerOpExecutors = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT);
    int maxPendingApplyTransactions = conf.getInt(
        ScmConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS,
        ScmConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS_DEFAULT);
    applyTransactionSemaphore = new Semaphore(maxPendingApplyTransactions);
    stateMachineHealthy = new AtomicBoolean(true);

    this.executor = Executors.newFixedThreadPool(numContainerOpExecutors);
    this.containerTaskQueues = new ConcurrentHashMap<>();
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
    ratisServer.notifyGroupAdd(gid);

    loadSnapshot(storage.getLatestSnapshot());
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot)
      throws IOException {
    if (snapshot == null) {
      TermIndex empty = TermIndex.valueOf(0, RaftLog.INVALID_LOG_INDEX);
      LOG.info("{}: The snapshot info is null. Setting the last applied index" +
              "to:{}", gid, empty);
      setLastAppliedTermIndex(empty);
      return empty.getIndex();
    }

    final File snapshotFile = snapshot.getFile().getPath().toFile();
    final TermIndex last =
        SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    LOG.info("{}: Setting the last applied index to {}", gid, last);
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
    try (FileInputStream fin = new FileInputStream(snapshotFile)) {
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

  @Override
  public long takeSnapshot() throws IOException {
    TermIndex ti = getLastAppliedTermIndex();
    long startTime = Time.monotonicNow();
    if (!isStateMachineHealthy()) {
      String msg =
          "Failed to take snapshot " + " for " + gid + " as the stateMachine"
              + " is unhealthy. The last applied index is at " + ti;
      StateMachineException sme = new StateMachineException(msg);
      LOG.error(msg);
      throw sme;
    }
    if (ti != null && ti.getIndex() != RaftLog.INVALID_LOG_INDEX) {
      final File snapshotFile =
          storage.getSnapshotFile(ti.getTerm(), ti.getIndex());
      LOG.info("{}: Taking a snapshot at:{} file {}", gid, ti, snapshotFile);
      try (FileOutputStream fos = new FileOutputStream(snapshotFile)) {
        persistContainerSet(fos);
        fos.flush();
        // make sure the snapshot file is synced
        fos.getFD().sync();
      } catch (IOException ioe) {
        LOG.error("{}: Failed to write snapshot at:{} file {}", gid, ti,
            snapshotFile);
        throw ioe;
      }
      LOG.info("{}: Finished taking a snapshot at:{} file:{} took: {} ms",
          gid, ti, snapshotFile, (Time.monotonicNow() - startTime));
      return ti.getIndex();
    }
    return -1;
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    long startTime = Time.monotonicNowNanos();
    final ContainerCommandRequestProto proto =
        message2ContainerCommandRequestProto(request.getMessage());
    Preconditions.checkArgument(request.getRaftGroupId().equals(gid));
    try {
      dispatcher.validateContainerCommand(proto);
    } catch (IOException ioe) {
      if (ioe instanceof ContainerNotOpenException) {
        metrics.incNumContainerNotOpenVerifyFailures();
      } else {
        metrics.incNumStartTransactionVerifyFailures();
        LOG.error("startTransaction validation failed on leader", ioe);
      }
      TransactionContext ctxt = TransactionContext.newBuilder()
          .setClientRequest(request)
          .setStateMachine(this)
          .setServerRole(RaftPeerRole.LEADER)
          .build();
      ctxt.setException(ioe);
      return ctxt;
    }
    if (proto.getCmdType() == Type.WriteChunk) {
      final WriteChunkRequestProto write = proto.getWriteChunk();
      // create the log entry proto
      final WriteChunkRequestProto commitWriteChunkProto =
          WriteChunkRequestProto.newBuilder()
              .setBlockID(write.getBlockID())
              .setChunkData(write.getChunkData())
              // skipping the data field as it is
              // already set in statemachine data proto
              .build();
      ContainerCommandRequestProto commitContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(commitWriteChunkProto)
              .setTraceID(proto.getTraceID())
              .build();

      return TransactionContext.newBuilder()
          .setClientRequest(request)
          .setStateMachine(this)
          .setServerRole(RaftPeerRole.LEADER)
          .setStateMachineContext(startTime)
          .setStateMachineData(write.getData())
          .setLogData(commitContainerCommandProto.toByteString())
          .build();
    } else {
      return TransactionContext.newBuilder()
          .setClientRequest(request)
          .setStateMachine(this)
          .setServerRole(RaftPeerRole.LEADER)
          .setStateMachineContext(startTime)
          .setLogData(proto.toByteString())
          .build();
    }

  }

  private ByteString getStateMachineData(StateMachineLogEntryProto entryProto) {
    return entryProto.getStateMachineEntry().getStateMachineData();
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
    return ContainerCommandRequestMessage.toProto(message.getContent(), gid);
  }

  private ContainerCommandResponseProto dispatchCommand(
      ContainerCommandRequestProto requestProto, DispatcherContext context) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: dispatch {} containerID={} pipelineID={} traceID={}", gid,
          requestProto.getCmdType(), requestProto.getContainerID(),
          requestProto.getPipelineID(), requestProto.getTraceID());
    }
    ContainerCommandResponseProto response =
        dispatcher.dispatch(requestProto, context);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: response {}", gid, response);
    }
    return response;
  }

  private ContainerCommandResponseProto runCommand(
      ContainerCommandRequestProto requestProto,
      DispatcherContext context) {
    return dispatchCommand(requestProto, context);
  }

  private CompletableFuture<Message> handleWriteChunk(
      ContainerCommandRequestProto requestProto, long entryIndex, long term,
      long startTime) {
    final WriteChunkRequestProto write = requestProto.getWriteChunk();
    try {
      RaftServer.Division division = ratisServer.getServerDivision();
      if (division.getInfo().isLeader()) {
        stateMachineDataCache.put(entryIndex, write.getData());
      }
    } catch (InterruptedException ioe) {
      Thread.currentThread().interrupt();
      return completeExceptionally(ioe);
    } catch (IOException ioe) {
      return completeExceptionally(ioe);
    }
    DispatcherContext context =
        new DispatcherContext.Builder()
            .setTerm(term)
            .setLogIndex(entryIndex)
            .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
            .setContainer2BCSIDMap(container2BCSIDMap)
            .build();
    CompletableFuture<Message> raftFuture = new CompletableFuture<>();
    // ensure the write chunk happens asynchronously in writeChunkExecutor pool
    // thread.
    CompletableFuture<ContainerCommandResponseProto> writeChunkFuture =
        CompletableFuture.supplyAsync(() -> {
          try {
            return runCommand(requestProto, context);
          } catch (Exception e) {
            LOG.error("{}: writeChunk writeStateMachineData failed: blockId" +
                "{} logIndex {} chunkName {}", gid, write.getBlockID(),
                entryIndex, write.getChunkData().getChunkName(), e);
            metrics.incNumWriteDataFails();
            // write chunks go in parallel. It's possible that one write chunk
            // see the stateMachine is marked unhealthy by other parallel thread
            stateMachineHealthy.set(false);
            raftFuture.completeExceptionally(e);
            throw e;
          }
        }, getChunkExecutor(requestProto.getWriteChunk()));

    writeChunkFutureMap.put(entryIndex, writeChunkFuture);
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: writeChunk writeStateMachineData : blockId" +
              "{} logIndex {} chunkName {}", gid, write.getBlockID(),
          entryIndex, write.getChunkData().getChunkName());
    }
    // Remove the future once it finishes execution from the
    // writeChunkFutureMap.
    writeChunkFuture.thenApply(r -> {
      if (r.getResult() != ContainerProtos.Result.SUCCESS
          && r.getResult() != ContainerProtos.Result.CONTAINER_NOT_OPEN
          && r.getResult() != ContainerProtos.Result.CLOSED_CONTAINER_IO) {
        StorageContainerException sce =
            new StorageContainerException(r.getMessage(), r.getResult());
        LOG.error(gid + ": writeChunk writeStateMachineData failed: blockId" +
            write.getBlockID() + " logIndex " + entryIndex + " chunkName " +
            write.getChunkData().getChunkName() + " Error message: " +
            r.getMessage() + " Container Result: " + r.getResult());
        metrics.incNumWriteDataFails();
        // If the write fails currently we mark the stateMachine as unhealthy.
        // This leads to pipeline close. Any change in that behavior requires
        // handling the entry for the write chunk in cache.
        stateMachineHealthy.set(false);
        raftFuture.completeExceptionally(sce);
      } else {
        metrics.incNumBytesWrittenCount(
            requestProto.getWriteChunk().getChunkData().getLen());
        if (LOG.isDebugEnabled()) {
          LOG.debug(gid +
              ": writeChunk writeStateMachineData  completed: blockId" +
              write.getBlockID() + " logIndex " + entryIndex + " chunkName " +
              write.getChunkData().getChunkName());
        }
        raftFuture.complete(r::toByteString);
        metrics.recordWriteStateMachineCompletion(
            Time.monotonicNowNanos() - startTime);
      }

      writeChunkFutureMap.remove(entryIndex);
      return r;
    });
    return raftFuture;
  }

  @Override
  public CompletableFuture<DataStream> stream(RaftClientRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        ContainerCommandRequestProto requestProto =
            getContainerCommandRequestProto(gid,
                request.getMessage().getContent());
        DispatcherContext context =
            new DispatcherContext.Builder()
                .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
                .setContainer2BCSIDMap(container2BCSIDMap)
                .build();

        ContainerCommandResponseProto response = runCommand(
            requestProto, context);
        String path = response.getMessage();
        return new LocalStream(new StreamDataChannel(Paths.get(path)));
      } catch (IOException e) {
        throw new CompletionException("Failed to create data stream", e);
      }
    }, executor);
  }

  private ExecutorService getChunkExecutor(WriteChunkRequestProto req) {
    int hash = Objects.hashCode(req.getBlockID());
    if (hash == Integer.MIN_VALUE) {
      hash = Integer.MAX_VALUE;
    }
    int i = Math.abs(hash) % chunkExecutors.size();
    return chunkExecutors.get(i);
  }

  /*
   * writeStateMachineData calls are not synchronized with each other
   * and also with applyTransaction.
   */
  @Override
  public CompletableFuture<Message> write(LogEntryProto entry) {
    try {
      metrics.incNumWriteStateMachineOps();
      long writeStateMachineStartTime = Time.monotonicNowNanos();
      ContainerCommandRequestProto requestProto =
          getContainerCommandRequestProto(gid,
              entry.getStateMachineLogEntry().getLogData());
      WriteChunkRequestProto writeChunk =
          WriteChunkRequestProto.newBuilder(requestProto.getWriteChunk())
              .setData(getStateMachineData(entry.getStateMachineLogEntry()))
              .build();
      requestProto = ContainerCommandRequestProto.newBuilder(requestProto)
          .setWriteChunk(writeChunk).build();
      Type cmdType = requestProto.getCmdType();

      // For only writeChunk, there will be writeStateMachineData call.
      // CreateContainer will happen as a part of writeChunk only.
      switch (cmdType) {
      case WriteChunk:
        return handleWriteChunk(requestProto, entry.getIndex(),
            entry.getTerm(), writeStateMachineStartTime);
      default:
        throw new IllegalStateException("Cmd Type:" + cmdType
            + " should not have state machine data");
      }
    } catch (IOException e) {
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
      return CompletableFuture
          .completedFuture(runCommand(requestProto, null)::toByteString);
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
    DispatcherContext context =
        new DispatcherContext.Builder().setTerm(term).setLogIndex(index)
            .setReadFromTmpFile(true).build();
    // read the chunk
    ContainerCommandResponseProto response =
        dispatchCommand(dataContainerCommandProto, context);
    if (response.getResult() != ContainerProtos.Result.SUCCESS) {
      StorageContainerException sce =
          new StorageContainerException(response.getMessage(),
              response.getResult());
      LOG.error("gid {} : ReadStateMachine failed. cmd {} logIndex {} msg : "
              + "{} Container Result: {}", gid, response.getCmdType(), index,
          response.getMessage(), response.getResult());
      stateMachineHealthy.set(false);
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
    Preconditions
        .checkNotNull(data, "read chunk data is null for chunk: %s",
            chunkInfo);
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
    List<CompletableFuture<ContainerCommandResponseProto>> futureList =
        writeChunkFutureMap.entrySet().stream().filter(x -> x.getKey() <= index)
            .map(Map.Entry::getValue).collect(Collectors.toList());
    return CompletableFuture.allOf(
        futureList.toArray(new CompletableFuture[futureList.size()]));
  }
  /*
   * This api is used by the leader while appending logs to the follower
   * This allows the leader to read the state machine data from the
   * state machine implementation in case cached state machine data has been
   * evicted.
   */
  @Override
  public CompletableFuture<ByteString> read(
      LogEntryProto entry) {
    StateMachineLogEntryProto smLogEntryProto = entry.getStateMachineLogEntry();
    metrics.incNumReadStateMachineOps();
    if (!getStateMachineData(smLogEntryProto).isEmpty()) {
      return CompletableFuture.completedFuture(ByteString.EMPTY);
    }
    try {
      final ContainerCommandRequestProto requestProto =
          getContainerCommandRequestProto(gid,
              entry.getStateMachineLogEntry().getLogData());
      // readStateMachineData should only be called for "write" to Ratis.
      Preconditions.checkArgument(!HddsUtils.isReadOnly(requestProto));
      if (requestProto.getCmdType() == Type.WriteChunk) {
        final CompletableFuture<ByteString> future = new CompletableFuture<>();
        ByteString data = stateMachineDataCache.get(entry.getIndex());
        if (data != null) {
          future.complete(data);
          return future;
        }

        CompletableFuture.supplyAsync(() -> {
          try {
            future.complete(
                readStateMachineData(requestProto, entry.getTerm(),
                    entry.getIndex()));
          } catch (IOException e) {
            metrics.incNumReadStateMachineFails();
            future.completeExceptionally(e);
          }
          return future;
        }, getChunkExecutor(requestProto.getWriteChunk()));
        return future;
      } else {
        throw new IllegalStateException("Cmd type:" + requestProto.getCmdType()
            + " cannot have state machine data");
      }
    } catch (Exception e) {
      metrics.incNumReadStateMachineFails();
      LOG.error("{} unable to read stateMachineData:", gid, e);
      return completeExceptionally(e);
    }
  }

  private synchronized void updateLastApplied() {
    Long appliedTerm = null;
    long appliedIndex = -1;
    for(long i = getLastAppliedTermIndex().getIndex() + 1;; i++) {
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
  }

  private CompletableFuture<ContainerCommandResponseProto> submitTask(
      ContainerCommandRequestProto request, DispatcherContext.Builder context,
      Consumer<Exception> exceptionHandler) {
    final long containerId = request.getContainerID();
    final TaskQueue queue = containerTaskQueues.computeIfAbsent(
        containerId, id -> new TaskQueue("container" + id));
    final CheckedSupplier<ContainerCommandResponseProto, Exception> task
        = () -> {
          try {
            return runCommand(request, context.build());
          } catch (Exception e) {
            exceptionHandler.accept(e);
            throw e;
          }
        };
    return queue.submit(task, executor);
  }

  /*
   * ApplyTransaction calls in Ratis are sequential.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    long index = trx.getLogEntry().getIndex();
    // Since leader and one of the followers has written the data, it can
    // be removed from the stateMachineDataMap.
    stateMachineDataCache.remove(index);

    DispatcherContext.Builder builder =
        new DispatcherContext.Builder()
            .setTerm(trx.getLogEntry().getTerm())
            .setLogIndex(index);

    long applyTxnStartTime = Time.monotonicNowNanos();
    try {
      applyTransactionSemaphore.acquire();
      metrics.incNumApplyTransactionsOps();
      ContainerCommandRequestProto requestProto =
          getContainerCommandRequestProto(gid,
              trx.getStateMachineLogEntry().getLogData());
      Type cmdType = requestProto.getCmdType();
      // Make sure that in write chunk, the user data is not set
      if (cmdType == Type.WriteChunk) {
        Preconditions
            .checkArgument(requestProto.getWriteChunk().getData().isEmpty());
        builder
            .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA);
      }
      if (cmdType == Type.WriteChunk || cmdType == Type.PutSmallFile
          || cmdType == Type.PutBlock || cmdType == Type.CreateContainer) {
        builder.setContainer2BCSIDMap(container2BCSIDMap);
      }
      CompletableFuture<Message> applyTransactionFuture =
          new CompletableFuture<>();
      final Consumer<Exception> exceptionHandler = e -> {
        LOG.error("gid {} : ApplyTransaction failed. cmd {} logIndex "
                + "{} exception {}", gid, requestProto.getCmdType(),
            index, e);
        stateMachineHealthy.compareAndSet(true, false);
        metrics.incNumApplyTransactionsFails();
        applyTransactionFuture.completeExceptionally(e);
      };

      // Ensure the command gets executed in a separate thread than
      // stateMachineUpdater thread which is calling applyTransaction here.
      final CompletableFuture<ContainerCommandResponseProto> future
          = submitTask(requestProto, builder, exceptionHandler);
      future.thenApply(r -> {
        if (trx.getServerRole() == RaftPeerRole.LEADER
            && trx.getStateMachineContext() != null) {
          long startTime = (long) trx.getStateMachineContext();
          metrics.incPipelineLatency(cmdType,
              (Time.monotonicNowNanos() - startTime) / 1000000L);
        }
        // ignore close container exception while marking the stateMachine
        // unhealthy
        if (r.getResult() != ContainerProtos.Result.SUCCESS
            && r.getResult() != ContainerProtos.Result.CONTAINER_NOT_OPEN
            && r.getResult() != ContainerProtos.Result.CLOSED_CONTAINER_IO) {
          StorageContainerException sce =
              new StorageContainerException(r.getMessage(), r.getResult());
          LOG.error(
              "gid {} : ApplyTransaction failed. cmd {} logIndex {} msg : "
                  + "{} Container Result: {}", gid, r.getCmdType(), index,
              r.getMessage(), r.getResult());
          metrics.incNumApplyTransactionsFails();
          // Since the applyTransaction now is completed exceptionally,
          // before any further snapshot is taken , the exception will be
          // caught in stateMachineUpdater in Ratis and ratis server will
          // shutdown.
          applyTransactionFuture.completeExceptionally(sce);
          stateMachineHealthy.compareAndSet(true, false);
          ratisServer.handleApplyTransactionFailure(gid, trx.getServerRole());
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "gid {} : ApplyTransaction completed. cmd {} logIndex {} msg : "
                    + "{} Container Result: {}", gid, r.getCmdType(), index,
                r.getMessage(), r.getResult());
          }
          applyTransactionFuture.complete(r::toByteString);
          if (cmdType == Type.WriteChunk || cmdType == Type.PutSmallFile) {
            metrics.incNumBytesCommittedCount(
                requestProto.getWriteChunk().getChunkData().getLen());
          }
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
      }).whenComplete((r, t) ->  {
        if (t != null) {
          stateMachineHealthy.set(false);
          LOG.error("gid {} : ApplyTransaction failed. cmd {} logIndex "
                  + "{} exception {}", gid, requestProto.getCmdType(),
              index, t);
        }
        applyTransactionSemaphore.release();
        metrics.recordApplyTransactionCompletion(
            Time.monotonicNowNanos() - applyTxnStartTime);
      });
      return applyTransactionFuture;
    } catch (InterruptedException e) {
      metrics.incNumApplyTransactionsFails();
      Thread.currentThread().interrupt();
      return completeExceptionally(e);
    } catch (IOException e) {
      metrics.incNumApplyTransactionsFails();
      return completeExceptionally(e);
    }
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @Override
  public CompletableFuture<Void> truncate(long index) {
    stateMachineDataCache.removeIf(k -> k >= index);
    return CompletableFuture.completedFuture(null);
  }

  @VisibleForTesting
  public void evictStateMachineCache() {
    stateMachineDataCache.clear();
  }

  @Override
  public void notifyFollowerSlowness(RoleInfoProto roleInfoProto) {
    ratisServer.handleNodeSlowness(gid, roleInfoProto);
  }

  @Override
  public void notifyExtendedNoLeader(RoleInfoProto roleInfoProto) {
    ratisServer.handleNoLeader(gid, roleInfoProto);
  }

  @Override
  public void notifyLogFailed(Throwable t, LogEntryProto failedEntry) {
    ratisServer.handleNodeLogFailure(gid, t);
  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    ratisServer.handleInstallSnapshotFromLeader(gid, roleInfoProto,
        firstTermIndexInLog);
    final CompletableFuture<TermIndex> future = new CompletableFuture<>();
    future.complete(firstTermIndexInLog);
    return future;
  }

  @Override
  public void notifyGroupRemove() {
    ratisServer.notifyGroupRemove(gid);
    // Make best effort to quasi-close all the containers on group removal.
    // Containers already in terminal state like CLOSED or UNHEALTHY will not
    // be affected.
    for (Long cid : container2BCSIDMap.keySet()) {
      try {
        containerController.markContainerForClose(cid);
        containerController.quasiCloseContainer(cid);
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
    return smProtoToString(gid, containerController, proto);
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
      builder.append("smProtoToString failed with");
      builder.append(t.getMessage());
    }
    return builder.toString();
  }
}

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

package org.apache.hadoop.hdds.scm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsUtils.processForDebug;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.DATA_TRANSFER_MAGIC_CODE;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.DATA_TRANSFER_VERSION;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link XceiverClientSpi} implementation, the client to read local replica through short circuit.
 */
public class XceiverClientShortCircuit extends XceiverClientSpi {
  private static final String GET_BLOCK_SPAN_NAME = "XceiverClientShortCircuit." + ContainerProtos.Type.GetBlock;
  private static final String ECHO_SPAN_NAME = "XceiverClientShortCircuit." + ContainerProtos.Type.Echo;

  public static final Logger LOG = LoggerFactory.getLogger(XceiverClientShortCircuit.class);
  // the fields below are just for log or error messages
  private final AtomicReference<String> name = new AtomicReference<>();
  private final AtomicLong requestSent = new AtomicLong();
  private final AtomicLong responseReceived = new AtomicLong();

  private final Pipeline pipeline;
  private final XceiverClientMetrics metrics;
  private final int readTimeoutMs;
  private final int writeTimeoutMs;
  // Cache the stream of blocks
  private final Map<BlockStreamKey, FileInputStream> blockStreamCache;
  private final Map<RequestKey, RequestEntry> sentRequests;
  private final Daemon readDaemon;
  private final TimeoutScheduler scheduler = new TimeoutScheduler();

  private boolean closed = false;
  private final DatanodeDetails dn;
  private final InetSocketAddress dnAddr;
  private final DomainSocketFactory domainSocketFactory;
  private DomainSocket domainSocket;
  private final AtomicBoolean isDomainSocketOpen = new AtomicBoolean(false);
  private final Lock lock = new ReentrantLock();
  private final int bufferSize;
  private final ByteString clientId = ByteString.copyFrom(UUID.randomUUID().toString().getBytes(UTF_8));
  private final AtomicLong callId = new AtomicLong(0);

  /**
   * Constructs a client that can communicate with the Container framework on local datanode through DomainSocket.
   */
  public XceiverClientShortCircuit(Pipeline pipeline, ConfigurationSource config, DatanodeDetails dn) {
    super();
    Objects.requireNonNull(config);
    this.readTimeoutMs = (int) config.getTimeDuration(OzoneConfigKeys.OZONE_CLIENT_READ_TIMEOUT,
        OzoneConfigKeys.OZONE_CLIENT_READ_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    this.writeTimeoutMs = (int) config.getTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WRITE_TIMEOUT,
        OzoneConfigKeys.OZONE_CLIENT_WRITE_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);

    this.pipeline = pipeline;
    this.dn = dn;
    this.domainSocketFactory = DomainSocketFactory.getInstance(config);
    this.metrics = XceiverClientManager.getXceiverClientMetrics();
    this.blockStreamCache = new ConcurrentHashMap<>();
    this.sentRequests = new ConcurrentHashMap<>();
    int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    this.dnAddr = NetUtils.createSocketAddr(dn.getIpAddress(), port);
    this.bufferSize = config.getObject(OzoneClientConfig.class).getShortCircuitBufferSize();
    this.readDaemon = new Daemon(new ReceiveResponseTask());

    updateName("Created-DomainSocket");
    LOG.info("Created: {}", this);
  }

  /**
   * Create the DomainSocket to connect to the local DataNode.
   */
  @Override
  public void connect() throws IOException {
    // Even the in & out stream has returned EOFException, domainSocket.isOpen() is still true.
    if (domainSocket != null && domainSocket.isOpen() && isDomainSocketOpen.get()) {
      return;
    }
    domainSocket = domainSocketFactory.createSocket(readTimeoutMs, writeTimeoutMs, dnAddr);
    updateName("Connected-" + domainSocket);
    isDomainSocketOpen.set(true);
    final String prefix = XceiverClientShortCircuit.class.getSimpleName() + "-" + domainSocket;
    scheduler.init(prefix);
    readDaemon.setName(prefix + "-ReceiveResponse");
    readDaemon.start();
    LOG.info("Connected successfully: {}", this);
  }

  /**
   * Close the DomainSocket.
   */
  @Override
  public synchronized void close() {
    closed = true;
    scheduler.close();
    if (domainSocket != null) {
      try {
        isDomainSocketOpen.set(false);
        domainSocket.close();
        LOG.info("Closed successfully (sent {}, received {}): {}", requestSent, responseReceived, this);
        updateName("Closed-" + domainSocket);
      } catch (IOException e) {
        LOG.warn("Failed to close: {}", this, e);
      }
    }
    readDaemon.interrupt();
    try {
      readDaemon.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  public DatanodeDetails getDn() {
    return this.dn;
  }

  public ByteString getClientId() {
    return clientId;
  }

  public long getCallId() {
    return callId.incrementAndGet();
  }

  @Override
  public ContainerCommandResponseProto sendCommand(ContainerCommandRequestProto request) throws IOException {
    return sendCommandWithTraceID(request);
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
      ContainerCommandRequestProto request) {
    throw new UnsupportedOperationException("Operation Not supported for " +
        DomainSocketFactory.FEATURE + " client");
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request, List<Validator> validators)
      throws IOException {
    final ContainerCommandResponseProto response = sendCommandWithTraceID(request);
    if (validators != null && !validators.isEmpty()) {
      for (Validator validator : validators) {
        validator.accept(request, response);
      }
    }
    return response;
  }

  private String getSpanName(ContainerProtos.Type cmdType) {
    if (cmdType == ContainerProtos.Type.GetBlock) {
      return GET_BLOCK_SPAN_NAME;
    } else if (cmdType == ContainerProtos.Type.Echo) {
      return ECHO_SPAN_NAME;
    }
    throw new UnsupportedOperationException("Command " + cmdType +
        " is not supported for " + DomainSocketFactory.FEATURE + " client");
  }

  private ContainerCommandResponseProto sendCommandWithTraceID(ContainerCommandRequestProto request)
      throws IOException {
    return TracingUtil.executeInNewSpan(getSpanName(request.getCmdType()),
        () -> sendCommandWithoutTraceID(request));
  }

  private ContainerCommandResponseProto sendCommandWithoutTraceID(ContainerCommandRequestProto request)
      throws IOException {
    try {
      ContainerCommandRequestProto finalPayload =
          ContainerCommandRequestProto.newBuilder(request)
              .setTraceID(TracingUtil.exportCurrentSpan()).build();
      final CompletableFuture<ContainerCommandResponseProto> response;

      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Executing {} on {}", processForDebug(request), dn);
        }
        response = sendCommandInternal(finalPayload);
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed: {} {}.", processForDebug(request), this, e);
        }
        throw e;
      }

      final ContainerCommandResponseProto proto = response.get();
      if (LOG.isDebugEnabled()) {
        LOG.debug("request {} {} {} finished", request.getCmdType(),
            request.getClientId().toStringUtf8(), request.getCallId());
      }
      return proto;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (Status.fromThrowable(cause).getCode() == Status.UNAUTHENTICATED.getCode()) {
        throw new SCMSecurityException("Unauthenticated: " + processForDebug(request) + " " + this, cause);
      }
      throw getIOExceptionForSendCommand(request, e);
    } catch (InterruptedException e) {
      final String s = "Interrupted: " + processForDebug(request) + " " + this;
      LOG.warn(s);
      Thread.currentThread().interrupt();
      throw (InterruptedIOException) new InterruptedIOException(s).initCause(e);
    }
  }

  private CompletableFuture<ContainerCommandResponseProto> sendCommandInternal(
      ContainerCommandRequestProto request) throws IOException {
    checkOpen();
    final CompletableFuture<ContainerCommandResponseProto> replyFuture = new CompletableFuture<>();
    RequestEntry entry = new RequestEntry(request, replyFuture);
    sendRequest(entry);
    return replyFuture;
  }

  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    throw new UnsupportedOperationException("Operation Not supported for " + DomainSocketFactory.FEATURE + " client");
  }

  public synchronized void checkOpen() throws IOException {
    if (closed) {
      throw new IOException("Closed: " + this);
    }

    if (!isDomainSocketOpen.get()) {
      throw new IOException("Not connected: " + this);
    }
  }

  @Override
  public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
    // there is no notion of watch for commit index in short-circuit local reads
    return null;
  }

  @Override
  public long getReplicatedMinCommitIndex() {
    return 0;
  }

  public FileInputStream getFileInputStream(long id, long blockLocalId) {
    return blockStreamCache.remove(new BlockStreamKey(id, blockLocalId));
  }

  @Override
  public HddsProtos.ReplicationType getPipelineType() {
    return HddsProtos.ReplicationType.STAND_ALONE;
  }

  void requestTimeout(RequestKey requestKey) {
    final RequestEntry entry = sentRequests.remove(requestKey);
    if (entry != null) {
      LOG.warn("Timeout: {}", processForDebug(entry.getRequest()));
      ContainerProtos.Type type = entry.getRequest().getCmdType();
      metrics.decrPendingContainerOpsMetrics(type);
      entry.getFuture().completeExceptionally(new TimeoutException("Timeout to receive response"));
    }
  }

  void sendRequest(RequestEntry entry) {
    ContainerCommandRequestProto request = entry.getRequest();
    try {
      final RequestKey key = new RequestKey(request.getClientId(), request.getCallId());
      TimerTask task = new TimerTask() {
        @Override
        public void run() {
          requestTimeout(key);
        }
      };
      entry.setTimerTask(task);
      scheduler.schedule(task, readTimeoutMs);
      sentRequests.put(key, entry);
      ContainerProtos.Type type = request.getCmdType();
      metrics.incrPendingContainerOpsMetrics(type);
      byte[] bytes = request.toByteArray();
      if (bytes.length != request.getSerializedSize()) {
        throw new IOException("Serialized request " + request.getCmdType()
            + " size mismatch, byte array size " + bytes.length +
            ", serialized size " + request.getSerializedSize());
      }

      lock.lock();
      try {
        DataOutputStream dataOut =
            new DataOutputStream(new BufferedOutputStream(domainSocket.getOutputStream(), bufferSize));
        // send version number
        dataOut.writeShort(DATA_TRANSFER_VERSION);
        // send command type
        dataOut.writeShort(type.getNumber());
        // send request body
        request.writeDelimitedTo(dataOut);
        dataOut.flush();
      } finally {
        lock.unlock();
        requestSent.incrementAndGet();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Sent command {} {}:{} on datanode {}, sent {}ns",
            type, entry.getRequest().getClientId().toStringUtf8(), entry.getRequest().getCallId(), dn,
            System.nanoTime() - entry.getCreateTimeNs());
      }
    } catch (IOException e) {
      LOG.error("Failed to send {}", processForDebug(request), e);
      entry.getFuture().completeExceptionally(e);
      metrics.decrPendingContainerOpsMetrics(request.getCmdType());
      metrics.addContainerOpsLatency(request.getCmdType(), System.nanoTime() - entry.getCreateTimeNs());
    }
  }

  private void updateName(String domainSocketString) {
    name.set(getClass().getSimpleName() +  "[ " + domainSocketString + ", " + dn + "]");
  }

  @Override
  public String toString() {
    return name.get();
  }

  /**
   * Task to receive responses from server.
   */
  public class ReceiveResponseTask implements Runnable {
    @Override
    public void run() {
      long timerTaskCancelledCount = 0;
      do {
        RequestEntry entry = null;
        ContainerProtos.Type type = null;
        long endToEndCost = 0;
        try {
          DataInputStream dataIn = new DataInputStream(domainSocket.getInputStream());
          final short version = dataIn.readShort();
          if (version != DATA_TRANSFER_VERSION) {
            throw new IOException("Version Mismatch (Expected: " +
                DATA_TRANSFER_VERSION + ", Received: " + version + ")");
          }
          long receiveStartTime = System.nanoTime();
          final short typeNumber = dataIn.readShort();
          type = ContainerProtos.Type.forNumber(typeNumber);
          ContainerCommandResponseProto responseProto =
              ContainerCommandResponseProto.parseFrom(vintPrefixed(dataIn));
          if (LOG.isDebugEnabled()) {
            LOG.debug("received response {} callId {}", type, responseProto.getCallId());
          }
          entry = sentRequests.remove(new RequestKey(responseProto.getClientId(), responseProto.getCallId()));
          if (entry == null) {
            // This could be two cases
            // 1. there is bug in the code
            // 2. the response is too late, the request is removed from sentRequests after it is timeout.
            throw new IOException("Failed to find request for response, type " + type +
                ", clientId " + responseProto.getClientId().toStringUtf8() + ", callId " + responseProto.getCallId());
          }

          // cancel timeout timer task
          if (entry.cancelTimerTask()) {
            timerTaskCancelledCount++;
            // purge timer every 1000 cancels
            if (timerTaskCancelledCount == 1000) {
              scheduler.purge();
              timerTaskCancelledCount = 0;
            }
          }

          long processStartTime = System.nanoTime();
          ContainerProtos.Result result = responseProto.getResult();
          if (result == ContainerProtos.Result.SUCCESS) {
            if (type == ContainerProtos.Type.GetBlock) {
              try {
                ContainerProtos.GetBlockResponseProto getBlockResponse = responseProto.getGetBlock();
                if (!getBlockResponse.getShortCircuitAccessGranted()) {
                  throw new IOException("Short-circuit access is denied on " + dn);
                }
                // read FS from domainSocket
                FileInputStream[] fis = new FileInputStream[1];
                byte[] buf = new byte[1];
                int ret = domainSocket.recvFileInputStreams(fis, buf, 0, buf.length);
                if (ret == -1) {
                  throw new IOException("failed to get a file descriptor from datanode " + dn +
                      " for peer is shutdown.");
                }
                if (fis[0] == null) {
                  throw new IOException("the datanode " + dn + " failed to " +
                      "pass a file descriptor (might have reached open file limit).");
                }
                if (buf[0] != DATA_TRANSFER_MAGIC_CODE) {
                  throw new IOException("Magic Code Mismatch (Expected: " +
                      DATA_TRANSFER_MAGIC_CODE + ", Received: " + buf[0] + ")");
                }
                DatanodeBlockID blockID = getBlockResponse.getBlockData().getBlockID();
                blockStreamCache.put(new BlockStreamKey(responseProto.getCallId(), blockID.getLocalID()), fis[0]);
              } catch (IOException e) {
                LOG.warn("Failed to handle short-circuit information exchange", e);
                // disable docket socket for a while
                domainSocketFactory.disableShortCircuit();
                entry.getFuture().completeExceptionally(e);
                continue;
              }
            }
            entry.getFuture().complete(responseProto);
          } else {
            // response result is not SUCCESS
            entry.getFuture().complete(responseProto);
          }
          long currentTime = System.nanoTime();
          endToEndCost = currentTime - entry.getCreateTimeNs();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Executed command {} {}:{} on datanode {}, end-to-end {}ns, receive {}ns, process {}ns",
                type, entry.getRequest().getClientId().toStringUtf8(), entry.getRequest().getCallId(), dn,
                endToEndCost,
                processStartTime - receiveStartTime,
                currentTime - processStartTime);
          }
          responseReceived.incrementAndGet();
        } catch (SocketTimeoutException | EOFException | ClosedChannelException e) {
          isDomainSocketOpen.set(false);
          LOG.info("ReceiveResponseTask closed:  (sent {}, received {}): {}",
              requestSent, responseReceived, XceiverClientShortCircuit.this, e);
          // fail all requests pending responses
          sentRequests.values().forEach(i -> i.fail(e));
        } catch (Throwable e) {
          isDomainSocketOpen.set(false);
          LOG.error("ReceiveResponseTask failed:  (sent {}, received {}): {}",
              requestSent, responseReceived, XceiverClientShortCircuit.this, e);
          if (entry != null) {
            entry.getFuture().completeExceptionally(e);
          }
          sentRequests.values().forEach(i -> i.fail(e));
          break;
        } finally {
          if (type != null) {
            metrics.decrPendingContainerOpsMetrics(type);
            if (endToEndCost > 0) {
              metrics.addContainerOpsLatency(type, endToEndCost);
            }
          }
        }
      } while (isDomainSocketOpen.get());
    }
  }

  public static InputStream vintPrefixed(final DataInputStream input) throws IOException {
    final int firstByte = input.read();
    int size = CodedInputStream.readRawVarint32(firstByte, input);
    assert size >= 0;
    return new LimitInputStream(input, size);
  }

  static class RequestKey {
    private final ByteString clientId;
    private final long callId;

    RequestKey(ByteString clientId, long callId) {
      this.clientId = clientId;
      this.callId = callId;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(callId);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof RequestKey)) {
        return false;
      }
      final RequestKey that = (RequestKey) obj;
      return this.callId == that.callId
          && Objects.equals(this.clientId, that.clientId);
    }
  }

  /**
   * Class wraps a container command request.
   */
  static class RequestEntry {
    private final ContainerCommandRequestProto request;
    private final CompletableFuture<ContainerCommandResponseProto> future;
    private final long createTimeNs;
    private final AtomicReference<TimerTask> timerTask = new AtomicReference<>();

    RequestEntry(ContainerCommandRequestProto requestProto,
                 CompletableFuture<ContainerCommandResponseProto> future) {
      this.request = requestProto;
      this.future = future;
      this.createTimeNs = System.nanoTime();
    }

    public ContainerCommandRequestProto getRequest() {
      return request;
    }

    public CompletableFuture<ContainerCommandResponseProto> getFuture() {
      return future;
    }

    public long getCreateTimeNs() {
      return createTimeNs;
    }

    public void setTimerTask(TimerTask task) {
      final boolean set = timerTask.compareAndSet(null, task);
      Preconditions.assertTrue(set);
    }

    boolean cancelTimerTask() {
      final TimerTask t = timerTask.getAndSet(null);
      if (t == null) {
        return false;
      }
      return t.cancel();
    }

    public void fail(Throwable e) {
      cancelTimerTask();
      future.completeExceptionally(e);
    }
  }

  static final class BlockStreamKey {
    private final long callId;
    private final long blockLocalId;

    BlockStreamKey(long callId, long blockLocalId) {
      this.callId = callId;
      this.blockLocalId = blockLocalId;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(callId);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof BlockStreamKey)) {
        return false;
      }
      final BlockStreamKey that = (BlockStreamKey) obj;
      return this.callId == that.callId
          && this.blockLocalId == that.blockLocalId;
    }
  }

  static final class TimeoutScheduler {
    private Timer timer;

    synchronized void init(String prefix) {
      if (timer != null) {
        timer.cancel();
      }
      timer = new Timer(prefix + "-Timer");
    }

    synchronized void schedule(TimerTask task, int timeoutMs) {
      Objects.requireNonNull(timer, "Timer is null");
      timer.schedule(task, timeoutMs);
    }

    synchronized void purge() {
      if (timer == null) {
        return;
      }
      timer.purge();
    }

    synchronized void close() {
      if (timer == null) {
        return;
      }
      timer.cancel();
      timer = null;
    }
  }
}

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

  private InterruptedIOException newInterruptedIOException(
      ContainerCommandRequestProto request, InterruptedException e) {
    final String s = "Interrupted: " + processForDebug(request) + " " + this;
    LOG.warn(s);
    Thread.currentThread().interrupt();
    return (InterruptedIOException) new InterruptedIOException(s).initCause(e);
  }

  private IOException newIOException(ContainerCommandRequestProto request, ExecutionException e) {
    if (Status.fromThrowable(e.getCause()).getCode() == Status.UNAUTHENTICATED.getCode()) {
      return new SCMSecurityException("Unauthenticated: " + processForDebug(request) + " " + this, e.getCause());
    }
    return getIOExceptionForSendCommand(request, e);
  }

  @Override
  public ContainerCommandResponseProto sendCommand(ContainerCommandRequestProto request) throws IOException {
    try {
      return sendCommandWithTraceID(request).getResponse().get();
    } catch (ExecutionException e) {
      throw newIOException(request, e);
    } catch (InterruptedException e) {
      throw newInterruptedIOException(request, e);
    }
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
    try {
      final ContainerCommandResponseProto response = sendCommandWithTraceID(request).getResponse().get();
      if (validators != null && !validators.isEmpty()) {
        for (Validator validator : validators) {
          validator.accept(request, response);
        }
      }
      return response;
    } catch (ExecutionException e) {
      throw newIOException(request, e);
    } catch (InterruptedException e) {
      throw newInterruptedIOException(request, e);
    }
  }

  private XceiverClientReply sendCommandWithTraceID(ContainerCommandRequestProto request) throws IOException {
    String spanName = "XceiverClientShortCircuit." + request.getCmdType().name();
    return TracingUtil.executeInNewSpan(spanName,
        () -> {
          ContainerCommandRequestProto finalPayload =
              ContainerCommandRequestProto.newBuilder(request)
                  .setTraceID(TracingUtil.exportCurrentSpan()).build();
          final CompletableFuture<ContainerCommandResponseProto> response;

          if (request.getCmdType() != ContainerProtos.Type.GetBlock &&
              request.getCmdType() != ContainerProtos.Type.Echo) {
            throw new UnsupportedOperationException("Command " + request.getCmdType() +
                " is not supported for " + DomainSocketFactory.FEATURE + " client");
          }

          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Executing {} on {}", processForDebug(request), dn);
            }
            response = sendCommandInternal(finalPayload);
            if (LOG.isDebugEnabled()) {
              LOG.debug("request {} {} {} finished", request.getCmdType(),
                  request.getClientId().toStringUtf8(), request.getCallId());
            }
          } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed: {} {}.", processForDebug(request), this, e);
            }
            throw e;
          }

          final XceiverClientReply reply = new XceiverClientReply(response);
          reply.addDatanode(dn);
          return reply;
        });
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
      scheduler.schedule(key, entry, readTimeoutMs);
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
        LOG.debug("Sent command {} {}:{} on datanode {}, sent {}ms",
            type, entry.getRequest().getClientId().toStringUtf8(), entry.getRequest().getCallId(), dn,
            nsToMs(System.nanoTime() - entry.getCreateTimeNs()));
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
      do {
        RequestEntry entry = null;
        try {
          DataInputStream dataIn = new DataInputStream(domainSocket.getInputStream());
          final short version = dataIn.readShort();
          if (version != DATA_TRANSFER_VERSION) {
            throw new IOException("Version Mismatch (Expected: " +
                DATA_TRANSFER_VERSION + ", Received: " + version + ")");
          }
          long receiveStartTime = System.nanoTime();
          final short typeNumber = dataIn.readShort();
          ContainerProtos.Type type = ContainerProtos.Type.forNumber(typeNumber);
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
          long endToEndCost = currentTime - entry.getCreateTimeNs();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Executed command {} {}:{} on datanode {}, end-to-end {}ms, receive {}ms, process {}ms",
                type, entry.getRequest().getClientId().toStringUtf8(), entry.getRequest().getCallId(), dn,
                nsToMs(endToEndCost),
                nsToMs(processStartTime - receiveStartTime),
                nsToMs(currentTime - processStartTime));
          }
          responseReceived.incrementAndGet();
          metrics.decrPendingContainerOpsMetrics(type);
          metrics.addContainerOpsLatency(type, endToEndCost);
        } catch (SocketTimeoutException | EOFException | ClosedChannelException e) {
          isDomainSocketOpen.set(false);
          LOG.debug("ReceiveResponseTask closed: {}", this, e);
          // fail all requests pending responses
          sentRequests.values().forEach(i -> i.fail(e));
        } catch (Throwable e) {
          isDomainSocketOpen.set(false);
          LOG.error("ReceiveResponseTask failed: {}", this, e);
          if (entry != null) {
            entry.getFuture().completeExceptionally(e);
          }
          sentRequests.values().forEach(i -> i.fail(e));
          break;
        }
      } while (isDomainSocketOpen.get());
    }
  }

  static long nsToMs(long ns) {
    return ns / 1000_000;
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

    RequestEntry(ContainerCommandRequestProto requestProto,
                 CompletableFuture<ContainerCommandResponseProto> future) {
      this.request = requestProto;
      this.future = future;
      this.createTimeNs = System.nanoTime();
    }

    ContainerCommandRequestProto getRequest() {
      return request;
    }

    CompletableFuture<ContainerCommandResponseProto> getFuture() {
      return future;
    }

    long getCreateTimeNs() {
      return createTimeNs;
    }

    public void fail(Throwable e) {
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

  class TimeoutScheduler {
    private Timer timer;
    private int cancelCount = 0;

    synchronized void init(String prefix) {
      Preconditions.assertNull(timer, "timer");
      timer = new Timer(prefix + "-Timer");
    }

    synchronized void schedule(RequestKey key, RequestEntry entry, int timeoutMs) {
      if (timer == null) {
        return;
      }
      final TimerTask task = new TimerTask() {
        @Override
        public void run() {
          requestTimeout(key);
        }
      };
      timer.schedule(task, timeoutMs);
      entry.getFuture().whenComplete((r, e) -> cancel(task));
    }

    synchronized void cancel(TimerTask task) {
      if (task.cancel()) {
        if (timer != null && ++cancelCount == 1000) {
          timer.purge();
          cancelCount = 0;
        }
      }
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

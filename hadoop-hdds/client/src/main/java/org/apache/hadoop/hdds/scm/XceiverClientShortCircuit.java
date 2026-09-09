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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link XceiverClientSpi} implementation, the client to read local replica through short circuit.
 */
public class XceiverClientShortCircuit extends XceiverClientSpi {
  public static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientShortCircuit.class);
  private final Pipeline pipeline;
  private final ConfigurationSource config;
  private final XceiverClientMetrics metrics;
  private final int readTimeoutMs;
  private final int writeTimeoutMs;
  // Cache the stream of blocks
  private final Map<BlockStreamKey, FileInputStream> blockStreamCache;
  private final Map<RequestKey, RequestEntry> sentRequests;
  private final Daemon readDaemon;
  private Timer timer;

  private boolean closed = false;
  private final DatanodeDetails dn;
  private final InetSocketAddress dnAddr;
  private final DomainSocketFactory domainSocketFactory;
  private DomainSocket domainSocket;
  private final AtomicBoolean isDomainSocketOpen = new AtomicBoolean(false);
  // Protects connection state, counters, and RequestEntry.sentTimeNs.
  private final Lock lock = new ReentrantLock();
  private final int bufferSize;
  private final ByteString clientId = ByteString.copyFrom(UUID.randomUUID().toString().getBytes(UTF_8));
  private final AtomicLong callId = new AtomicLong(0);
  private long requestSent = 0;
  private long responseReceived = 0;
  private String prefix;

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
    this.config = config;
    this.metrics = XceiverClientManager.getXceiverClientMetrics();
    this.blockStreamCache = new ConcurrentHashMap<>();
    this.sentRequests = new ConcurrentHashMap<>();
    int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    this.dnAddr = NetUtils.createSocketAddr(dn.getIpAddress(), port);
    this.bufferSize = config.getObject(OzoneClientConfig.class).getShortCircuitBufferSize();
    this.readDaemon = new Daemon(new ReceiveResponseTask());
    LOG.info("{} is created for pipeline {}", XceiverClientShortCircuit.class.getSimpleName(), pipeline);
  }

  /**
   * Create the DomainSocket to connect to the local DataNode.
   */
  @Override
  public void connect() throws IOException {
    lock.lock();
    try {
      if (closed) {
        throw new IOException("DomainSocket is closed.");
      }
      if (domainSocket != null) {
        checkOpen();
        return;
      }
      boolean connected = false;
      try {
        domainSocket = domainSocketFactory.createSocket(readTimeoutMs, writeTimeoutMs, dnAddr);
        if (domainSocket == null) {
          throw new IOException("DomainSocket is not available for " + dn);
        }
        prefix = XceiverClientShortCircuit.class.getSimpleName() + "-" + domainSocket;
        timer = new Timer(prefix + "-Timer");
        isDomainSocketOpen.set(true);
        readDaemon.start();
        connected = true;
        LOG.info("{} is started", prefix);
      } finally {
        if (!connected) {
          closed = true;
          isDomainSocketOpen.set(false);
          if (timer != null) {
            timer.cancel();
          }
          if (domainSocket != null) {
            try {
              domainSocket.close();
            } catch (IOException e) {
              LOG.warn("Failed to close domain socket for datanode {}", dn, e);
            }
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Close the DomainSocket.
   */
  @Override
  public void close() {
    final List<RequestEntry> pending;
    lock.lock();
    try {
      if (!closed) {
        closed = true;
        isDomainSocketOpen.set(false);
        if (timer != null) {
          timer.cancel();
        }
        if (domainSocket != null) {
          try {
            domainSocket.close();
            LOG.info("{} is closed for {} with {} requests sent and {} responses received",
                domainSocket, dn, requestSent, responseReceived);
          } catch (IOException e) {
            LOG.warn("Failed to close domain socket for datanode {}", dn, e);
          }
        }
        readDaemon.interrupt();
      }
      pending = new ArrayList<>(sentRequests.values());
    } finally {
      lock.unlock();
    }
    pending.forEach(entry -> entry.fail(new ClosedChannelException()));
    if (Thread.currentThread() != readDaemon) {
      try {
        readDaemon.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public boolean isClosed() {
    lock.lock();
    try {
      return closed;
    } finally {
      lock.unlock();
    }
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
    try {
      return sendCommandWithTraceID(request, null).getResponse().get();
    } catch (ExecutionException e) {
      throw getIOExceptionForSendCommand(request, e);
    } catch (InterruptedException e) {
      LOG.error("Command execution was interrupted.");
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException(
          "Command " + processForDebug(request) + " was interrupted.")
          .initCause(e);
    }
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto>
      sendCommandOnAllNodes(
      ContainerCommandRequestProto request) throws IOException {
    throw new UnsupportedOperationException("Operation Not supported for " +
        DomainSocketFactory.FEATURE + " client");
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request, List<Validator> validators)
      throws IOException {
    try {
      XceiverClientReply reply;
      reply = sendCommandWithTraceID(request, validators);
      return reply.getResponse().get();
    } catch (ExecutionException e) {
      throw getIOExceptionForSendCommand(request, e);
    } catch (InterruptedException e) {
      LOG.error("Command execution was interrupted.");
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException(
          "Command " + processForDebug(request) + " was interrupted.")
          .initCause(e);
    }
  }

  private XceiverClientReply sendCommandWithTraceID(
      ContainerCommandRequestProto request, List<Validator> validators)
      throws IOException {
    String spanName = "XceiverClientShortCircuit." + request.getCmdType().name();
    return TracingUtil.executeInNewSpan(spanName,
        () -> {
          ContainerCommandRequestProto finalPayload =
              ContainerCommandRequestProto.newBuilder(request)
                  .setTraceID(TracingUtil.exportCurrentSpan()).build();
          ContainerCommandResponseProto responseProto = null;
          IOException ioException = null;
          XceiverClientReply reply = new XceiverClientReply(null);

          if (request.getCmdType() != ContainerProtos.Type.GetBlock &&
              request.getCmdType() != ContainerProtos.Type.Echo) {
            throw new UnsupportedOperationException("Command " + request.getCmdType() +
                " is not supported for " + DomainSocketFactory.FEATURE + " client");
          }

          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Executing command {} on datanode {}", request, dn);
            }
            reply.addDatanode(dn);
            responseProto = sendCommandInternal(finalPayload).getResponse().get();
            if (validators != null && !validators.isEmpty()) {
              for (Validator validator : validators) {
                validator.accept(request, responseProto);
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("request {} {} {} finished", request.getCmdType(),
                  request.getClientId().toStringUtf8(), request.getCallId());
            }
          } catch (IOException e) {
            ioException = e;
            responseProto = null;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to execute command {} on datanode {}", request, dn, e);
            }
          } catch (ExecutionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to execute command {} on datanode {}", request, dn, e);
            }
            if (Status.fromThrowable(e.getCause()).getCode()
                == Status.UNAUTHENTICATED.getCode()) {
              throw new SCMSecurityException("Failed to authenticate with "
                  + "datanode DomainSocket XceiverServer with Ozone block token.");
            }
            ioException = new IOException(e);
          } catch (InterruptedException e) {
            LOG.error("Command execution was interrupted ", e);
            Thread.currentThread().interrupt();
          }

          if (responseProto != null) {
            reply.setResponse(CompletableFuture.completedFuture(responseProto));
            return reply;
          } else {
            Objects.requireNonNull(ioException);
            String message = "Failed to execute command {}";
            if (LOG.isDebugEnabled()) {
              lock.lock();
              try {
                LOG.debug(message + " on the datanode {} {}.", request, dn, domainSocket, ioException);
              } finally {
                lock.unlock();
              }
            }
            throw ioException;
          }
        });
  }

  @VisibleForTesting
  public XceiverClientReply sendCommandInternal(ContainerCommandRequestProto request)
      throws IOException, InterruptedException {
    final CompletableFuture<ContainerCommandResponseProto> replyFuture =
        new CompletableFuture<>();
    final RequestKey key = new RequestKey(request.getClientId(), request.getCallId());
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        requestTimeout(key);
      }
    };
    RequestEntry entry = new RequestEntry(request, replyFuture, task);
    sendRequest(entry);
    return new XceiverClientReply(replyFuture);
  }

  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    throw new UnsupportedOperationException("Operation Not supported for " + DomainSocketFactory.FEATURE + " client");
  }

  public void checkOpen() throws IOException {
    lock.lock();
    try {
      if (closed || domainSocket == null) {
        throw new IOException("DomainSocket is not connected.");
      }
      // isOpen() may remain true after EOF, so also check the receiver's state.
      if (!domainSocket.isOpen() || !isDomainSocketOpen.get()) {
        throw new IOException(domainSocket + " is not open.");
      }
    } finally {
      lock.unlock();
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

  public ConfigurationSource getConfig() {
    return config;
  }

  @VisibleForTesting
  public static Logger getLogger() {
    return LOG;
  }

  void requestTimeout(RequestKey requestKey) {
    final RequestEntry entry;
    lock.lock();
    try {
      entry = sentRequests.remove(requestKey);
    } finally {
      lock.unlock();
    }
    if (entry != null) {
      LOG.warn("Timeout to receive response for command {}", entry.getRequest());
      ContainerProtos.Type type = entry.getRequest().getCmdType();
      metrics.decrPendingContainerOpsMetrics(type);
      entry.getFuture().completeExceptionally(new TimeoutException("Timeout to receive response"));
    }
  }

  void sendRequest(RequestEntry entry) throws IOException {
    ContainerCommandRequestProto request = entry.getRequest();
    IOException failure = null;
    List<RequestEntry> pending = null;
    lock.lock();
    try {
      checkOpen();
      final RequestKey key = new RequestKey(request.getClientId(), request.getCallId());
      sentRequests.put(key, entry);
      ContainerProtos.Type type = request.getCmdType();
      metrics.incrPendingContainerOpsMetrics(type);
      timer.schedule(entry.getTimerTask(), readTimeoutMs);
      try {
        byte[] bytes = request.toByteArray();
        if (bytes.length != request.getSerializedSize()) {
          throw new IOException("Serialized request " + request.getCmdType()
              + " size mismatch, byte array size " + bytes.length +
              ", serialized size " + request.getSerializedSize());
        }
        DataOutputStream dataOut =
            new DataOutputStream(new BufferedOutputStream(domainSocket.getOutputStream(), bufferSize));
        // send version number
        dataOut.writeShort(DATA_TRANSFER_VERSION);
        // send command type
        dataOut.writeShort(type.getNumber());
        // send request body
        request.writeDelimitedTo(dataOut);
        dataOut.flush();
      } catch (IOException e) {
        isDomainSocketOpen.set(false);
        failure = e;
        pending = new ArrayList<>(sentRequests.values());
      } finally {
        entry.setSentTimeNs();
        requestSent++;
      }
    } finally {
      lock.unlock();
    }
    if (failure != null) {
      LOG.error("Failed to send command {}", request, failure);
      for (RequestEntry requestEntry : pending) {
        requestEntry.fail(failure);
      }
      metrics.decrPendingContainerOpsMetrics(request.getCmdType());
      metrics.addContainerOpsLatency(request.getCmdType(), System.nanoTime() - entry.getCreateTimeNs());
    }
  }

  @Override
  public String toString() {
    lock.lock();
    try {
      final StringBuilder b =
          new StringBuilder(getClass().getSimpleName())
              .append('[').append(" DomainSocket: ").append(domainSocket)
              .append(" Pipeline: ").append(pipeline.toString())
              .append(" ]");
      return b.toString();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Task to receive responses from server.
   */
  public class ReceiveResponseTask implements Runnable {
    @Override
    public void run() {
      final DomainSocket socket;
      final Timer responseTimer;
      lock.lock();
      try {
        socket = domainSocket;
        responseTimer = timer;
        Thread.currentThread().setName(prefix + "-ReceiveResponse");
      } finally {
        lock.unlock();
      }
      long timerTaskCancelledCount = 0;
      while (true) {
        lock.lock();
        try {
          if (!isDomainSocketOpen.get()) {
            return;
          }
        } finally {
          lock.unlock();
        }
        RequestEntry entry = null;
        try {
          DataInputStream dataIn = new DataInputStream(socket.getInputStream());
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
          final long sentTimeNs;
          lock.lock();
          try {
            entry = sentRequests.remove(new RequestKey(responseProto.getClientId(), responseProto.getCallId()));
            sentTimeNs = entry == null ? 0 : entry.getSentTimeNs();
          } finally {
            lock.unlock();
          }
          if (entry == null) {
            // This could be two cases
            // 1. there is bug in the code
            // 2. the response is too late, the request is removed from sentRequests after it is timeout.
            throw new IOException("Failed to find request for response, type " + type +
                ", clientId " + responseProto.getClientId().toStringUtf8() + ", callId " + responseProto.getCallId());
          }

          // cancel timeout timer task
          if (entry.getTimerTask().cancel()) {
            timerTaskCancelledCount++;
            // purge timer every 1000 cancels
            if (timerTaskCancelledCount == 1000) {
              responseTimer.purge();
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
                int ret = socket.recvFileInputStreams(fis, buf, 0, buf.length);
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
          long sentCost = sentTimeNs - entry.getCreateTimeNs();
          long receiveCost = processStartTime - receiveStartTime;
          long processCost = currentTime - processStartTime;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Executed command {} {}:{} on datanode {}, end-to-end {} ns, sent {} ns, receive {} ns, " +
                    "process {} ns", type, entry.getRequest().getClientId().toStringUtf8(),
                entry.getRequest().getCallId(), dn, endToEndCost, sentCost, receiveCost, processCost);
          }
          lock.lock();
          try {
            responseReceived++;
          } finally {
            lock.unlock();
          }
          metrics.decrPendingContainerOpsMetrics(type);
          metrics.addContainerOpsLatency(type, endToEndCost);
        } catch (Throwable e) {
          final List<RequestEntry> pending;
          lock.lock();
          try {
            isDomainSocketOpen.set(false);
            if (e instanceof SocketTimeoutException || e instanceof EOFException
                || e instanceof ClosedChannelException) {
              LOG.info("{} receiveResponseTask is closed after send {} requests and received {} responses, due to {}",
                  socket, requestSent, responseReceived, e.getClass().getName(), e);
            } else {
              LOG.error("{} failed after send {} requests and received {} responses",
                  socket, requestSent, responseReceived, e);
            }
            pending = new ArrayList<>(sentRequests.values());
          } finally {
            lock.unlock();
          }
          if (entry != null) {
            entry.getFuture().completeExceptionally(e);
          }
          pending.forEach(i -> i.fail(e));
          break;
        }
      }
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
    // Accessed under the enclosing client's lock.
    private long sentTimeNs;
    private final TimerTask timerTask;

    RequestEntry(ContainerCommandRequestProto requestProto,
                 CompletableFuture<ContainerCommandResponseProto> future, TimerTask timerTask) {
      this.request = requestProto;
      this.future = future;
      this.timerTask = timerTask;
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

    public long getSentTimeNs() {
      return sentTimeNs;
    }

    public void setSentTimeNs() {
      sentTimeNs = System.nanoTime();
    }

    public TimerTask getTimerTask() {
      return timerTask;
    }

    public void fail(Throwable e) {
      timerTask.cancel();
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
}

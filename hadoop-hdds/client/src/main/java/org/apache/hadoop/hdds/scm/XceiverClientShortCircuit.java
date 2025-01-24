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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsUtils.processForDebug;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.DATA_TRANSFER_MAGIC_CODE;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.DATA_TRANSFER_VERSION;

/**
 * {@link XceiverClientSpi} implementation, the client to read local replica through short circuit.
 */
public class XceiverClientShortCircuit extends XceiverClientSpi {
  public static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientShortCircuit.class);
  private final Pipeline pipeline;
  private final ConfigurationSource config;
  private final XceiverClientMetrics metrics;
  private int readTimeoutMs;
  private int writeTimeoutMs;
  // Cache the stream of blocks
  private final Map<String, FileInputStream> blockStreamCache;
  private final Map<String, RequestEntry> sentRequests;
  private final Daemon readDaemon;
  private Timer timer;

  private boolean closed = false;
  private final DatanodeDetails dn;
  private final InetSocketAddress dnAddr;
  private final DomainSocketFactory domainSocketFactory;
  private DomainSocket domainSocket;
  private AtomicBoolean isDomainSocketOpen = new AtomicBoolean(false);
  private Lock lock = new ReentrantLock();
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
    Preconditions.checkNotNull(config);
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
    // Even the in & out stream has returned EOFException, domainSocket.isOpen() is still true.
    if (domainSocket != null && domainSocket.isOpen() && isDomainSocketOpen.get()) {
      return;
    }
    domainSocket = domainSocketFactory.createSocket(readTimeoutMs, writeTimeoutMs, dnAddr);
    isDomainSocketOpen.set(true);
    prefix = XceiverClientShortCircuit.class.getSimpleName() + "-" + domainSocket.toString();
    timer = new Timer(prefix + "-Timer");
    readDaemon.start();
    LOG.info("{} is started", prefix);
  }

  /**
   * Close the DomainSocket.
   */
  @Override
  public synchronized void close() {
    closed = true;
    timer.cancel();
    if (domainSocket != null) {
      try {
        isDomainSocketOpen.set(false);
        domainSocket.close();
        LOG.info("{} is closed for {} with {} requests sent and {} responses received",
            domainSocket.toString(), dn, requestSent, responseReceived);
      } catch (IOException e) {
        LOG.warn("Failed to close domain socket for datanode {}", dn, e);
      }
    }
    readDaemon.interrupt();
    try {
      readDaemon.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

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
              LOG.debug(message + " on the datanode {} {}.", request, dn, domainSocket, ioException);
            } else {
              LOG.error(message + " on the datanode {} {}.", request, dn, domainSocket, ioException);
            }
            throw ioException;
          }
        });
  }

  @VisibleForTesting
  public XceiverClientReply sendCommandInternal(ContainerCommandRequestProto request)
      throws IOException, InterruptedException {
    checkOpen();
    final CompletableFuture<ContainerCommandResponseProto> replyFuture =
        new CompletableFuture<>();
    RequestEntry entry = new RequestEntry(request, replyFuture);
    sendRequest(entry);
    return new XceiverClientReply(replyFuture);
  }

  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    throw new UnsupportedOperationException("Operation Not supported for " + DomainSocketFactory.FEATURE + " client");
  }

  public synchronized void checkOpen() throws IOException {
    if (closed) {
      throw new IOException("DomainSocket is not connected.");
    }

    if (!isDomainSocketOpen.get()) {
      throw new IOException(domainSocket.toString() + " is not open.");
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

  public FileInputStream getFileInputStream(long id, DatanodeBlockID blockID) {
    return blockStreamCache.remove(getFileInputStreamMapKey(id, blockID));
  }

  private String getFileInputStreamMapKey(long id, DatanodeBlockID blockID) {
    return id + "-" + blockID.getLocalID();
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

  public void setReadTimeout(int timeout) {
    this.readTimeoutMs = timeout;
  }

  public int getReadTimeout() {
    return this.readTimeoutMs;
  }

  String getRequestUniqueID(ContainerCommandRequestProto request) {
    return request.getClientId().toStringUtf8() + request.getCallId();
  }

  String getRequestUniqueID(ContainerCommandResponseProto response) {
    return response.getClientId().toStringUtf8() + response.getCallId();
  }

  void requestTimeout(String requestId) {
    final RequestEntry entry = sentRequests.remove(requestId);
    if (entry != null) {
      LOG.warn("Timeout to receive response for command {}", entry.getRequest());
      ContainerProtos.Type type = entry.getRequest().getCmdType();
      metrics.decrPendingContainerOpsMetrics(type);
      entry.getFuture().completeExceptionally(new TimeoutException("Timeout to receive response"));
    }
  }

  public void sendRequest(RequestEntry entry) {
    ContainerCommandRequestProto request = entry.getRequest();
    try {
      String key = getRequestUniqueID(request);
      TimerTask task = new TimerTask() {
        @Override
        public void run() {
          requestTimeout(key);
        }
      };
      entry.setTimerTask(task);
      timer.schedule(task, readTimeoutMs);
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
        entry.setSentTimeNs();
        requestSent++;
      }
    } catch (IOException e) {
      LOG.error("Failed to send command {}", request, e);
      entry.getFuture().completeExceptionally(e);
      metrics.decrPendingContainerOpsMetrics(request.getCmdType());
      metrics.addContainerOpsLatency(request.getCmdType(), System.nanoTime() - entry.getCreateTimeNs());
    }
  }

  @Override
  public String toString() {
    final StringBuilder b =
        new StringBuilder(getClass().getSimpleName()).append("[");
    b.append(" DomainSocket: ").append(domainSocket.toString());
    b.append(" Pipeline: ").append(pipeline.toString());
    return b.toString();
  }

  /**
   * Task to receive responses from server.
   */
  public class ReceiveResponseTask implements Runnable {
    @Override
    public void run() {
      long timerTaskCancelledCount = 0;
      do {
        Thread.currentThread().setName(prefix + "-ReceiveResponse");
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
          String key = getRequestUniqueID(responseProto);
          entry = sentRequests.remove(key);
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
              timer.purge();
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
                blockStreamCache.put(getFileInputStreamMapKey(responseProto.getCallId(), blockID), fis[0]);
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
          long sentCost = entry.getSentTimeNs() - entry.getCreateTimeNs();
          long receiveCost = processStartTime - receiveStartTime;
          long processCost = currentTime - processStartTime;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Executed command {} {}:{} on datanode {}, end-to-end {} ns, sent {} ns, receive {} ns, " +
                    "process {} ns", type, entry.getRequest().getClientId().toStringUtf8(),
                entry.getRequest().getCallId(), dn, endToEndCost, sentCost, receiveCost, processCost);
          }
          responseReceived++;
          metrics.decrPendingContainerOpsMetrics(type);
          metrics.addContainerOpsLatency(type, endToEndCost);
        } catch (SocketTimeoutException | EOFException | ClosedChannelException e) {
          isDomainSocketOpen.set(false);
          LOG.info("{} receiveResponseTask is closed after send {} requests and received {} responses, due to {}",
              domainSocket.toString(), requestSent, responseReceived, e.getClass().getName(), e);
          // fail all requests pending responses
          sentRequests.values().forEach(i -> i.fail(e));
        } catch (Throwable e) {
          isDomainSocketOpen.set(false);
          LOG.error("{} failed after send {} requests and received {} responses",
              domainSocket.toString(), requestSent, responseReceived, e);
          if (entry != null) {
            entry.getFuture().completeExceptionally(e);
          }
          sentRequests.values().forEach(i -> i.fail(e));
          break;
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

  /**
   * Class wraps a container command request.
   */
  public static class RequestEntry {
    private ContainerCommandRequestProto request;
    private CompletableFuture<ContainerCommandResponseProto> future;
    private long createTimeNs;
    private long sentTimeNs;
    private TimerTask timerTask;

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

    public long getSentTimeNs() {
      return sentTimeNs;
    }

    public void setSentTimeNs() {
      sentTimeNs = System.nanoTime();
    }

    public void setTimerTask(TimerTask task) {
      timerTask = task;
    }

    public TimerTask getTimerTask() {
      return timerTask;
    }

    public void fail(Throwable e) {
      timerTask.cancel();
      future.completeExceptionally(e);
    }
  }
}

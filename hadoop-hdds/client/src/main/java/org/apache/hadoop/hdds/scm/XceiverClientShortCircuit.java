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
import org.apache.hadoop.hdds.client.ReplicationType;
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.HddsUtils.processForDebug;
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
  private final BlockingDeque<RequestEntry> pendingRequests;
  private final Daemon writeDaemon;
  private final Daemon readDaemon;
  private final Timer timer;

  private boolean closed = false;
  private final DatanodeDetails dn;
  private final InetSocketAddress dnAddr;
  private final DomainSocketFactory domainSocketFactory;
  private DomainSocket domainSocket;
  private DataOutputStream dataOut;
  private DataInputStream dataIn;
  private final int bufferSize;
  private final ByteString clientId = ByteString.copyFrom(UUID.randomUUID().toString().getBytes());
  private final AtomicLong callId = new AtomicLong(0);
  private final String prefix;

  /**
   * Constructs a client that can communicate with the Container framework on local datanode through DomainSocket
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
    this.pendingRequests = new LinkedBlockingDeque<>();
    int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    this.dnAddr = NetUtils.createSocketAddr(dn.getIpAddress(), port);
    this.bufferSize = config.getObject(OzoneClientConfig.class).getShortCircuitBufferSize();
    this.prefix = "Pipeline-" + pipeline.getId().getId() + "-" + XceiverClientShortCircuit.class.getSimpleName();
    this.timer = new Timer(prefix + "-Timer");
    this.writeDaemon = new Daemon(new SendRequestTask());
    this.readDaemon = new Daemon(new ReceiveResponseTask());
    LOG.info("{} is created", prefix);
  }

  /**
   * Create the DomainSocket to connect to the local DataNode.
   */
  @Override
  public void connect() throws IOException {
    if (domainSocket != null && domainSocket.isOpen()) {
      return;
    }
    domainSocket = domainSocketFactory.createSocket(readTimeoutMs, writeTimeoutMs, dnAddr);
    dataOut = new DataOutputStream(new BufferedOutputStream(domainSocket.getOutputStream(), bufferSize));
    dataIn = new DataInputStream(new BufferedInputStream(domainSocket.getInputStream(),bufferSize));
    writeDaemon.start();
    readDaemon.start();
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
        dataOut.close();
        dataIn.close();
        domainSocket.close();
        dataOut = null;
        dataIn = null;
        LOG.info("{} is closed for {}", domainSocket.toString(), dn);
      } catch (IOException e) {
        LOG.warn("Failed to close domain socket for datanode {}", dn, e);
      }
    }
    writeDaemon.interrupt();
    readDaemon.interrupt();
    try {
      writeDaemon.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
      return sendCommandWithTraceID(request, null).
          getResponse().get();
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
    String spanName = "XceiverClientGrpc." + request.getCmdType().name();
    return TracingUtil.executeInNewSpan(spanName,
        () -> {
          ContainerCommandRequestProto finalPayload =
              ContainerCommandRequestProto.newBuilder(request)
                  .setTraceID(TracingUtil.exportCurrentSpan()).build();
          ContainerCommandResponseProto responseProto = null;
          IOException ioException = null;

          // In case of an exception or an error, we will try to read from the
          // datanodes in the pipeline in a round-robin fashion.
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
              LOG.debug(message + " on the pipeline {}.", request, pipeline);
            } else {
              LOG.error(message + " on the pipeline {}.", request.getCmdType(), pipeline);
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
    // pendingRequests.add(entry);
    // CompletableFuture.runAsync(() -> sendRequest(entry), poolExecutor);
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
      throw new IOException("This DomainSocket is not connected.");
    }

    // try to connect again
    if (!domainSocket.isOpen()) {
      connect();
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

  public FileInputStream getFileInputStream(long callId, DatanodeBlockID blockID) {
    String mapKey = callId + blockID.toString();
    return blockStreamCache.remove(mapKey);
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
      LOG.warn("Timeout to receive response for command {}", entry.getRequest());;
      ContainerProtos.Type type = entry.getRequest().getCmdType();
      metrics.decrPendingContainerOpsMetrics(type);
      entry.getFuture().completeExceptionally(new TimeoutException("Timeout to receive response"));
    }
  }

  public class SendRequestTask implements Runnable {
    @Override
    public void run() {
      do {
        Thread.currentThread().setName(prefix + "-SendRequest");
        try {
          RequestEntry entry = pendingRequests.take();
          ContainerCommandRequestProto request = entry.getRequest();
          long requestTime = System.nanoTime();
          try {
            String key = getRequestUniqueID(request);
            entry.setSentTimeNs(System.nanoTime());
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
            // send version number
            dataOut.writeShort(DATA_TRANSFER_VERSION);
            // send command type
            dataOut.writeShort(type.getNumber());
            // send request body size
            dataOut.writeInt(bytes.length);
            // send request body
            dataOut.write(bytes);
            dataOut.flush();
            // LOG.info("send {} {} bytes with id {}", type, bytes.length, key);
          } catch (IOException e) {
            LOG.error("Failed to send command {}", request, e);
            entry.getFuture().completeExceptionally(e);
            metrics.decrPendingContainerOpsMetrics(request.getCmdType());
            metrics.addContainerOpsLatency(request.getCmdType(), System.nanoTime() - requestTime);
          }
        } catch (InterruptedException ex) {
          LOG.info("sendRequestTask is interrupted");
          Thread.currentThread().interrupt();
        }
      } while (!isClosed());
    }
  }

  public void sendRequest(RequestEntry entry) {
    ContainerCommandRequestProto request = entry.getRequest();
    long requestTime = System.nanoTime();
    try {
      String key = getRequestUniqueID(request);
      entry.setSentTimeNs(System.nanoTime());
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
      synchronized (dataOut) {
        // send version number
        dataOut.writeShort(DATA_TRANSFER_VERSION);
        // send command type
        dataOut.writeShort(type.getNumber());
        // send request body size
        dataOut.writeInt(bytes.length);
        // send request body
        dataOut.write(bytes);
        dataOut.flush();
      }
      // LOG.info("send {} {} bytes with id {}", type, bytes.length, key);
    } catch (IOException e) {
      LOG.error("Failed to send command {}", request, e);
      entry.getFuture().completeExceptionally(e);
      metrics.decrPendingContainerOpsMetrics(request.getCmdType());
      metrics.addContainerOpsLatency(request.getCmdType(), System.nanoTime() - requestTime);
    }
  }

  public class ReceiveResponseTask implements Runnable {
    @Override
    public void run() {
      long timerTaskCancelledCount = 0;
      do {
        Thread.currentThread().setName(prefix + "-ReceiveResponse");
        RequestEntry entry = null;
        try {
          short version = dataIn.readShort();
          if (version != DATA_TRANSFER_VERSION) {
            throw new IOException("Version Mismatch (Expected: " +
                DATA_TRANSFER_VERSION + ", Received: " + version + " )");
          }
          final short typeNumber = dataIn.readShort();
          ContainerProtos.Type type = ContainerProtos.Type.forNumber(typeNumber);
          final int size = dataIn.readInt();
          byte[] responseBytes = new byte[size];
          int totalReadSize = dataIn.read(responseBytes);
          while (totalReadSize < size) {
            int readSize = dataIn.read(responseBytes, totalReadSize, size - totalReadSize);
            totalReadSize += readSize;
          }
          ContainerCommandResponseProto responseProto = ContainerCommandResponseProto.parseFrom(responseBytes);
          String key = getRequestUniqueID(responseProto);
          entry = sentRequests.remove(key);
          if (entry == null) {
            // This could be two cases
            // 1. there is bug in the code
            // 2. the response is too late, the request is removed from sentRequests after it is timeout.
            throw new IOException("Failed to find request for response, type " + type +
                ", clientId " + responseProto.getClientId().toStringUtf8() + ", callId " + responseProto.getCallId());
          }
          // LOG.info("received type {} bytes {} id {}", type, size, key);
          // cancel timeout timer task
          if (entry.getTimerTask().cancel()) {
            timerTaskCancelledCount++;
            // purge timer every 1000 cancels
            if (timerTaskCancelledCount == 1000) {
              timer.purge();
              timerTaskCancelledCount = 0;
            }
          }

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
                byte buf[] = new byte[1];
                domainSocket.recvFileInputStreams(fis, buf, 0, buf.length);
                if (fis[0] == null) {
                  throw new IOException("the datanode " + dn + " failed to " +
                      "pass a file descriptor (might have reached open file limit).");
                }
                DatanodeBlockID blockID = getBlockResponse.getBlockData().getBlockID();
                String mapKey = responseProto.getCallId() + blockID.toString();
                blockStreamCache.put(mapKey, fis[0]);
                metrics.decrPendingContainerOpsMetrics(type);
                long endToEndCost = System.nanoTime() - entry.getCreateTimeNs();
                long sentCost = entry.getSentTimeNs() - entry.getCreateTimeNs();
                metrics.addContainerOpsLatency(type, endToEndCost);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Executed command {} on datanode {}, cost = {}ns {}ns, cmdType = {}",
                      entry.getRequest(), dn, endToEndCost, sentCost, type);
                }
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
        } catch (EOFException | ClosedChannelException e) {
          LOG.info("receiveResponseTask is closed with {}", e.getClass().getName());
          // fail all requests pending responses
          sentRequests.values().forEach(i ->
              i.getFuture().completeExceptionally(new IOException("DomainSocket InputStream is closed")));
        } catch (Throwable e) {
          LOG.error("Failed to receive response", e);
          if (entry != null) {
            entry.getFuture().completeExceptionally(e);
          }
          sentRequests.values().forEach(i ->
              i.getFuture().completeExceptionally(new IOException("Unexpected failure", e)));
          break;
        }
      } while (!isClosed());
    }
  }

  public class RequestEntry {
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

    public void setSentTimeNs(long nanoSecond) {
      sentTimeNs = nanoSecond;
    }

    public void setTimerTask(TimerTask task) {
      timerTask = task;
    }

    public TimerTask getTimerTask() {
      return timerTask;
    }
  }
}

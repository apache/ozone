/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.transport.server;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.DomainPeer;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import com.google.common.base.Preconditions;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedInputStream;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.OzoneClientConfig.DATA_TRANSFER_MAGIC_CODE;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.DATA_TRANSFER_VERSION;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getContainerCommandResponse;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils.getBlockMapKey;

/**
 * Class for processing incoming/outgoing requests.
 */
final class Receiver implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(Receiver.class);

  private DomainPeer peer;
  private final String remoteAddress; // address of remote side
  private final String remoteAddressWithoutPort; // only the address, no port
  private final String localAddress;  // local address of this daemon
  private final XceiverServerDomainSocket domainSocketServer;
  private final ContainerDispatcher dispatcher;
  private final ContainerMetrics metrics;
  private final InputStream socketIn;
  private OutputStream socketOut;
  private final int bufferSize;
  private final ThreadPoolExecutor readExecutors;
  private DataInputStream input;
  private Lock lock = new ReentrantLock();

  public static Receiver create(DomainPeer peer, ConfigurationSource conf, XceiverServerDomainSocket server,
      ContainerDispatcher dispatcher, ThreadPoolExecutor executor, ContainerMetrics metrics) throws IOException {
    return new Receiver(peer, conf, server, dispatcher, executor, metrics);
  }
  
  private Receiver(DomainPeer peer, ConfigurationSource conf, XceiverServerDomainSocket server,
      ContainerDispatcher dispatcher, ThreadPoolExecutor executor, ContainerMetrics metrics) throws IOException {
    this.peer = peer;
    this.socketIn = peer.getInputStream();
    this.socketOut = peer.getOutputStream();
    this.domainSocketServer = server;
    this.dispatcher = dispatcher;
    this.readExecutors = executor;
    this.metrics = metrics;
    this.bufferSize = conf.getObject(OzoneClientConfig.class).getShortCircuitBufferSize();
    remoteAddress = peer.getRemoteAddressString();
    localAddress = peer.getLocalAddressString();
    final int colonIdx = remoteAddress.indexOf(':');
    remoteAddressWithoutPort = (colonIdx < 0) ? remoteAddress : remoteAddress.substring(0, colonIdx);
  }

  @Override
  public void run() {
    long opsReceived = 0;
    final AtomicLong opsHandled = new AtomicLong(0);
    TaskEntry entry = null;
    try {
      domainSocketServer.addPeer(peer, Thread.currentThread(), this);
      input = new DataInputStream(new BufferedInputStream(socketIn, bufferSize));

      // We process requests in a loop, and stay around for a short timeout.
      // This optimistic behaviour allows the other end to reuse connections.
      // Setting keepalive timeout to 0 disable this behavior.
      do {
        try {
          entry = readRequest(input);
        } catch (SocketTimeoutException | EOFException | ClosedChannelException e) {
          // Since we optimistically expect the next request, it's quite normal to
          // get EOF here.
          LOG.info("{} is closed with {} after received {} ops and handled {} ops.",
              peer, e.getClass().getName(), opsReceived, opsHandled.get());
          throw e;
        }

        readExecutors.submit(new ProcessRequestTask(entry, opsHandled));
        ++opsReceived;
        // reset request variable
        entry = null;
      } while (peer != null && !peer.isClosed());
    } catch (Throwable t) {
      if ((!(t instanceof SocketTimeoutException) && !(t instanceof EOFException))
          && !(t instanceof ClosedChannelException)) {
        String s = "Receiver error"
            + ((entry == null) ? ", " : ", processing " + entry.getRequest().getCmdType() + " operation, " +
            "after received " + opsReceived + " ops and handled " + opsHandled.get() + " ops.");
        LOG.warn(s, t);
      }
    } finally {
      if (peer != null) {
        try {
          domainSocketServer.closePeer(peer);
        } catch (IOException e) {
          LOG.warn("Failed to close peer {}", peer, e);
        }
      }
      if (input != null) {
        IOUtils.closeStream(input);
      }
    }
  }

  /** Read the request. **/
  private TaskEntry readRequest(DataInputStream in) throws IOException {
    // first short is DATA_TRANSFER_VERSION
    final short version = in.readShort();
    if (version != DATA_TRANSFER_VERSION) {
      throw new IOException("Version Mismatch (Expected: " +
          DATA_TRANSFER_VERSION  + ", Received: " +  version + " )");
    }
    long startTime = System.nanoTime();
    // second short is ContainerProtos#Type
    final short typeNumber = in.readShort();
    ContainerProtos.Type type = ContainerProtos.Type.forNumber(typeNumber);

    ContainerCommandRequestProto requestProto =
        ContainerCommandRequestProto.parseFrom(vintPrefixed(in));
    if (requestProto.getCmdType() != type) {
      throw new IOException("Type mismatch, " + type + " in header while " + requestProto.getCmdType() +
          " in request body");
    }
    TaskEntry entry = new TaskEntry(requestProto, startTime);
    return entry;
  }

  public static InputStream vintPrefixed(final DataInputStream input)
      throws IOException {
    final int firstByte = input.read();
    int size = CodedInputStream.readRawVarint32(firstByte, input);
    assert size >= 0;
    return new LimitInputStream(input, size);
  }

  /** Process the request. **/
  public class ProcessRequestTask implements Runnable {
    private final TaskEntry entry;
    private final ContainerCommandRequestProto request;
    private final AtomicLong counter;

    ProcessRequestTask(TaskEntry entry, AtomicLong counter) {
      this.entry = entry;
      this.request = entry.getRequest();
      this.counter = counter;
      this.entry.setInQueueStartTimeNs();
    }

    @Override
    public void run() {
      entry.setOutQueueStartTimeNs();
      ContainerProtos.Type type = request.getCmdType();
      if (isSupportedCmdType(type)) {
        metrics.incContainerLocalOpsMetrics(type);
        metrics.incContainerLocalOpsInQueueLatencies(type, entry.getInQueueTimeNs());
      }
      Span span = TracingUtil.importAndCreateSpan("Receiver." + type.name(),
          request.getTraceID());
      try (Scope scope = GlobalTracer.get().activateSpan(span)) {
        ContainerCommandResponseProto responseProto;
        if (isSupportedCmdType(type)) {
          responseProto = dispatcher.dispatch(request, null);
        } else {
          responseProto = getContainerCommandResponse(request, ContainerProtos.Result.UNSUPPORTED_REQUEST,
              "This command is not supported through DomainSocket channel.")
              .build();
        }
        if (responseProto.getResult() == ContainerProtos.Result.SUCCESS && type == ContainerProtos.Type.GetBlock) {
          // get FileDescriptor
          Handler handler = dispatcher.getHandler(ContainerProtos.ContainerType.KeyValueContainer);
          FileInputStream fis = handler.getBlockInputStream(request);
          Preconditions.checkNotNull(fis,
              "Failed to get block InputStream for block " + request.getGetBlock().getBlockID());
          entry.setFis(fis);
        }
        entry.setResponse(responseProto);
        sendResponse(entry);
      } catch (Throwable e) {
        LOG.error("Failed to processRequest {} {} {}", type, request.getClientId(), request.getCallId(), e);
      } finally {
        span.finish();
        counter.incrementAndGet();
      }
    }
  }

  void sendResponse(TaskEntry entry) {
    byte[] buf = new byte[1];
    buf[0] = DATA_TRANSFER_MAGIC_CODE;
    ContainerCommandResponseProto responseProto = entry.getResponse();
    ContainerProtos.Type type = responseProto.getCmdType();
    lock.lock();
    try {
      entry.setSendStartTimeNs();
      FileInputStream fis = entry.getFis();
      DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socketOut, bufferSize));
      output.writeShort(DATA_TRANSFER_VERSION);
      output.writeShort(type.getNumber());
      responseProto.writeDelimitedTo(output);
      if (LOG.isDebugEnabled()) {
        LOG.debug("send response size {} for request {} through {}", responseProto.getSerializedSize(),
            getBlockMapKey(entry.getRequest()), peer.getDomainSocket());
      }
      output.flush();
      if (fis != null) {
        // send FileDescriptor
        FileDescriptor[] fds = new FileDescriptor[1];
        fds[0] = fis.getFD();
        DomainSocket sock = peer.getDomainSocket();
        // this API requires send at least one byte buf.
        sock.sendFileDescriptors(fds, buf, 0, buf.length);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} send fd for {}", peer.getDomainSocket(), getBlockMapKey(entry.getRequest()));
        }
      }
    } catch (Throwable e) {
      LOG.error("Failed to send response {} {}", responseProto.getCmdType(), peer.getDomainSocket(), e);
    } finally {
      lock.unlock();
      entry.setSendFinishTimeNs();
      if (entry.getFis() != null) {
        try {
          entry.getFis().close();
          if (LOG.isDebugEnabled()) {
            LOG.info("FD is closed for {}", getBlockMapKey(entry.getRequest()));
          }
        } catch (IOException e) {
          LOG.warn("Failed to close FD for {}", getBlockMapKey(entry.getRequest()), e);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request {} {}:{}, receive {} ns, in queue {} ns, " +
                " handle {} ns, send out {} ns, total {} ns", type, responseProto.getClientId().toStringUtf8(),
            responseProto.getCallId(), entry.getReceiveTimeNs(), entry.getInQueueTimeNs(),
            entry.getProcessTimeNs(), entry.getSendTimeNs(), entry.getTotalTimeNs());
      }
      if (isSupportedCmdType(type)) {
        metrics.incContainerLocalOpsLatencies(type, entry.getTotalTimeNs());
      }
    }
  }

  private boolean isSupportedCmdType(ContainerProtos.Type type) {
    return type == ContainerProtos.Type.GetBlock || type == ContainerProtos.Type.Echo;
  }

  static class TaskEntry {
    private ContainerCommandRequestProto request;
    private ContainerCommandResponseProto response;
    private FileInputStream fis;
    private long receiveStartTimeNs;
    private long inQueueStartTimeNs;
    private long outQueueStartTimeNs;
    private long sendStartTimeNs;
    private long sendFinishTimeNs;

    TaskEntry(ContainerCommandRequestProto requestProto, long startTimeNs) {
      this.request = requestProto;
      this.receiveStartTimeNs = startTimeNs;
    }

    public ContainerCommandResponseProto getResponse() {
      return response;
    }

    public FileInputStream getFis() {
      return fis;
    }

    public ContainerCommandRequestProto getRequest() {
      return request;
    }

    public void setInQueueStartTimeNs() {
      inQueueStartTimeNs = System.nanoTime();
    }

    public void setOutQueueStartTimeNs() {
      outQueueStartTimeNs = System.nanoTime();
    }

    public long getReceiveTimeNs() {
      return inQueueStartTimeNs - receiveStartTimeNs;
    }

    public long getInQueueTimeNs() {
      return outQueueStartTimeNs - inQueueStartTimeNs;
    }

    public long getProcessTimeNs() {
      return sendStartTimeNs - outQueueStartTimeNs;
    }

    public long getSendTimeNs() {
      return sendFinishTimeNs - sendStartTimeNs;
    }

    public void setResponse(ContainerCommandResponseProto responseProto) {
      this.response = responseProto;
    }

    public void setFis(FileInputStream is) {
      this.fis = is;
    }

    public void setSendStartTimeNs() {
      this.sendStartTimeNs = System.nanoTime();
    }

    public void setSendFinishTimeNs() {
      this.sendFinishTimeNs = System.nanoTime();
    }

    public long getTotalTimeNs() {
      return this.sendFinishTimeNs - this.receiveStartTimeNs;
    }
  }
}

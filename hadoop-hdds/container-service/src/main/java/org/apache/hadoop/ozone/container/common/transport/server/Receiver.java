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
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.OzoneClientConfig.DATA_TRANSFER_VERSION;

/**
 * Class for processing incoming/outgoing requests.
 */
class Receiver implements XceiverClientProtocol, Runnable {
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
  private DataOutputStream output;
  private final AtomicLong opsHandled = new AtomicLong(0);

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
    ContainerCommandRequestProto requestProto = null;
    try {
      domainSocketServer.addPeer(peer, Thread.currentThread(), this);
      // "java.net.SocketException: setsockopt(SO_SNDTIMEO) error: Invalid argument" on MacOS
      // peer.setWriteTimeout(domainSocketServer.writeTimeout);
      // peer.setReadTimeout(domainSocketServer.readTimeout);
      input = new DataInputStream(new BufferedInputStream(socketIn, bufferSize));
      output = new DataOutputStream(new BufferedOutputStream(socketOut, bufferSize));

      // We process requests in a loop, and stay around for a short timeout.
      // This optimistic behaviour allows the other end to reuse connections.
      // Setting keepalive timeout to 0 disable this behavior.
      do {
        try {
          requestProto = readRequest(input);
        } catch (InterruptedIOException ignored) {
          // Time out while we wait for client rpc
          throw ignored;
        } catch (EOFException | ClosedChannelException e) {
          // Since we optimistically expect the next request, it's quite normal to
          // get EOF here.
          LOG.warn("{} is closed with {} after received {} ops and handled {} ops.",
              peer, e.getClass().getName(), opsReceived, opsHandled.get());
          throw e;
        }

        readExecutors.submit(new processRequestTask(requestProto, output, opsHandled));
        ++opsReceived;
        // reset request variable
        requestProto = null;
      } while (peer != null && !peer.isClosed());
    } catch (Throwable t) {
      String s = "Receiver error processing "
          + ((requestProto == null) ? "unknown" : requestProto.getCmdType()) + " operation "
          + " src: " + remoteAddress + " dst: " + localAddress;
      LOG.warn(s, t);
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
      if (output != null) {
        IOUtils.closeStream(output);
      }
    }
  }

  /** Read the request **/
  private final ContainerCommandRequestProto readRequest(DataInputStream in) throws IOException {
    // first short is DATA_TRANSFER_VERSION
    final short version = in.readShort();
    if (version != DATA_TRANSFER_VERSION) {
      throw new IOException( "Version Mismatch (Expected: " +
          DATA_TRANSFER_VERSION  + ", Received: " +  version + " )");
    }
    // second short is ContainerProtos#Type
    final short typeNumber = in.readShort();
    ContainerProtos.Type type = ContainerProtos.Type.forNumber(typeNumber);
    // first 4 bytes indicates the serialized ContainerCommandRequestProto size
    final int size = in.readInt();
    // LOG.info("received {} {} bytes", type, size);
    byte[] bytes = new byte[size];
    int totalReadSize = in.read(bytes);
    while (totalReadSize < size) {
      int readSize = in.read(bytes, totalReadSize, size - totalReadSize);
      totalReadSize += readSize;
    }
    ContainerCommandRequestProto requestProto = ContainerCommandRequestProto.parseFrom(bytes);
    if (requestProto.getCmdType() != type) {
      throw new IOException("Type mismatch, " + type + " in header while " + requestProto. getCmdType() +
          " in request body");
    }
    return requestProto;
  }

  /** Process the request **/
  public class processRequestTask implements Runnable {
    private final ContainerCommandRequestProto request;
    private final DataOutputStream out;
    private final long createTime;
    private final AtomicLong counter;

    public processRequestTask(ContainerCommandRequestProto request, DataOutputStream out, AtomicLong counter) {
      this.request = request;
      this.out = out;
      this.counter = counter;
      this.createTime = System.nanoTime();
    }

    @Override
    public void run() {
      ContainerProtos.Type type = request.getCmdType();
      Span span = TracingUtil.importAndCreateSpan("XceiverServerDomainSocket." + type.name(),
          request.getTraceID());
      try (Scope scope = GlobalTracer.get().activateSpan(span)) {
        metrics.incContainerLocalOpsMetrics(type);
        metrics.incContainerLocalOpsInQueueLatencies(type, System.nanoTime() - createTime);
        long startTime = System.nanoTime();
        ContainerCommandResponseProto responseProto = dispatcher.dispatch(request, null);
        FileInputStream fis = null;
        if (responseProto.getResult() == ContainerProtos.Result.SUCCESS && type == ContainerProtos.Type.GetBlock) {
          // get FileDescriptor
          Handler handler = dispatcher.getHandler(ContainerProtos.ContainerType.KeyValueContainer);
          fis = handler.getBlockInputStream(request);
          Preconditions.checkNotNull(fis,
              "Failed to get block InputStream for block " + request.getGetBlock().getBlockID());
        }
        ResponseEntry responseEntry = new ResponseEntry(responseProto, fis, System.nanoTime() - startTime);
        sendResponse(responseEntry);
      } catch (Throwable e) {
        LOG.error("Failed to processRequest {} {} {}", type, request.getClientId(), request.getCallId(), e);
      } finally {
        span.finish();
        counter.incrementAndGet();
      }
    }
  }

  void sendResponse(ResponseEntry entry) {
    byte buf[] = new byte[1];
    buf[0] = (byte) 99;
    ContainerCommandResponseProto responseProto = entry.getResponse();
    ContainerProtos.Type type = responseProto.getCmdType();
    synchronized (output) {
      long cost = 0;
      FileInputStream fis = entry.getFis();
      try {
        byte[] bytes = responseProto.toByteArray();
        output.writeShort(DATA_TRANSFER_VERSION);
        output.writeShort(type.getNumber());
        output.writeInt(bytes.length);
        // send response proto
        output.write(bytes);
        long startTime = System.nanoTime();
        output.flush();
        cost = System.nanoTime() - startTime;
        if (fis != null) {
          // send FileDescriptor
          FileDescriptor[] fds = new FileDescriptor[1];
          fds[0] = fis.getFD();
          DomainSocket sock = peer.getDomainSocket();
          // this API requires send at least one byte for buf.
          sock.sendFileDescriptors(fds, buf, 0, buf.length);
        }
      } catch (Throwable e) {
        LOG.error("Failed to send response {}", responseProto.getCmdType(), e);
      } finally {
        // metrics.incContainerLocalOpsLatencies(type, entry.getProcessTimeNs());
        if (fis != null) {
          try {
            fis.close();
            LOG.info("fis {} for {} is closed", fis.getFD(), responseProto.getClientId().toStringUtf8() + "-" + responseProto.getCallId());
          } catch (IOException e) {
            LOG.warn("Failed to close {}", fis, e);
          }
        }
        metrics.incContainerLocalOpsLatencies(type, cost);
        opsHandled.incrementAndGet();
      }
    }
  }

  @Override
  public ContainerCommandResponseProto send(ContainerCommandRequestProto request) throws IOException {
    throw new UnsupportedOperationException("Operation is not supported for " + this.getClass().getSimpleName());
  }

  class ResponseEntry {
    ContainerCommandResponseProto response;
    FileInputStream fis;
    long processTimeNs;

    ResponseEntry(ContainerCommandResponseProto responseProto, FileInputStream fis, long processTimeNs) {
      this.response = responseProto;
      this.fis = fis;
      this.processTimeNs = processTimeNs;
    }

    public ContainerCommandResponseProto getResponse() {
      return response;
    }

    public FileInputStream getFis() {
      return fis;
    }

    public long getProcessTimeNs() {
      return processTimeNs;
    }
  }
}

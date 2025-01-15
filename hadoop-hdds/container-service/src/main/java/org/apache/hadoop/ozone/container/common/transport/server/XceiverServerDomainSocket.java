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

package org.apache.hadoop.ozone.container.common.transport.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.DomainPeer;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates a DomainSocket server endpoint that acts as the communication layer for Ozone containers.
 */
public final class XceiverServerDomainSocket implements XceiverServerSpi, Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(XceiverServerDomainSocket.class);
  private int port;
  private Daemon server;
  private ContainerDispatcher dispatcher;
  private ContainerMetrics metrics;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  /**
   * Maximal number of concurrent readers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   */
  private final int maxXceiverCount;
  private final AtomicInteger xceriverCount;
  private DomainSocket domainSocket;
  private final ConfigurationSource config;
  private final String threadPrefix;
  private final HashMap<DomainPeer, Thread> peers = new HashMap<>();
  private final HashMap<DomainPeer, Receiver> peersReceiver = new HashMap<>();
  private int readTimeoutMs;
  private int writeTimeoutMs;
  private final ThreadPoolExecutor readExecutors;
  private FaultInjector injector;

  /**
   * Constructs a DomainSocket server class, used to listen for requests from local clients.
   */
  public XceiverServerDomainSocket(DatanodeDetails datanodeDetails, ConfigurationSource conf,
      ContainerDispatcher dispatcher, ThreadPoolExecutor executor,
      ContainerMetrics metrics, DomainSocketFactory domainSocketFactory) {
    Preconditions.checkNotNull(conf);
    this.port = conf.getInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
    if (conf.getBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT_DEFAULT)) {
      this.port = 0;
    }
    this.config = conf;
    final int threadCountPerDisk =
        conf.getObject(DatanodeConfiguration.class).getNumReadThreadPerVolume();
    final int numberOfDisks = HddsServerUtil.getDatanodeStorageDirs(conf).size();
    this.maxXceiverCount = threadCountPerDisk * numberOfDisks * 5;
    this.xceriverCount = new AtomicInteger(0);
    this.dispatcher = dispatcher;
    this.readExecutors = executor;
    this.metrics = metrics;
    LOG.info("Max allowed {} xceiver", maxXceiverCount);
    this.threadPrefix = datanodeDetails.threadNamePrefix() + XceiverServerDomainSocket.class.getSimpleName();

    if (domainSocketFactory.isServiceEnabled() && domainSocketFactory.isServiceReady()) {
      this.readTimeoutMs = (int) config.getTimeDuration(OzoneConfigKeys.OZONE_CLIENT_READ_TIMEOUT,
          OzoneConfigKeys.OZONE_CLIENT_READ_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
      this.writeTimeoutMs = (int) config.getTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WRITE_TIMEOUT,
          OzoneConfigKeys.OZONE_CLIENT_WRITE_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
      try {
        domainSocket = DomainSocket.bindAndListen(
            DomainSocket.getEffectivePath(conf.get(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH), port));
        OzoneClientConfig ozoneClientConfig = conf.getObject(OzoneClientConfig.class);
        domainSocket.setAttribute(DomainSocket.RECEIVE_TIMEOUT, readTimeoutMs);
        domainSocket.setAttribute(DomainSocket.SEND_TIMEOUT, writeTimeoutMs);
        LOG.info("UNIX domain socket {} is created: {}, timeout for read {} ms, timeout for write {} ms, " +
                "send/receive buffer {} bytes", domainSocket, domainSocket.getPath(), readTimeoutMs, writeTimeoutMs,
            ozoneClientConfig.getShortCircuitBufferSize());
      } catch (IOException e) {
        LOG.warn("Although short-circuit local reads are configured, we cannot " +
            "enable the short circuit read because DomainSocket operation failed", e);
        domainSocket = null;
        throw new IllegalArgumentException(e);
      }
    }
  }

  @Override
  public int getIPCPort() {
    return this.port;
  }

  /**
   * Returns the Replication type supported by this end-point.
   *
   * @return enum STAND_ALONE
   */
  @Override
  public HddsProtos.ReplicationType getServerType() {
    return HddsProtos.ReplicationType.STAND_ALONE;
  }

  @Override
  public void start() throws IOException {
    if (isRunning.compareAndSet(false, true)) {
      if (domainSocket != null) {
        this.server = new Daemon(this);
        this.server.setName(threadPrefix);
        this.server.start();
        LOG.info("Listening on UNIX domain socket: {}", domainSocket.getPath());
        isRunning.set(true);
      } else {
        LOG.warn("Cannot start XceiverServerDomainSocket because domainSocket is null");
      }
    } else {
      LOG.info("UNIX domain socket server listening on {} is already stopped", domainSocket.getPath());
    }
  }

  @Override
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      if (server != null) {
        try {
          if (domainSocket != null) {
            // TODO: once HADOOP-19261 is merged, change it to domainSocket.close(true);
            domainSocket.close();
            LOG.info("UNIX domain socket server listening on {} is stopped", domainSocket.getPath());
          }
        } catch (IOException e) {
          LOG.error("Failed to force close DomainSocket", e);
        }
        server.interrupt();
        try {
          server.join();
        } catch (InterruptedException e) {
          LOG.error("Failed to shutdown XceiverServerDomainSocket", e);
          Thread.currentThread().interrupt();
        }
      }
    } else {
      LOG.info("UNIX domain socket server listening on {} is already stopped", domainSocket.getPath());
    }
  }

  @Override
  public boolean isStarted() {
    return isRunning.get();
  }

  @Override
  public void submitRequest(ContainerCommandRequestProto request,
      HddsProtos.PipelineID pipelineID) throws IOException {
    throw new UnsupportedOperationException("Operation is not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public boolean isExist(HddsProtos.PipelineID pipelineId) {
    throw new UnsupportedOperationException("Operation is not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public List<PipelineReport> getPipelineReport() {
    throw new UnsupportedOperationException("Operation is not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public void run() {
    while (isRunning.get()) {
      DomainPeer peer = null;
      try {
        DomainSocket connSock = domainSocket.accept();
        xceriverCount.incrementAndGet();
        peer = new DomainPeer(connSock);
        peer.setReadTimeout(readTimeoutMs);
        peer.setWriteTimeout(writeTimeoutMs);
        LOG.info("Accepted a new connection {}, xceriverCount {}", connSock, xceriverCount.get());

        // Make sure the xceiver count is not exceeded
        if (xceriverCount.get() > maxXceiverCount) {
          throw new IOException("Xceiver count exceeds the limit " + maxXceiverCount);
        }
        Daemon daemon = new Daemon(Receiver.create(peer, config, this, dispatcher, readExecutors, metrics));
        daemon.setName(threadPrefix + "@" + peer.getDomainSocket().toString());
        daemon.start();
      } catch (SocketTimeoutException ignored) {
        // wake up to see if should continue to run
      } catch (AsynchronousCloseException ace) {
        // another thread closed our listener socket - that's expected during shutdown, but not in other circumstances
        LOG.info("XceiverServerDomainSocket is closed", ace);
      } catch (IOException ie) {
        // usually when the xceiver count limit is hit.
        LOG.warn("Got an exception. Peer {}", peer, ie);
        IOUtils.closeQuietly(peer);
      } catch (OutOfMemoryError ie) {
        IOUtils.closeQuietly(peer);
        // DataNode can run out of memory if there is too many transfers.
        // Log the event, Sleep for 30 seconds, other transfers may complete by
        // then.
        LOG.error("DataNode is out of memory. Will retry in 30 seconds.", ie);
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
        } catch (InterruptedException e) {
          // ignore
        }
      } catch (Throwable te) {
        LOG.error("XceiverServerDomainSocket: Exiting.", te);
      }
    }

    close();
  }

  void close() {
    try {
      // Close the server to accept more requests.
      if (domainSocket != null) {
        domainSocket.getChannel().close();
        LOG.info("DomainSocket {} is closed", domainSocket.toString());
      }
    } catch (IOException ie) {
      LOG.warn("Failed to close domainSocket {}", domainSocket.toString(), ie);
    }

    closeAllPeers();
  }

  /**
   * Notify all Receiver thread of the shutdown.
   */
  void closeAllPeers() {
    // interrupt each and every Receiver thread.
    peers.values().forEach(t -> t.interrupt());

    // wait 3s for peers to close
    long mills = 3000;
    try {
      while (!peers.isEmpty() && mills > 0) {
        Thread.sleep(1000);
        mills -= 1000;
      }
    } catch (InterruptedException e) {
      LOG.info("Interrupted waiting for peers to close");
      Thread.currentThread().interrupt();
    }

    peers.keySet().forEach(org.apache.hadoop.io.IOUtils::closeStream);
    peers.clear();
    peersReceiver.clear();
  }

  void addPeer(DomainPeer peer, Thread t, Receiver receiver) throws IOException {
    if (!isRunning.get()) {
      throw new IOException("XceiverServerDomainSocket is closed.");
    }
    peers.put(peer, t);
    peersReceiver.put(peer, receiver);
    LOG.info("Peer {} is added", peer.getDomainSocket());
  }

  void closePeer(DomainPeer peer) throws IOException {
    if (!isRunning.get()) {
      throw new IOException("XceiverServerDomainSocket is closed.");
    }
    peers.remove(peer);
    peersReceiver.remove(peer);
    org.apache.hadoop.io.IOUtils.closeStream(peer);
    xceriverCount.decrementAndGet();
    LOG.info("Peer {} is closed", peer.getDomainSocket());
  }

  @VisibleForTesting
  public void setContainerDispatcher(ContainerDispatcher containerDispatcher) {
    this.dispatcher = containerDispatcher;
  }

  @VisibleForTesting
  public FaultInjector getInjector() {
    return injector;
  }

  @VisibleForTesting
  public void setInjector(FaultInjector injector) {
    this.injector = injector;
  }

  /**
   * Inject pause for test only.
   *
   * @throws IOException
   */
  private void injectPause() throws IOException {
    if (injector != null) {
      injector.pause();
    }
  }
}

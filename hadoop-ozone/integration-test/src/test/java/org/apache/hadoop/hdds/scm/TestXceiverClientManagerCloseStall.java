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

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.RATIS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closing an {@link XceiverClientManager} must not wait for its Ratis clients one after another (HDDS-15255).
 * <p>
 * Ratis waits up to 3 s for each gRPC channel to terminate gracefully when a client is closed. A channel
 * doesn't terminate until the datanode completes the client's half-closed stream, so a datanode that doesn't
 * answer costs the full grace period. To reproduce that deterministically, every client here talks to its
 * datanodes through a {@link StallableForwarder}, which is then switched to keep the connections open but
 * drop all traffic.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
public class TestXceiverClientManagerCloseStall {

  private static final Logger LOG = LoggerFactory.getLogger(TestXceiverClientManagerCloseStall.class);

  /** Hard-coded in Ratis {@code GrpcUtil.shutdownManagedChannel}. */
  private static final long GRACEFUL_SHUTDOWN_MS = 3000;
  private static final String GRACEFUL_SHUTDOWN_TIMED_OUT = "Timed out gracefully shutting down connection";
  private static final int MIN_PIPELINES = 3;

  private static final AtomicLong CONTAINER_ID = new AtomicLong(1_000_000);

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private final List<StallableForwarder> forwarders = new CopyOnWriteArrayList<>();

  @BeforeAll
  void init() throws Exception {
    conf = new OzoneConfiguration();
    // several RATIS/THREE pipelines on the same three datanodes
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, MIN_PIPELINES + 1);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE, 60000);
    GenericTestUtils.waitFor(() -> openRatisThreePipelines().size() >= MIN_PIPELINES, 500, 120000);
  }

  @AfterAll
  void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @AfterEach
  void closeForwarders() {
    forwarders.forEach(f -> IOUtils.cleanupWithLogger(LOG, f));
    forwarders.clear();
  }

  @Test
  void closeDoesNotWaitForEachStalledClientInTurn() throws Exception {
    final List<Pipeline> pipelines = openRatisThreePipelines();
    final XceiverClientManager manager = newManagerWithCachedClients(pipelines);
    forwarders.forEach(StallableForwarder::stall);

    try (LogCapturer logs = LogCapturer.captureLogs(LoggerFactory.getLogger("org.apache.ratis.grpc.GrpcUtil"))) {
      final long elapsedMs = timeClose(manager);
      final int timedOut = StringUtils.countMatches(logs.getOutput(), GRACEFUL_SHUTDOWN_TIMED_OUT);
      LOG.info("Closed {} clients of stalled datanodes in {} ms, {} channels timed out", pipelines.size(),
          elapsedMs, timedOut);

      // each client really waited for its stalled channel
      assertThat(timedOut).isGreaterThanOrEqualTo(pipelines.size());
      // ... but not one after another: closing them one at a time takes pipelines.size() * 3 s or more
      assertThat(elapsedMs).isLessThan(2 * GRACEFUL_SHUTDOWN_MS);
    }
  }

  @Test
  void closeIsFastWhenDatanodesRespond() throws Exception {
    final List<Pipeline> pipelines = openRatisThreePipelines();
    final XceiverClientManager manager = newManagerWithCachedClients(pipelines);

    try (LogCapturer logs = LogCapturer.captureLogs(LoggerFactory.getLogger("org.apache.ratis.grpc.GrpcUtil"))) {
      final long elapsedMs = timeClose(manager);
      LOG.info("Closed {} clients of responsive datanodes in {} ms", pipelines.size(), elapsedMs);

      assertThat(logs.getOutput()).doesNotContain(GRACEFUL_SHUTDOWN_TIMED_OUT);
      assertThat(elapsedMs).isLessThan(GRACEFUL_SHUTDOWN_MS);
    }
  }

  private List<Pipeline> openRatisThreePipelines() {
    return cluster.getStorageContainerManager().getPipelineManager().getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), Pipeline.PipelineState.OPEN);
  }

  /**
   * @return a manager holding one released (refcount 0) Ratis client per pipeline, as at the exit of a client
   *     process, each with an open stream to its datanodes through a forwarder
   */
  private XceiverClientManager newManagerWithCachedClients(List<Pipeline> pipelines) throws IOException {
    final XceiverClientManager manager = new XceiverClientManager(conf, conf.getObject(ScmClientConfig.class), null);
    for (Pipeline pipeline : pipelines) {
      final XceiverClientSpi client = manager.acquireClient(throughForwarders(pipeline));
      assertThat(client).isInstanceOf(XceiverClientRatis.class);
      ContainerProtocolCalls.createContainer(client, CONTAINER_ID.incrementAndGet(), null);
      manager.releaseClient(client, false);
    }
    assertThat(manager.getClientCache().size()).isEqualTo(pipelines.size());
    return manager;
  }

  /** The same pipeline, with each datanode's client port replaced by a forwarder to it. */
  private Pipeline throughForwarders(Pipeline pipeline) throws IOException {
    final List<DatanodeDetails> nodes = new ArrayList<>();
    for (DatanodeDetails dn : pipeline.getNodes()) {
      final InetAddress address = InetAddress.getByName(dn.getIpAddress());
      final StallableForwarder forwarder = new StallableForwarder(address,
          new InetSocketAddress(address, dn.getPort(RATIS).getValue()));
      forwarders.add(forwarder);
      nodes.add(DatanodeDetails.newBuilder().setDatanodeDetails(dn)
          .addPort(DatanodeDetails.newPort(RATIS, forwarder.getPort())).build());
    }
    final Pipeline proxied = pipeline.toBuilder().setNodes(nodes).build();
    assertThat(proxied.getId()).isEqualTo(pipeline.getId());
    return proxied;
  }

  private static long timeClose(XceiverClientManager manager) {
    final long start = System.nanoTime();
    manager.close();
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
  }

  /**
   * Forwards TCP connections to a target. After {@link #stall()} it keeps the connections open but drops all
   * traffic in both directions, like a datanode that stopped responding.
   */
  private static final class StallableForwarder implements Closeable {
    private final ServerSocket server;
    private final InetSocketAddress target;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private volatile boolean stalled;

    StallableForwarder(InetAddress bindAddress, InetSocketAddress target) throws IOException {
      this.target = target;
      this.server = new ServerSocket(0, 50, bindAddress);
      startDaemon(this::accept, "forwarder-accept-" + getPort());
    }

    int getPort() {
      return server.getLocalPort();
    }

    void stall() {
      stalled = true;
    }

    private void accept() {
      try {
        while (!server.isClosed()) {
          final Socket fromClient = server.accept();
          final Socket toTarget = new Socket(target.getAddress(), target.getPort());
          sockets.add(fromClient);
          sockets.add(toTarget);
          startDaemon(() -> pump(fromClient, toTarget), "forwarder-c2s-" + getPort());
          startDaemon(() -> pump(toTarget, fromClient), "forwarder-s2c-" + getPort());
        }
      } catch (IOException e) {
        // server socket closed
      }
    }

    private void pump(Socket from, Socket to) {
      final byte[] buffer = new byte[64 << 10];
      try {
        final InputStream in = from.getInputStream();
        final OutputStream out = to.getOutputStream();
        for (int n; (n = in.read(buffer)) >= 0;) {
          if (!stalled) {
            out.write(buffer, 0, n);
            out.flush();
          }
        }
      } catch (IOException e) {
        // connection closed
      } finally {
        IOUtils.cleanupWithLogger(null, from, to);
      }
    }

    private static void startDaemon(Runnable runnable, String name) {
      final Thread thread = new Thread(runnable, name);
      thread.setDaemon(true);
      thread.start();
    }

    @Override
    public void close() throws IOException {
      server.close();
      sockets.forEach(s -> IOUtils.cleanupWithLogger(null, s));
    }
  }
}

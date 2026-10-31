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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link XceiverClientGrpc} delivers the reply of a blocking call
 * on the waiting thread, and that the async path is unchanged.
 */
class TestXceiverClientGrpcCallerRuns {

  private final List<Server> servers = new ArrayList<>();
  private Server server;
  private volatile boolean fail;
  private volatile ContainerCommandResponseProto response;
  private volatile boolean hang;
  private volatile Server hangingServer;
  private volatile CountDownLatch barrier;
  private final AtomicInteger streamsOpened = new AtomicInteger();
  private final AtomicInteger streamsEnded = new AtomicInteger();
  private DatanodeDetails dn;
  private XceiverClientGrpc client;

  @BeforeEach
  void setUp() throws Exception {
    dn = startServer();
    server = servers.get(0);
    client = newClient(Collections.singletonList(dn), new OzoneConfiguration());
  }

  /** Starts a server and returns the datanode that it stands for. */
  private DatanodeDetails startServer() throws IOException {
    final AtomicReference<Server> self = new AtomicReference<>();
    final Server started = NettyServerBuilder.forPort(0)
        .addService(new XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase() {
          @Override
          public StreamObserver<ContainerCommandRequestProto> send(
              StreamObserver<ContainerCommandResponseProto> responseObserver) {
            streamsOpened.incrementAndGet();
            return new StreamObserver<ContainerCommandRequestProto>() {
              @Override
              public void onNext(ContainerCommandRequestProto request) {
                final CountDownLatch latch = barrier;
                if (latch != null) {
                  latch.countDown();
                  try {
                    latch.await(30, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                }
                if (hang || self.get() == hangingServer) {
                  return;
                }
                if (fail) {
                  responseObserver.onError(Status.INTERNAL.withDescription("injected").asException());
                  return;
                }
                final ContainerCommandResponseProto reply = response;
                responseObserver.onNext(reply != null ? reply : ContainerCommandResponseProto.newBuilder()
                    .setCmdType(request.getCmdType())
                    .setResult(ContainerProtos.Result.SUCCESS)
                    .build());
              }

              @Override
              public void onError(Throwable t) {
                streamsEnded.incrementAndGet();
              }

              @Override
              public void onCompleted() {
                streamsEnded.incrementAndGet();
                // like a datanode, a server that is still serving the request does not complete the stream
                if (!fail && !hang && self.get() != hangingServer) {
                  responseObserver.onCompleted();
                }
              }
            };
          }
        })
        .build()
        .start();
    self.set(started);
    servers.add(started);
    return MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID(), "localhost", "127.0.0.1", null,
        started.getPort());
  }

  private static XceiverClientGrpc newClient(List<DatanodeDetails> nodes, OzoneConfiguration config)
      throws Exception {
    XceiverClientGrpc created = new XceiverClientGrpc(MockPipeline.createPipeline(nodes), config);
    created.connect();
    return created;
  }

  @AfterEach
  void tearDown() throws Exception {
    client.close();
    for (Server s : servers) {
      s.shutdownNow();
      s.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private ContainerCommandRequestProto readContainer() {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.ReadContainer)
        .setContainerID(1)
        .setDatanodeUuid(dn.getUuidString())
        .setReadContainer(ContainerProtos.ReadContainerRequestProto.getDefaultInstance())
        .build();
  }

  /** Holds the next replies until the test thread has been waiting for a while. */
  private void releaseRepliesLater() {
    CountDownLatch gate = new CountDownLatch(2);
    barrier = gate;
    Thread release = new Thread(() -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      gate.countDown();
    });
    release.setDaemon(true);
    release.start();
  }

  @Test
  void blockingCallCompletesOnCaller() throws Exception {
    for (int i = 0; i < 10; i++) {
      releaseRepliesLater();
      CompletableFuture<ContainerCommandResponseProto> future =
          client.sendCommandAsync(readContainer(), dn).getResponse();
      AtomicReference<Thread> completedOn = new AtomicReference<>();
      CompletableFuture<Void> dependent = future.thenRun(() -> completedOn.set(Thread.currentThread()));

      assertEquals(ContainerProtos.Result.SUCCESS, future.get().getResult());
      dependent.get();
      assertSame(Thread.currentThread(), completedOn.get());
    }
    assertEquals(ContainerProtos.Result.SUCCESS,
        client.sendCommand(readContainer(), Collections.emptyList()).getResult());
  }

  @Test
  void blockingCallFailureSurfacesAsIOException() {
    fail = true;
    IOException e = assertThrows(IOException.class,
        () -> client.sendCommand(readContainer(), Collections.emptyList()));
    assertThat(e).hasMessageContaining("injected");
  }

  @Test
  void asyncCallCompletesWithoutWaiter() throws Exception {
    CompletableFuture<ContainerCommandResponseProto> future =
        client.sendCommandAsync(readContainer()).getResponse();
    assertThat(future).isNotInstanceOf(XceiverClientGrpc.CallerRunsReplyFuture.class);
    // a plain future's get runs no queued work: the reply arrives on gRPC's executor
    assertEquals(ContainerProtos.Result.SUCCESS, future.get(30, TimeUnit.SECONDS).getResult());
    // the call used a stream of its own, ended with the call and never pooled
    assertEquals(1, streamsOpened.get());
    GenericTestUtils.waitFor(() -> streamsEnded.get() == 1, 10, 10_000);
    assertEquals(0, client.getIdleStreamCount(dn));
  }

  @Test
  void callbacksRunDirectlyAfterWaiterLeft() throws Exception {
    XceiverClientGrpc.CallerRunsReplyFuture future = new XceiverClientGrpc.CallerRunsReplyFuture();
    AtomicReference<Thread> ranOn = new AtomicReference<>();
    future.execute(() -> ranOn.set(Thread.currentThread()));
    assertThrows(TimeoutException.class, () -> future.get(10, TimeUnit.MILLISECONDS));
    // the queued task ran on the waiter
    assertSame(Thread.currentThread(), ranOn.get());

    Thread other = new Thread(() -> future.execute(() -> ranOn.set(Thread.currentThread())));
    other.start();
    other.join();
    assertSame(other, ranOn.get());

    future.execute(() -> future.completeExceptionally(new IOException("late")));
    ExecutionException e = assertThrows(ExecutionException.class, future::get);
    assertThat(e).hasCauseInstanceOf(IOException.class);
  }

  /**
   * ChunkInputStream uses {@code limit()} as the size of each reply buffer and
   * resets it with {@code position(0)}, so the buffers of a received reply must
   * start at position 0, which a response marshaller aliasing one array breaks.
   */
  @Test
  void readChunkReplyBuffersStartAtZero() throws Exception {
    List<ByteString> buffers = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      byte[] data = new byte[16 << 10];
      ThreadLocalRandom.current().nextBytes(data);
      buffers.add(ByteString.copyFrom(data));
    }
    response = ContainerCommandResponseProto.newBuilder()
        .setCmdType(ContainerProtos.Type.ReadChunk)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setReadChunk(ContainerProtos.ReadChunkResponseProto.newBuilder()
            .setBlockID(ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(1).setLocalID(2))
            .setChunkData(ContainerProtos.ChunkInfo.newBuilder()
                .setChunkName("chunk").setOffset(0).setLen(5 * (16 << 10))
                .setChecksumData(ContainerProtos.ChecksumData.newBuilder()
                    .setType(ContainerProtos.ChecksumType.CRC32).setBytesPerChecksum(16 << 10)))
            .setDataBuffers(ContainerProtos.DataBuffers.newBuilder().addAllBuffers(buffers)))
        .build();

    List<ContainerCommandResponseProto> replies = new ArrayList<>();
    replies.add(client.sendCommandAsync(readContainer(), dn).getResponse().get());
    replies.add(client.sendCommandAsync(readContainer()).getResponse().get(30, TimeUnit.SECONDS));
    for (ContainerCommandResponseProto reply : replies) {
      assertEquals(response, reply);
      ByteBuffer[] received =
          BufferUtils.getReadOnlyByteBuffersArray(reply.getReadChunk().getDataBuffers().getBuffersList());
      assertEquals(buffers.size(), received.length);
      for (int i = 0; i < received.length; i++) {
        assertEquals(0, received[i].position());
        assertEquals(buffers.get(i).size(), received[i].limit());
        assertEquals(buffers.get(i).asReadOnlyByteBuffer(), received[i]);
      }
    }
  }

  @Test
  void sequentialCommandsReuseOneStream() throws Exception {
    for (int i = 0; i < 10; i++) {
      assertEquals(ContainerProtos.Result.SUCCESS,
          client.sendCommand(readContainer(), Collections.emptyList()).getResult());
    }
    assertEquals(1, streamsOpened.get());
    assertEquals(0, streamsEnded.get());

    client.close();
    GenericTestUtils.waitFor(() -> streamsEnded.get() == 1, 10, 10_000);
  }

  @Test
  void concurrentCommandsUseDistinctStreams() throws Exception {
    final int n = 4;
    // the server holds each request until all n arrived, so they cannot share a stream
    barrier = new CountDownLatch(n);
    ExecutorService executor = Executors.newFixedThreadPool(n);
    try {
      List<Future<ContainerCommandResponseProto>> futures = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        futures.add(executor.submit(() -> client.sendCommand(readContainer(), Collections.emptyList())));
      }
      for (Future<ContainerCommandResponseProto> f : futures) {
        assertEquals(ContainerProtos.Result.SUCCESS, f.get(30, TimeUnit.SECONDS).getResult());
      }
    } finally {
      executor.shutdownNow();
    }
    assertEquals(n, streamsOpened.get());

    // the streams went back to the pool
    barrier = null;
    for (int i = 0; i < 2 * n; i++) {
      client.sendCommand(readContainer(), Collections.emptyList());
    }
    assertEquals(n, streamsOpened.get());
  }

  @Test
  void streamErrorFailsOnlyItsRequestAndDropsTheStream() throws Exception {
    client.sendCommand(readContainer(), Collections.emptyList());
    fail = true;
    assertThrows(IOException.class, () -> client.sendCommand(readContainer(), Collections.emptyList()));
    fail = false;
    assertEquals(ContainerProtos.Result.SUCCESS,
        client.sendCommand(readContainer(), Collections.emptyList()).getResult());
    assertEquals(2, streamsOpened.get());
  }

  @Test
  void timeoutFailsWithDeadlineExceededAndCancelsTheStream() throws Exception {
    client.setTimeout(1000);
    hang = true;
    ExecutionException e = assertThrows(ExecutionException.class,
        () -> client.sendCommandAsync(readContainer(), dn).getResponse().get());
    assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(e.getCause()).getCode());
    GenericTestUtils.waitFor(() -> streamsEnded.get() == 1, 10, 10_000);

    hang = false;
    assertEquals(ContainerProtos.Result.SUCCESS,
        client.sendCommand(readContainer(), Collections.emptyList()).getResult());
    assertEquals(2, streamsOpened.get());
  }

  @Test
  void interruptedWaiterCancelsTheStream() throws Exception {
    hang = true;
    CompletableFuture<ContainerCommandResponseProto> future = client.sendCommandAsync(readContainer(), dn)
        .getResponse();
    Thread.currentThread().interrupt();
    assertThrows(InterruptedException.class, future::get);
    // the stream may be cancelled before the server saw it, so check the client side
    GenericTestUtils.waitFor(future::isDone, 10, 10_000);
    ExecutionException e = assertThrows(ExecutionException.class, future::get);
    assertEquals(Status.Code.CANCELLED, Status.fromThrowable(e.getCause()).getCode());

    hang = false;
    assertEquals(ContainerProtos.Result.SUCCESS,
        client.sendCommand(readContainer(), Collections.emptyList()).getResult());
  }

  @Test
  void idleStreamsDoNotHoldUpServerShutdown() throws Exception {
    client.sendCommand(readContainer(), Collections.emptyList());
    server.shutdown();
    assertTrue(server.awaitTermination(3, TimeUnit.SECONDS));
  }

  @Test
  void tracedCommandsUseStreamPerCall() throws Exception {
    SpanContext context = SpanContext.create("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331",
        TraceFlags.getSampled(), TraceState.getDefault());
    try (Scope ignored = Span.wrap(context).makeCurrent()) {
      for (int i = 0; i < 3; i++) {
        releaseRepliesLater();
        CompletableFuture<ContainerCommandResponseProto> future =
            client.sendCommandAsync(readContainer(), dn).getResponse();
        AtomicReference<Thread> completedOn = new AtomicReference<>();
        CompletableFuture<Void> dependent = future.thenRun(() -> completedOn.set(Thread.currentThread()));

        assertEquals(ContainerProtos.Result.SUCCESS, future.get().getResult());
        dependent.get();
        assertSame(Thread.currentThread(), completedOn.get());
      }
    }
    assertEquals(3, streamsOpened.get());
  }

  @Test
  void requestOnDeadChannelFailsFastWithoutLeakingPermits() throws Exception {
    client.sendCommand(readContainer(), Collections.emptyList());
    final int permits = client.getAvailablePermits();
    server.shutdownNow();
    assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
    client.setTimeout(30_000);
    // the first request finds the connection gone, the next one the channel backing off: neither waits for the deadline
    for (int i = 0; i < 2; i++) {
      final long start = System.nanoTime();
      ExecutionException e = assertThrows(ExecutionException.class,
          () -> client.sendCommandAsync(readContainer(), dn).getResponse().get());
      assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(5));
      assertNotEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(e.getCause()).getCode());
      GenericTestUtils.waitFor(() -> client.getAvailablePermits() == permits, 10, 10_000);
    }
  }

  @Test
  void requestOnAStreamThatEndedAfterTheLeaseFailsAtOnce() throws Exception {
    final int permits = client.getAvailablePermits();
    XceiverClientGrpc.PooledStream stream = new XceiverClientGrpc.PooledStream(null);
    @SuppressWarnings("unchecked")
    ClientCallStreamObserver<ContainerCommandRequestProto> call = mock(ClientCallStreamObserver.class);
    stream.beforeStart(call);
    // the connection broke after the stream was leased, before the request was attached to it
    stream.onError(Status.UNAVAILABLE.withDescription("connection reset").asRuntimeException());
    XceiverClientGrpc.CallerRunsReplyFuture future = new XceiverClientGrpc.CallerRunsReplyFuture();
    XceiverClientGrpc.ReplyObserver reply = client.new ReplyObserver(readContainer(), dn, future, 0);

    stream.send(readContainer(), reply, future, System.nanoTime() + TimeUnit.SECONDS.toNanos(30));

    // failed without waiting for the deadline, so the caller can retry another datanode
    assertTrue(future.isDone());
    ExecutionException e = assertThrows(ExecutionException.class, future::get);
    assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(e.getCause()).getCode());
    verify(call, never()).onNext(any());
    // the request released its permit, which this test did not acquire
    assertEquals(permits + 1, client.getAvailablePermits());
  }

  @Test
  void saturatedClientOpensNoMoreStreamsThanPermits() throws Exception {
    final int permits = 2;
    OzoneConfiguration config = new OzoneConfiguration();
    RatisClientConfig.RaftConfig raftConfig = config.getObject(RatisClientConfig.RaftConfig.class);
    raftConfig.setMaxOutstandingRequests(permits);
    config.setFromObject(raftConfig);
    client.close();
    client = newClient(Collections.singletonList(dn), config);
    assertEquals(permits, client.getAvailablePermits());

    final int threads = 4 * permits;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(executor.submit(() -> {
          for (int j = 0; j < 50; j++) {
            client.sendCommand(readContainer(), Collections.emptyList());
          }
          return null;
        }));
      }
      for (Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
    assertThat(streamsOpened.get()).isLessThanOrEqualTo(permits);
    assertThat(client.getIdleStreamCount(dn)).isLessThanOrEqualTo(permits);
  }

  @Test
  void fanOutDeliversOneReplyPerNode() throws Exception {
    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(dn);
    nodes.add(startServer());
    nodes.add(startServer());
    client.close();
    client = newClient(nodes, new OzoneConfiguration());
    final int permits = client.getAvailablePermits();

    // every node holds its request until all of them arrived, so the requests must be in flight at once
    barrier = new CountDownLatch(nodes.size());
    Map<DatanodeDetails, ContainerCommandResponseProto> replies = client.sendCommandOnAllNodes(readContainer());
    assertEquals(nodes.size(), replies.size());
    for (ContainerCommandResponseProto reply : replies.values()) {
      assertEquals(ContainerProtos.Result.SUCCESS, reply.getResult());
    }
    // one stream per call, none pooled
    assertEquals(nodes.size(), streamsOpened.get());
    assertEquals(0, client.getIdleStreamCount(dn));
    GenericTestUtils.waitFor(() -> client.getAvailablePermits() == permits, 10, 10_000);

    // one node never replies: its request times out, the replies of the other nodes are still delivered
    barrier = null;
    client.setTimeout(500);
    hangingServer = server;
    replies = client.sendCommandOnAllNodes(readContainer());
    assertEquals(nodes.size() - 1, replies.size());
    assertThat(replies).doesNotContainKey(dn);
    for (ContainerCommandResponseProto reply : replies.values()) {
      assertEquals(ContainerProtos.Result.SUCCESS, reply.getResult());
    }
    GenericTestUtils.waitFor(() -> client.getAvailablePermits() == permits, 10, 10_000);

    hangingServer = null;
    replies = client.sendCommandOnAllNodes(readContainer());
    assertEquals(nodes.size(), replies.size());
  }

  @Test
  void fanOutThatCannotReachANodeReleasesThePermitsOfTheRequestsAlreadyIssued() throws Exception {
    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(dn);
    nodes.add(startServer());
    final DatanodeDetails unreachable = startServer();
    nodes.add(unreachable);
    client.close();
    client = new XceiverClientGrpc(MockPipeline.createPipeline(nodes), new OzoneConfiguration()) {
      @Override
      protected NettyChannelBuilder createChannel(DatanodeDetails node, int port) throws IOException {
        if (node.equals(unreachable)) {
          throw new IOException("injected");
        }
        return super.createChannel(node, port);
      }
    };
    client.connect();
    final int permits = client.getAvailablePermits();

    // the requests to the first two nodes are in flight when the channel to the third one cannot be opened
    IOException e = assertThrows(IOException.class, () -> client.sendCommandOnAllNodes(readContainer()));
    assertThat(e).hasMessageContaining("injected");
    // nobody waits for their replies, which still release the permits
    GenericTestUtils.waitFor(() -> client.getAvailablePermits() == permits, 10, 10_000);
  }
}

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

package org.apache.hadoop.ozone.container.common.transport.server;

import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.utils.io.RandomAccessFileChannel;
import org.apache.hadoop.ozone.container.common.helpers.ReadBlockContext;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.io.grpc.Context;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the streaming ReadBlock handling in {@link GrpcXceiverService}: the block file held open for a
 * stream is closed once the stream is idle and reopened by the next request.
 */
class TestGrpcXceiverService {

  private static final Duration IDLE_TIMEOUT = Duration.ofMillis(200);

  @TempDir
  private Path tempDir;

  private GrpcXceiverService service;
  private final ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>());
  private final XceiverServerGrpcMetrics metrics = new XceiverServerGrpcMetrics(readExecutor, "test-datanode");

  @AfterEach
  void shutdown() {
    if (service != null) {
      service.shutdown();
    }
    readExecutor.shutdownNow();
  }

  private static ContainerCommandRequestProto readBlockRequest() {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setContainerID(1)
        .setDatanodeUuid("dn")
        .setReadBlock(ReadBlockRequestProto.newBuilder()
            .setBlockID(DatanodeBlockID.newBuilder().setContainerID(1).setLocalID(1))
            .setOffset(0))
        .build();
  }

  /**
   * Mimics {@code KeyValueHandler.readBlockImpl}: open the block file on first use, then serve the request.
   * The optional hook runs while the request is in flight.
   */
  private ContainerDispatcher mockDispatcher(File blockFile, AtomicReference<RandomAccessFileChannel> channelRef,
      Runnable inFlight) throws Exception {
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    doAnswer(inv -> {
      RandomAccessFileChannel channel = inv.<ReadBlockContext>getArgument(2).getBlockFile();
      channelRef.set(channel);
      if (!channel.isOpen()) {
        channel.open(blockFile);
      }
      inFlight.run();
      return null;
    }).when(dispatcher).streamDataReadOnly(any(), any(), any(), any());
    return dispatcher;
  }

  @Test
  void idleStreamClosesBlockFileAndNextRequestReopensIt() throws Exception {
    File blockFile = Files.createFile(tempDir.resolve("block")).toFile();
    AtomicReference<RandomAccessFileChannel> channelRef = new AtomicReference<>();
    // Checked while the request is in flight: after onNext returns, the idle closer may already have run.
    AtomicReference<Boolean> openInFlight = new AtomicReference<>();
    Runnable recordOpen = () -> openInFlight.set(channelRef.get().isOpen());
    service = new GrpcXceiverService(mockDispatcher(blockFile, channelRef, recordOpen), IDLE_TIMEOUT, "test-");
    StreamObserver<ContainerCommandResponseProto> responseObserver = mock(StreamObserver.class);
    StreamObserver<ContainerCommandRequestProto> requestObserver = service.send(responseObserver);

    requestObserver.onNext(readBlockRequest());
    RandomAccessFileChannel channel = channelRef.get();
    assertNotNull(channel);
    assertTrue(openInFlight.get(), "block file should be open while a request is served");

    GenericTestUtils.waitFor(() -> !channel.isOpen(), 20, 5000);
    verify(responseObserver, never()).onError(any());
    verify(responseObserver, never()).onCompleted();

    openInFlight.set(null);
    requestObserver.onNext(readBlockRequest());
    assertTrue(openInFlight.get(), "next request should reopen the block file");
    GenericTestUtils.waitFor(() -> !channel.isOpen(), 20, 5000);

    requestObserver.onCompleted();
    assertFalse(channel.isOpen());
    verify(responseObserver).onCompleted();
  }

  @Test
  void inFlightRequestKeepsBlockFileOpen() throws Exception {
    File blockFile = Files.createFile(tempDir.resolve("block")).toFile();
    AtomicReference<RandomAccessFileChannel> channelRef = new AtomicReference<>();
    AtomicReference<Boolean> openDuringSlowRequest = new AtomicReference<>();
    Runnable slowRequest = () -> {
      // Stay in flight well past the idle timeout, then record whether the file survived.
      try {
        Thread.sleep(3 * IDLE_TIMEOUT.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      openDuringSlowRequest.set(channelRef.get().isOpen());
    };
    AtomicReference<Runnable> inFlight = new AtomicReference<>(() -> { });
    service = new GrpcXceiverService(mockDispatcher(blockFile, channelRef, () -> inFlight.get().run()),
        IDLE_TIMEOUT, "test-");
    StreamObserver<ContainerCommandRequestProto> requestObserver = service.send(mock(StreamObserver.class));

    // First request arms the idle check; the second one is still running when it fires.
    requestObserver.onNext(readBlockRequest());
    inFlight.set(slowRequest);
    requestObserver.onNext(readBlockRequest());
    assertTrue(openDuringSlowRequest.get(), "an in-flight request must not lose its block file");

    RandomAccessFileChannel channel = channelRef.get();
    GenericTestUtils.waitFor(() -> !channel.isOpen(), 20, 5000);
    requestObserver.onCompleted();
  }

  @Test
  void errorClosesBlockFileImmediately() throws Exception {
    File blockFile = Files.createFile(tempDir.resolve("block")).toFile();
    AtomicReference<RandomAccessFileChannel> channelRef = new AtomicReference<>();
    service = new GrpcXceiverService(mockDispatcher(blockFile, channelRef, () -> { }), Duration.ofHours(1), "test-");
    StreamObserver<ContainerCommandRequestProto> requestObserver = service.send(mock(StreamObserver.class));

    requestObserver.onNext(readBlockRequest());
    assertTrue(channelRef.get().isOpen());
    requestObserver.onError(new RuntimeException("client went away"));
    assertFalse(channelRef.get().isOpen(), "stream teardown must close the block file");
  }

  @Test
  void cancelledCallIsNotDispatched() {
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    service = new GrpcXceiverService(dispatcher, IDLE_TIMEOUT, "test-", metrics);
    StreamObserver<ContainerCommandResponseProto> responseObserver = mock(StreamObserver.class);
    StreamObserver<ContainerCommandRequestProto> requestObserver = service.send(responseObserver);

    // gRPC cancels the call context when the client deadline expires, even if the request is still queued.
    Context.CancellableContext call = Context.current().withCancellation();
    call.cancel(null);
    call.run(() -> requestObserver.onNext(readBlockRequest()));

    verify(dispatcher, never()).streamDataReadOnly(any(), any(), any(), any());
    verify(dispatcher, never()).dispatch(any(), any());
    verify(responseObserver, never()).onNext(any());
    assertCounter("SkippedCancelledRequests", 1L, getMetrics(metrics));
  }

  @Test
  void countsCallsEndedByDeadlineOrCancellation() {
    service = new GrpcXceiverService(mock(ContainerDispatcher.class), IDLE_TIMEOUT, "test-", metrics);

    // gRPC cancels the call context when a call ends; only the cause tells how it ended.
    endCall(null);
    endCall(new TimeoutException("context timed out"));
    endCall(Status.CANCELLED.asRuntimeException());
    endCall(Status.UNAVAILABLE.asRuntimeException());

    assertCounter("DeadlineExceededCalls", 1L, getMetrics(metrics));
    assertCounter("CancelledCalls", 2L, getMetrics(metrics));
  }

  private void endCall(Throwable cause) {
    Context.CancellableContext call = Context.current().withCancellation();
    call.run(() -> service.send(mock(StreamObserver.class)));
    call.cancel(cause);
  }
}

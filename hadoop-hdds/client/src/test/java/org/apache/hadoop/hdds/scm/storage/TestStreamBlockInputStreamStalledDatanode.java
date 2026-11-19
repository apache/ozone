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

package org.apache.hadoop.hdds.scm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A streaming read that times out on a stalled datanode and fails over must end the stalled gRPC call.
 * Runs a real gRPC server and a real {@link XceiverClientGrpc}: the first ReadBlock call stalls, every later
 * call answers at once.
 */
class TestStreamBlockInputStreamStalledDatanode {

  private static final long STALL_MILLIS = 2000;
  private static final byte[] DATA = {1, 2, 3, 4};

  /** Events seen by the server on the stalled (first) call, in order. */
  private final List<String> stalledCallEvents = new CopyOnWriteArrayList<>();
  private final CountDownLatch stalledCallEnded = new CountDownLatch(1);
  private final LogCapturer logs = LogCapturer.captureLogs(StreamBlockInputStream.class);
  private Server server;

  @AfterEach
  void stopServer() throws InterruptedException {
    logs.close();
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void failoverFromStalledDatanodeCancelsTheStalledCall() throws Exception {
    assertFailoverCancelsTheStalledCall(StreamBlockInputStream::read);
  }

  @Test
  void positionedReadFailoverFromStalledDatanodeCancelsTheStalledCall() throws Exception {
    assertFailoverCancelsTheStalledCall((sbis, buf) -> sbis.readPositioned(0, buf));
  }

  private interface ReadOperation {
    int read(StreamBlockInputStream sbis, ByteBuffer buf) throws IOException;
  }

  private void assertFailoverCancelsTheStalledCall(ReadOperation readOperation) throws Exception {
    final int port = GenericTestUtils.PortAllocator.getFreePort();
    server = NettyServerBuilder.forPort(port).addService(new StallFirstCallService()).build().start();

    // Two replicas served by the same test server; the first call (to the first replica) stalls.
    final DatanodeDetails dn1 = localDatanode(port);
    final DatanodeDetails dn2 = localDatanode(port);
    final Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(Arrays.asList(dn1, dn2))
        .build();

    final OzoneClientConfig clientConfig = new OzoneClientConfig();
    clientConfig.setChecksumVerify(false);
    clientConfig.setStreamReadPreReadSize(0);
    clientConfig.setStreamReadResponseDataSize(1024);
    clientConfig.setStreamReadTimeout(Duration.ofSeconds(1));
    clientConfig.setMaxReadRetryCount(1);
    clientConfig.setReadRetryInterval(0);

    try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, new OzoneConfiguration())) {
      client.connect();
      final XceiverClientFactory factory = mock(XceiverClientFactory.class);
      when(factory.acquireClientForReadData(any(Pipeline.class))).thenReturn(client);
      final BlockLocationInfo blockInfo = new BlockLocationInfo.Builder()
          .setBlockID(new BlockID(1L, 1L)).setLength(DATA.length).setPipeline(pipeline).build();

      try (StreamBlockInputStream sbis = new StreamBlockInputStream(blockInfo, pipeline, null, factory,
          b -> blockInfo, clientConfig)) {
        final ByteBuffer buf = ByteBuffer.allocate(DATA.length);
        assertEquals(DATA.length, readOperation.read(sbis, buf), "read should fail over to the second replica");
        assertArrayEquals(DATA, buf.array());
        // Keep the input stream open: the failed-over call must be ended by the failover, not by close().
        assertTrue(stalledCallEnded.await(STALL_MILLIS * 2, TimeUnit.MILLISECONDS), "stalled call did not end");
      }
    }

    // Without the cancel the client only half-closes the call: the server sees that after the stalled request
    // returns, and by then it has sent its reply to the abandoned reader.
    assertThat(stalledCallEvents).contains("server: cancelled");
    assertThat(stalledCallEvents).doesNotContain("server: late reply sent");
    assertThat(logs.getOutput()).doesNotContain("Failed to setCompleted");
  }

  private static DatanodeDetails localDatanode(int port) {
    return MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID(),
        GenericTestUtils.PortAllocator.HOSTNAME, GenericTestUtils.PortAllocator.HOST_ADDRESS, null, port);
  }

  /** Stands in for a datanode whose first ReadBlock call is stuck, e.g. on a slow disk, for STALL_MILLIS. */
  private final class StallFirstCallService
      extends XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase {
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public StreamObserver<ContainerCommandRequestProto> send(
        StreamObserver<ContainerCommandResponseProto> responseObserver) {
      final boolean stalled = calls.getAndIncrement() == 0;
      final ServerCallStreamObserver<ContainerCommandResponseProto> call =
          (ServerCallStreamObserver<ContainerCommandResponseProto>) responseObserver;
      if (stalled) {
        call.setOnCancelHandler(() -> {
          stalledCallEvents.add("server: cancelled");
          stalledCallEnded.countDown();
        });
      }
      return new StreamObserver<ContainerCommandRequestProto>() {
        @Override
        public void onNext(ContainerCommandRequestProto request) {
          if (!stalled) {
            responseObserver.onNext(readBlockResponse());
            return;
          }
          stalledCallEvents.add("server: request");
          try {
            Thread.sleep(STALL_MILLIS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          if (call.isCancelled()) {
            stalledCallEvents.add("server: late reply dropped, call cancelled");
          } else {
            responseObserver.onNext(readBlockResponse());
            stalledCallEvents.add("server: late reply sent");
          }
        }

        @Override
        public void onError(Throwable t) {
          if (stalled) {
            stalledCallEvents.add("server: onError " + t);
          }
        }

        @Override
        public void onCompleted() {
          if (stalled) {
            stalledCallEvents.add("server: client half-closed");
            stalledCallEnded.countDown();
          }
          responseObserver.onCompleted();
        }
      };
    }
  }

  private static ContainerCommandResponseProto readBlockResponse() {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(ContainerProtos.Type.ReadBlock)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setReadBlock(ReadBlockResponseProto.newBuilder()
            .setOffset(0)
            .setData(ByteString.copyFrom(DATA))
            .addChunkInfoList(ContainerProtos.ChunkInfo.newBuilder()
                .setChunkName("chunk").setOffset(0).setLen(DATA.length)
                .setChecksumData(ContainerProtos.ChecksumData.newBuilder()
                    .setType(ContainerProtos.ChecksumType.NONE)
                    .setBytesPerChecksum(DATA.length))))
        .build();
  }
}

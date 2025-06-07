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

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * UNIT test for BlockOutputStream.
 * <p>
 * Compares bytes written to the stream and received in the ChunkWriteRequests.
 */
class TestBlockOutputStreamCorrectness {

  private static final int DATA_SIZE = 256 * (int) OzoneConsts.MB;
  private static final byte[] DATA = RandomUtils.secure().randomBytes(DATA_SIZE);

  @ParameterizedTest
  @ValueSource(ints = { 1, 1024, 1024 * 1024 })
  void test(final int writeSize) throws IOException {
    assertEquals(0, DATA_SIZE % writeSize);

    final BufferPool bufferPool = new BufferPool(4 * 1024 * 1024, 32 / 4);

    for (int block = 0; block < 10; block++) {
      try (BlockOutputStream outputStream = createBlockOutputStream(bufferPool)) {
        for (int i = 0; i < DATA_SIZE / writeSize; i++) {
          if (writeSize > 1) {
            outputStream.write(DATA, i * writeSize, writeSize);
          } else {
            outputStream.write(DATA[i]);
          }
        }
      }
    }
  }

  /**
   * Tests an EC offline reconstruction scenario in which none of the ChunkInfo in an EC stripe have stripeChecksum.
   * Such ChunkInfo will exist for any EC data that was written in a version in which the ChunkInfo protobuf message did
   * not have the stripeChecksum field. Here, we assert that executePutBlock during reconstruction does not throw an
   * exception because of missing stripeChecksum. This essentially tests compatibility between an Ozone version that
   * did not have stripeChecksum and a version that has stripeChecksum.
   */
  @Test
  public void testMissingStripeChecksumDoesNotMakeExecutePutBlockFailDuringECReconstruction() throws IOException {
    // setup some parameters required for creating ECBlockOutputStream
    OzoneClientConfig config = new OzoneClientConfig();
    ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
    BlockID blockID = new BlockID(1, 1);
    DatanodeDetails datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(datanodeDetails.getID())
        .setReplicationConfig(replicationConfig)
        .setNodes(ImmutableList.of(datanodeDetails))
        .setState(Pipeline.PipelineState.CLOSED)
        // we'll executePutBlock for the parity index 5 because stripeChecksum is written to either the first or the
        // parity indexes
        .setReplicaIndexes(ImmutableMap.of(datanodeDetails, 5)).build();

    BlockLocationInfo locationInfo = new BlockLocationInfo.Builder()
        .setBlockID(blockID)
        .setOffset(1)
        .setLength(10)
        .setPipeline(pipeline).build();

    /*
    The array of BlockData contains metadata about blocks and their chunks, and is read in executePutBlock. In
    this test, we deliberately don't write stripeChecksum to any chunk. The expectation is that executePutBlock
    should not throw an exception because of missing stripeChecksum.
     */
    BlockData[] blockData = createBlockDataWithoutStripeChecksum(blockID, replicationConfig);
    try (ECBlockOutputStream ecBlockOutputStream = createECBlockOutputStream(config, replicationConfig, blockID,
        pipeline)) {
      Assertions.assertDoesNotThrow(() -> ecBlockOutputStream.executePutBlock(true, true, locationInfo.getLength(),
          blockData));
    }
  }

  /**
   * Creates a BlockData array with {@link ECReplicationConfig#getRequiredNodes()} number of elements.
   */
  private BlockData[] createBlockDataWithoutStripeChecksum(BlockID blockID, ECReplicationConfig replicationConfig) {
    int requiredNodes = replicationConfig.getRequiredNodes();
    BlockData[] blockDataArray = new BlockData[requiredNodes];

    // add just one ChunkInfo to each BlockData.
    for (int i = 0; i < requiredNodes; i++) {
      BlockData data = new BlockData(blockID);
      // create a ChunkInfo with no stripeChecksum
      ChunkInfo chunkInfo = new ChunkInfo("abc", 0, 10);
      data.addChunk(chunkInfo.getProtoBufMessage());
      blockDataArray[i] = data;
    }
    return blockDataArray;
  }

  private BlockOutputStream createBlockOutputStream(BufferPool bufferPool)
      throws IOException {

    final Pipeline pipeline = MockPipeline.createRatisPipeline();

    final XceiverClientManager xcm = mock(XceiverClientManager.class);
    when(xcm.acquireClient(any()))
        .thenReturn(new MockXceiverClientSpi(pipeline));

    OzoneClientConfig config = new OzoneClientConfig();
    config.setStreamBufferSize(4 * 1024 * 1024);
    config.setStreamBufferMaxSize(32 * 1024 * 1024);
    config.setStreamBufferFlushDelay(true);
    config.setStreamBufferFlushSize(16 * 1024 * 1024);
    config.setChecksumType(ChecksumType.NONE);
    config.setBytesPerChecksum(256 * 1024);
    StreamBufferArgs streamBufferArgs =
        StreamBufferArgs.getDefaultStreamBufferArgs(pipeline.getReplicationConfig(), config);

    return new RatisBlockOutputStream(
        new BlockID(1L, 1L),
        -1,
        xcm,
        pipeline,
        bufferPool,
        config,
        null,
        ContainerClientMetrics.acquire(),
        streamBufferArgs,
        () -> newFixedThreadPool(10));
  }

  private ECBlockOutputStream createECBlockOutputStream(OzoneClientConfig clientConfig,
      ECReplicationConfig repConfig, BlockID blockID, Pipeline pipeline) throws IOException {
    final XceiverClientManager xcm = mock(XceiverClientManager.class);
    when(xcm.acquireClient(any()))
        .thenReturn(new MockXceiverClientSpi(pipeline));

    ContainerClientMetrics clientMetrics = ContainerClientMetrics.acquire();
    StreamBufferArgs streamBufferArgs =
        StreamBufferArgs.getDefaultStreamBufferArgs(repConfig, clientConfig);

    return new ECBlockOutputStream(blockID, xcm, pipeline, BufferPool.empty(), clientConfig, null,
        clientMetrics, streamBufferArgs, () -> newFixedThreadPool(2));
  }

  /**
   * XCeiverClient which simulates responses.
   */
  private static class MockXceiverClientSpi extends XceiverClientSpi {

    private final Pipeline pipeline;

    private final AtomicInteger counter = new AtomicInteger();
    private int i;

    MockXceiverClientSpi(Pipeline pipeline) {
      super();
      this.pipeline = pipeline;
    }

    @Override
    public void connect() {

    }

    @Override
    public void close() {

    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) {

      if (!request.hasVersion()) {
        request = ContainerCommandRequestProto.newBuilder(request)
            .setVersion(ClientVersion.CURRENT.toProtoValue()).build();
      }
      final ContainerCommandResponseProto.Builder builder =
          ContainerCommandResponseProto.newBuilder()
              .setResult(Result.SUCCESS)
              .setCmdType(request.getCmdType());

      switch (request.getCmdType()) {
      case PutBlock:
        builder.setPutBlock(PutBlockResponseProto.newBuilder()
            .setCommittedBlockLength(
                GetCommittedBlockLengthResponseProto.newBuilder()
                    .setBlockID(
                        request.getPutBlock().getBlockData().getBlockID())
                    .setBlockLength(
                        request.getPutBlock().getBlockData().getSize())
                    .build())
            .build());
        break;
      case WriteChunk:
        ByteString data = request.getWriteChunk().getData();
        final byte[] writePayload = data.toByteArray();
        for (byte b : writePayload) {
          assertEquals(DATA[i], b);
          ++i;
        }
        break;
      default:
        //no-op
      }
      final XceiverClientReply result = new XceiverClientReply(
          CompletableFuture.completedFuture(builder.build()));
      result.setLogIndex(counter.incrementAndGet());
      return result;

    }

    @Override
    public ReplicationType getPipelineType() {
      return null;
    }

    @Override
    public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
      final ContainerCommandResponseProto.Builder builder =
          ContainerCommandResponseProto.newBuilder()
              .setCmdType(Type.WriteChunk)
              .setResult(Result.SUCCESS);
      final XceiverClientReply xceiverClientReply = new XceiverClientReply(
          CompletableFuture.completedFuture(builder.build()));
      xceiverClientReply.setLogIndex(index);
      return CompletableFuture.completedFuture(xceiverClientReply);
    }

    @Override
    public long getReplicatedMinCommitIndex() {
      return 0;
    }

    @Override
    public Map<DatanodeDetails, ContainerCommandResponseProto>
        sendCommandOnAllNodes(ContainerCommandRequestProto request) {
      return null;
    }
  }

}

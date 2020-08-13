package org.apache.hadoop.ozone.genesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.Builder;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.scm.storage.BufferPool;

import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

@State(Scope.Thread)
public class BenchmarkBlockOutputStream {

  private final int writeUnit = 1204;
  private BlockOutputStream blockOutputStream;
  private byte[] dataToWrite;

  @Setup()
  public void init() throws IOException {
    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());

    final Pipeline pipeline = new Builder()
        .setFactor(ReplicationFactor.THREE)
        .setType(ReplicationType.RATIS)
        .setState(PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setNodes(nodes)
        .build();

    final XceiverClientManager xcm = Mockito.mock(XceiverClientManager.class);
    Mockito.when(xcm.acquireClient(Mockito.any()))
        .thenReturn(new MockXceiverClientSpi(pipeline));

    blockOutputStream = new BlockOutputStream(
        new BlockID(1L, 1L),
        xcm,
        pipeline,
        4 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
        32 * 1024 * 1024,
        new BufferPool(4 * 1024 * 1024, 32 / 4),
        ChecksumType.NONE,
        256 * 1024);

    dataToWrite = new byte[writeUnit];
    new Random().nextBytes(dataToWrite);

  }

  @TearDown
  public void tearDown() throws IOException {
    blockOutputStream.close();
  }

  @Benchmark
  @Threads(1)
  public void writeBlock() throws IOException {
    int iteration = 1 * 1024 * 1024 / writeUnit;
    for (int i = 0; i < iteration; i++) {
      blockOutputStream.write(dataToWrite);
    }
  }

  private static class MockXceiverClientSpi extends XceiverClientSpi {

    private final Pipeline pipeline;

    private final AtomicInteger counter = new AtomicInteger();

    public MockXceiverClientSpi(Pipeline pipeline) {
      super();
      this.pipeline = pipeline;
    }

    @Override
    public void connect() throws Exception {

    }

    @Override
    public void connect(String encodedToken) throws Exception {

    }

    @Override
    public void close() {

    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request)
        throws IOException, ExecutionException, InterruptedException {

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
    public XceiverClientReply watchForCommit(long index)
        throws InterruptedException, ExecutionException, TimeoutException,
        IOException {
      final ContainerCommandResponseProto.Builder builder =
          ContainerCommandResponseProto.newBuilder()
              .setCmdType(Type.WriteChunk)
              .setResult(Result.SUCCESS);
      final XceiverClientReply xceiverClientReply = new XceiverClientReply(
          CompletableFuture.completedFuture(builder.build()));
      xceiverClientReply.setLogIndex(index);
      return xceiverClientReply;
    }

    @Override
    public long getReplicatedMinCommitIndex() {
      return 0;
    }
  }

}

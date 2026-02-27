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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput.PUT_BLOCK_REQUEST_LENGTH_MAX;
import static org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput.executePutBlockClose;
import static org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput.getProtoLength;
import static org.apache.hadoop.ozone.container.keyvalue.impl.KeyValueStreamDataChannel.writeBuffers;
import static org.apache.hadoop.ozone.container.keyvalue.impl.KeyValueStreamDataChannel.writeFully;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.impl.KeyValueStreamDataChannel.WriteMethod;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.io.FilePositionCount;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.ReferenceCountedObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** For testing {@link KeyValueStreamDataChannel}. */
public class TestKeyValueStreamDataChannel {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyValueStreamDataChannel.class);

  private static final ContainerCommandRequestProto PUT_BLOCK_PROTO
      = ContainerCommandRequestProto.newBuilder()
      .setCmdType(Type.PutBlock)
      .setPutBlock(PutBlockRequestProto.newBuilder().setBlockData(
          BlockData.newBuilder().setBlockID(DatanodeBlockID.newBuilder()
              .setContainerID(222).setLocalID(333).build()).build()))
      .setDatanodeUuid("datanodeId")
      .setContainerID(111L)
      .setVersion(ClientVersion.CURRENT.serialize())
      .build();
  static final int PUT_BLOCK_PROTO_SIZE = PUT_BLOCK_PROTO.toByteString().size();

  static {
    LOG.info("PUT_BLOCK_PROTO_SIZE = {}", PUT_BLOCK_PROTO_SIZE);
  }

  @Test
  public void testSerialization() throws Exception {
    final int max = PUT_BLOCK_REQUEST_LENGTH_MAX;
    final ByteBuffer putBlockBuf = ContainerCommandRequestMessage.toMessage(
        PUT_BLOCK_PROTO, null).getContent().asReadOnlyByteBuffer();
    final ByteBuffer protoLengthBuf = getProtoLength(putBlockBuf, max);

    // random data size
    final int dataSize = ThreadLocalRandom.current().nextInt(1000) + 100;
    final byte[] data = new byte[dataSize];

    //serialize
    final ByteBuf buf = Unpooled.buffer(max);
    buf.writeBytes(data);
    buf.writeBytes(putBlockBuf);
    buf.writeBytes(protoLengthBuf);

    final ContainerCommandRequestProto proto = readPutBlockRequest(buf);
    assertEquals(PUT_BLOCK_PROTO, proto);
  }

  static ContainerCommandRequestProto readPutBlockRequest(ByteBuf b) throws IOException {
    //   readerIndex   protoIndex   lengthIndex    readerIndex+readableBytes
    //         V            V             V                              V
    // format: |--- data ---|--- proto ---|--- proto length (4 bytes) ---|
    final int readerIndex = b.readerIndex();
    final int lengthIndex = readerIndex + b.readableBytes() - 4;
    final int protoLength = KeyValueStreamDataChannel.readProtoLength(b.duplicate(), lengthIndex);
    final int protoIndex = lengthIndex - protoLength;

    final ContainerCommandRequestProto proto;
    try {
      proto = readPutBlockRequest(b.slice(protoIndex, protoLength).nioBuffer());
    } catch (Throwable t) {
      RatisHelper.debug(b, "catch", LOG);
      throw new IOException("Failed to readPutBlockRequest from " + b
          + ": readerIndex=" + readerIndex
          + ", protoIndex=" + protoIndex
          + ", protoLength=" + protoLength
          + ", lengthIndex=" + lengthIndex, t);
    }

    // set index for reading data
    b.writerIndex(protoIndex);

    return proto;
  }

  private static ContainerCommandRequestProto readPutBlockRequest(ByteBuffer b)
      throws IOException {
    RatisHelper.debug(b, "readPutBlockRequest", LOG);
    final ByteString byteString = ByteString.copyFrom(b);

    final ContainerCommandRequestProto request =
        ContainerCommandRequestMessage.toProto(byteString, null);

    if (!request.hasPutBlock()) {
      throw new StorageContainerException(
          "Malformed PutBlock request. trace ID: " + request.getTraceID(),
          Result.MALFORMED_REQUEST);
    }
    return request;
  }

  @Test
  public void testVolumeFullCase() throws Exception {
    File tempFile = File.createTempFile("test-kv-stream", ".tmp");
    tempFile.deleteOnExit();
    HddsVolume mockVolume = mock(HddsVolume.class);
    when(mockVolume.getStorageID()).thenReturn("storageId");
    when(mockVolume.getCurrentUsage()).thenReturn(new SpaceUsageSource.Fixed(100L, 0L, 100L));
    ContainerData mockContainerData = mock(ContainerData.class);
    when(mockContainerData.getContainerID()).thenReturn(123L);
    when(mockContainerData.getVolume()).thenReturn(mockVolume);
    ContainerMetrics mockMetrics = mock(ContainerMetrics.class);
    KeyValueStreamDataChannel writeChannel = new KeyValueStreamDataChannel(tempFile, mockContainerData, mockMetrics);
    assertThrows(StorageContainerException.class,
        () -> writeChannel.assertSpaceAvailability(1));
    final ByteBuffer putBlockBuf = ContainerCommandRequestMessage.toMessage(
        PUT_BLOCK_PROTO, null).getContent().asReadOnlyByteBuffer();
    ReferenceCountedObject<ByteBuffer> wrap = ReferenceCountedObject.wrap(putBlockBuf);
    wrap.retain();
    assertThrows(StorageContainerException.class, () -> writeChannel.write(wrap));
    wrap.release();
  }

  @Test
  public void testBuffers() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(32);
    final List<CompletableFuture<String>> futures = new ArrayList<>();

    final int min = PUT_BLOCK_PROTO_SIZE + 4;
    final int[] maxValues = {min, 2 * min, 10 * min};
    final int[] dataSizes = {0, 10, 100, 10_000};
    for (int max : maxValues) {
      for (int dataSize : dataSizes) {
        futures.add(CompletableFuture.supplyAsync(
            () -> runTestBuffers(dataSize, max), executor));
      }
    }

    for (CompletableFuture<String> f : futures) {
      f.get();
    }
  }

  static String runTestBuffers(int dataSize, int max) {
    final int seed = ThreadLocalRandom.current().nextInt();
    final String name = String.format("[dataSize=%d,max=%d,seed=%H]",
        dataSize, max, seed);
    LOG.info(name);
    try {
      runTestBuffers(dataSize, max, seed, name);
    } catch (Throwable t) {
      throw new CompletionException("Failed " + name, t);
    }
    return name;
  }

  static void runTestBuffers(int dataSize, int max, int seed, String name)
      throws Exception {
    assertThat(max).isGreaterThanOrEqualTo(PUT_BLOCK_PROTO_SIZE);

    // random data
    final byte[] data = RandomUtils.secure().randomBytes(dataSize);

    // write output
    final Buffers buffers = new Buffers(max);
    final Output out = new Output(buffers);
    for (int offset = 0; offset < dataSize;) {
      final int randomLength = RandomUtils.secure().randomInt(1, 4 * max);
      final int length = Math.min(randomLength, dataSize - offset);
      LOG.info("{}: offset = {}, length = {}", name, offset, length);
      final ByteBuffer b = ByteBuffer.wrap(data, offset, length);
      final DataStreamReply writeReply = out.writeAsync(b).get();
      assertReply(writeReply, length, null);
      offset += length;
    }

    // close
    final DataStreamReply closeReply = executePutBlockClose(
        PUT_BLOCK_PROTO, max, out).get();
    assertReply(closeReply, 0, PUT_BLOCK_PROTO);

    // check output
    final ByteBuf outBuf = out.getOutBuf();
    LOG.info("outBuf = {}", outBuf);
    assertEquals(dataSize, outBuf.readableBytes());
    for (int i = 0; i < dataSize; i++) {
      assertEquals(data[i], outBuf.readByte());
    }
    outBuf.release();
  }

  static void assertReply(DataStreamReply reply, int byteWritten,
      ContainerCommandRequestProto proto) {
    assertTrue(reply.isSuccess());
    assertEquals(byteWritten, reply.getBytesWritten());
    assertEquals(proto, ((Reply)reply).getPutBlockRequest());
  }

  static class Output implements DataStreamOutput {
    private final Buffers buffers;
    private final ByteBuf outBuf = Unpooled.buffer();
    private final WriteMethod writeMethod = src -> {
      final int remaining = src.remaining();
      outBuf.writeBytes(src);
      return remaining;
    };

    Output(Buffers buffers) {
      this.buffers = buffers;
    }

    ByteBuf getOutBuf() {
      return outBuf;
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(
        ByteBuffer src, Iterable<WriteOption> writeOptions) {
      final int written;
      try {
        written = writeBuffers(
            ReferenceCountedObject.wrap(src, () -> { }, () -> { }),
            buffers, writeMethod);
      } catch (IOException e) {
        return completeExceptionally(e);
      }
      if (WriteOption.containsOption(writeOptions, StandardWriteOption.CLOSE)) {
        return closeAsync();
      }
      return CompletableFuture.completedFuture(
          new Reply(true, written));
    }

    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      final ContainerCommandRequestProto putBlockRequest;
      try {
        putBlockRequest = closeBuffers(buffers, writeMethod);
      } catch (IOException e) {
        return completeExceptionally(e);
      }
      return CompletableFuture.completedFuture(
          new Reply(true, 0, putBlockRequest));
    }

    static ContainerCommandRequestProto closeBuffers(
        Buffers buffers, WriteMethod writeMethod) throws IOException {
      final ReferenceCountedObject<ByteBuf> ref = buffers.pollAll();
      final ByteBuf buf = ref.retain();
      final ContainerCommandRequestProto putBlockRequest;
      try {
        putBlockRequest = readPutBlockRequest(buf);
        // write the remaining data
        writeFully(buf.nioBuffer(), writeMethod);
      } finally {
        ref.release();
      }
      return putBlockRequest;
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(
        FilePositionCount filePositionCount, WriteOption... writeOptions) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RaftClientReply> getRaftClientReplyFuture() {
      throw new UnsupportedOperationException();
    }

    @Override
    public WritableByteChannel getWritableByteChannel() {
      throw new UnsupportedOperationException();
    }
  }

  static class Reply implements DataStreamReply {
    private final boolean success;
    private final long bytesWritten;
    private final ContainerCommandRequestProto putBlockRequest;

    Reply(boolean success, long bytesWritten) {
      this(success, bytesWritten, null);
    }

    Reply(boolean success, long bytesWritten,
        ContainerCommandRequestProto putBlockRequest) {
      this.success = success;
      this.bytesWritten = bytesWritten;
      this.putBlockRequest = putBlockRequest;
    }

    ContainerCommandRequestProto getPutBlockRequest() {
      return putBlockRequest;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public long getBytesWritten() {
      return bytesWritten;
    }

    @Override
    public Collection<CommitInfoProto> getCommitInfos() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ClientId getClientId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public DataStreamPacketHeaderProto.Type getType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getStreamId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getStreamOffset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getDataLength() {
      throw new UnsupportedOperationException();
    }
  }

  static CompletableFuture<DataStreamReply> completeExceptionally(Throwable t) {
    final CompletableFuture<DataStreamReply> f = new CompletableFuture<>();
    f.completeExceptionally(t);
    return f;
  }
}

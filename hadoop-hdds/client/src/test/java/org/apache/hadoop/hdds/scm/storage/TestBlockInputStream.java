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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.scm.storage.TestChunkInputStream.generateRandomData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.primitives.Bytes;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.event.Level;

/**
 * Tests for {@link BlockInputStream}'s functionality.
 */
public class TestBlockInputStream {

  private static final int CHUNK_SIZE = 100;

  private Checksum checksum;
  private BlockInputStream blockStream;
  private byte[] blockData;
  private int blockSize;
  private List<ChunkInfo> chunks;
  private Map<String, byte[]> chunkDataMap;

  private Function<BlockID, BlockLocationInfo> refreshFunction;

  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    refreshFunction = mock(Function.class);
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    checksum = new Checksum(ChecksumType.NONE, CHUNK_SIZE);
    createChunkList(5);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);

    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    blockStream = new DummyBlockInputStream(blockID, blockSize, pipeline, null,
        null, refreshFunction, chunks, chunkDataMap, clientConfig);
  }

  /**
   * Create a mock list of chunks. The first n-1 chunks of length CHUNK_SIZE
   * and the last chunk with length CHUNK_SIZE/2.
   */
  private void createChunkList(int numChunks)
      throws Exception {

    chunks = new ArrayList<>(numChunks);
    chunkDataMap = new HashMap<>();
    blockData = new byte[0];
    int i, chunkLen;
    byte[] byteData;
    String chunkName;

    for (i = 0; i < numChunks; i++) {
      chunkName = "chunk-" + i;
      chunkLen = CHUNK_SIZE;
      if (i == numChunks - 1) {
        chunkLen = CHUNK_SIZE / 2;
      }
      byteData = generateRandomData(chunkLen);
      ChunkInfo chunkInfo = ChunkInfo.newBuilder()
          .setChunkName(chunkName)
          .setOffset(0)
          .setLen(chunkLen)
          .setChecksumData(checksum.computeChecksum(
              byteData, 0, chunkLen).getProtoBufMessage())
          .build();

      chunkDataMap.put(chunkName, byteData);
      chunks.add(chunkInfo);

      blockSize += chunkLen;
      blockData = Bytes.concat(blockData, byteData);
    }
  }

  private void seekAndVerify(int pos) throws Exception {
    blockStream.seek(pos);
    assertEquals(pos, blockStream.getPos(),
        "Current position of buffer does not match with the sought position");
  }

  /**
   * Match readData with the chunkData byte-wise.
   * @param readData Data read through ChunkInputStream
   * @param inputDataStartIndex first index (inclusive) in chunkData to compare
   *                            with read data
   * @param length the number of bytes of data to match starting from
   *               inputDataStartIndex
   */
  private void matchWithInputData(byte[] readData, int inputDataStartIndex,
      int length) {
    for (int i = inputDataStartIndex; i < inputDataStartIndex + length; i++) {
      assertEquals(blockData[i], readData[i - inputDataStartIndex]);
    }
  }

  @Test
  public void testSeek() throws Exception {
    // Seek to position 0
    int pos = 0;
    seekAndVerify(pos);
    assertEquals(0, blockStream.getChunkIndex(), "ChunkIndex is incorrect");

    // Before BlockInputStream is initialized (initialization happens during
    // read operation), seek should update the BlockInputStream#blockPosition
    pos = CHUNK_SIZE;
    seekAndVerify(pos);
    assertEquals(0, blockStream.getChunkIndex(), "ChunkIndex is incorrect");
    assertEquals(pos, blockStream.getBlockPosition());

    // Initialize the BlockInputStream. After initialization, the chunkIndex
    // should be updated to correspond to the sought position.
    blockStream.initialize();
    assertEquals(1, blockStream.getChunkIndex(), "ChunkIndex is incorrect");

    pos = (CHUNK_SIZE * 4) + 5;
    seekAndVerify(pos);
    assertEquals(4, blockStream.getChunkIndex(), "ChunkIndex is incorrect");
    pos = blockSize + 10;

    int finalPos = pos;
    assertThrows(EOFException.class, () -> seekAndVerify(finalPos));

    // Seek to random positions between 0 and the block size.
    for (int i = 0; i < 10; i++) {
      pos = RandomUtils.secure().randomInt(0, blockSize);
      seekAndVerify(pos);
    }
  }

  @Test
  public void testRead() throws Exception {
    // read 200 bytes of data starting from position 50. Chunk0 contains
    // indices 0 to 99, chunk1 from 100 to 199 and chunk3 from 200 to 299. So
    // the read should result in 3 ChunkInputStream reads
    seekAndVerify(50);
    byte[] b = new byte[200];
    int bytesRead = blockStream.read(b, 0, 200);
    assertEquals(200, bytesRead, "Expected to read 200 bytes");
    matchWithInputData(b, 50, 200);

    // The new position of the blockInputStream should be the last index read
    // + 1.
    assertEquals(250, blockStream.getPos());
    assertEquals(2, blockStream.getChunkIndex());
  }

  @Test
  public void testReadWithByteBuffer() throws Exception {
    // read 200 bytes of data starting from position 50. Chunk0 contains
    // indices 0 to 99, chunk1 from 100 to 199 and chunk3 from 200 to 299. So
    // the read should result in 3 ChunkInputStream reads
    seekAndVerify(50);
    ByteBuffer buffer = ByteBuffer.allocate(200);
    blockStream.read(buffer);
    matchWithInputData(buffer.array(), 50, 200);

    // The new position of the blockInputStream should be the last index read
    // + 1.
    assertEquals(250, blockStream.getPos());
    assertEquals(2, blockStream.getChunkIndex());
  }

  @Test
  public void testReadWithDirectByteBuffer() throws Exception {
    // read 200 bytes of data starting from position 50. Chunk0 contains
    // indices 0 to 99, chunk1 from 100 to 199 and chunk3 from 200 to 299. So
    // the read should result in 3 ChunkInputStream reads
    seekAndVerify(50);
    ByteBuffer buffer = ByteBuffer.allocateDirect(200);
    blockStream.read(buffer);
    for (int i = 50; i < 50 + 200; i++) {
      assertEquals(blockData[i], buffer.get(i - 50));
    }

    // The new position of the blockInputStream should be the last index read
    // + 1.
    assertEquals(250, blockStream.getPos());
    assertEquals(2, blockStream.getChunkIndex());
  }

  @Test
  public void testSeekAndRead() throws Exception {
    // Seek to a position and read data
    seekAndVerify(50);
    byte[] b1 = new byte[100];
    int bytesRead1 = blockStream.read(b1, 0, 100);
    assertEquals(100, bytesRead1, "Expected to read 100 bytes");
    matchWithInputData(b1, 50, 100);

    // Next read should start from the position of the last read + 1 i.e. 100
    byte[] b2 = new byte[100];
    int bytesRead2 = blockStream.read(b2, 0, 100);
    assertEquals(100, bytesRead2, "Expected to read 100 bytes");
    matchWithInputData(b2, 150, 100);
  }

  @Test
  public void testRefreshPipelineFunction() throws Exception {
    LogCapturer logCapturer = LogCapturer.captureLogs(BlockExtendedInputStream.class);
    GenericTestUtils.setLogLevel(BlockExtendedInputStream.class, Level.DEBUG);
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    AtomicBoolean isRefreshed = new AtomicBoolean();
    createChunkList(5);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);

    try (BlockInputStream blockInputStreamWithRetry =
             new DummyBlockInputStreamWithRetry(blockID, blockSize,
                 MockPipeline.createSingleNodePipeline(), null,
                 null, chunks, chunkDataMap, isRefreshed, null,
                 clientConfig)) {
      assertFalse(isRefreshed.get());
      seekAndVerify(50);
      byte[] b = new byte[200];
      int bytesRead = blockInputStreamWithRetry.read(b, 0, 200);
      assertEquals(200, bytesRead, "Expected to read 200 bytes");
      assertThat(logCapturer.getOutput()).contains("Retry read after");
      assertTrue(isRefreshed.get());
    }
  }

  @ParameterizedTest
  @MethodSource("exceptionsTriggersRefresh")
  void refreshesPipelineOnReadFailure(IOException ex) throws Exception {
    // GIVEN
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    BlockLocationInfo blockLocationInfo = mock(BlockLocationInfo.class);
    when(blockLocationInfo.getPipeline()).thenReturn(pipeline);
    Pipeline newPipeline = MockPipeline.createSingleNodePipeline();
    BlockLocationInfo newBlockLocationInfo = mock(BlockLocationInfo.class);

    testRefreshesPipelineOnReadFailure(ex, blockLocationInfo,
        id -> newBlockLocationInfo);

    when(newBlockLocationInfo.getPipeline()).thenReturn(newPipeline);
    testRefreshesPipelineOnReadFailure(ex, blockLocationInfo,
        id -> blockLocationInfo);

    when(newBlockLocationInfo.getPipeline()).thenReturn(null);
    testRefreshesPipelineOnReadFailure(ex, blockLocationInfo,
        id -> newBlockLocationInfo);
  }

  private void testRefreshesPipelineOnReadFailure(IOException ex,
      BlockLocationInfo blockLocationInfo,
      Function<BlockID, BlockLocationInfo> refreshPipelineFunction)
      throws Exception {

    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));

    final int len = 200;
    final ChunkInputStream stream = throwingChunkInputStream(ex, len, true);

    when(this.refreshFunction.apply(any()))
        .thenAnswer(inv -> refreshPipelineFunction.apply(blockID));

    try (BlockInputStream subject = createSubject(blockID,
        blockLocationInfo.getPipeline(), stream)) {
      subject.initialize();

      // WHEN
      byte[] b = new byte[len];
      int bytesRead = subject.read(b, 0, len);

      // THEN
      assertEquals(len, bytesRead);
      verify(this.refreshFunction).apply(blockID);
    } finally {
      reset(this.refreshFunction);
    }
  }

  private static Stream<Arguments> exceptionsNotTriggerRefresh() {
    return Stream.of(
        Arguments.of(new SCMSecurityException("Security problem")),
        Arguments.of(new OzoneChecksumException("checksum missing")),
        Arguments.of(new IOException("Some random exception."))
    );
  }

  private static ChunkInputStream throwingChunkInputStream(IOException ex,
      int len, boolean succeedOnRetry) throws IOException {
    final ChunkInputStream stream = mock(ChunkInputStream.class);
    OngoingStubbing<Integer> stubbing =
        when(stream.read(any(), anyInt(), anyInt()))
            .thenThrow(ex);
    if (succeedOnRetry) {
      stubbing.thenReturn(len);
    }
    when(stream.getRemaining())
        .thenReturn((long) len);
    return stream;
  }

  private BlockInputStream createSubject(BlockID blockID, Pipeline pipeline,
      ChunkInputStream stream) throws IOException {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    return new DummyBlockInputStream(blockID, blockSize, pipeline, null,
        null, refreshFunction, chunks, null, clientConfig) {
      @Override
      protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
        return stream;
      }
    };
  }

  @ParameterizedTest
  @MethodSource("exceptionsNotTriggerRefresh")
  public void testReadNotRetriedOnOtherException(IOException ex)
      throws Exception {
    // GIVEN
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();

    final int len = ThreadLocalRandom.current().nextInt(100, 300);
    final ChunkInputStream stream = throwingChunkInputStream(ex, len, false);

    try (BlockInputStream subject = createSubject(blockID, pipeline, stream)) {
      subject.initialize();

      // WHEN
      assertThrows(ex.getClass(),
          () -> {
            byte[] buffer = new byte[len];
            int bytesRead = subject.read(buffer, 0, len);
            // This line should never be reached due to the exception
            assertEquals(len, bytesRead);
          });

      // THEN
      verify(refreshFunction, never()).apply(blockID);
    }
  }

  @ParameterizedTest
  @MethodSource("exceptionsTriggersRefresh")
  public void testRefreshOnReadFailureAfterUnbuffer(IOException ex)
      throws Exception {
    // GIVEN
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    Pipeline newPipeline = MockPipeline.createSingleNodePipeline();
    XceiverClientFactory clientFactory = mock(XceiverClientFactory.class);
    XceiverClientSpi client = mock(XceiverClientSpi.class);
    BlockLocationInfo blockLocationInfo = mock(BlockLocationInfo.class);
    when(clientFactory.acquireClientForReadData(pipeline))
        .thenReturn(client);

    final int len = 200;
    final ChunkInputStream stream = throwingChunkInputStream(ex, len, true);

    when(refreshFunction.apply(blockID))
        .thenReturn(blockLocationInfo);
    when(blockLocationInfo.getPipeline()).thenReturn(newPipeline);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    BlockInputStream subject = new BlockInputStream(
        new BlockLocationInfo(new BlockLocationInfo.Builder().setBlockID(blockID).setLength(blockSize)),
        pipeline, null, clientFactory, refreshFunction,
        clientConfig) {
      @Override
      protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
        return stream;
      }

      @Override
      protected ContainerProtos.BlockData getBlockDataUsingClient() throws IOException {
        BlockID blockID = getBlockID();
        ContainerProtos.DatanodeBlockID datanodeBlockID = blockID.getDatanodeBlockIDProtobuf();
        return ContainerProtos.BlockData.newBuilder().addAllChunks(chunks).setBlockID(datanodeBlockID).build();
      }
    };

    try {
      subject.initialize();
      subject.unbuffer();

      // WHEN
      byte[] b = new byte[len];
      int bytesRead = subject.read(b, 0, len);

      // THEN
      assertEquals(len, bytesRead);
      verify(refreshFunction).apply(blockID);
      verify(clientFactory).acquireClientForReadData(pipeline);
      verify(clientFactory).releaseClientForReadData(client, false);
    } finally {
      subject.close();
    }
  }

  private static Stream<Arguments> exceptionsTriggersRefresh() {
    return Stream.of(
        Arguments.of(new StorageContainerException(CONTAINER_NOT_FOUND)),
        Arguments.of(new IOException(new ExecutionException(
            new StatusException(Status.UNAVAILABLE))))
    );
  }
}

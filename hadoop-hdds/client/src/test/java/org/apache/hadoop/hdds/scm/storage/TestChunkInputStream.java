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

import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadChunkResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link ChunkInputStream}'s functionality.
 */
public class TestChunkInputStream {

  private static final int CHUNK_SIZE = 100;
  private static final int BYTES_PER_CHECKSUM = 20;
  private static final String CHUNK_NAME = "dummyChunk";
  private static final Random RANDOM = new Random();
  private static final AtomicLong CONTAINER_ID = new AtomicLong();

  private DummyChunkInputStream chunkStream;
  private BlockID blockID;
  private ChunkInfo chunkInfo;
  private byte[] chunkData;

  @BeforeEach
  public void setup() throws Exception {
    Checksum checksum = new Checksum(ChecksumType.CRC32, BYTES_PER_CHECKSUM);

    chunkData = generateRandomData(CHUNK_SIZE);

    blockID = new BlockID(CONTAINER_ID.incrementAndGet(), 0);

    chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(CHUNK_NAME)
        .setOffset(0)
        .setLen(CHUNK_SIZE)
        .setChecksumData(checksum.computeChecksum(
            chunkData, 0, CHUNK_SIZE).getProtoBufMessage())
        .build();

    chunkStream = new DummyChunkInputStream(chunkInfo, blockID, null, true,
        chunkData, null);
  }

  static byte[] generateRandomData(int length) {
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
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
      assertEquals(chunkData[i], readData[i - inputDataStartIndex]);
    }
  }

  private void matchWithInputData(List<ByteString> byteStrings,
      int inputDataStartIndex, int length) {
    int offset = inputDataStartIndex;
    int totalBufferLen = 0;
    for (ByteString byteString : byteStrings) {
      int bufferLen = byteString.size();
      matchWithInputData(byteString.toByteArray(), offset, bufferLen);
      offset += bufferLen;
      totalBufferLen += bufferLen;
    }
    assertEquals(length, totalBufferLen);
  }

  /**
   * Seek to a position and verify through getPos().
   */
  private void seekAndVerify(int pos) throws Exception {
    chunkStream.seek(pos);
    assertEquals(pos, chunkStream.getPos(),
        "Current position of buffer does not match with the sought position");
  }

  @Test
  public void testFullChunkRead() throws Exception {
    byte[] b = new byte[CHUNK_SIZE];
    int bytesRead = chunkStream.read(b, 0, CHUNK_SIZE);
    assertEquals(CHUNK_SIZE, bytesRead, "Expected to read full chunk size");
    matchWithInputData(b, 0, CHUNK_SIZE);
  }

  @Test
  public void testPartialChunkRead() throws Exception {
    int len = CHUNK_SIZE / 2;
    byte[] b = new byte[len];

    int bytesRead = chunkStream.read(b, 0, len);
    assertEquals(len, bytesRead, "Expected to read half chunk size");

    matchWithInputData(b, 0, len);

    // To read chunk data from index 0 to 49 (len = 50), we need to read
    // chunk from offset 0 to 60 as the checksum boundary is at every 20
    // bytes. Verify that 60 bytes of chunk data are read and stored in the
    // buffers. Since checksum boundary is at every 20 bytes, there should be
    // 60/20 number of buffers.
    matchWithInputData(chunkStream.getReadByteBuffers(), 0, 60);
  }

  @Test
  public void testSeek() throws Exception {
    seekAndVerify(0);
    EOFException eofException = assertThrows(EOFException.class, () ->  seekAndVerify(CHUNK_SIZE + 1));
    assertThat(eofException).hasMessage("EOF encountered at pos: " + (CHUNK_SIZE + 1) + " for chunk: " + CHUNK_NAME);

    // Seek before read should update the ChunkInputStream#chunkPosition
    seekAndVerify(25);
    assertEquals(25, chunkStream.getChunkPosition());

    // Read from the sought position.
    // Reading from index 25 to 54 should result in the ChunkInputStream
    // copying chunk data from index 20 to 59 into the buffers (checksum
    // boundaries).
    byte[] b = new byte[30];
    int bytesRead = chunkStream.read(b, 0, 30);
    assertEquals(30, bytesRead, "Expected to read 30 bytes");
    matchWithInputData(b, 25, 30);
    matchWithInputData(chunkStream.getReadByteBuffers(), 20, 40);

    // After read, the position of the chunkStream is evaluated from the
    // buffers and the chunkPosition should be reset to -1.
    assertEquals(-1, chunkStream.getChunkPosition());

    // Only the last BYTES_PER_CHECKSUM will be cached in the buffers as
    // buffers are released after each checksum boundary is read. So the
    // buffers should contain data from index 40 to 59.
    // Seek to a position within the cached buffers. ChunkPosition should
    // still not be used to set the position.
    seekAndVerify(45);
    assertEquals(-1, chunkStream.getChunkPosition());

    // Seek to a position outside the current cached buffers. In this case, the
    // chunkPosition should be updated to the seeked position.
    seekAndVerify(75);
    assertEquals(75, chunkStream.getChunkPosition());

    // Read upto checksum boundary should result in all the buffers being
    // released and hence chunkPosition updated with current position of chunk.
    seekAndVerify(25);
    b = new byte[15];
    int bytesRead2 = chunkStream.read(b, 0, 15);
    assertEquals(15, bytesRead2, "Expected to read 15 bytes");
    matchWithInputData(b, 25, 15);
    assertEquals(40, chunkStream.getChunkPosition());
  }

  @Test
  public void testSeekAndRead() throws Exception {
    // Seek to a position and read data
    seekAndVerify(50);
    byte[] b1 = new byte[20];
    int bytesRead1 = chunkStream.read(b1, 0, 20);
    assertEquals(20, bytesRead1, "Expected to read 20 bytes");
    matchWithInputData(b1, 50, 20);

    // Next read should start from the position of the last read + 1 i.e. 70
    byte[] b2 = new byte[20];
    int bytesRead2 = chunkStream.read(b2, 0, 20);
    assertEquals(20, bytesRead2, "Expected to read 20 bytes");
    matchWithInputData(b2, 70, 20);
  }

  @Test
  public void testUnbuffered() throws Exception {
    byte[] b1 = new byte[20];
    int bytesRead = chunkStream.read(b1, 0, 20);
    assertEquals(20, bytesRead, "Expected to read 20 bytes");
    matchWithInputData(b1, 0, 20);

    chunkStream.unbuffer();

    assertFalse(chunkStream.buffersAllocated());

    // Next read should start from the position of the last read + 1 i.e. 20
    byte[] b2 = new byte[20];
    int bytesRead2 = chunkStream.read(b2, 0, 20);
    assertEquals(20, bytesRead2, "Expected to read 20 bytes");
    matchWithInputData(b2, 20, 20);
  }

  @Test
  public void connectsToNewPipeline() throws Exception {
    // GIVEN
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    Pipeline newPipeline = MockPipeline.createSingleNodePipeline();

    Token<?> token = mock(Token.class);
    when(token.encodeToUrlString())
        .thenReturn("oldToken");
    Token<?> newToken = mock(Token.class);
    when(newToken.encodeToUrlString())
        .thenReturn("newToken");

    AtomicReference<Pipeline> pipelineRef = new AtomicReference<>(pipeline);
    AtomicReference<Token<?>> tokenRef = new AtomicReference<>(token);

    XceiverClientFactory clientFactory = mock(XceiverClientFactory.class);
    XceiverClientSpi client = mock(XceiverClientSpi.class);
    when(clientFactory.acquireClientForReadData(any()))
        .thenReturn(client);
    ArgumentCaptor<ContainerCommandRequestProto> requestCaptor =
        ArgumentCaptor.forClass(ContainerCommandRequestProto.class);
    when(client.getPipeline())
        .thenAnswer(invocation -> pipelineRef.get());
    when(client.sendCommand(requestCaptor.capture(), any()))
        .thenAnswer(invocation ->
            getReadChunkResponse(
                requestCaptor.getValue(),
                ChunkBuffer.wrap(ByteBuffer.wrap(chunkData)),
                ByteStringConversion::safeWrap));

    try (ChunkInputStream subject = new ChunkInputStream(chunkInfo, blockID,
        clientFactory, pipelineRef::get, false, tokenRef::get)) {
      // WHEN
      subject.unbuffer();
      pipelineRef.set(newPipeline);
      tokenRef.set(newToken);
      byte[] buffer = new byte[CHUNK_SIZE];
      int read = subject.read(buffer);

      // THEN
      assertEquals(CHUNK_SIZE, read);
      assertArrayEquals(chunkData, buffer);
      verify(clientFactory).acquireClientForReadData(newPipeline);
      verify(newToken).encodeToUrlString();
    }
  }
}

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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.primitives.Bytes;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TestStreamBlockInputStream}'s functionality.
 */
public class TestStreamBlockInputStream {
  private int blockSize;
  private static final int CHUNK_SIZE = 100;
  private static final int BYTES_PER_CHECKSUM = 20;
  private static final Random RANDOM = new Random();
  private DummyStreamBlockInputStream blockStream;
  private byte[] blockData;
  private List<ChunkInfo> chunks;
  private Map<String, byte[]> chunkDataMap;
  private Checksum checksum;
  private BlockID blockID;
  private static final String CHUNK_NAME = "chunk-";
  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  public void setup() throws Exception {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamReadBlock(true);
    blockID = new BlockID(new ContainerBlockID(1, 1));
    checksum = new Checksum(ChecksumType.CRC32, BYTES_PER_CHECKSUM);
    createChunkList(5);

    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    blockStream = new DummyStreamBlockInputStream(blockID, blockSize, pipeline,
        null, null, mock(Function.class), clientConfig, chunks, chunkDataMap);
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
      chunkName = CHUNK_NAME + i;
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
      assertEquals(blockData[i], readData[i - inputDataStartIndex], "i: " + i);
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
    blockStream.seek(pos);
    assertEquals(pos, blockStream.getPos(),
        "Current position of buffer does not match with the sought position");
  }

  @Test
  public void testFullChunkRead() throws Exception {
    byte[] b = new byte[blockSize];
    int numBytesRead = blockStream.read(b, 0, blockSize);
    assertEquals(blockSize, numBytesRead);
    matchWithInputData(b, 0, blockSize);
  }

  @Test
  public void testPartialChunkRead() throws Exception {
    int len = blockSize / 2;
    byte[] b = new byte[len];

    int numBytesRead = blockStream.read(b, 0, len);
    assertEquals(len, numBytesRead);
    matchWithInputData(b, 0, len);

    // To read block data from index 0 to 225 (len = 225), we need to read
    // chunk from offset 0 to 240 as the checksum boundary is at every 20
    // bytes. Verify that 60 bytes of chunk data are read and stored in the
    // buffers. Since checksum boundary is at every 20 bytes, there should be
    // 240/20 number of buffers.
    matchWithInputData(blockStream.getReadByteBuffers(), 0, 240);
  }

  @Test
  public void testSeek() throws Exception {
    seekAndVerify(0);
    EOFException eofException = assertThrows(EOFException.class, () ->  seekAndVerify(blockSize + 1));
    assertThat(eofException).hasMessage("EOF encountered at pos: " + (blockSize + 1) + " for block: " + blockID);

    // Seek before read should update the BlockInputStream#blockPosition
    seekAndVerify(25);

    // Read from the sought position.
    // Reading from index 25 to 54 should result in the BlockInputStream
    // copying chunk data from index 20 to 59 into the buffers (checksum
    // boundaries).
    byte[] b = new byte[30];
    int numBytesRead = blockStream.read(b, 0, 30);
    assertEquals(30, numBytesRead);
    matchWithInputData(b, 25, 30);
    matchWithInputData(blockStream.getReadByteBuffers(), 20, 40);

    // After read, the position of the blockStream is evaluated from the
    // buffers and the chunkPosition should be reset to -1.

    // Only the last BYTES_PER_CHECKSUM will be cached in the buffers as
    // buffers are released after each checksum boundary is read. So the
    // buffers should contain data from index 40 to 59.
    // Seek to a position within the cached buffers. BlockPosition should
    // still not be used to set the position.
    seekAndVerify(45);

    // Seek to a position outside the current cached buffers. In this case, the
    // chunkPosition should be updated to the seeked position.
    seekAndVerify(75);

    // Read upto checksum boundary should result in all the buffers being
    // released and hence chunkPosition updated with current position of chunk.
    seekAndVerify(25);
    b = new byte[15];
    numBytesRead = blockStream.read(b, 0, 15);
    assertEquals(15, numBytesRead);
    matchWithInputData(b, 25, 15);
  }

  @Test
  public void testSeekAndRead() throws Exception {
    // Seek to a position and read data
    seekAndVerify(50);
    byte[] b1 = new byte[20];
    int numBytesRead = blockStream.read(b1, 0, 20);
    assertEquals(20, numBytesRead);
    matchWithInputData(b1, 50, 20);

    // Next read should start from the position of the last read + 1 i.e. 70
    byte[] b2 = new byte[20];
    numBytesRead = blockStream.read(b2, 0, 20);
    assertEquals(20, numBytesRead);
    matchWithInputData(b2, 70, 20);

    byte[] b3 = new byte[20];
    seekAndVerify(80);
    numBytesRead = blockStream.read(b3, 0, 20);
    assertEquals(20, numBytesRead);
    matchWithInputData(b3, 80, 20);
  }

  @Test
  public void testUnbuffered() throws Exception {
    byte[] b1 = new byte[20];
    int numBytesRead = blockStream.read(b1, 0, 20);
    assertEquals(20, numBytesRead);
    matchWithInputData(b1, 0, 20);

    blockStream.unbuffer();

    assertFalse(blockStream.buffersAllocated());

    // Next read should start from the position of the last read + 1 i.e. 20
    byte[] b2 = new byte[20];
    numBytesRead = blockStream.read(b2, 0, 20);
    assertEquals(20, numBytesRead);
    matchWithInputData(b2, 20, 20);
  }

}

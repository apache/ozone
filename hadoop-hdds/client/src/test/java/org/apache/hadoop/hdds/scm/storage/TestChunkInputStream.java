/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ChunkInputStream}'s functionality.
 */
public class TestChunkInputStream {

  private static final int CHUNK_SIZE = 100;
  private static final int BYTES_PER_CHECKSUM = 20;
  private static final String CHUNK_NAME = "dummyChunk";
  private static final Random RANDOM = new Random();

  private Checksum checksum;
  private DummyChunkInputStream chunkStream;
  private ChunkInfo chunkInfo;
  private byte[] chunkData;

  @Before
  public void setup() throws Exception {
    checksum = new Checksum(ChecksumType.CRC32, BYTES_PER_CHECKSUM);

    chunkData = generateRandomData(CHUNK_SIZE);

    chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(CHUNK_NAME)
        .setOffset(0)
        .setLen(CHUNK_SIZE)
        .setChecksumData(checksum.computeChecksum(
            chunkData, 0, CHUNK_SIZE).getProtoBufMessage())
        .build();

    chunkStream = new DummyChunkInputStream(chunkInfo, null, null, true,
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
      Assert.assertEquals(chunkData[i], readData[i - inputDataStartIndex]);
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
    Assert.assertEquals(length, totalBufferLen);
  }

  /**
   * Seek to a position and verify through getPos().
   */
  private void seekAndVerify(int pos) throws Exception {
    chunkStream.seek(pos);
    Assert.assertEquals("Current position of buffer does not match with the " +
        "seeked position", pos, chunkStream.getPos());
  }

  @Test
  public void testFullChunkRead() throws Exception {
    byte[] b = new byte[CHUNK_SIZE];
    chunkStream.read(b, 0, CHUNK_SIZE);

    matchWithInputData(b, 0, CHUNK_SIZE);
  }

  @Test
  public void testPartialChunkRead() throws Exception {
    int len = CHUNK_SIZE / 2;
    byte[] b = new byte[len];

    chunkStream.read(b, 0, len);

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

    try {
      seekAndVerify(CHUNK_SIZE);
      Assert.fail("Seeking to Chunk Length should fail.");
    } catch (EOFException e) {
      GenericTestUtils.assertExceptionContains("EOF encountered at pos: "
          + CHUNK_SIZE + " for chunk: " + CHUNK_NAME, e);
    }

    // Seek before read should update the ChunkInputStream#chunkPosition
    seekAndVerify(25);
    Assert.assertEquals(25, chunkStream.getChunkPosition());

    // Read from the seeked position.
    // Reading from index 25 to 54 should result in the ChunkInputStream
    // copying chunk data from index 20 to 59 into the buffers (checksum
    // boundaries).
    byte[] b = new byte[30];
    chunkStream.read(b, 0, 30);
    matchWithInputData(b, 25, 30);
    matchWithInputData(chunkStream.getReadByteBuffers(), 20, 40);

    // After read, the position of the chunkStream is evaluated from the
    // buffers and the chunkPosition should be reset to -1.
    Assert.assertEquals(-1, chunkStream.getChunkPosition());

    // Seek to a position within the current buffers. Current buffers contain
    // data from index 20 to 59. ChunkPosition should still not be used to
    // set the position.
    seekAndVerify(35);
    Assert.assertEquals(-1, chunkStream.getChunkPosition());

    // Seek to a position outside the current buffers. In this case, the
    // chunkPosition should be updated to the seeked position.
    seekAndVerify(75);
    Assert.assertEquals(75, chunkStream.getChunkPosition());
  }

  @Test
  public void testSeekAndRead() throws Exception {
    // Seek to a position and read data
    seekAndVerify(50);
    byte[] b1 = new byte[20];
    chunkStream.read(b1, 0, 20);
    matchWithInputData(b1, 50, 20);

    // Next read should start from the position of the last read + 1 i.e. 70
    byte[] b2 = new byte[20];
    chunkStream.read(b2, 0, 20);
    matchWithInputData(b2, 70, 20);
  }

  @Test
  public void testUnbuffer() throws Exception {
    byte[] b1 = new byte[20];
    chunkStream.read(b1, 0, 20);
    matchWithInputData(b1, 0, 20);

    chunkStream.unbuffer();

    Assert.assertFalse(chunkStream.buffersAllocated());

    // Next read should start from the position of the last read + 1 i.e. 20
    byte[] b2 = new byte[20];
    chunkStream.read(b2, 0, 20);
    matchWithInputData(b2, 20, 20);
  }

  @Test
  public void connectsToNewPipeline() throws Exception {
    // GIVEN
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    Pipeline newPipeline = MockPipeline.createSingleNodePipeline();
    XceiverClientFactory clientFactory = mock(XceiverClientFactory.class);
    XceiverClientSpi client = mock(XceiverClientSpi.class);
    when(clientFactory.acquireClientForReadData(pipeline))
        .thenReturn(client);

    AtomicReference<Pipeline> pipelineRef = new AtomicReference<>(pipeline);

    ChunkInputStream subject = new ChunkInputStream(chunkInfo, null,
        clientFactory, pipelineRef::get, false, null) {
      @Override
      protected List<ByteBuffer> readChunk(ChunkInfo readChunkInfo) {
        return ByteString.copyFrom(chunkData).asReadOnlyByteBufferList();
      }
    };

    try {
      // WHEN
      subject.unbuffer();
      pipelineRef.set(newPipeline);
      int b = subject.read();

      // THEN
      Assert.assertNotEquals(-1, b);
      verify(clientFactory).acquireClientForReadData(newPipeline);
    } finally {
      subject.close();
    }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.ChunkInputStream;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link ChunkInputStream}
 */
public class TestChunkInputStream extends TestInputStreamBase {

  public TestChunkInputStream(ChunkLayOutVersion layout) {
    super(layout);
  }

  /**
   * Test to verify that data read from chunks is stored in a list of buffers
   * with max capacity equal to the bytes per checksum.
   */
  @Test
  public void testChunkReadBuffers() throws Exception {
    String keyName = getNewKeyName();
    int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
    byte[] inputData = writeRandomBytes(keyName, dataLength);

    KeyInputStream keyInputStream = getKeyInputStream(keyName);

    BlockInputStream block0Stream = keyInputStream.getBlockStreams().get(0);
    block0Stream.initialize();

    ChunkInputStream chunk0Stream = block0Stream.getChunkStreams().get(0);
    ChunkInputStream chunk1Stream = block0Stream.getChunkStreams().get(1);

    // To read 1 byte of chunk data, ChunkInputStream should get one full
    // checksum boundary worth of data from Container and store it in buffers.
    chunk0Stream.read(new byte[1]);
    checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 1,
        BYTES_PER_CHECKSUM);

    // Read > checksum boundary of data from chunk0
    int readDataLen = BYTES_PER_CHECKSUM + (BYTES_PER_CHECKSUM / 2);
    byte[] readData = readDataFromChunk(chunk0Stream, readDataLen, 0);
    validateData(inputData, 0, readData);

    // The first checksum boundary size of data was already existing in the
    // ChunkStream buffers. Once that data is read, the next checksum
    // boundary size of data will be fetched again to read the remaining data.
    // Hence there should be 1 checksum boundary size of data stored in the
    // ChunkStreams buffers at the end of the read.
    checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 1,
        BYTES_PER_CHECKSUM);

    // Seek to a position in the third checksum boundary (so that current
    // buffers do not have the seeked position) and read > BYTES_PER_CHECKSUM
    // bytes of data. This should result in 2 * BYTES_PER_CHECKSUM amount of
    // data being read into the buffers. There should be 2 buffers each with
    // BYTES_PER_CHECKSUM capacity.
    readDataLen = BYTES_PER_CHECKSUM + (BYTES_PER_CHECKSUM / 2);
    int offset = 2 * BYTES_PER_CHECKSUM + 1;
    readData = readDataFromChunk(chunk0Stream, readDataLen, offset);
    validateData(inputData, offset, readData);
    checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 2,
        BYTES_PER_CHECKSUM);


    // Read the full chunk data -1 and verify that all chunk data is read into
    // buffers. We read CHUNK_SIZE - 1 as otherwise the buffers will be
    // released once all chunk data is read.
    readData = readDataFromChunk(chunk0Stream, CHUNK_SIZE - 1, 0);
    validateData(inputData, 0, readData);
    checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(),
        CHUNK_SIZE / BYTES_PER_CHECKSUM, BYTES_PER_CHECKSUM);

    // Read the last byte of chunk and verify that the buffers are released.
    chunk0Stream.read(new byte[1]);
    Assert.assertNull("ChunkInputStream did not release buffers after " +
        "reaching EOF.", chunk0Stream.getCachedBuffers());

  }

  private byte[] readDataFromChunk(ChunkInputStream chunkInputStream,
      int readDataLength, int offset) throws IOException {
    byte[] readData = new byte[readDataLength];
    chunkInputStream.seek(offset);
    chunkInputStream.read(readData, 0, readDataLength);
    return readData;
  }

  private void checkBufferSizeAndCapacity(List<ByteBuffer> buffers,
      int expectedNumBuffers, long expectedBufferCapacity) {
    Assert.assertEquals("ChunkInputStream does not have expected number of " +
        "ByteBuffers", expectedNumBuffers, buffers.size());
    for (ByteBuffer buffer : buffers) {
      Assert.assertEquals("ChunkInputStream ByteBuffer capacity is wrong",
          expectedBufferCapacity, buffer.capacity());
    }
  }
}

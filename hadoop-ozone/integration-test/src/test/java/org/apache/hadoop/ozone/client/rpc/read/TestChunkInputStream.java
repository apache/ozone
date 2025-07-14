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

package org.apache.hadoop.ozone.client.rpc.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.ChunkInputStream;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.om.TestBucket;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests {@link ChunkInputStream}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestChunkInputStream extends TestInputStreamBase {

  /**
   * Run the tests as a single test method to avoid needing a new mini-cluster
   * for each test.
   */
  @ContainerLayoutTestInfo.ContainerTest
  void testAll(ContainerLayoutVersion layout) throws Exception {
    try (OzoneClient client = getCluster().newClient()) {
      updateConfig(layout);

      TestBucket bucket = TestBucket.newBuilder(client).build();

      testChunkReadBuffers(bucket);
      testBufferRelease(bucket);
      testCloseReleasesBuffers(bucket);
    }
  }

  /**
   * Test to verify that data read from chunks is stored in a list of buffers
   * with max capacity equal to the bytes per checksum.
   */
  private void testChunkReadBuffers(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
    byte[] inputData = bucket.writeRandomBytes(keyName, dataLength);

    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {

      BlockInputStream block0Stream =
          (BlockInputStream)keyInputStream.getPartStreams().get(0);
      block0Stream.initialize();

      ChunkInputStream chunk0Stream = block0Stream.getChunkStreams().get(0);

      // To read 1 byte of chunk data, ChunkInputStream should get one full
      // checksum boundary worth of data from Container and store it in buffers.
      IOUtils.readFully(chunk0Stream, new byte[1]);
      checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 1, 0,
          BYTES_PER_CHECKSUM);

      // Read > checksum boundary of data from chunk0
      int readDataLen = BYTES_PER_CHECKSUM + (BYTES_PER_CHECKSUM / 2);
      byte[] readData = readDataFromChunk(chunk0Stream, 0, readDataLen);
      bucket.validateData(inputData, 0, readData);

      // The first checksum boundary size of data was already existing in the
      // ChunkStream buffers. Once that data is read, the next checksum
      // boundary size of data will be fetched again to read the remaining data.
      // Hence there should be 1 checksum boundary size of data stored in the
      // ChunkStreams buffers at the end of the read.
      checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 1, 0,
          BYTES_PER_CHECKSUM);

      // Seek to a position in the third checksum boundary (so that current
      // buffers do not have the seeked position) and read > BYTES_PER_CHECKSUM
      // bytes of data. This should result in 2 * BYTES_PER_CHECKSUM amount of
      // data being read into the buffers. There should be 2 buffers in the
      // stream but the the first buffer should be released after it is read
      // and the second buffer should have BYTES_PER_CHECKSUM capacity.
      readDataLen = BYTES_PER_CHECKSUM + (BYTES_PER_CHECKSUM / 2);
      int offset = 2 * BYTES_PER_CHECKSUM + 1;
      readData = readDataFromChunk(chunk0Stream, offset, readDataLen);
      bucket.validateData(inputData, offset, readData);
      checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 2, 1,
          BYTES_PER_CHECKSUM);


      // Read the full chunk data -1 and verify that all chunk data is read into
      // buffers. We read CHUNK_SIZE - 1 as otherwise all the buffers will be
      // released once all chunk data is read.
      readData = readDataFromChunk(chunk0Stream, 0, CHUNK_SIZE - 1);
      bucket.validateData(inputData, 0, readData);
      int expectedNumBuffers = CHUNK_SIZE / BYTES_PER_CHECKSUM;
      checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(),
          expectedNumBuffers, expectedNumBuffers - 1, BYTES_PER_CHECKSUM);

      // Read the last byte of chunk and verify that the buffers are released.
      IOUtils.readFully(chunk0Stream, new byte[1]);
      assertNull(chunk0Stream.getCachedBuffers(),
          "ChunkInputStream did not release buffers after reaching EOF.");
    }
  }

  private void testCloseReleasesBuffers(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    bucket.writeRandomBytes(keyName, CHUNK_SIZE);

    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      BlockInputStream block0Stream =
          (BlockInputStream) keyInputStream.getPartStreams().get(0);
      block0Stream.initialize();

      ChunkInputStream chunk0Stream = block0Stream.getChunkStreams().get(0);
      readDataFromChunk(chunk0Stream, 0, 1);
      assertNotNull(chunk0Stream.getCachedBuffers());

      chunk0Stream.close();

      assertNull(chunk0Stream.getCachedBuffers());
    }
  }

  /**
   * Test that ChunkInputStream buffers are released as soon as the last byte
   * of the buffer is read.
   */
  private void testBufferRelease(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    byte[] inputData = bucket.writeRandomBytes(keyName, CHUNK_SIZE);

    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {

      BlockInputStream block0Stream =
          (BlockInputStream)keyInputStream.getPartStreams().get(0);
      block0Stream.initialize();

      ChunkInputStream chunk0Stream = block0Stream.getChunkStreams().get(0);

      // Read checksum boundary - 1 bytes of data
      int readDataLen = BYTES_PER_CHECKSUM - 1;
      byte[] readData = readDataFromChunk(chunk0Stream, 0, readDataLen);
      bucket.validateData(inputData, 0, readData);

      // There should be 1 byte of data remaining in the buffer which is not
      // yet read. Hence, the buffer should not be released.
      checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(),
          1, 0, BYTES_PER_CHECKSUM);
      assertEquals(1, chunk0Stream.getCachedBuffers()[0].remaining());

      // Reading the last byte in the buffer should result in all the buffers
      // being released.
      readData = readDataFromChunk(chunk0Stream, 1);
      bucket.validateData(inputData, readDataLen, readData);
      assertNull(chunk0Stream.getCachedBuffers(),
          "Chunk stream buffers not released after last byte is read");

      // Read more data to get the data till the next checksum boundary.
      readDataLen = BYTES_PER_CHECKSUM / 2;
      readData = readDataFromChunk(chunk0Stream, readDataLen);
      // There should be one buffer and the buffer should not be released as
      // there is data pending to be read from the buffer
      checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 1, 0,
          BYTES_PER_CHECKSUM);
      ByteBuffer lastCachedBuffer = chunk0Stream.getCachedBuffers()[0];
      assertEquals(BYTES_PER_CHECKSUM - readDataLen,
          lastCachedBuffer.remaining());

      // Read more than the remaining data in buffer (but less than the next
      // checksum boundary).
      int position = (int) chunk0Stream.getPos();
      readDataLen = lastCachedBuffer.remaining() + BYTES_PER_CHECKSUM / 2;
      readData = readDataFromChunk(chunk0Stream, readDataLen);
      bucket.validateData(inputData, position, readData);
      // After reading the remaining data in the buffer, the buffer should be
      // released and next checksum size of data must be read into the buffers
      checkBufferSizeAndCapacity(chunk0Stream.getCachedBuffers(), 1, 0,
          BYTES_PER_CHECKSUM);
      // Verify that the previously cached buffer is released by comparing it
      // with the current cached buffer
      assertNotEquals(lastCachedBuffer,
          chunk0Stream.getCachedBuffers()[0]);
    }
  }

  private byte[] readDataFromChunk(ChunkInputStream chunkInputStream,
      int offset, int readDataLength) throws IOException {
    byte[] readData = new byte[readDataLength];
    chunkInputStream.seek(offset);
    IOUtils.readFully(chunkInputStream, readData);
    return readData;
  }

  private byte[] readDataFromChunk(ChunkInputStream chunkInputStream,
      int readDataLength) throws IOException {
    byte[] readData = new byte[readDataLength];
    IOUtils.readFully(chunkInputStream, readData);
    return readData;
  }

  /**
   * Verify number of buffers and their capacities.
   * @param buffers chunk stream buffers
   * @param expectedNumBuffers expected number of buffers
   * @param numReleasedBuffers first numReleasedBuffers are expected to
   *                           be released and hence null
   * @param expectedBufferCapacity expected buffer capacity of unreleased
   *                               buffers
   */
  private void checkBufferSizeAndCapacity(ByteBuffer[] buffers,
      int expectedNumBuffers, int numReleasedBuffers,
      long expectedBufferCapacity) {
    assertEquals(expectedNumBuffers, buffers.length,
        "ChunkInputStream does not have expected number of " +
        "ByteBuffers");
    for (int i = 0; i < buffers.length; i++) {
      if (i <= numReleasedBuffers - 1) {
        // This buffer should have been released and hence null
        assertNull(buffers[i],
            "ChunkInputStream Buffer not released after being read");
      } else {
        assertEquals(expectedBufferCapacity, buffers[i].capacity(),
            "ChunkInputStream ByteBuffer capacity is wrong");
      }
    }
  }
}

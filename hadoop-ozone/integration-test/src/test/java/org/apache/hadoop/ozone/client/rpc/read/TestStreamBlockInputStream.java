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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.om.TestBucket;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.ozone.client.rpc.read.TestInputStreamBase.BLOCK_SIZE;
import static org.apache.hadoop.ozone.client.rpc.read.TestInputStreamBase.BYTES_PER_CHECKSUM;
import static org.apache.hadoop.ozone.client.rpc.read.TestInputStreamBase.CHUNK_SIZE;
import static org.apache.hadoop.ozone.client.rpc.read.TestInputStreamBase.getNewKeyName;
import static org.apache.hadoop.ozone.client.rpc.read.TestInputStreamBase.newCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link StreamBlockInputStream}.
 */
public class TestStreamBlockInputStream {
  /**
   * Run the tests as a single test method to avoid needing a new mini-cluster
   * for each test.
   */
  @ContainerLayoutTestInfo.ContainerTest
  void testAll(ContainerLayoutVersion layout) throws Exception {
    try (MiniOzoneCluster cluster = newCluster(layout)) {
      cluster.waitForClusterToBeReady();

      OzoneConfiguration conf = cluster.getConf();
      OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamReadBlock(true);
      OzoneConfiguration copy = new OzoneConfiguration(conf);
      copy.setFromObject(clientConfig);
      try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
        TestBucket bucket = TestBucket.newBuilder(client).build();

        testBlockReadBuffers(bucket);
        testBufferRelease(bucket);
        testCloseReleasesBuffers(bucket);
        testReadEmptyBlock(bucket);
      }
    }
  }


  /**
   * Test to verify that data read from blocks is stored in a list of buffers
   * with max capacity equal to the bytes per checksum.
   */
  private void testBlockReadBuffers(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
    byte[] inputData = bucket.writeRandomBytes(keyName, dataLength);

    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {

      StreamBlockInputStream block0Stream =
          (StreamBlockInputStream) keyInputStream.getPartStreams().get(0);


      // To read 1 byte of chunk data, ChunkInputStream should get one full
      // checksum boundary worth of data from Container and store it in buffers.
      block0Stream.read(new byte[1]);
      checkBufferSizeAndCapacity(block0Stream.getCachedBuffers(), 1,
          BYTES_PER_CHECKSUM);

      // Read > checksum boundary of data from chunk0
      int readDataLen = BYTES_PER_CHECKSUM + (BYTES_PER_CHECKSUM / 2);
      byte[] readData = readDataFromBlock(block0Stream, 0, readDataLen);
      bucket.validateData(inputData, 0, readData);

      // The first checksum boundary size of data was already existing in the
      // ChunkStream buffers. Once that data is read, the next checksum
      // boundary size of data will be fetched again to read the remaining data.
      // Hence there should be 1 checksum boundary size of data stored in the
      // ChunkStreams buffers at the end of the read.
      checkBufferSizeAndCapacity(block0Stream.getCachedBuffers(), 1,
          BYTES_PER_CHECKSUM);

      // Seek to a position in the third checksum boundary (so that current
      // buffers do not have the seeked position) and read > BYTES_PER_CHECKSUM
      // bytes of data. This should result in 2 * BYTES_PER_CHECKSUM amount of
      // data being read into the buffers. There should be 2 buffers in the
      // stream but the the first buffer should be released after it is read
      // and the second buffer should have BYTES_PER_CHECKSUM capacity.
      readDataLen = BYTES_PER_CHECKSUM + (BYTES_PER_CHECKSUM / 2);
      int offset = 2 * BYTES_PER_CHECKSUM + 1;
      readData = readDataFromBlock(block0Stream, offset, readDataLen);
      bucket.validateData(inputData, offset, readData);
      checkBufferSizeAndCapacity(block0Stream.getCachedBuffers(), 1,
          BYTES_PER_CHECKSUM);


      // Read the full chunk data -1 and verify that all chunk data is read into
      // buffers. We read CHUNK_SIZE - 1 as otherwise all the buffers will be
      // released once all chunk data is read.
      readData = readDataFromBlock(block0Stream, 0, CHUNK_SIZE - 1);
      bucket.validateData(inputData, 0, readData);
      int expectedNumBuffers = CHUNK_SIZE / BYTES_PER_CHECKSUM;
      checkBufferSizeAndCapacity(block0Stream.getCachedBuffers(), 1, BYTES_PER_CHECKSUM);

      // Read the last byte of chunk and verify that the buffers are released.
      block0Stream.read(new byte[1]);
      assertNull(block0Stream.getCachedBuffers(),
          "ChunkInputStream did not release buffers after reaching EOF.");
    }
  }

  private void testCloseReleasesBuffers(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    bucket.writeRandomBytes(keyName, CHUNK_SIZE);

    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      StreamBlockInputStream block0Stream =
          (StreamBlockInputStream) keyInputStream.getPartStreams().get(0);

      readDataFromBlock(block0Stream, 0, 1);
      assertNotNull(block0Stream.getCachedBuffers());

      block0Stream.close();

      assertNull(block0Stream.getCachedBuffers());
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

      StreamBlockInputStream block0Stream =
          (StreamBlockInputStream) keyInputStream.getPartStreams().get(0);

      // Read checksum boundary - 1 bytes of data
      int readDataLen = BYTES_PER_CHECKSUM - 1;
      byte[] readData = readDataFromBlock(block0Stream, 0, readDataLen);
      bucket.validateData(inputData, 0, readData);

      // There should be 1 byte of data remaining in the buffer which is not
      // yet read. Hence, the buffer should not be released.
      checkBufferSizeAndCapacity(block0Stream.getCachedBuffers(),
          1, BYTES_PER_CHECKSUM);
      assertEquals(1, block0Stream.getCachedBuffers()[0].remaining());

      // Reading the last byte in the buffer should result in all the buffers
      // being released.
      readData = readDataFromBlock(block0Stream, 1);
      bucket.validateData(inputData, readDataLen, readData);
      assertNull(block0Stream.getCachedBuffers(),
          "Chunk stream buffers not released after last byte is read");

      // Read more data to get the data till the next checksum boundary.
      readDataLen = BYTES_PER_CHECKSUM / 2;
      readData = readDataFromBlock(block0Stream, readDataLen);
      // There should be one buffer and the buffer should not be released as
      // there is data pending to be read from the buffer
      checkBufferSizeAndCapacity(block0Stream.getCachedBuffers(), 1,
          BYTES_PER_CHECKSUM);
      ByteBuffer lastCachedBuffer = block0Stream.getCachedBuffers()[0];
      assertEquals(BYTES_PER_CHECKSUM - readDataLen,
          lastCachedBuffer.remaining());

      // Read more than the remaining data in buffer (but less than the next
      // checksum boundary).
      int position = (int) block0Stream.getPos();
      readDataLen = lastCachedBuffer.remaining() + BYTES_PER_CHECKSUM / 2;
      readData = readDataFromBlock(block0Stream, readDataLen);
      bucket.validateData(inputData, position, readData);
      // After reading the remaining data in the buffer, the buffer should be
      // released and next checksum size of data must be read into the buffers
      checkBufferSizeAndCapacity(block0Stream.getCachedBuffers(), 1,
          BYTES_PER_CHECKSUM);
      // Verify that the previously cached buffer is released by comparing it
      // with the current cached buffer
      assertNotEquals(lastCachedBuffer,
          block0Stream.getCachedBuffers()[0]);
    }
  }

  private byte[] readDataFromBlock(StreamBlockInputStream streamBlockInputStream,
                                   int offset, int readDataLength) throws IOException {
    byte[] readData = new byte[readDataLength];
    streamBlockInputStream.seek(offset);
    streamBlockInputStream.read(readData, 0, readDataLength);
    return readData;
  }

  private byte[] readDataFromBlock(StreamBlockInputStream streamBlockInputStream,
                                   int readDataLength) throws IOException {
    byte[] readData = new byte[readDataLength];
    streamBlockInputStream.read(readData, 0, readDataLength);
    return readData;
  }

  /**
   * Verify number of buffers and their capacities.
   * @param buffers chunk stream buffers
   * @param expectedNumBuffers expected number of buffers
   * @param expectedBufferCapacity expected buffer capacity of unreleased
   *                               buffers
   */
  private void checkBufferSizeAndCapacity(ByteBuffer[] buffers,
                                          int expectedNumBuffers,
                                          long expectedBufferCapacity) {
    assertEquals(expectedNumBuffers, buffers.length,
        "ChunkInputStream does not have expected number of " +
            "ByteBuffers");
    for (int i = 0; i < buffers.length; i++) {
      assertEquals(expectedBufferCapacity, buffers[i].capacity(),
          "ChunkInputStream ByteBuffer capacity is wrong");
    }
  }

  private void testReadEmptyBlock(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    int dataLength = 10;
    bucket.writeRandomBytes(keyName, 0);

    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {

      byte[] readData = new byte[dataLength];
      assertTrue(keyInputStream.getPartStreams().isEmpty());
      keyInputStream.read(readData);
      for (int i = 0; i < readData.length; i++) {
        assertEquals(readData[i], (byte) 0);
      }
    }
  }
}

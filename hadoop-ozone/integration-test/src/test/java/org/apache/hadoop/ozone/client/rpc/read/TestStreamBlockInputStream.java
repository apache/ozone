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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.om.TestBucket;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link StreamBlockInputStream}.
 */
public class TestStreamBlockInputStream extends TestInputStreamBase {
  /**
   * Run the tests as a single test method to avoid needing a new mini-cluster
   * for each test.
   */

  private String keyName = getNewKeyName();
  private int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
  byte[] inputData;
  private TestBucket bucket;

 // @ContainerLayoutTestInfo.ContainerTest
  @Test
  void testAll() throws Exception {
    try (MiniOzoneCluster cluster = newCluster()) {
      cluster.waitForClusterToBeReady();

      OzoneConfiguration conf = cluster.getConf();
      OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamReadBlock(true);
      OzoneConfiguration copy = new OzoneConfiguration(conf);
      copy.setFromObject(clientConfig);
      try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
        bucket = TestBucket.newBuilder(client).build();
        inputData = bucket.writeRandomBytes(keyName, dataLength);
        testReadKeyFully();
        testSeek();
      //  testBufferRelease(bucket);
      //  testCloseReleasesBuffers(bucket);
      //  testReadEmptyBlock(bucket);
      }
    }
  }

  /**
   * Test to verify that data read from blocks is stored in a list of buffers
   * with max capacity equal to the bytes per checksum.
   */
  private void testReadKeyFully() throws Exception {
    // Read the data fully into a large enough byte array
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      byte[] readData = new byte[dataLength];
      int totalRead = keyInputStream.read(readData, 0, dataLength);
      assertEquals(dataLength, totalRead);
      for (int i = 0; i < dataLength; i++) {
        assertEquals(inputData[i], readData[i],
            "Read data is not same as written data at index " + i);
      }
    }
    // Read the data 1 byte at a time
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      for (int i = 0; i < dataLength; i++) {
        int b = keyInputStream.read();
        assertEquals(inputData[i], (byte) b,
            "Read data is not same as written data at index " + i);
      }
    }
    // Read the data into a large enough ByteBuffer
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      ByteBuffer readBuf = ByteBuffer.allocate(dataLength);
      int totalRead = keyInputStream.read(readBuf);
      assertEquals(dataLength, totalRead);
      readBuf.flip();
      for (int i = 0; i < dataLength; i++) {
        assertEquals(inputData[i], readBuf.get(),
            "Read data is not same as written data at index " + i);
      }
    }
  }

  private void testSeek() throws IOException {
    java.util.Random random = new java.util.Random();
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      for (int i = 0; i < 100; i++) {
        int position = random.nextInt(dataLength);
        keyInputStream.seek(position);
        int b = keyInputStream.read();
        assertEquals(inputData[position], (byte) b, "Read data is not same as written data at index " + position);
      }
      StreamBlockInputStream blockStream = (StreamBlockInputStream) keyInputStream.getPartStreams().get(0);
      long length = blockStream.getLength();
      blockStream.seek(10);
      long position = blockStream.getPos();
      assertThrows(IOException.class, () -> blockStream.seek(length + 1),
          "Seek beyond block length should throw exception");
      assertThrows(IOException.class, () -> blockStream.seek(-1),
          "Seeking to a negative position should throw exception");
      assertEquals(position, blockStream.getPos(),
          "Position should not change after failed seek attempts");
    }
  }

  private void testCloseReleasesBuffers(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    bucket.writeRandomBytes(keyName, CHUNK_SIZE);

    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      StreamBlockInputStream block0Stream =
          (StreamBlockInputStream) keyInputStream.getPartStreams().get(0);

      readDataFromBlock(block0Stream, 0, 1);
    //  assertNotNull(block0Stream.getCachedBuffers());

      block0Stream.close();

  //    assertNull(block0Stream.getCachedBuffers());
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
   //   checkBufferSizeAndCapacity(block0Stream.getCachedBuffers());
    //  assertEquals(1, block0Stream.getCachedBuffers()[0].remaining());

      // Reading the last byte in the buffer should result in all the buffers
      // being released.
      readData = readDataFromBlock(block0Stream, 1);
      bucket.validateData(inputData, readDataLen, readData);
   //   assertNull(block0Stream.getCachedBuffers(),
   //       "Chunk stream buffers not released after last byte is read");
//
      // Read more data to get the data till the next checksum boundary.
      readDataLen = BYTES_PER_CHECKSUM / 2;
      readDataFromBlock(block0Stream, readDataLen);
      // There should be one buffer and the buffer should not be released as
      // there is data pending to be read from the buffer
     // checkBufferSizeAndCapacity(block0Stream.getCachedBuffers());
     // ByteBuffer lastCachedBuffer = block0Stream.getCachedBuffers()[0];
   //   assertEquals(BYTES_PER_CHECKSUM - readDataLen,
   //       lastCachedBuffer.remaining());

      // Read more than the remaining data in buffer (but less than the next
      // checksum boundary).
      int position = (int) block0Stream.getPos();
    //  readDataLen = lastCachedBuffer.remaining() + BYTES_PER_CHECKSUM / 2;
      readData = readDataFromBlock(block0Stream, readDataLen);
      bucket.validateData(inputData, position, readData);
      // After reading the remaining data in the buffer, the buffer should be
      // released and next checksum size of data must be read into the buffers
    //  checkBufferSizeAndCapacity(block0Stream.getCachedBuffers());
      // Verify that the previously cached buffer is released by comparing it
      // with the current cached buffer
     // assertNotEquals(lastCachedBuffer,
    //      block0Stream.getCachedBuffers()[0]);
    }
  }

  private byte[] readDataFromBlock(StreamBlockInputStream streamBlockInputStream,
                                   int offset, int readDataLength) throws IOException {
    byte[] readData = new byte[readDataLength];
    streamBlockInputStream.seek(offset);
    IOUtils.readFully(streamBlockInputStream, readData);
    return readData;
  }

  private byte[] readDataFromBlock(StreamBlockInputStream streamBlockInputStream,
                                   int readDataLength) throws IOException {
    byte[] readData = new byte[readDataLength];
    IOUtils.readFully(streamBlockInputStream, readData);
    return readData;
  }

  /**
   * Verify number of buffers and their capacities.
   * @param buffers chunk stream buffers
   */
  private void checkBufferSizeAndCapacity(ByteBuffer[] buffers) {
    assertEquals(1, buffers.length,
        "ChunkInputStream does not have expected number of " +
            "ByteBuffers");
    for (ByteBuffer buffer : buffers) {
      assertEquals(BYTES_PER_CHECKSUM, buffer.capacity(),
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
      IOUtils.read(keyInputStream, readData);
      for (byte b : readData) {
        assertEquals((byte) 0, b);
      }
    }
  }
}

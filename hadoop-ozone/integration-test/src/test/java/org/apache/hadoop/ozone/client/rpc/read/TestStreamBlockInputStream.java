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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
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
  private static final int DATA_LENGTH = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
  private byte[] inputData;
  private TestBucket bucket;

  @Test
  void testAll() throws Exception {
    try (MiniOzoneCluster cluster = newCluster()) {
      cluster.waitForClusterToBeReady();

      OzoneConfiguration conf = cluster.getConf();
      OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamReadBlock(true);
      OzoneConfiguration copy = new OzoneConfiguration(conf);
      copy.setFromObject(clientConfig);
      String keyName = getNewKeyName();
      try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
        bucket = TestBucket.newBuilder(client).build();
        inputData = bucket.writeRandomBytes(keyName, DATA_LENGTH);
        testReadKeyFully(keyName);
        testSeek(keyName);
        testReadEmptyBlock();
      }
      keyName = getNewKeyName();
      clientConfig.setChecksumType(ContainerProtos.ChecksumType.NONE);
      copy.setFromObject(clientConfig);
      try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
        bucket = TestBucket.newBuilder(client).build();
        inputData = bucket.writeRandomBytes(keyName, DATA_LENGTH);
        testReadKeyFully(keyName);
        testSeek(keyName);
      }
    }
  }

  /**
   * Test to verify that data read from blocks is stored in a list of buffers
   * with max capacity equal to the bytes per checksum.
   */
  private void testReadKeyFully(String key) throws Exception {
    // Read the data fully into a large enough byte array
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(key)) {
      byte[] readData = new byte[DATA_LENGTH];
      int totalRead = keyInputStream.read(readData, 0, DATA_LENGTH);
      assertEquals(DATA_LENGTH, totalRead);
      for (int i = 0; i < DATA_LENGTH; i++) {
        assertEquals(inputData[i], readData[i],
            "Read data is not same as written data at index " + i);
      }
    }
    // Read the data 1 byte at a time
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(key)) {
      for (int i = 0; i < DATA_LENGTH; i++) {
        int b = keyInputStream.read();
        assertEquals(inputData[i], (byte) b,
            "Read data is not same as written data at index " + i);
      }
    }
    // Read the data into a large enough ByteBuffer
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(key)) {
      ByteBuffer readBuf = ByteBuffer.allocate(DATA_LENGTH);
      int totalRead = keyInputStream.read(readBuf);
      assertEquals(DATA_LENGTH, totalRead);
      readBuf.flip();
      for (int i = 0; i < DATA_LENGTH; i++) {
        assertEquals(inputData[i], readBuf.get(),
            "Read data is not same as written data at index " + i);
      }
    }
  }

  private void testSeek(String key) throws IOException {
    java.util.Random random = new java.util.Random();
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(key)) {
      for (int i = 0; i < 100; i++) {
        int position = random.nextInt(DATA_LENGTH);
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

  private void testReadEmptyBlock() throws Exception {
    String keyName = getNewKeyName();
    bucket.writeRandomBytes(keyName, 0);
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      assertTrue(keyInputStream.getPartStreams().isEmpty());
      assertEquals(-1, keyInputStream.read());
    }
  }
}

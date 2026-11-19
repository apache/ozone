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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.common.transport.server.GrpcXceiverService;
import org.apache.hadoop.ozone.om.BucketForTesting;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests {@link StreamBlockInputStream}.
 */
public class TestStreamBlockInputStream extends InputStreamTests {
  private static final Logger LOG = LoggerFactory.getLogger(TestStreamBlockInputStream.class);

  {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("com"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ipc"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.server.http"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.scm.container"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.scm.ha"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.scm.safemode"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ozone.container.common"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ozone.om"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.ratis"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("BackgroundPipelineScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("ExpiredContainerReplicaOpScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("SCMHATransactionMonitor"), Level.ERROR);
    GenericTestUtils.setLogLevel(GrpcXceiverService.class, Level.ERROR);

//    GenericTestUtils.setLogLevel(StreamBlockInputStream.class, Level.DEBUG);
//    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(XceiverClientGrpc.class), Level.TRACE);
  }

  /**
   * Run the tests as a single test method to avoid needing a new mini-cluster
   * for each test.
   */
  private static final int DATA_LENGTH = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
  /** Representative read buffer sizes relative to key length (keyLength / divisor). */
  private static final int[] READ_BUFFER_DIVISORS = {1, 2, 5, 10};
  /** Representative fixed read buffer sizes from 4 KB to 16 MB. */
  private static final int[] READ_BUFFER_SIZES = {4 << 10, 256 << 10, 16 << 20};
  private static final int RANDOM_SEEK_COUNT = 20;
  private byte[] inputData;
  private BucketForTesting bucket;

  @Override
  int getDatanodeCount() {
    return getRepConfig().getRequiredNodes();
  }

  @Override
  ReplicationConfig getRepConfig() {
    return RatisReplicationConfig.getInstance(ONE);
  }

  @Test
  void testReadKey() throws Exception {
    OzoneConfiguration conf = getCluster().getConf();

    runTestReadKey(DATA_LENGTH, false, conf);
    final int keyLength = DATA_LENGTH + ThreadLocalRandom.current().nextInt(DATA_LENGTH);
    runTestReadKey(keyLength, true, conf);
  }

  void runTestReadKey(int keyLength, boolean randomReadOffset, OzoneConfiguration conf) throws Exception {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamReadBlock(true);
    OzoneConfiguration copy = new OzoneConfiguration(conf);
    copy.setFromObject(clientConfig);
    String keyName = getNewKeyName();
    try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
      bucket = BucketForTesting.newBuilder(client).build();
      inputData = bucket.writeRandomBytes(keyName, getRepConfig(), keyLength);
      LOG.info("---------------------------------------------------------");
      LOG.info("writeRandomBytes {} bytes", inputData.length);

      runTestPositionedRead(keyName, ByteBuffer.wrap(new byte[inputData.length]));

      for (int divisor : READ_BUFFER_DIVISORS) {
        runTestReadKey(keyName, keyLength / divisor, randomReadOffset, keyLength);
      }

      for (int bufferSize : READ_BUFFER_SIZES) {
        runTestReadKey(keyName, bufferSize, randomReadOffset, keyLength);
      }
    }
  }

  private void runTestReadKey(String key, int bufferSize, boolean randomReadOffset, int keyLength) throws Exception {
    final int readOffset = randomReadOffset ? ThreadLocalRandom.current().nextInt(keyLength / 2) : 0;
    LOG.info("read {} bytes with bufferSize {}, readOffset {}", keyLength, bufferSize, readOffset);
    // Read the data fully into a large enough byte array
    final byte[] buffer = new byte[bufferSize];
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(key)) {
      if (readOffset > 0) {
        keyInputStream.seek(readOffset);
      }

      int pos = readOffset;
      for (; pos < keyLength;) {
        final int read = keyInputStream.read(buffer, 0, buffer.length);
        if (read == -1) {
          break;
        }
        assertEquals(ByteBuffer.wrap(inputData, pos, read),
            ByteBuffer.wrap(buffer, 0, read), "pos=" + pos);
        pos += read;
      }
      assertEquals(keyLength, pos);
    }
  }

  void runTestPositionedRead(String key, ByteBuffer buffer) throws Exception {
    try (KeyInputStream in = bucket.getKeyInputStream(key)) {
      runTestPositionedRead(buffer, in, 0, 0);
      runTestPositionedRead(buffer, in, 0, 1);
      runTestPositionedRead(buffer, in, inputData.length, 0);
      runTestPositionedRead(buffer, in, inputData.length - 1, 1);
      for (int i = 0; i < 2; i++) {
        runTestPositionedRead(buffer, in);
      }
    }
  }

  void runTestPositionedRead(ByteBuffer buffer, KeyInputStream in) throws Exception {
    final int position = ThreadLocalRandom.current().nextInt(inputData.length - 1);
    runTestPositionedRead(buffer, in, position, 0);
    runTestPositionedRead(buffer, in, position, 1);
    final int n = 2 + ThreadLocalRandom.current().nextInt(inputData.length - 1 - position);
    runTestPositionedRead(buffer, in, position, n);
  }

  void runTestPositionedRead(ByteBuffer buffer, KeyInputStream in, int pos, int length) throws Exception {
    LOG.info("runTestPositionedRead: position={}, length={}", pos, length);
    assertTrue(pos + length <= inputData.length);
    buffer = buffer.duplicate();

    // seek and read
    buffer.position(0).limit(length);
    in.seek(pos);
    while (buffer.hasRemaining()) {
      in.read(buffer);
    }
    assertData(pos, length, buffer);

    // positioned read
    buffer.position(0).limit(length);
    in.readFully(pos, buffer);
    assertData(pos, length, buffer);
  }

  void assertData(int pos, int length, ByteBuffer buffer) {
    buffer.flip();
    assertEquals(length, buffer.remaining());
    assertEquals(ByteBuffer.wrap(inputData, pos, length), buffer, () -> "pos=" + pos);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testAll(boolean preRead) throws Exception {
    runTestAll(preRead);
  }

  void runTestAll(boolean preRead) throws Exception {
    OzoneConfiguration conf = getCluster().getConf();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamReadBlock(true);
    if (!preRead) {
      clientConfig.setStreamReadPreReadSize(0);
    }
    OzoneConfiguration copy = new OzoneConfiguration(conf);
    copy.setFromObject(clientConfig);
    String keyName = getNewKeyName();
    try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
      bucket = BucketForTesting.newBuilder(client).build();
      inputData = bucket.writeRandomBytes(keyName, getRepConfig(), DATA_LENGTH);
      testReadKeyFully(keyName);
      testSeek(keyName);
      testReadEmptyBlock();
    }
    keyName = getNewKeyName();
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.NONE);
    copy.setFromObject(clientConfig);
    try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
      bucket = BucketForTesting.newBuilder(client).build();
      inputData = bucket.writeRandomBytes(keyName, getRepConfig(), DATA_LENGTH);
      testReadKeyFully(keyName);
      testSeek(keyName);
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
      assertArrayEquals(inputData, readData);
    }
    // Read the first checksum segment 1 byte at a time to verify single-byte reads.
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(key)) {
      for (int i = 0; i < BYTES_PER_CHECKSUM; i++) {
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
      byte[] readData = new byte[DATA_LENGTH];
      readBuf.get(readData);
      assertArrayEquals(inputData, readData);
    }
  }

  void assertSeekRead(KeyInputStream in, int position) throws IOException {
    in.seek(position);
    int b = in.read();
    assertEquals(inputData[position], (byte) b, "Read data is not same as written data at index " + position);
  }

  private void runTestSeek(KeyInputStream in, int seekSize, Random random) throws IOException {
    LOG.info("runTestSeek: seekSize={}", seekSize);
    for (int i = 0; i < RANDOM_SEEK_COUNT; i++) {
      int position = random.nextInt(seekSize);
      assertSeekRead(in, position);
    }

    for (int position = 0; position < DATA_LENGTH; position += seekSize) {
      assertSeekRead(in, position);
    }

    for (int position = DATA_LENGTH - 1; position >= 0; position -= seekSize) {
      assertSeekRead(in, position);
    }
    assertSeekRead(in, 0);
  }

  private void testSeek(String key) throws IOException {
    final Random random = new Random();
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(key)) {
      runTestSeek(keyInputStream, CHUNK_SIZE / 8, random);
      runTestSeek(keyInputStream, CHUNK_SIZE, random);
      runTestSeek(keyInputStream, BLOCK_SIZE, random);
      runTestSeek(keyInputStream, DATA_LENGTH, random);

      // error cases
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
    bucket.writeRandomBytes(keyName, getRepConfig(), 0);
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      assertTrue(keyInputStream.getPartStreams().isEmpty());
      assertEquals(-1, keyInputStream.read());
    }
  }
}

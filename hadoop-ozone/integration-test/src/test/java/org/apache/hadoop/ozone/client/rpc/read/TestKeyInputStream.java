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

import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.RS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.container.TestHelper.countReplicas;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.ChunkInputStream;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.om.TestBucket;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link KeyInputStream}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestKeyInputStream extends TestInputStreamBase {

  /**
   * This method does random seeks and reads and validates the reads are
   * correct or not.
   */
  private void randomSeek(TestBucket bucket, int dataLength,
      KeyInputStream keyInputStream, byte[] inputData) throws Exception {
    // Do random seek.
    for (int i = 0; i < dataLength - 300; i += 20) {
      validate(bucket, keyInputStream, inputData, i, 200);
    }

    // Seek to end and read in reverse order. And also this is partial chunks
    // as readLength is 20, chunk length is 100.
    for (int i = dataLength - 100; i >= 100; i -= 20) {
      validate(bucket, keyInputStream, inputData, i, 20);
    }

    // Start from begin and seek such that we read partially chunks.
    for (int i = 0; i < dataLength - 300; i += 20) {
      validate(bucket, keyInputStream, inputData, i, 90);
    }

  }

  /**
   * This method does random seeks and reads and validates the reads are
   * correct or not.
   */
  private void randomPositionSeek(TestBucket bucket, int dataLength,
      KeyInputStream keyInputStream,
      byte[] inputData, int readSize) throws Exception {
    for (int i = 0; i < 100; i++) {
      int position = RandomUtils.secure().randomInt(0, dataLength - readSize);
      validate(bucket, keyInputStream, inputData, position, readSize);
    }
  }

  /**
   * This method seeks to specified seek value and read the data specified by
   * readLength and validate the read is correct or not.
   */
  private void validate(TestBucket bucket, KeyInputStream keyInputStream,
      byte[] inputData, long seek, int readLength) throws Exception {
    keyInputStream.seek(seek);

    byte[] readData = new byte[readLength];
    IOUtils.readFully(keyInputStream, readData);

    bucket.validateData(inputData, (int) seek, readData);
  }

  /**
   * This test runs the others as a single test, so to avoid creating a new
   * mini-cluster for each test.
   */
  @ContainerLayoutTestInfo.ContainerTest
  void testNonReplicationReads(ContainerLayoutVersion layout) throws Exception {
    try (OzoneClient client = getCluster().newClient()) {
      updateConfig(layout);

      TestBucket bucket = TestBucket.newBuilder(client).build();

      testInputStreams(bucket);
      testSeekRandomly(bucket);
      testSeek(bucket);
      testReadChunkWithByteArray(bucket);
      testReadChunkWithByteBuffer(bucket);
      testSkip(bucket);
      testECSeek(bucket);
    }
  }

  private void testInputStreams(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE) + 1;
    bucket.writeRandomBytes(keyName, dataLength);

    KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName);

    // Verify BlockStreams and ChunkStreams
    int expectedNumBlockStreams = BufferUtils.getNumberOfBins(
        dataLength, BLOCK_SIZE);
    List<BlockExtendedInputStream> blockStreams =
        keyInputStream.getPartStreams();
    assertEquals(expectedNumBlockStreams, blockStreams.size());

    int readBlockLength = 0;
    for (BlockExtendedInputStream stream : blockStreams) {
      BlockInputStream blockStream = (BlockInputStream) stream;
      int blockStreamLength = Math.min(BLOCK_SIZE,
          dataLength - readBlockLength);
      assertEquals(blockStreamLength, blockStream.getLength());

      int expectedNumChunkStreams =
          BufferUtils.getNumberOfBins(blockStreamLength, CHUNK_SIZE);
      blockStream.initialize();
      List<ChunkInputStream> chunkStreams = blockStream.getChunkStreams();
      assertEquals(expectedNumChunkStreams, chunkStreams.size());

      int readChunkLength = 0;
      for (ChunkInputStream chunkStream : chunkStreams) {
        int chunkStreamLength = Math.min(CHUNK_SIZE,
            blockStreamLength - readChunkLength);
        assertEquals(chunkStreamLength, chunkStream.getRemaining());

        readChunkLength += chunkStreamLength;
      }

      readBlockLength += blockStreamLength;
    }
  }

  private void testSeekRandomly(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();
    int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
    byte[] inputData = bucket.writeRandomBytes(keyName, dataLength);

    KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName);

    // Seek to some where end.
    validate(bucket, keyInputStream, inputData, dataLength - 200, 100);

    // Now seek to start.
    validate(bucket, keyInputStream, inputData, 0, 140);

    validate(bucket, keyInputStream, inputData, 200, 300);

    validate(bucket, keyInputStream, inputData, 30, 500);

    randomSeek(bucket, dataLength, keyInputStream, inputData);

    // Read entire key.
    validate(bucket, keyInputStream, inputData, 0, dataLength);

    // Repeat again and check.
    randomSeek(bucket, dataLength, keyInputStream, inputData);

    validate(bucket, keyInputStream, inputData, 0, dataLength);

    keyInputStream.close();
  }

  public void testECSeek(TestBucket bucket) throws Exception {
    int ecChunkSize = 1024 * 1024;
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2, RS,
        ecChunkSize);
    String keyName = getNewKeyName();
    // 3 full EC blocks plus one chunk
    int dataLength = (9 * BLOCK_SIZE + ecChunkSize);

    byte[] inputData = bucket.writeRandomBytes(keyName, repConfig, dataLength);
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {

      validate(bucket, keyInputStream, inputData, 0, ecChunkSize + 1234);

      validate(bucket, keyInputStream, inputData, 200, ecChunkSize);

      validate(bucket, keyInputStream, inputData, BLOCK_SIZE * 4, ecChunkSize);

      validate(bucket, keyInputStream, inputData,
          BLOCK_SIZE * 4 + 200, ecChunkSize);

      validate(bucket, keyInputStream, inputData,
          dataLength - ecChunkSize - 100, ecChunkSize + 50);

      randomPositionSeek(bucket, dataLength, keyInputStream, inputData,
          ecChunkSize + 200);

      // Read entire key.
      validate(bucket, keyInputStream, inputData, 0, dataLength);
    }
  }

  public void testSeek(TestBucket bucket) throws Exception {
    XceiverClientManager.resetXceiverClientMetrics();
    XceiverClientMetrics metrics = XceiverClientManager
        .getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long readChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.ReadChunk);

    String keyName = getNewKeyName();
    // write data spanning 3 chunks
    int dataLength = (2 * CHUNK_SIZE) + (CHUNK_SIZE / 2);
    byte[] inputData = bucket.writeKey(keyName, dataLength);

    assertEquals(writeChunkCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));

    KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName);

    // Seek to position 150
    keyInputStream.seek(150);

    assertEquals(150, keyInputStream.getPos());

    // Seek operation should not result in any readChunk operation.
    assertEquals(readChunkCount, metrics
        .getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    byte[] readData = new byte[CHUNK_SIZE];
    IOUtils.readFully(keyInputStream, readData);

    // Since we read data from index 150 to 250 and the chunk boundary is
    // 100 bytes, we need to read 2 chunks.
    assertEquals(readChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    keyInputStream.close();

    // Verify that the data read matches with the input data at corresponding
    // indices.
    for (int i = 0; i < CHUNK_SIZE; i++) {
      assertEquals(inputData[CHUNK_SIZE + 50 + i], readData[i]);
    }
  }

  private void testReadChunkWithByteArray(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();

    // write data spanning multiple blocks/chunks
    int dataLength = 2 * BLOCK_SIZE + (BLOCK_SIZE / 2);
    byte[] data = bucket.writeRandomBytes(keyName, dataLength);

    // read chunk data using Byte Array
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {

      int[] bufferSizeList = {BYTES_PER_CHECKSUM + 1, CHUNK_SIZE / 4,
          CHUNK_SIZE / 2, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1,
          BLOCK_SIZE - 1, BLOCK_SIZE, BLOCK_SIZE + 1, BLOCK_SIZE * 2};
      for (int bufferSize : bufferSizeList) {
        assertReadFully(data, keyInputStream, bufferSize, 0);
        keyInputStream.seek(0);
      }
    }
  }

  public void testReadChunkWithByteBuffer(TestBucket bucket) throws Exception {
    String keyName = getNewKeyName();

    // write data spanning multiple blocks/chunks
    int dataLength = 2 * BLOCK_SIZE + (BLOCK_SIZE / 2);
    byte[] data = bucket.writeRandomBytes(keyName, dataLength);

    // read chunk data using ByteBuffer
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {

      int[] bufferSizeList = {BYTES_PER_CHECKSUM + 1, CHUNK_SIZE / 4,
          CHUNK_SIZE / 2, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1,
          BLOCK_SIZE - 1, BLOCK_SIZE, BLOCK_SIZE + 1, BLOCK_SIZE * 2};
      for (int bufferSize : bufferSizeList) {
        assertReadFullyUsingByteBuffer(data, keyInputStream, bufferSize, 0);
        keyInputStream.seek(0);
      }
    }
  }

  private void testSkip(TestBucket bucket) throws Exception {
    XceiverClientManager.resetXceiverClientMetrics();
    XceiverClientMetrics metrics = XceiverClientManager
        .getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long readChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.ReadChunk);

    String keyName = getNewKeyName();
    // write data spanning 3 chunks
    int dataLength = (2 * CHUNK_SIZE) + (CHUNK_SIZE / 2);
    byte[] inputData = bucket.writeKey(keyName, dataLength);

    assertEquals(writeChunkCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));

    KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName);

    // skip 150
    long skipped = keyInputStream.skip(70);
    assertEquals(70, skipped);
    assertEquals(70, keyInputStream.getPos());

    skipped = keyInputStream.skip(0);
    assertEquals(0, skipped);
    assertEquals(70, keyInputStream.getPos());

    skipped = keyInputStream.skip(80);
    assertEquals(80, skipped);
    assertEquals(150, keyInputStream.getPos());

    // Skip operation should not result in any readChunk operation.
    assertEquals(readChunkCount, metrics
        .getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    byte[] readData = new byte[CHUNK_SIZE];
    IOUtils.readFully(keyInputStream, readData);

    // Since we reading data from index 150 to 250 and the chunk boundary is
    // 100 bytes, we need to read 2 chunks.
    assertEquals(readChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    keyInputStream.close();

    // Verify that the data read matches with the input data at corresponding
    // indices.
    for (int i = 0; i < CHUNK_SIZE; i++) {
      assertEquals(inputData[CHUNK_SIZE + 50 + i], readData[i]);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Order(Integer.MAX_VALUE) // shuts down datanodes
  void readAfterReplication(boolean doUnbuffer) throws Exception {
    try (OzoneClient client = getCluster().newClient()) {
      TestBucket bucket = TestBucket.newBuilder(client).build();

      testReadAfterReplication(bucket, doUnbuffer);
    }
  }

  private void testReadAfterReplication(TestBucket bucket, boolean doUnbuffer) throws Exception {
    int dataLength = 2 * CHUNK_SIZE;
    String keyName = getNewKeyName();
    byte[] data = bucket.writeRandomBytes(keyName, dataLength);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.delegate().getVolumeName())
        .setBucketName(bucket.delegate().getName())
        .setKeyName(keyName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .build();
    OmKeyInfo keyInfo = getCluster().getOzoneManager()
        .getKeyInfo(keyArgs, false)
        .getKeyInfo();

    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    assertNotNull(locations);
    List<OmKeyLocationInfo> locationInfoList = locations.getLocationList();
    assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo loc = locationInfoList.get(0);
    long containerID = loc.getContainerID();
    assertEquals(3, countReplicas(containerID, getCluster()));

    TestHelper.waitForContainerClose(getCluster(), containerID);

    List<DatanodeDetails> pipelineNodes = loc.getPipeline().getNodes();

    // read chunk data with ByteBuffer
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      int b = keyInputStream.read();
      assertNotEquals(-1, b);
      if (doUnbuffer) {
        keyInputStream.unbuffer();
      }
      getCluster().shutdownHddsDatanode(pipelineNodes.get(0));
      // check that we can still read it
      assertReadFullyUsingByteBuffer(data, keyInputStream, dataLength - 1, 1);
    }
  }

  private void assertReadFully(byte[] data, InputStream in,
      int bufferSize, int totalRead) throws IOException {

    byte[] buffer = new byte[bufferSize];
    while (totalRead < data.length) {
      int numBytesRead = in.read(buffer);
      if (numBytesRead == -1 || numBytesRead == 0) {
        break;
      }
      byte[] tmp1 =
          Arrays.copyOfRange(data, totalRead, totalRead + numBytesRead);
      byte[] tmp2 =
          Arrays.copyOfRange(buffer, 0, numBytesRead);
      assertArrayEquals(tmp1, tmp2);
      totalRead += numBytesRead;
    }
    assertEquals(data.length, totalRead);
  }

  private void assertReadFullyUsingByteBuffer(byte[] data, KeyInputStream in,
      int bufferSize, int totalRead) throws IOException {

    ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
    while (totalRead < data.length) {
      int numBytesRead = in.read(buffer);
      if (numBytesRead == -1 || numBytesRead == 0) {
        break;
      }
      byte[] tmp1 =
          Arrays.copyOfRange(data, totalRead, totalRead + numBytesRead);
      byte[] tmp2 = new byte[numBytesRead];
      buffer.flip();
      buffer.get(tmp2, 0, numBytesRead);
      assertArrayEquals(tmp1, tmp2);
      totalRead += numBytesRead;
      buffer.clear();
    }
    assertEquals(data.length, totalRead);
  }
}

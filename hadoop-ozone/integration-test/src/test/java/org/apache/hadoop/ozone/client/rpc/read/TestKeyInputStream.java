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
import java.io.InputStream;
import java.util.Arrays;

import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.container.TestHelper.countReplicas;
import static org.junit.Assert.fail;

/**
 * Tests {@link KeyInputStream}.
 */
public class TestKeyInputStream extends TestInputStreamBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyInputStream.class);

  public TestKeyInputStream(ChunkLayOutVersion layout) {
    super(layout);
  }

  /**
   * This method does random seeks and reads and validates the reads are
   * correct or not.
   * @param dataLength
   * @param keyInputStream
   * @param inputData
   * @throws Exception
   */
  private void randomSeek(int dataLength, KeyInputStream keyInputStream,
      byte[] inputData) throws Exception {
    // Do random seek.
    for (int i=0; i<dataLength - 300; i+=20) {
      validate(keyInputStream, inputData, i, 200);
    }

    // Seek to end and read in reverse order. And also this is partial chunks
    // as readLength is 20, chunk length is 100.
    for (int i=dataLength - 100; i>=100; i-=20) {
      validate(keyInputStream, inputData, i, 20);
    }

    // Start from begin and seek such that we read partially chunks.
    for (int i=0; i<dataLength - 300; i+=20) {
      validate(keyInputStream, inputData, i, 90);
    }

  }

  /**
   * This method seeks to specified seek value and read the data specified by
   * readLength and validate the read is correct or not.
   * @param keyInputStream
   * @param inputData
   * @param seek
   * @param readLength
   * @throws Exception
   */
  private void validate(KeyInputStream keyInputStream, byte[] inputData,
      long seek, int readLength) throws Exception {
    keyInputStream.seek(seek);

    byte[] readData = new byte[readLength];
    keyInputStream.read(readData, 0, readLength);

    validateData(inputData, (int) seek, readData);
  }

  @Test
  public void testSeekRandomly() throws Exception {
    String keyName = getNewKeyName();
    int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
    byte[] inputData = writeRandomBytes(keyName, dataLength);

    KeyInputStream keyInputStream = getKeyInputStream(keyName);

    // Seek to some where end.
    validate(keyInputStream, inputData, dataLength-200, 100);

    // Now seek to start.
    validate(keyInputStream, inputData, 0, 140);

    validate(keyInputStream, inputData, 200, 300);

    validate(keyInputStream, inputData, 30, 500);

    randomSeek(dataLength, keyInputStream, inputData);

    // Read entire key.
    validate(keyInputStream, inputData, 0, dataLength);

    // Repeat again and check.
    randomSeek(dataLength, keyInputStream, inputData);

    validate(keyInputStream, inputData, 0, dataLength);

    keyInputStream.close();
  }

  @Test
  public void testSeek() throws Exception {
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
    byte[] inputData = writeKey(keyName, dataLength);

    Assert.assertEquals(writeChunkCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));

    KeyInputStream keyInputStream = getKeyInputStream(keyName);

    // Seek to position 150
    keyInputStream.seek(150);

    Assert.assertEquals(150, keyInputStream.getPos());

    // Seek operation should not result in any readChunk operation.
    Assert.assertEquals(readChunkCount, metrics
        .getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    byte[] readData = new byte[CHUNK_SIZE];
    keyInputStream.read(readData, 0, CHUNK_SIZE);

    // Since we reading data from index 150 to 250 and the chunk boundary is
    // 100 bytes, we need to read 2 chunks.
    Assert.assertEquals(readChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    keyInputStream.close();

    // Verify that the data read matches with the input data at corresponding
    // indices.
    for (int i = 0; i < CHUNK_SIZE; i++) {
      Assert.assertEquals(inputData[CHUNK_SIZE + 50 + i], readData[i]);
    }
  }

  @Test
  public void testReadChunk() throws Exception {
    String keyName = getNewKeyName();

    // write data spanning multiple blocks/chunks
    int dataLength = 2 * BLOCK_SIZE + (BLOCK_SIZE / 2);
    byte[] data = writeRandomBytes(keyName, dataLength);

    // read chunk data
    try (KeyInputStream keyInputStream = getKeyInputStream(keyName)) {

      int[] bufferSizeList = {BYTES_PER_CHECKSUM + 1, CHUNK_SIZE / 4,
          CHUNK_SIZE / 2, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1,
          BLOCK_SIZE - 1, BLOCK_SIZE, BLOCK_SIZE + 1, BLOCK_SIZE * 2};
      for (int bufferSize : bufferSizeList) {
        assertReadFully(data, keyInputStream, bufferSize, 0);
        keyInputStream.seek(0);
      }
    }
  }

  @Test
  public void testSkip() throws Exception {
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
    byte[] inputData = writeKey(keyName, dataLength);

    Assert.assertEquals(writeChunkCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));

    KeyInputStream keyInputStream = getKeyInputStream(keyName);

    // skip 150
    keyInputStream.skip(70);
    Assert.assertEquals(70, keyInputStream.getPos());
    keyInputStream.skip(0);
    Assert.assertEquals(70, keyInputStream.getPos());
    keyInputStream.skip(80);

    Assert.assertEquals(150, keyInputStream.getPos());

    // Skip operation should not result in any readChunk operation.
    Assert.assertEquals(readChunkCount, metrics
        .getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    byte[] readData = new byte[CHUNK_SIZE];
    keyInputStream.read(readData, 0, CHUNK_SIZE);

    // Since we reading data from index 150 to 250 and the chunk boundary is
    // 100 bytes, we need to read 2 chunks.
    Assert.assertEquals(readChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    keyInputStream.close();

    // Verify that the data read matches with the input data at corresponding
    // indices.
    for (int i = 0; i < CHUNK_SIZE; i++) {
      Assert.assertEquals(inputData[CHUNK_SIZE + 50 + i], readData[i]);
    }
  }

  @Test
  public void readAfterReplication() throws Exception {
    testReadAfterReplication(false);
  }

  @Test
  public void readAfterReplicationWithUnbuffering() throws Exception {
    testReadAfterReplication(true);
  }

  private void testReadAfterReplication(boolean doUnbuffer) throws Exception {
    Assume.assumeTrue(cluster.getHddsDatanodes().size() > 3);

    int dataLength = 2 * CHUNK_SIZE;
    String keyName = getNewKeyName();
    byte[] data = writeRandomBytes(keyName, dataLength);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    Assert.assertNotNull(locations);
    List<OmKeyLocationInfo> locationInfoList = locations.getLocationList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo loc = locationInfoList.get(0);
    long containerID = loc.getContainerID();
    Assert.assertEquals(3, countReplicas(containerID, cluster));

    TestHelper.waitForContainerClose(cluster, containerID);

    List<DatanodeDetails> pipelineNodes = loc.getPipeline().getNodes();

    // read chunk data
    try (KeyInputStream keyInputStream = getKeyInputStream(keyName)) {

      int b = keyInputStream.read();
      Assert.assertNotEquals(-1, b);

      if (doUnbuffer) {
        keyInputStream.unbuffer();
      }

      cluster.shutdownHddsDatanode(pipelineNodes.get(0));

      // check that we can still read it
      assertReadFully(data, keyInputStream, dataLength - 1, 1);
    }
  }

  private void waitForNodeToBecomeDead(
      DatanodeDetails datanode) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() ->
        HddsProtos.NodeState.DEAD == getNodeHealth(datanode),
        100, 30000);
    LOG.info("Node {} is {}", datanode.getUuidString(),
        getNodeHealth(datanode));
  }

  private HddsProtos.NodeState getNodeHealth(DatanodeDetails dn) {
    HddsProtos.NodeState health = null;
    try {
      NodeManager nodeManager =
          cluster.getStorageContainerManager().getScmNodeManager();
      health = nodeManager.getNodeStatus(dn).getHealth();
    } catch (NodeNotFoundException e) {
      fail("Unexpected NodeNotFound exception");
    }
    return health;
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
      Assert.assertArrayEquals(tmp1, tmp2);
      totalRead += numBytesRead;
    }
    Assert.assertEquals(data.length, totalRead);
  }
}

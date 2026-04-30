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

package org.apache.hadoop.ozone.client.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for ECBlockInputStream.
 */
public class TestECBlockInputStream {

  private static final int ONEMB = 1024 * 1024;

  private ECReplicationConfig repConfig;
  private TestBlockInputStreamFactory streamFactory;
  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  public void setup() {
    repConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, ONEMB);
    streamFactory = new TestBlockInputStreamFactory();
  }

  @Test
  public void testSufficientLocations() {
    // EC-3-2, 5MB block, so all 3 data locations are needed
    BlockLocationInfo keyInfo = ECStreamTestUtil
        .createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertTrue(ecb.hasSufficientLocations());
    }

    // EC-3-2, very large block, so all 3 data locations are needed
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, 5000 * ONEMB);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertTrue(ecb.hasSufficientLocations());
    }

    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();

    // EC-3-2, 1 byte short of 1MB with 1 location
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB - 1, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertTrue(ecb.hasSufficientLocations());
    }

    // EC-3-2, 5MB blocks, only 2 locations passed so we do not have sufficient
    // locations.
    dnMap.clear();
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5 * ONEMB, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertFalse(ecb.hasSufficientLocations());
    }

    // EC-3-2, 5MB blocks, only 1 data and 2 parity locations present. For now
    // this will fail as we don't support reconstruction reads yet.
    dnMap.clear();
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 4);
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 5);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5 * ONEMB, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertFalse(ecb.hasSufficientLocations());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamLessThanCell()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB - 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.read(buf);
      // We expect only 1 block stream and it should have a length passed of
      // ONEMB - 100.
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB - 100, streams.get(0).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamTwoCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB + 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB, streams.get(0).getLength());
      assertEquals(100, streams.get(1).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamThreeCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 2 * ONEMB + 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB, streams.get(0).getLength());
      assertEquals(ONEMB, streams.get(1).getLength());
      assertEquals(100, streams.get(2).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamThreeFullAndPartialStripe()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 10 * ONEMB + 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(4 * ONEMB, streams.get(0).getLength());
      assertEquals(3 * ONEMB + 100, streams.get(1).getLength());
      assertEquals(3 * ONEMB, streams.get(2).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamSingleFullCell()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB, streams.get(0).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamSeveralFullCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 9 * ONEMB);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(3 * ONEMB, streams.get(0).getLength());
      assertEquals(3 * ONEMB, streams.get(1).getLength());
      assertEquals(3 * ONEMB, streams.get(2).getLength());
    }
  }

  @Test
  public void testSimpleRead() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      assertEquals(100, read);
      validateBufferContents(buf, 0, 100, (byte) 0);
      assertEquals(100, ecb.getPos());
    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      assertTrue(s.isClosed());
    }
  }

  /**
   * This test is to ensure we can read a small key of 1 chunk or less when only
   * the first replica index is available.
   */
  @Test
  public void testSimpleReadUnderOneChunk() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 1, ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      assertEquals(100, read);
      validateBufferContents(buf, 0, 100, (byte) 0);
      assertEquals(100, ecb.getPos());
    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      assertTrue(s.isClosed());
    }
  }

  @Test
  public void testReadPastEOF() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 50);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      assertEquals(50, read);
      read = ecb.read(buf);
      assertEquals(read, -1);
    }
  }

  @Test
  public void testReadCrossingMultipleECChunkBounds() throws IOException {
    // EC-3-2, 5MB block, so all 3 data locations are needed
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        100);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {

      // EC Chunk size is 100 and 3-2. Create a byte buffer to read 3.5 chunks,
      // so 350
      ByteBuffer buf = ByteBuffer.allocate(350);
      int read = ecb.read(buf);
      assertEquals(350, read);

      validateBufferContents(buf, 0, 100, (byte) 0);
      validateBufferContents(buf, 100, 200, (byte) 1);
      validateBufferContents(buf, 200, 300, (byte) 2);
      validateBufferContents(buf, 300, 350, (byte) 0);

      buf.clear();
      read = ecb.read(buf);
      assertEquals(350, read);

      validateBufferContents(buf, 0, 50, (byte) 0);
      validateBufferContents(buf, 50, 150, (byte) 1);
      validateBufferContents(buf, 150, 250, (byte) 2);
      validateBufferContents(buf, 250, 350, (byte) 0);

    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      assertTrue(s.isClosed());
    }
  }

  @Test
  public void testSeekPastBlockLength() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 100);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      assertThrows(EOFException.class, () -> ecb.seek(1000));
    }
  }

  @Test
  public void testSeekToLength() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 100);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      // When seek more than the length, should throw EOFException.
      assertThrows(EOFException.class, () -> ecb.seek(101));
    }
  }

  @Test
  public void testSeekToLengthZeroLengthBlock() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 0);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.seek(0);
      assertEquals(0, ecb.getPos());
      assertEquals(0, ecb.getRemaining());
    }
  }

  @Test
  public void testSeekToValidPosition() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      ecb.seek(ONEMB - 1);
      assertEquals(ONEMB - 1, ecb.getPos());
      assertEquals(ONEMB * 4 + 1, ecb.getRemaining());
      // First read should read the last byte of the first chunk
      assertEquals(0, ecb.read());
      assertEquals(ONEMB,
          streamFactory.getBlockStreams().get(0).position);
      // Second read should be the first byte of the second chunk.
      assertEquals(1, ecb.read());

      // Seek to the end of the file minus one byte
      ecb.seek(ONEMB * 5 - 1);
      assertEquals(1, ecb.read());
      assertEquals(ONEMB * 2,
          streamFactory.getBlockStreams().get(1).position);
      // Second read should be EOF as there is no data left
      assertEquals(-1, ecb.read());
      assertEquals(0, ecb.getRemaining());
    }
  }

  @Test
  public void testErrorReadingBlockReportsBadLocation() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      // Read a full stripe to ensure all streams are created in the stream
      // factory
      ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
      int read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);
      // Now make replication index 2 error on the next read
      streamFactory.getBlockStreams().get(1).setThrowException(true);
      buf.clear();
      BadDataLocationException e =
          assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      assertEquals(1, e.getFailedLocations().size());
      assertEquals(2,
          keyInfo.getPipeline().getReplicaIndex(e.getFailedLocations().get(0)));
    }
  }

  @Test
  public void testNoErrorIfSpareLocationToRead() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    Map<DatanodeDetails, Integer> datanodes = new LinkedHashMap<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }
    // Add a second index = 1
    datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), 1);

    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 8 * ONEMB, datanodes);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      // Read a full stripe to ensure all streams are created in the stream
      // factory
      ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
      int read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);
      // Now make replication index 1 error on the next read but as there is a
      // spare it should read from it with no errors
      streamFactory.getBlockStreams().get(0).setThrowException(true);
      buf.clear();
      read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);

      // Now make the spare one error on the next read, and we should get an
      // error with two failed locations. As each stream is created, a new
      // stream will be created in the stream factory. Our read will read from
      // DNs with EC indexes 1 - 3 first, creating streams 0 to 2. Then when
      // stream(0) is failed for index=1 a new steam is created for the
      // alternative index=1 at stream(3). Hence, to make it error we set
      // stream(3) to throw as below.
      streamFactory.getBlockStreams().get(3).setThrowException(true);
      buf.clear();
      BadDataLocationException e =
          assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      List<DatanodeDetails> failed = e.getFailedLocations();
      // Expect 2 different DNs reported as failure
      assertEquals(2, failed.size());
      assertNotEquals(failed.get(0), failed.get(1));
      // Both failures should map to index = 1.
      for (DatanodeDetails dn : failed) {
        assertEquals(1, datanodes.get(dn));
      }
    }
  }

  @Test
  public void testEcPipelineRefreshFunction() {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);

    BlockID blockID = new BlockID(1, 1);
    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();
    for (int i = 1; i <= 5; i++) {
      dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }

    // Create a refreshFunction that returns a hard-coded EC pipeline.
    Function<BlockID, BlockLocationInfo> refreshFunction = blkID -> {
      Pipeline pipeline = Pipeline.newBuilder()
          .setReplicationConfig(repConfig)
          .setNodes(new ArrayList<>(dnMap.keySet()))
          .setReplicaIndexes(dnMap)
          .setState(Pipeline.PipelineState.CLOSED)
          .setId(PipelineID.randomId())
          .build();
      BlockLocationInfo blockLocation = new BlockLocationInfo.Builder()
          .setPipeline(pipeline)
          .build();
      return blockLocation;
    };

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory,
        clientConfig)) {
      Pipeline pipeline =
          ecb.ecPipelineRefreshFunction(3, refreshFunction)
              .apply(blockID)
              .getPipeline();
      // Check the pipeline is built with the correct Datanode
      // with right replicaIndex.
      assertEquals(HddsProtos.ReplicationType.STAND_ALONE,
          pipeline.getReplicationConfig().getReplicationType());
      assertEquals(1, pipeline.getNodes().size());
      assertEquals(3, dnMap.get(pipeline.getNodes().get(0)));
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void testZeroByteReadThrowsBadDataLocationException() throws Exception {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, ONEMB);
    Map<DatanodeDetails, Integer> datanodes = new LinkedHashMap<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }

    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 8 * ONEMB, datanodes);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, streamFactory, clientConfig)) {

      // Read a full stripe first to initialize and create streams in the factory
      ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
      int read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);

      // Simulate the Bug: Force the underlying stream to return 0 bytes (Short Read).
      // Note: If the test stub `TestBlockInputStream` does not currently have
      // a method to simulate a 0-byte read, you should add a simple boolean flag
      // like `simulateZeroByteRead` to that stub class, making its read() return 0.
      streamFactory.getBlockStreams().get(0).setSimulateZeroByteRead(true);

      buf.clear();

      // Assert that instead of spinning infinitely, the short read (0 bytes)
      // immediately triggers the strict validation and throws BadDataLocationException.
      // This exception is essential for the Proxy to initiate the Failover to Reconstruction.
      BadDataLocationException e = assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      List<DatanodeDetails> failed = e.getFailedLocations();

      // Expect exactly 1 DN reported as failure due to the inconsistent read
      assertEquals(1, failed.size());
      // The failure should map to index = 1 (stream 0)
      assertEquals(1, datanodes.get(failed.get(0)));
    }
  }

  private void validateBufferContents(ByteBuffer buf, int from, int to,
      byte val) {
    for (int i = from; i < to; i++) {
      assertEquals(val, buf.get(i));
    }
  }

  private static class TestBlockInputStreamFactory implements
      BlockInputStreamFactory {

    private List<TestBlockInputStream> blockStreams = new ArrayList<>();

    public synchronized List<TestBlockInputStream> getBlockStreams() {
      return blockStreams;
    }

    @Override
    public synchronized BlockExtendedInputStream create(
        ReplicationConfig repConfig, BlockLocationInfo blockInfo,
        Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
        XceiverClientFactory xceiverFactory,
        Function<BlockID, BlockLocationInfo> refreshFunction,
        OzoneClientConfig config) {
      TestBlockInputStream stream = new TestBlockInputStream(
          blockInfo.getBlockID(), blockInfo.getLength(),
          (byte)blockStreams.size());
      blockStreams.add(stream);
      return stream;
    }
  }

  private static class TestBlockInputStream extends BlockExtendedInputStream {

    private long position = 0;
    private boolean closed = false;
    private byte dataVal = 1;
    private BlockID blockID;
    private long length;
    private boolean throwException = false;
    private boolean simulateZeroByteRead = false;
    private static final byte EOF = -1;

    @SuppressWarnings("checkstyle:parameternumber")
    TestBlockInputStream(BlockID blockId, long blockLen, byte dataVal) {
      this.dataVal = dataVal;
      this.blockID = blockId;
      this.length = blockLen;
    }

    public boolean isClosed() {
      return closed;
    }

    public void setThrowException(boolean shouldThrow) {
      this.throwException = shouldThrow;
    }

    public void setSimulateZeroByteRead(boolean simulateZeroByteRead) {
      this.simulateZeroByteRead = simulateZeroByteRead;
    }

    @Override
    public BlockID getBlockID() {
      return blockID;
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public int read(byte[] b, int off, int len)
        throws IOException {
      return read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      if (getRemaining() == 0) {
        return EOF;
      }

      if (throwException) {
        throw new IOException("Simulated exception");
      }

      if (simulateZeroByteRead) {
        return 0;
      }

      int toRead = Math.min(buf.remaining(), (int)getRemaining());
      for (int i = 0; i < toRead; i++) {
        buf.put(dataVal);
      }
      position += toRead;
      return toRead;
    }

    @Override
    protected int readWithStrategy(ByteReaderStrategy strategy) throws
        IOException {
      throw new IOException("Should not be called");
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public void unbuffer() {
    }

    @Override
    public void seek(long pos) {
      this.position = pos;
    }

    @Override
    public long getPos() {
      return position;
    }
  }

}

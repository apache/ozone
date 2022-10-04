/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for ECBlockInputStream.
 */
public class TestECBlockInputStream {

  private static final int ONEMB = 1024 * 1024;

  private ECReplicationConfig repConfig;
  private TestBlockInputStreamFactory streamFactory;

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
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assertions.assertTrue(ecb.hasSufficientLocations());
    }

    // EC-3-2, very large block, so all 3 data locations are needed
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, 5000 * ONEMB);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assertions.assertTrue(ecb.hasSufficientLocations());
    }

    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();

    // EC-3-2, 1 byte short of 1MB with 1 location
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB - 1, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assertions.assertTrue(ecb.hasSufficientLocations());
    }

    // EC-3-2, 5MB blocks, only 2 locations passed so we do not have sufficient
    // locations.
    dnMap.clear();
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5 * ONEMB, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assertions.assertFalse(ecb.hasSufficientLocations());
    }

    // EC-3-2, 5MB blocks, only 1 data and 2 parity locations present. For now
    // this will fail as we don't support reconstruction reads yet.
    dnMap.clear();
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 4);
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 5);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5 * ONEMB, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assertions.assertFalse(ecb.hasSufficientLocations());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamLessThanCell()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB - 100);

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.read(buf);
      // We expect only 1 block stream and it should have a length passed of
      // ONEMB - 100.
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      Assertions.assertEquals(ONEMB - 100, streams.get(0).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamTwoCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB + 100);

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      Assertions.assertEquals(ONEMB, streams.get(0).getLength());
      Assertions.assertEquals(100, streams.get(1).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamThreeCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 2 * ONEMB + 100);

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      Assertions.assertEquals(ONEMB, streams.get(0).getLength());
      Assertions.assertEquals(ONEMB, streams.get(1).getLength());
      Assertions.assertEquals(100, streams.get(2).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamThreeFullAndPartialStripe()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 10 * ONEMB + 100);

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      Assertions.assertEquals(4 * ONEMB, streams.get(0).getLength());
      Assertions.assertEquals(3 * ONEMB + 100, streams.get(1).getLength());
      Assertions.assertEquals(3 * ONEMB, streams.get(2).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamSingleFullCell()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB);

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      Assertions.assertEquals(ONEMB, streams.get(0).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamSeveralFullCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 9 * ONEMB);

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      Assertions.assertEquals(3 * ONEMB, streams.get(0).getLength());
      Assertions.assertEquals(3 * ONEMB, streams.get(1).getLength());
      Assertions.assertEquals(3 * ONEMB, streams.get(2).getLength());
    }
  }

  @Test
  public void testSimpleRead() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      Assertions.assertEquals(100, read);
      validateBufferContents(buf, 0, 100, (byte) 0);
      Assertions.assertEquals(100, ecb.getPos());
    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      Assertions.assertTrue(s.isClosed());
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
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      Assertions.assertEquals(100, read);
      validateBufferContents(buf, 0, 100, (byte) 0);
      Assertions.assertEquals(100, ecb.getPos());
    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      Assertions.assertTrue(s.isClosed());
    }
  }

  @Test
  public void testReadPastEOF() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 50);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      Assertions.assertEquals(50, read);
      read = ecb.read(buf);
      Assertions.assertEquals(read, -1);
    }
  }

  @Test
  public void testReadCrossingMultipleECChunkBounds() throws IOException {
    // EC-3-2, 5MB block, so all 3 data locations are needed
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        100);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {

      // EC Chunk size is 100 and 3-2. Create a byte buffer to read 3.5 chunks,
      // so 350
      ByteBuffer buf = ByteBuffer.allocate(350);
      int read = ecb.read(buf);
      Assertions.assertEquals(350, read);

      validateBufferContents(buf, 0, 100, (byte) 0);
      validateBufferContents(buf, 100, 200, (byte) 1);
      validateBufferContents(buf, 200, 300, (byte) 2);
      validateBufferContents(buf, 300, 350, (byte) 0);

      buf.clear();
      read = ecb.read(buf);
      Assertions.assertEquals(350, read);

      validateBufferContents(buf, 0, 50, (byte) 0);
      validateBufferContents(buf, 50, 150, (byte) 1);
      validateBufferContents(buf, 150, 250, (byte) 2);
      validateBufferContents(buf, 250, 350, (byte) 0);

    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      Assertions.assertTrue(s.isClosed());
    }
  }

  @Test
  public void testSeekPastBlockLength() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 100);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      assertThrows(EOFException.class, () -> ecb.seek(1000));
    }
  }

  @Test
  public void testSeekToLength() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 100);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
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
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.seek(0);
      Assertions.assertEquals(0, ecb.getPos());
      Assertions.assertEquals(0, ecb.getRemaining());
    }
  }

  @Test
  public void testSeekToValidPosition() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      ecb.seek(ONEMB - 1);
      Assertions.assertEquals(ONEMB - 1, ecb.getPos());
      Assertions.assertEquals(ONEMB * 4 + 1, ecb.getRemaining());
      // First read should read the last byte of the first chunk
      Assertions.assertEquals(0, ecb.read());
      Assertions.assertEquals(ONEMB,
          streamFactory.getBlockStreams().get(0).position);
      // Second read should be the first byte of the second chunk.
      Assertions.assertEquals(1, ecb.read());

      // Seek to the end of the file minus one byte
      ecb.seek(ONEMB * 5 - 1);
      Assertions.assertEquals(1, ecb.read());
      Assertions.assertEquals(ONEMB * 2,
          streamFactory.getBlockStreams().get(1).position);
      // Second read should be EOF as there is no data left
      Assertions.assertEquals(-1, ecb.read());
      Assertions.assertEquals(0, ecb.getRemaining());
    }
  }

  @Test
  public void testErrorReadingBlockReportsBadLocation() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      // Read a full stripe to ensure all streams are created in the stream
      // factory
      ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
      int read = ecb.read(buf);
      Assertions.assertEquals(3 * ONEMB, read);
      // Now make replication index 2 error on the next read
      streamFactory.getBlockStreams().get(1).setThrowException(true);
      buf.clear();
      BadDataLocationException e =
          assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      Assertions.assertEquals(2,
          keyInfo.getPipeline().getReplicaIndex(e.getFailedLocation()));
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
    Function<BlockID, Pipeline> refreshFunction = blkID -> Pipeline.newBuilder()
        .setReplicationConfig(repConfig)
        .setNodes(new ArrayList<>(dnMap.keySet()))
        .setReplicaIndexes(dnMap)
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .build();

    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, true, null, null, streamFactory)) {
      Pipeline pipeline =
          ecb.ecPipelineRefreshFunction(3, refreshFunction).apply(blockID);
      // Check the pipeline is built with the correct Datanode
      // with right replicaIndex.
      Assertions.assertEquals(HddsProtos.ReplicationType.STAND_ALONE,
          pipeline.getReplicationConfig().getReplicationType());
      Assertions.assertEquals(1, pipeline.getNodes().size());
      Assertions.assertEquals(3, dnMap.get(pipeline.getNodes().get(0)));
    }
  }

  private void validateBufferContents(ByteBuffer buf, int from, int to,
      byte val) {
    for (int i = from; i < to; i++) {
      Assertions.assertEquals(val, buf.get(i));
    }
  }

  private static class TestBlockInputStreamFactory implements
      BlockInputStreamFactory {

    private List<TestBlockInputStream> blockStreams = new ArrayList<>();

    public synchronized List<TestBlockInputStream> getBlockStreams() {
      return blockStreams;
    }

    public synchronized BlockExtendedInputStream create(
        ReplicationConfig repConfig, BlockLocationInfo blockInfo,
        Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
        boolean verifyChecksum, XceiverClientFactory xceiverFactory,
        Function<BlockID, Pipeline> refreshFunction) {
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

    @Override
    public BlockID getBlockID() {
      return blockID;
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public long getRemaining() {
      return getLength() - position;
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

      int toRead = Math.min(buf.remaining(), (int)getRemaining());
      for (int i = 0; i < toRead; i++) {
        buf.put(dataVal);
      }
      position += toRead;
      return toRead;
    };

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

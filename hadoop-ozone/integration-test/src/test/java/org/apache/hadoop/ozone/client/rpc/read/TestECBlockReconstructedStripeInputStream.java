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
package org.apache.hadoop.ozone.client.rpc.read;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.ECBlockInputStream;
import org.apache.hadoop.ozone.client.io.ECBlockReconstructedStripeInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.erasurecode.CodecRegistry;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Test for the ECBlockReconstructedStripeInputStream.
 */
public class TestECBlockReconstructedStripeInputStream {


  private static final int ONEMB = 1024 * 1024;

  private ECReplicationConfig repConfig;
  private TestBlockInputStreamFactory streamFactory;

  @Before
  public void setup() {
    repConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, ONEMB);
    streamFactory = new TestBlockInputStreamFactory();
  }

  @Test
  public void testSufficientLocations() {
    // One chunk, only 1 location.
    OmKeyLocationInfo keyInfo = createKeyInfo(repConfig, 1, ONEMB);
    try (ECBlockInputStream ecb =
        new ECBlockReconstructedStripeInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assert.assertTrue(ecb.hasSufficientLocations());
    }

    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();

    // Two Chunks, but missing data block 2.
    dnMap = createIndexMap(1, 4, 5);
    keyInfo = createKeyInfo(repConfig, ONEMB * 2, dnMap);
    try (ECBlockInputStream ecb =
        new ECBlockReconstructedStripeInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assert.assertTrue(ecb.hasSufficientLocations());
    }

    // Three Chunks, but missing data block 2 and 3.
    dnMap = createIndexMap(1, 4, 5);
    keyInfo = createKeyInfo(repConfig, ONEMB * 3, dnMap);
    try (ECBlockInputStream ecb =
        new ECBlockReconstructedStripeInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assert.assertTrue(ecb.hasSufficientLocations());
    }

    // Three Chunks, but missing data block 2 and 3 and parity 1.
    dnMap = createIndexMap(1, 4);
    keyInfo = createKeyInfo(repConfig, ONEMB * 3, dnMap);
    try (ECBlockInputStream ecb =
        new ECBlockReconstructedStripeInputStream(repConfig,
        keyInfo, true, null, null, new TestBlockInputStreamFactory())) {
      Assert.assertFalse(ecb.hasSufficientLocations());
    }
  }

  @Test
  public void testReadFullStripesWithPartial() throws IOException {
    // Generate the input data for 3 full stripes and generate the parity.
    int chunkSize = repConfig.getEcChunkSize();
    int partialStripeSize = chunkSize * 2 - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 4 * chunkSize);
    dataBufs[1].limit(4 * chunkSize - 1);
    dataBufs[2].limit(3 * chunkSize);
    for (ByteBuffer b : dataBufs) {
      randomFill(b);
    }
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    List<Map<DatanodeDetails, Integer>> locations = new ArrayList<>();
    // Two data missing
    locations.add(createIndexMap(1, 4, 5));
    // One data missing
    locations.add(createIndexMap(1, 2, 4, 5));
    // Two data missing including first
    locations.add(createIndexMap(2, 4, 5));
    // One data and one parity missing
    locations.add(createIndexMap(2, 3, 4));

    for (Map<DatanodeDetails, Integer> dnMap : locations) {
      streamFactory = new TestBlockInputStreamFactory();
      addDataStreamsToFactory(dataBufs, parity);

      OmKeyLocationInfo keyInfo = createKeyInfo(repConfig,
          stripeSize() * 3 + partialStripeSize, dnMap);
      streamFactory.setCurrentPipeline(keyInfo.getPipeline());

      ByteBuffer[] bufs = allocateByteBuffers(repConfig);
      try (ECBlockReconstructedStripeInputStream ecb =
          new ECBlockReconstructedStripeInputStream(repConfig, keyInfo, true,
              null, null, streamFactory)) {
        // Read 3 full stripes
        for (int i = 0; i < 3; i++) {
          int read = ecb.readStripe(bufs);
          for (int j = 0; j < bufs.length; j++) {
            validateContents(dataBufs[j], bufs[j], i * chunkSize, chunkSize);
          }
          Assert.assertEquals(stripeSize(), read);

          // Check the underlying streams have read 1 chunk per read:
          for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
            Assert.assertEquals(chunkSize * (i + 1),
                bis.getPos());
          }
          Assert.assertEquals(stripeSize() * (i + 1), ecb.getPos());
          clearBuffers(bufs);
        }
        // The next read is a partial stripe
        int read = ecb.readStripe(bufs);
        Assert.assertEquals(partialStripeSize, read);
        validateContents(dataBufs[0], bufs[0], 3 * chunkSize, chunkSize);
        validateContents(dataBufs[1], bufs[1], 3 * chunkSize, chunkSize - 1);
        Assert.assertEquals(0, bufs[2].remaining());
        Assert.assertEquals(0, bufs[2].position());

        // A further read should give EOF
        clearBuffers(bufs);
        read = ecb.readStripe(bufs);
        Assert.assertEquals(-1, read);
      }
    }
  }

  @Test
  public void testReadPartialStripe() throws IOException {
    int blockLength = repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    // First buffer has only the blockLength, the other two will have no data.
    dataBufs[0].limit(blockLength);
    dataBufs[1].limit(0);
    dataBufs[2].limit(0);
    for (ByteBuffer b : dataBufs) {
      randomFill(b);
    }
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    ByteBuffer[] bufs = allocateByteBuffers(repConfig);
    // We have a length that is less than a single chunk, so blocks 2 and 3
    // are padding and will not be present. Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.
    Map<DatanodeDetails, Integer> dnMap = createIndexMap(4, 5);
    OmKeyLocationInfo keyInfo =
        createKeyInfo(repConfig, blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());
    try (ECBlockReconstructedStripeInputStream ecb =
        new ECBlockReconstructedStripeInputStream(repConfig, keyInfo, true,
            null, null, streamFactory)) {
      int read = ecb.readStripe(bufs);
      Assert.assertEquals(blockLength, read);
      validateContents(dataBufs[0], bufs[0], 0, blockLength);
      Assert.assertEquals(0, bufs[1].remaining());
      Assert.assertEquals(0, bufs[1].position());
      Assert.assertEquals(0, bufs[2].remaining());
      Assert.assertEquals(0, bufs[2].position());
      // Check the underlying streams have been advanced by 1 blockLength:
      for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
        Assert.assertEquals(blockLength, bis.getPos());
      }
      Assert.assertEquals(ecb.getPos(), blockLength);
      clearBuffers(bufs);
      // A further read should give EOF
      read = ecb.readStripe(bufs);
      Assert.assertEquals(-1, read);
    }
  }

  @Test
  public void testReadPartialStripeTwoChunks() throws IOException {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * 2 - 1;

    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    // First buffer has only the blockLength, the other two will have no data.
    dataBufs[0].limit(chunkSize);
    dataBufs[1].limit(chunkSize - 1);
    dataBufs[2].limit(0);
    for (ByteBuffer b : dataBufs) {
      randomFill(b);
    }
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    ByteBuffer[] bufs = allocateByteBuffers(repConfig);
    // We have a length that is less than a single chunk, so blocks 2 and 3
    // are padding and will not be present. Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.
    Map<DatanodeDetails, Integer> dnMap = createIndexMap(4, 5);
    OmKeyLocationInfo keyInfo =
        createKeyInfo(repConfig, blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());
    try (ECBlockReconstructedStripeInputStream ecb =
        new ECBlockReconstructedStripeInputStream(repConfig, keyInfo, true,
            null, null, streamFactory)) {
      int read = ecb.readStripe(bufs);
      Assert.assertEquals(blockLength, read);
      validateContents(dataBufs[0], bufs[0], 0, chunkSize);
      validateContents(dataBufs[1], bufs[1], 0, chunkSize - 1);
      Assert.assertEquals(0, bufs[2].remaining());
      Assert.assertEquals(0, bufs[2].position());
      // Check the underlying streams have been advanced by 1 chunk:
      for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
        Assert.assertEquals(chunkSize, bis.getPos());
      }
      Assert.assertEquals(ecb.getPos(), blockLength);
      clearBuffers(bufs);
      // A further read should give EOF
      read = ecb.readStripe(bufs);
      Assert.assertEquals(-1, read);
    }
  }

  @Test
  public void testReadPartialStripeThreeChunks() throws IOException {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * 3 - 1;

    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    // First buffer has only the blockLength, the other two will have no data.
    dataBufs[0].limit(chunkSize);
    dataBufs[1].limit(chunkSize);
    dataBufs[2].limit(chunkSize - 1);
    for (ByteBuffer b : dataBufs) {
      randomFill(b);
    }
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    // We have a length that is less than a stripe, so chunks 1 and 2 are full.
    // Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.

    List<Map<DatanodeDetails, Integer>> locations = new ArrayList<>();
    // Two data missing
    locations.add(createIndexMap(3, 4, 5));
    // Two data missing
    locations.add(createIndexMap(1, 4, 5));
    // One data missing - the last one
    locations.add(createIndexMap(1, 2, 5));
    // One data and one parity missing
    locations.add(createIndexMap(2, 3, 4));
    // One data and one parity missing
    locations.add(createIndexMap(1, 2, 4));

    for (Map<DatanodeDetails, Integer> dnMap : locations) {
      streamFactory = new TestBlockInputStreamFactory();
      addDataStreamsToFactory(dataBufs, parity);
      ByteBuffer[] bufs = allocateByteBuffers(repConfig);

      OmKeyLocationInfo keyInfo =
          createKeyInfo(repConfig, blockLength, dnMap);
      streamFactory.setCurrentPipeline(keyInfo.getPipeline());
      try (ECBlockReconstructedStripeInputStream ecb =
          new ECBlockReconstructedStripeInputStream(repConfig, keyInfo, true,
              null, null, streamFactory)) {
        int read = ecb.readStripe(bufs);
        Assert.assertEquals(blockLength, read);
        validateContents(dataBufs[0], bufs[0], 0, chunkSize);
        validateContents(dataBufs[1], bufs[1], 0, chunkSize);
        validateContents(dataBufs[2], bufs[2], 0, chunkSize - 1);
        // Check the underlying streams have been advanced by 1 chunk:
        for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
          Assert.assertEquals(0, bis.getRemaining());
        }
        Assert.assertEquals(ecb.getPos(), blockLength);
        clearBuffers(bufs);
        // A further read should give EOF
        read = ecb.readStripe(bufs);
        Assert.assertEquals(-1, read);
      }
    }
  }

  private void addDataStreamsToFactory(ByteBuffer[] data, ByteBuffer[] parity) {
    List<ByteBuffer> dataStreams = new ArrayList<>();
    for (ByteBuffer b : data) {
      dataStreams.add(b);
    }
    for (ByteBuffer b : parity) {
      dataStreams.add(b);
    }
    streamFactory.setBlockStreamData(dataStreams);
  }

  /**
   * Validates that the data buffer has the same contents as the source buffer,
   * starting the checks in the src at offset and for count bytes.
   * @param src The source of the data
   * @param data The data which should be checked against the source
   * @param offset The starting point in the src buffer
   * @param count How many bytes to check.
   */
  private void validateContents(ByteBuffer src, ByteBuffer data, int offset,
      int count) {
    byte[] srcArray = src.array();
    Assert.assertEquals(count, data.remaining());
    for (int i = offset; i < offset + count; i++) {
      Assert.assertEquals("Element " + i, srcArray[i], data.get());
    }
    data.flip();
  }

  /**
   * Returns a new map containing a random DatanodeDetails for each index in
   * inputs.
   * @param idxs A list of indexes to add to the map
   * @return A map of DatanodeDetails to index.
   */
  private Map<DatanodeDetails, Integer> createIndexMap(int... idxs) {
    Map<DatanodeDetails, Integer> map = new HashMap<>();
    for (int i : idxs) {
      map.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }
    return map;
  }

  /**
   * Given a set of data buffers, generate the parity data for the inputs.
   * @param data A set of data buffers
   * @param ecConfig The ECReplicationConfig representing the scheme
   * @return
   * @throws IOException
   */
  private ByteBuffer[] generateParity(ByteBuffer[] data,
      ECReplicationConfig ecConfig) throws IOException {
    // First data buffer dictates the size
    int cellSize = data[0].limit();
    // Store the positions of the remaining data buffers so we can restore them
    int[] dataLimits = new int[data.length];
    for (int i=1; i<data.length; i++) {
      dataLimits[i] = data[i].limit();
      data[i].limit(cellSize);
      zeroFill(data[i]);
      data[i].flip();
    }
    ByteBuffer[] parity = new ByteBuffer[ecConfig.getParity()];
    for (int i = 0; i < ecConfig.getParity(); i++) {
      parity[i] = ByteBuffer.allocate(cellSize);
    }
    RawErasureEncoder encoder = CodecRegistry.getInstance()
        .getCodecFactory(repConfig.getCodec().toString())
        .createEncoder(repConfig);
    encoder.encode(data, parity);

    data[0].flip();
    for (int i = 1; i < data.length; i++) {
      data[i].limit(dataLimits[i]);
      data[i].position(0);
    }
    return parity;
  }

  /**
   * Fill the remaining space in a buffer random bytes.
   * @param buf
   */
  private void randomFill(ByteBuffer buf) {
    while (buf.hasRemaining()) {
      buf.put((byte)ThreadLocalRandom.current().nextInt(255));
    }
    buf.flip();
  }

  /**
   * Fill / Pad the remaining space in a buffer with zeros.
   * @param buf
   */
  private void zeroFill(ByteBuffer buf) {
    byte[] a = buf.array();
    Arrays.fill(a, buf.position(), buf.limit(), (byte)0);
    buf.position(buf.limit());
  }

  /**
   * Return a list of num ByteBuffers of the given size.
   * @param num Number of buffers to create
   * @param size The size of each buffer
   * @return
   */
  private ByteBuffer[] allocateBuffers(int num, int size) {
    ByteBuffer[] bufs = new ByteBuffer[num];
    for (int i = 0; i < num; i++) {
      bufs[i] = ByteBuffer.allocate(size);
    }
    return bufs;
  }

  private int stripeSize() {
    return stripeSize(repConfig);
  }

  private int stripeSize(ECReplicationConfig rconfig) {
    return rconfig.getEcChunkSize() * rconfig.getData();
  }

  private void clearBuffers(ByteBuffer[] bufs) {
    for (ByteBuffer b : bufs) {
      b.clear();
    }
  }

  private ByteBuffer[] allocateByteBuffers(ECReplicationConfig rConfig) {
    ByteBuffer[] bufs = new ByteBuffer[repConfig.getData()];
    for (int i = 0; i < bufs.length; i++) {
      bufs[i] = ByteBuffer.allocate(rConfig.getEcChunkSize());
    }
    return bufs;
  }

  private OmKeyLocationInfo createKeyInfo(ReplicationConfig repConf,
      long blockLength, Map<DatanodeDetails, Integer> dnMap) {

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .setNodes(new ArrayList<>(dnMap.keySet()))
        .setReplicaIndexes(dnMap)
        .setReplicationConfig(repConf)
        .build();

    OmKeyLocationInfo keyInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, 1))
        .setLength(blockLength)
        .setOffset(0)
        .setPipeline(pipeline)
        .setPartNumber(0)
        .build();
    return keyInfo;
  }

  private OmKeyLocationInfo createKeyInfo(ReplicationConfig repConf,
      int nodeCount, long blockLength) {
    Map<DatanodeDetails, Integer> datanodes = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i + 1);
    }
    return createKeyInfo(repConf, blockLength, datanodes);
  }

  private static class TestBlockInputStreamFactory implements
      BlockInputStreamFactory {

    private List<TestBlockInputStream> blockStreams = new ArrayList<>();
    private List<ByteBuffer> blockStreamData;

    private Pipeline currentPipeline;

    public List<TestBlockInputStream> getBlockStreams() {
      return blockStreams;
    }

    public void setBlockStreamData(List<ByteBuffer> bufs) {
      this.blockStreamData = bufs;
    }

    public void setCurrentPipeline(Pipeline pipeline) {
      this.currentPipeline = pipeline;
    }

    public BlockExtendedInputStream create(ReplicationConfig repConfig,
        OmKeyLocationInfo blockInfo, Pipeline pipeline,
        Token<OzoneBlockTokenIdentifier> token, boolean verifyChecksum,
        XceiverClientFactory xceiverFactory,
        Function<BlockID, Pipeline> refreshFunction) {

      int repInd = currentPipeline.getReplicaIndex(pipeline.getNodes().get(0));
      TestBlockInputStream stream = new TestBlockInputStream(
          blockInfo.getBlockID(), blockInfo.getLength(),
          blockStreamData.get(repInd -1));
      blockStreams.add(stream);
      return stream;
    }
  }

  private static class TestBlockInputStream extends BlockExtendedInputStream {

    private ByteBuffer data;
    private boolean closed = false;
    private BlockID blockID;
    private long length;
    private static final byte EOF = -1;

    TestBlockInputStream(BlockID blockId, long blockLen, ByteBuffer data) {
      this.blockID = blockId;
      this.length = blockLen;
      this.data = data;
      data.position(0);
    }

    public boolean isClosed() {
      return closed;
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
      return data.remaining();
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
      int toRead = Math.min(buf.remaining(), (int)getRemaining());
      for (int i = 0; i < toRead; i++) {
        buf.put(data.get());
      }
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
    public long getPos() {
      return data.position();
    }

  }

}

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.apache.ratis.util.Preconditions;

/**
 * Utility class providing methods useful in EC tests.
 */
public final class ECStreamTestUtil {

  private ECStreamTestUtil() {
  }

  public static BlockLocationInfo createKeyInfo(ReplicationConfig repConf,
      long blockLength, Map<DatanodeDetails, Integer> dnMap) {

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .setNodes(new ArrayList<>(dnMap.keySet()))
        .setReplicaIndexes(dnMap)
        .setReplicationConfig(repConf)
        .build();

    BlockLocationInfo keyInfo = new BlockLocationInfo.Builder()
        .setBlockID(new BlockID(1, 1))
        .setLength(blockLength)
        .setOffset(0)
        .setPipeline(pipeline)
        .setPartNumber(0)
        .build();
    return keyInfo;
  }

  public static BlockLocationInfo createKeyInfo(ReplicationConfig repConf,
      int nodeCount, long blockLength) {
    Map<DatanodeDetails, Integer> datanodes = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i + 1);
    }
    return createKeyInfo(repConf, blockLength, datanodes);
  }

  /**
   * Fill / Pad the remaining space in a buffer with zeros.
   * @param buf
   */
  public static void zeroFill(ByteBuffer buf) {
    byte[] a = buf.array();
    Arrays.fill(a, buf.position(), buf.limit(), (byte)0);
    buf.position(buf.limit());
  }

  /**
   * Given a List of ByteBuffers, write length of random bytes from the given
   * Random generator to the byte buffers. The data is striped across the
   * buffers in stripeSize chunks.
   * When the length of data has been written, the buffer limits are set to
   * their final positions.
   *
   * @param bufs The list of buffers to fill with random data
   * @param stripeSize The stripe size to use
   * @param rand The random generator to use
   * @param length The length of data to write.
   */
  public static void randomFill(ByteBuffer[] bufs, int stripeSize,
      SplittableRandom rand, int length) {
    Preconditions.assertTrue(totalSpaceAvailable(bufs) >= length);
    int remaining = length;
    while (remaining > 0) {
      for (ByteBuffer b : bufs) {
        int toWrite = Math.min(stripeSize, remaining);
        for (int i = 0; i < toWrite; i++) {
          b.put((byte) rand.nextInt(255));
        }
        remaining -= toWrite;
      }
    }
    // Set the buffer limits to the final position
    for (ByteBuffer b : bufs) {
      b.limit(b.position());
    }
  }

  public static void randomFill(ByteBuffer buf, SplittableRandom rand) {
    while (buf.remaining() > 0) {
      buf.put((byte) rand.nextInt(255));
    }
  }

  private static int totalSpaceAvailable(ByteBuffer[] bufs) {
    int space = 0;
    for (ByteBuffer b : bufs) {
      space += b.remaining();
    }
    return space;
  }

  /**
   * Given a buffer which has data loaded, flip the buffer and ensure it matches
   * byte for byte the next series of bytes from the Random generator.
   * @param b Byte Buffers containing data
   * @param rand The random generator
   */
  public static void assertBufferMatches(ByteBuffer b, SplittableRandom rand) {
    b.flip();
    int i = 0;
    while (b.hasRemaining()) {
      i++;
      assertEquals((byte) rand.nextInt(255), b.get(),
          "Failed on iteration " + i);
    }
  }

  /**
   * Given a List of ByteBuffers and the RepConfig, encode the parity buffers
   * from the data buffers. The data buffers should be passed "as is" after
   * reading data. That is, the position will be at the last data byte read in
   * or the buffer limit.
   * The data buffers and parity will be returned "ready to read" with the
   * position reset to zero.
   * @param data List of data buffers
   * @param ecConfig The ECReplicationConfig.
   * @return List of encoded parity buffers.
   * @throws IOException
   */
  public static ByteBuffer[] generateParity(ByteBuffer[] data,
      ECReplicationConfig ecConfig) throws IOException {
    // First data buffer dictates the size
    int cellSize = data[0].limit();
    data[0].flip();
    // Store the positions of the remaining data buffers so we can restore them
    int[] dataLimits = new int[data.length];
    for (int i = 1; i < data.length; i++) {
      dataLimits[i] = data[i].limit();
      data[i].limit(cellSize);
      zeroFill(data[i]);
      data[i].flip();
    }
    ByteBuffer[] parity = new ByteBuffer[ecConfig.getParity()];
    for (int i = 0; i < ecConfig.getParity(); i++) {
      parity[i] = ByteBuffer.allocate(cellSize);
    }
    RawErasureEncoder encoder =
        CodecUtil.createRawEncoderWithFallback(ecConfig);
    encoder.encode(data, parity);

    data[0].flip();
    for (int i = 1; i < data.length; i++) {
      data[i].limit(dataLimits[i]);
      data[i].position(0);
    }
    return parity;
  }

  /**
   * Returns a new map containing a random DatanodeDetails for each index in
   * inputs.
   * @param idxs A list of indexes to add to the map
   * @return A map of DatanodeDetails to index.
   */
  public static Map<DatanodeDetails, Integer> createIndexMap(int... idxs) {
    Map<DatanodeDetails, Integer> map = new HashMap<>();
    for (int i : idxs) {
      map.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }
    return map;
  }

  /**
   * A stream factory which can be used in tests to provide TestBlockStream
   * instances.
   */
  public static class TestBlockInputStreamFactory implements
      BlockInputStreamFactory {

    private Map<Integer, TestBlockInputStream> blockStreams =
        new LinkedHashMap<>();
    private List<ByteBuffer> blockStreamData;
    // List of EC indexes that should fail immediately on read
    private final List<Integer> failIndexes = new ArrayList<>();

    private Pipeline currentPipeline;

    public synchronized
        List<ECStreamTestUtil.TestBlockInputStream> getBlockStreams() {
      return new ArrayList<>(blockStreams.values());
    }

    public synchronized Set<Integer> getStreamIndexes() {
      return blockStreams.keySet();
    }

    public synchronized ECStreamTestUtil.TestBlockInputStream getBlockStream(
        int ecIndex) {
      return blockStreams.get(ecIndex);
    }

    public synchronized void setBlockStreamData(List<ByteBuffer> bufs) {
      this.blockStreamData = bufs;
    }

    public synchronized void setCurrentPipeline(Pipeline pipeline) {
      this.currentPipeline = pipeline;
    }

    // fail each index in the list once
    public synchronized void setFailIndexes(Integer... fail) {
      failIndexes.addAll(Arrays.asList(fail));
    }

    @Override
    public synchronized BlockExtendedInputStream create(
        ReplicationConfig repConfig,
        BlockLocationInfo blockInfo, Pipeline pipeline,
        Token<OzoneBlockTokenIdentifier> token,
        XceiverClientFactory xceiverFactory,
        Function<BlockID, BlockLocationInfo> refreshFunction,
        OzoneClientConfig config) {

      int repInd = currentPipeline.getReplicaIndex(pipeline.getNodes().get(0));
      TestBlockInputStream stream = new TestBlockInputStream(
          blockInfo.getBlockID(), blockInfo.getLength(),
          blockStreamData.get(repInd - 1), repInd);
      if (failIndexes.remove(Integer.valueOf(repInd))) {
        stream.setShouldError(true);
      }
      blockStreams.put(repInd, stream);
      return stream;
    }
  }

  /**
   * A block stream that returns data from the provided ByteBuffer. Intended to
   * be used in tests, rather than reading from a real block stream.
   */
  public static class TestBlockInputStream extends BlockExtendedInputStream {

    private ByteBuffer data;
    private BlockID blockID;
    private long length;
    private boolean shouldError = false;
    private int shouldErrorPosition = 0;
    private boolean shouldErrorOnSeek = false;
    private IOException errorToThrow = null;
    private int ecReplicaIndex = 0;
    private static final byte EOF = -1;

    TestBlockInputStream(BlockID blockId, long blockLen, ByteBuffer data) {
      this(blockId, blockLen, data, 0);
    }

    TestBlockInputStream(BlockID blockId, long blockLen, ByteBuffer data,
        int replicaIndex) {
      this.blockID = blockId;
      this.length = blockLen;
      this.data = data;
      this.ecReplicaIndex = replicaIndex;
      data.position(0);
    }

    public void setShouldErrorOnSeek(boolean val) {
      this.shouldErrorOnSeek = val;
    }

    public void setShouldError(boolean val) {
      shouldError = val;
      shouldErrorPosition = 0;
    }

    public void setShouldError(boolean val, int position,
        IOException errorThrowable) {
      this.shouldError = val;
      this.shouldErrorPosition = position;
      this.errorToThrow = errorThrowable;
    }

    public int getEcReplicaIndex() {
      return ecReplicaIndex;
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
      if (shouldError && data.position() >= shouldErrorPosition) {
        throwError();
      }
      if (getRemaining() == 0) {
        return EOF;
      }
      int toRead = (int)Math.min(buf.remaining(), getRemaining());
      for (int i = 0; i < toRead; i++) {
        if (shouldError && data.position() >= shouldErrorPosition) {
          throwError();
        }
        buf.put(data.get());
      }
      return toRead;
    }

    private void throwError() throws IOException {
      if (errorToThrow != null) {
        throw errorToThrow;
      } else {
        throw new IOException("Simulated error reading block");
      }
    }

    @Override
    protected int readWithStrategy(ByteReaderStrategy strategy) throws
        IOException {
      throw new IOException("Should not be called");
    }

    @Override
    public void close() { }

    @Override
    public void unbuffer() {
    }

    @Override
    public long getPos() {
      return data.position();
    }

    @Override
    public void seek(long pos) throws IOException {
      if (shouldErrorOnSeek) {
        throw new IOException("Simulated exception");
      }
      data.position((int)pos);
    }

  }

}

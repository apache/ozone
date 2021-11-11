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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Class to read data from an EC Block Group.
 */
public class ECBlockInputStream extends BlockExtendedInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECBlockInputStream.class);

  private final ECReplicationConfig repConfig;
  private final int ecChunkSize;
  private final long stripeSize;
  private final BlockInputStreamFactory streamFactory;
  private final boolean verifyChecksum;
  private final XceiverClientFactory xceiverClientFactory;
  private final Function<BlockID, Pipeline> refreshFunction;
  private final OmKeyLocationInfo blockInfo;
  private final DatanodeDetails[] dataLocations;
  private final BlockExtendedInputStream[] blockStreams;
  private final int maxLocations;

  private long position = 0;
  private boolean closed = false;
  private boolean seeked = false;

  protected OmKeyLocationInfo getBlockInfo() {
    return blockInfo;
  }

  protected ECReplicationConfig getRepConfig() {
    return repConfig;
  }

  protected DatanodeDetails[] getDataLocations() {
    return dataLocations;
  }

  protected long getStripeSize() {
    return stripeSize;
  }

  protected int availableDataLocations() {
    int count = 0;
    for (int i = 0; i < repConfig.getData(); i++) {
      if (dataLocations[i] != null) {
        count++;
      }
    }
    return count;
  }

  protected int availableParityLocations() {
    int count = 0;
    for (int i = repConfig.getData();
         i < repConfig.getData() + repConfig.getParity(); i++) {
      if (dataLocations[i] != null) {
        count++;
      }
    }
    return count;
  }

  public ECBlockInputStream(ECReplicationConfig repConfig,
      OmKeyLocationInfo blockInfo, boolean verifyChecksum,
      XceiverClientFactory xceiverClientFactory, Function<BlockID,
      Pipeline> refreshFunction, BlockInputStreamFactory streamFactory) {
    this.repConfig = repConfig;
    this.ecChunkSize = repConfig.getEcChunkSize();
    this.verifyChecksum = verifyChecksum;
    this.blockInfo = blockInfo;
    this.streamFactory = streamFactory;
    this.xceiverClientFactory = xceiverClientFactory;
    this.refreshFunction = refreshFunction;
    this.maxLocations = repConfig.getData() + repConfig.getParity();
    this.dataLocations =
        new DatanodeDetails[repConfig.getData() + repConfig.getParity()];
    this.blockStreams = new BlockExtendedInputStream[repConfig.getData()];

    this.stripeSize = (long)ecChunkSize * repConfig.getData();
    setBlockLocations(this.blockInfo.getPipeline());
  }

  public synchronized boolean hasSufficientLocations() {
    // The number of locations needed is a function of the EC Chunk size. If the
    // block length is <= the chunk size, we should only have location 1. If it
    // is greater than the chunk size but less than chunk_size * 2, then we must
    // have two locations. If it is greater than chunk_size * data_num, then we
    // must have all data_num locations.
    // We only consider data locations here.
    int expectedDataBlocks = calculateExpectedDataBlocks(repConfig);
    return expectedDataBlocks == availableDataLocations();
  }

  protected int calculateExpectedDataBlocks(ECReplicationConfig rConfig) {
    return (int)Math.min(Math.ceil(
        (double)getBlockInfo().getLength() / rConfig.getEcChunkSize()),
        rConfig.getData());
  }

  /**
   * Using the current position, returns the index of the blockStream we should
   * be reading from. This is the index in the internal array holding the
   * stream reference. The block group index will be one greater than this.
   * @return
   */
  protected int currentStreamIndex() {
    return (int)((position / ecChunkSize) % repConfig.getData());
  }

  /**
   * Uses the current position and ecChunkSize to determine which of the
   * internal block streams the next read should come from. Also opens the
   * stream if it has not been opened already.
   * @return BlockInput stream to read from.
   */
  protected BlockExtendedInputStream getOrOpenStream(
      int streamIndex, int locationIndex) {
    BlockExtendedInputStream stream = blockStreams[streamIndex];
    if (stream == null) {
      // To read an EC block, we create a STANDALONE pipeline that contains the
      // single location for the block index we want to read. The EC blocks are
      // indexed from 1 to N, however the data locations are stored in the
      // dataLocations array indexed from zero.
      Pipeline pipeline = Pipeline.newBuilder()
          .setReplicationConfig(new StandaloneReplicationConfig(
              HddsProtos.ReplicationFactor.ONE))
          .setNodes(Arrays.asList(dataLocations[locationIndex]))
          .setId(PipelineID.randomId())
          .setState(Pipeline.PipelineState.CLOSED)
          .build();

      OmKeyLocationInfo blkInfo = new OmKeyLocationInfo.Builder()
          .setBlockID(blockInfo.getBlockID())
          .setLength(internalBlockLength(locationIndex+1))
          .setPipeline(blockInfo.getPipeline())
          .setToken(blockInfo.getToken())
          .setPartNumber(blockInfo.getPartNumber())
          .build();
      stream = streamFactory.create(
          new StandaloneReplicationConfig(HddsProtos.ReplicationFactor.ONE),
          blkInfo, pipeline,
          blockInfo.getToken(), verifyChecksum, xceiverClientFactory,
          refreshFunction);
      blockStreams[streamIndex] = stream;
    }
    return stream;
  }

  /**
   * Returns the length of the Nth block in the block group, taking account of a
   * potentially partial last stripe. Note that the internal block index is
   * numbered starting from 1.
   * @param index - Index number of the internal block, starting from 1
   * @return
   */
  protected long internalBlockLength(int index) {
    long lastStripe = blockInfo.getLength() % stripeSize;
    long blockSize = (blockInfo.getLength() - lastStripe) / repConfig.getData();
    long lastCell = lastStripe / ecChunkSize + 1;
    long lastCellLength = lastStripe % ecChunkSize;

    if (index > repConfig.getData()) {
      // Its a parity block and their size is driven by the size of the
      // first block of the block group. All parity blocks have the same size
      // as block_1.
      index = 1;
    }

    if (index < lastCell) {
      return blockSize + ecChunkSize;
    } else if (index == lastCell) {
      return blockSize + lastCellLength;
    } else {
      return blockSize;
    }
  }

  private void setBlockLocations(Pipeline pipeline) {
    for (DatanodeDetails node : pipeline.getNodes()) {
      int index = pipeline.getReplicaIndex(node);
      addBlockLocation(index, node);
    }
  }

  private void addBlockLocation(int index, DatanodeDetails location) {
    if (index > maxLocations) {
      throw new IndexOutOfBoundsException("The index " + index + " is greater "
          + "than the EC Replication Config (" + repConfig + ")");
    }
    dataLocations[index - 1] = location;
  }

  protected long blockLength() {
    return blockInfo.getLength();
  }

  protected long remaining() {
    return blockLength() - position;
  }

  /**
   * Read from the internal BlockInputStreams one EC cell at a time into the
   * strategy buffer. This call may read from several internal BlockInputStreams
   * if there is sufficient space in the buffer.
   * @param strategy
   * @return
   * @throws IOException
   */
  @Override
  protected synchronized int readWithStrategy(ByteReaderStrategy strategy)
      throws IOException {
    Preconditions.checkArgument(strategy != null);
    checkOpen();

    if (remaining() == 0) {
      return EOF;
    }

    int totalRead = 0;
    while(strategy.getTargetLength() > 0 && remaining() > 0) {
      int currentIndex = currentStreamIndex();
      BlockExtendedInputStream stream =
          getOrOpenStream(currentIndex, currentIndex);
      int read = readFromStream(stream, strategy);
      totalRead += read;
      position += read;
    }
    return totalRead;
  }

  @Override
  public synchronized long getRemaining() {
    return blockInfo.getLength() - position;
  }

  @Override
  public synchronized long getLength() {
    return blockInfo.getLength();
  }

  @Override
  public BlockID getBlockID() {
    return blockInfo.getBlockID();
  }

  protected void seekStreamIfNecessary(BlockExtendedInputStream stream,
      long partialChunkSize) throws IOException {
    if (seeked) {
      // Seek on the underlying streams is performed lazily, as there is a
      // possibility a read after a seek may only read a small amount of data.
      // Once this block stream has been seeked, we always check the position,
      // but in the usual case, where there are no seeks at all, we don't need
      // to do this extra work.
      long basePosition = (position / stripeSize) * (long)ecChunkSize;
      long streamPosition = basePosition + partialChunkSize;
      if (streamPosition != stream.getPos()) {
        // This ECBlockInputStream has been seeked, so the underlying
        // block stream is no longer at the correct position. Therefore we need
        // to seek it too.
        stream.seek(streamPosition);
      }
    }
  }

  /**
   * Read the most allowable amount of data from the current stream. This
   * ensures we don't read past the end of an EC cell or the overall block
   * group length.
   * @param stream Stream to read from
   * @param strategy The ReaderStrategy to read data into
   * @return
   * @throws IOException
   */
  private int readFromStream(BlockExtendedInputStream stream,
      ByteReaderStrategy strategy)
      throws IOException {
    long partialPosition = position % ecChunkSize;
    seekStreamIfNecessary(stream, partialPosition);
    long ecLimit = ecChunkSize - partialPosition;
    // Free space in the buffer to read into
    long bufLimit = strategy.getTargetLength();
    // How much we can read, the lower of the EC Cell, buffer and overall block
    // remaining.
    int expectedRead = (int)Math.min(Math.min(ecLimit, bufLimit), remaining());
    int actualRead = strategy.readFromBlock(stream, expectedRead);
    if (actualRead == -1) {
      // The Block Stream reached EOF, but we did not expect it to, so the block
      // might be corrupt.
      throw new IOException("Expected to read " + expectedRead + " but got EOF"
          + " from blockGroup " + stream.getBlockID() + " index "
          + currentStreamIndex()+1);
    }
    return actualRead;
  }

  /**
   * Verify that the input stream is open.
   * @throws IOException if the connection is closed.
   */
  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Block: "
              + blockInfo.getBlockID());
    }
  }

  @Override
  public synchronized void close() {
    for (BlockExtendedInputStream stream : blockStreams) {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          LOG.error("Failed to close stream {}", stream, e);
        }
      }
    }
    closed = true;
  }

  @Override
  public synchronized void unbuffer() {
    for (BlockExtendedInputStream stream : blockStreams) {
      if (stream != null) {
        stream.unbuffer();
      }
    }
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkOpen();
    if (pos < 0 || pos >= getLength()) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException(
          "EOF encountered at pos: " + pos + " for block: "
              + blockInfo.getBlockID());
    }
    if (position != pos) {
      position = pos;
      seeked = true;
    }
  }

  @Override
  public synchronized long getPos() {
    return position;
  }

  protected synchronized void setPos(long pos) {
    position = pos;
  }

  @Override
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }
}
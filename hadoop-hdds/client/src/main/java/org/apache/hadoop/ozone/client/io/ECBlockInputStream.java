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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final XceiverClientFactory xceiverClientFactory;
  private final Function<BlockID, BlockLocationInfo> refreshFunction;
  private final BlockLocationInfo blockInfo;
  private final DatanodeDetails[] dataLocations;
  private final BlockExtendedInputStream[] blockStreams;
  private final Map<Integer, LinkedList<DatanodeDetails>> spareDataLocations
      = new TreeMap<>();
  private final List<DatanodeDetails> failedLocations = new ArrayList<>();
  private final int maxLocations;
  private final String string;

  private long position = 0;
  private boolean closed = false;
  private boolean seeked = false;
  private OzoneClientConfig config;

  protected ECReplicationConfig getRepConfig() {
    return repConfig;
  }

  protected DatanodeDetails[] getDataLocations() {
    return dataLocations;
  }

  protected long getStripeSize() {
    return stripeSize;
  }

  /**
   * Returns the number of available data locations, taking account of the
   * expected number of locations. Eg, if the block is less than 1 EC chunk,
   * we only expect 1 data location. If it is between 1 and 2 chunks, we expect
   * there to be 2 locations, and so on.
   * @param expectedLocations The maximum number of allowed data locations,
   *                          depending on the block size.
   * @return The number of available data locations.
   */
  protected int availableDataLocations(int expectedLocations) {
    int count = 0;
    for (int i = 0; i < repConfig.getData() && i < expectedLocations; i++) {
      if (dataLocations[i] != null) {
        count++;
      }
    }
    return count;
  }

  public ECBlockInputStream(ECReplicationConfig repConfig,
      BlockLocationInfo blockInfo,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      BlockInputStreamFactory streamFactory,
      OzoneClientConfig config) {
    this.repConfig = repConfig;
    this.ecChunkSize = repConfig.getEcChunkSize();
    this.blockInfo = blockInfo;
    this.streamFactory = streamFactory;
    this.xceiverClientFactory = xceiverClientFactory;
    this.refreshFunction = refreshFunction;
    this.maxLocations = repConfig.getData() + repConfig.getParity();
    this.dataLocations = new DatanodeDetails[repConfig.getRequiredNodes()];
    this.blockStreams =
        new BlockExtendedInputStream[repConfig.getRequiredNodes()];
    this.config = config;

    this.stripeSize = (long)ecChunkSize * repConfig.getData();
    setBlockLocations(this.blockInfo.getPipeline());
    string = getClass().getSimpleName() + "{" + blockIdForDebug() + "}@"
        + Integer.toHexString(hashCode());
    LOG.debug("{}: config: {}, locations: {} / {}", this,
        repConfig, dataLocations, spareDataLocations);
  }

  public synchronized boolean hasSufficientLocations() {
    // The number of locations needed is a function of the EC Chunk size. If the
    // block length is <= the chunk size, we should only have location 1. If it
    // is greater than the chunk size but less than chunk_size * 2, then we must
    // have two locations. If it is greater than chunk_size * data_num, then we
    // must have all data_num locations.
    // We only consider data locations here.
    int expectedDataBlocks = calculateExpectedDataBlocks(repConfig);
    return expectedDataBlocks == availableDataLocations(expectedDataBlocks);
  }

  protected int calculateExpectedDataBlocks(ECReplicationConfig rConfig) {
    return ECBlockInputStreamProxy.expectedDataLocations(rConfig, getLength());
  }

  /**
   * Using the current position, returns the index of the blockStream we should
   * be reading from. This is the index in the internal array holding the
   * stream reference. The block group index will be one greater than this.
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
  protected BlockExtendedInputStream getOrOpenStream(int locationIndex) throws IOException {
    BlockExtendedInputStream stream = blockStreams[locationIndex];
    if (stream == null) {
      // To read an EC block, we create a STANDALONE pipeline that contains the
      // single location for the block index we want to read. The EC blocks are
      // indexed from 1 to N, however the data locations are stored in the
      // dataLocations array indexed from zero.
      DatanodeDetails dataLocation = dataLocations[locationIndex];
      Pipeline pipeline = Pipeline.newBuilder()
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(
              HddsProtos.ReplicationFactor.ONE))
          .setNodes(Collections.singletonList(dataLocation))
          .setId(dataLocation.getID())
          .setReplicaIndexes(ImmutableMap.of(dataLocation, locationIndex + 1))
          .setState(Pipeline.PipelineState.CLOSED)
          .build();

      BlockLocationInfo blkInfo = new BlockLocationInfo.Builder()
          .setBlockID(blockInfo.getBlockID())
          .setLength(internalBlockLength(locationIndex + 1, this.repConfig, this.blockInfo.getLength()))
          .setPipeline(blockInfo.getPipeline())
          .setToken(blockInfo.getToken())
          .setPartNumber(blockInfo.getPartNumber())
          .build();
      stream = streamFactory.create(
          StandaloneReplicationConfig.getInstance(
              HddsProtos.ReplicationFactor.ONE),
          blkInfo, pipeline,
          blockInfo.getToken(), xceiverClientFactory,
          ecPipelineRefreshFunction(locationIndex + 1, refreshFunction),
          config);
      blockStreams[locationIndex] = stream;
      LOG.debug("{}: created stream [{}]: {}", this, locationIndex, stream);
    }
    return stream;
  }

  /**
   * Returns a function that builds a Standalone pipeline corresponding
   * to the replicaIndex given based on the EC pipeline fetched from SCM.
   * @param replicaIndex
   * @param refreshFunc
   */
  protected Function<BlockID, BlockLocationInfo> ecPipelineRefreshFunction(
      int replicaIndex, Function<BlockID, BlockLocationInfo> refreshFunc) {
    return (blockID) -> {
      BlockLocationInfo blockLocationInfo = refreshFunc.apply(blockID);
      if (blockLocationInfo == null) {
        return null;
      }
      Pipeline ecPipeline = blockLocationInfo.getPipeline();
      DatanodeDetails curIndexNode = ecPipeline.getNodes()
          .stream().filter(dn ->
              ecPipeline.getReplicaIndex(dn) == replicaIndex)
          .findAny().orElse(null);
      if (curIndexNode == null) {
        return null;
      }
      Pipeline pipeline = Pipeline.newBuilder().setReplicationConfig(
              StandaloneReplicationConfig.getInstance(
                  HddsProtos.ReplicationFactor.ONE))
          .setNodes(Collections.singletonList(curIndexNode))
          .setId(PipelineID.randomId())
          .setReplicaIndexes(Collections.singletonMap(curIndexNode, replicaIndex))
          .setState(Pipeline.PipelineState.CLOSED)
          .build();
      blockLocationInfo.setPipeline(pipeline);
      return blockLocationInfo;
    };
  }

  /**
   * Returns the length of the Nth block in the block group, taking account of a
   * potentially partial last stripe. Note that the internal block index is
   * numbered starting from 1.
   * @param index index number of the internal block, starting from 1.
   * @param repConfig EC replication config.
   * @param length length of the whole block group.
   */
  public static long internalBlockLength(int index, ECReplicationConfig repConfig, long length) {
    if (index <= 0) {
      throw new IllegalArgumentException("Index must start from 1.");
    }
    if (length < 0) {
      throw new IllegalArgumentException("Block length cannot be negative.");
    }
    long ecChunkSize = (long) repConfig.getEcChunkSize();
    long stripeSize = ecChunkSize * repConfig.getData();
    long lastStripe = length % stripeSize;
    long blockSize = (length - lastStripe) / repConfig.getData();
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
    int arrayIndex = index - 1;
    if (dataLocations[arrayIndex] == null) {
      dataLocations[arrayIndex] = location;
    } else {
      spareDataLocations.computeIfAbsent(arrayIndex,
          k -> new LinkedList<>()).add(location);
    }
  }

  protected long blockLength() {
    return blockInfo.getLength();
  }

  protected long remaining() {
    return blockLength() - position;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    return read(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    while (true) {
      int currentBufferPosition = byteBuffer.position();
      long currentPosition = getPos();
      try {
        return super.read(byteBuffer);
      } catch (BadDataLocationException e) {
        int failedIndex = e.getFailedLocationIndex();
        if (LOG.isDebugEnabled()) {
          String cause = e.getCause() != null
              ? " due to " + e.getCause().getMessage()
              : "";
          LOG.debug("{}: read [{}] failed from {}{}", this,
              failedIndex, dataLocations[failedIndex], cause);
        }
        closeStream(failedIndex);
        if (shouldRetryFailedRead(failedIndex)) {
          byteBuffer.position(currentBufferPosition);
          seek(currentPosition);
        } else {
          e.addFailedLocations(failedLocations);
          throw e;
        }
      }
    }
  }

  protected boolean shouldRetryFailedRead(int failedIndex) {
    Deque<DatanodeDetails> spareLocations = spareDataLocations.get(failedIndex);
    if (spareLocations != null && !spareLocations.isEmpty()) {
      failedLocations.add(dataLocations[failedIndex]);
      DatanodeDetails spare = spareLocations.removeFirst();
      dataLocations[failedIndex] = spare;
      LOG.debug("{}: switching [{}] to spare {}", this, failedIndex, spare);
      return true;
    }
    return false;
  }

  /**
   * Read from the internal BlockInputStreams one EC cell at a time into the
   * strategy buffer. This call may read from several internal BlockInputStreams
   * if there is sufficient space in the buffer.
   * @param strategy
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
    while (strategy.getTargetLength() > 0 && remaining() > 0) {
      int currentIndex = currentStreamIndex();
      try {
        BlockExtendedInputStream stream = getOrOpenStream(currentIndex);
        int read = readFromStream(stream, strategy);
        LOG.trace("{}: read {} bytes for [{}]", this, read, currentIndex);
        totalRead += read;
        position += read;
      } catch (IOException ioe) {
        throw new BadDataLocationException(
            dataLocations[currentIndex], currentIndex, ioe);
      }
    }
    return totalRead;
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
          + currentStreamIndex() + 1);
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
    LOG.debug("{}: close", this);
    closeStreams();
    closed = true;
  }

  protected synchronized void closeStreams() {
    for (int i = 0; i < blockStreams.length; i++) {
      closeStream(i);
    }
    // If the streams have been closed outside of a close() call, then it may
    // be due to freeing resources. If they are reopened, then we will need to
    // seek the stream to its expected position when the next read is attempted.
    seeked = true;
  }

  protected void closeStream(int i) {
    if (blockStreams[i] != null) {
      try {
        blockStreams[i].close();
        blockStreams[i] = null;
        LOG.debug("{}: closed stream [{}]", this, i);
      } catch (IOException e) {
        LOG.error("{}: failed to close stream [{}]: {}", this,
            i, blockStreams[i], e);
      }
    }
  }

  @Override
  public synchronized void unbuffer() {
    LOG.trace("{}: unbuffer", this);
    for (BlockExtendedInputStream stream : blockStreams) {
      if (stream != null) {
        stream.unbuffer();
      }
    }
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    LOG.trace("{}: seek({})", this, pos);
    checkOpen();
    if (pos < 0 || pos > getLength()) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException(
          "EOF encountered at pos: " + pos + " for block: "
              + blockInfo.getBlockID());
    }
    position = pos;
    seeked = true;
  }

  @Override
  public synchronized long getPos() {
    return position;
  }

  protected synchronized void setPos(long pos) {
    LOG.trace("{}: setPos({})", this, pos);
    position = pos;
  }

  @Override
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  protected ContainerBlockID blockIdForDebug() {
    return getBlockID().getContainerBlockID();
  }

  @Override
  public String toString() {
    return string;
  }
}

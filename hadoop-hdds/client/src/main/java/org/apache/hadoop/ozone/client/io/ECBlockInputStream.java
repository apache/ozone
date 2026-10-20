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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.StreamCapabilities;
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
  // Executor for reading the cells of a multi-cell read in parallel, or null to read them one at a time.
  private final Supplier<ExecutorService> parallelReadExecutor;

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
    this(repConfig, blockInfo, xceiverClientFactory, refreshFunction, streamFactory, config, null);
  }

  /**
   * @param parallelReadExecutor executor for reading the cells of different data indexes in parallel when a read
   *     spans more than one cell. It is used only if {@link OzoneClientConfig#isEcParallelReadEnabled()}; with
   *     null, cells are always read one at a time.
   */
  public ECBlockInputStream(ECReplicationConfig repConfig,
      BlockLocationInfo blockInfo,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      BlockInputStreamFactory streamFactory,
      OzoneClientConfig config,
      Supplier<ExecutorService> parallelReadExecutor) {
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
    this.parallelReadExecutor = config != null && config.isEcParallelReadEnabled() ? parallelReadExecutor : null;

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
  protected int readPositioned(long offset, ByteBuffer buffer) {
    throw new UnsupportedOperationException("Use ECBlockInputStreamProxy for positioned reads");
  }

  @Override
  public boolean hasCapability(String capability) {
    return !StreamCapabilities.PREADBYTEBUFFER.equalsIgnoreCase(capability)
        && super.hasCapability(capability);
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
        return spansMultipleCells(byteBuffer) ? readCellsInParallel(byteBuffer) : super.read(byteBuffer);
      } catch (BadDataLocationException e) {
        boolean retry = true;
        for (int failedIndex : e.getFailedLocationIndexes()) {
          if (LOG.isDebugEnabled()) {
            String cause = e.getCause() != null
                ? " due to " + e.getCause().getMessage()
                : "";
            LOG.debug("{}: read [{}] failed from {}{}", this,
                failedIndex, dataLocations[failedIndex], cause);
          }
          closeStream(failedIndex);
          // every failed index is switched to a spare, if it has one
          retry &= shouldRetryFailedRead(failedIndex);
        }
        if (retry) {
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

  /**
   * Returns true if the parallel read path is enabled and the next read into the buffer covers more than one cell.
   */
  private boolean spansMultipleCells(ByteBuffer buffer) {
    if (parallelReadExecutor == null || closed) {
      return false;
    }
    long length = Math.min(buffer.remaining(), remaining());
    return position % ecChunkSize + length > ecChunkSize;
  }

  /**
   * Reads the cells covered by the buffer from the internal block streams of their data indexes in parallel. The
   * cells of one data index, which come from consecutive stripes, are read in order by a single task, so the
   * fan-out is at most the number of data units. The calling thread runs the task of the first cell, and any task
   * the executor has not started yet, and returns only after all tasks have finished.
   *
   * <p>Like {@link #readWithStrategy}, the position and the buffer are advanced past the data that was read. If a
   * read fails, they are advanced only by the cells before the first failed cell, and a
   * {@link BadDataLocationException} carrying every failed index is thrown.
   */
  private int readCellsInParallel(ByteBuffer buffer) throws IOException {
    final long startPosition = position;
    final int length = (int) Math.min(buffer.remaining(), remaining());
    final int bufferStart = buffer.position();

    // Split the range into cells, grouped by data index in the order they are first needed.
    final List<CellRead> cells = new ArrayList<>();
    final Map<Integer, List<CellRead>> cellsByIndex = new LinkedHashMap<>();
    for (int offset = 0; offset < length;) {
      long cellPosition = startPosition + offset;
      int cellLength = (int) Math.min(ecChunkSize - cellPosition % ecChunkSize, length - offset);
      int index = (int) ((cellPosition / ecChunkSize) % repConfig.getData());
      List<CellRead> indexCells = cellsByIndex.computeIfAbsent(index, k -> new ArrayList<>());
      CellRead cell = new CellRead(index, indexCells.size(), cellPosition, offset, cellLength);
      indexCells.add(cell);
      cells.add(cell);
      offset += cellLength;
    }

    // getOrOpenStream is not thread safe, so open every stream before reading.
    final List<FutureTask<IndexReadResult>> tasks = new ArrayList<>();
    for (Map.Entry<Integer, List<CellRead>> entry : cellsByIndex.entrySet()) {
      int index = entry.getKey();
      BlockExtendedInputStream stream;
      try {
        stream = getOrOpenStream(index);
      } catch (IOException e) {
        throw new BadDataLocationException(dataLocations[index], index, e);
      }
      List<CellRead> indexCells = entry.getValue();
      boolean checkPosition = seeked;
      tasks.add(new FutureTask<>(() -> readCells(stream, indexCells, buffer, bufferStart, checkPosition)));
    }

    ExecutorService executor = parallelReadExecutor.get();
    for (int i = 1; i < tasks.size(); i++) {
      try {
        executor.execute(tasks.get(i));
      } catch (RejectedExecutionException e) {
        // the task is run by this thread below
        LOG.debug("{}: parallel read task rejected", this, e);
      }
    }
    // FutureTask.run does nothing if the task has already been started by the executor.
    for (FutureTask<IndexReadResult> task : tasks) {
      task.run();
    }
    // Wait for every task, even after an unexpected error, as they all write into the caller's buffer.
    Map<Integer, IndexReadResult> results = new TreeMap<>();
    Throwable unexpected = null;
    for (FutureTask<IndexReadResult> task : tasks) {
      try {
        IndexReadResult result = Uninterruptibles.getUninterruptibly(task);
        results.put(result.index, result);
      } catch (ExecutionException e) {
        unexpected = unexpected == null ? e.getCause() : unexpected;
      }
    }
    // After a failure, the streams of indexes read past the cells that position advances by below, so a later read
    // must check the position of every stream, as after a seek.
    if (unexpected != null || results.values().stream().anyMatch(result -> result.error != null)) {
      seeked = true;
    }
    if (unexpected != null) {
      Throwables.throwIfUnchecked(unexpected);
      throw new IOException(unexpected);
    }

    // A failure on one index must not be hidden by the success of a later cell, so only advance by the cells
    // before the first failed cell.
    int read = 0;
    for (CellRead cell : cells) {
      if (cell.ordinal >= results.get(cell.index).cellsRead) {
        break;
      }
      read += cell.length;
    }
    position = startPosition + read;
    buffer.position(bufferStart + read);
    LOG.trace("{}: read {} bytes from {} indexes in parallel", this, read, tasks.size());

    Map<Integer, DatanodeDetails> failures = new TreeMap<>();
    IOException cause = null;
    for (IndexReadResult result : results.values()) {
      if (result.error != null) {
        failures.put(result.index, dataLocations[result.index]);
        if (cause == null) {
          cause = result.error;
        } else {
          cause.addSuppressed(result.error);
        }
      }
    }
    if (cause != null) {
      throw new BadDataLocationException(failures, cause);
    }
    return read;
  }

  /**
   * Reads the given cells of one data index, in order, each into its own slice of the buffer. Called without the
   * stream monitor, so it only uses the given stream and state that does not change while the caller waits.
   */
  private IndexReadResult readCells(BlockExtendedInputStream stream, List<CellRead> indexCells, ByteBuffer buffer,
      int bufferStart, boolean checkPosition) {
    int index = indexCells.get(0).index;
    int cellsRead = 0;
    try {
      for (CellRead cell : indexCells) {
        long streamPosition = (cell.position / stripeSize) * ecChunkSize + cell.position % ecChunkSize;
        // The cells of one index are contiguous in its stream, so its position only needs to be checked once
        // the stream has been seeked.
        if (checkPosition && streamPosition != stream.getPos()) {
          stream.seek(streamPosition);
        }
        ByteBuffer slice = buffer.duplicate();
        slice.position(bufferStart + cell.bufferOffset);
        slice.limit(bufferStart + cell.bufferOffset + cell.length);
        int actualRead = stream.read(slice);
        if (actualRead == EOF) {
          throw new IOException("Expected to read " + cell.length + " but got EOF"
              + " from blockGroup " + stream.getBlockID() + " index " + (index + 1));
        }
        if (actualRead != cell.length) {
          throw new IOException(String.format(
              "Inconsistent read for blockID=%s index=%d expectedRead=%d actualRead=%d",
              stream.getBlockID(), index + 1, cell.length, actualRead));
        }
        cellsRead++;
      }
      return new IndexReadResult(index, cellsRead, null);
    } catch (IOException e) {
      return new IndexReadResult(index, cellsRead, e);
    }
  }

  /** The part of a parallel read that falls in one cell. */
  private static final class CellRead {
    private final int index;
    // position of this cell among the cells of the same index in the read
    private final int ordinal;
    private final long position;
    private final int bufferOffset;
    private final int length;

    CellRead(int index, int ordinal, long position, int bufferOffset, int length) {
      this.index = index;
      this.ordinal = ordinal;
      this.position = position;
      this.bufferOffset = bufferOffset;
      this.length = length;
    }
  }

  /** Outcome of reading the cells of one data index in a parallel read. */
  private static final class IndexReadResult {
    private final int index;
    private final int cellsRead;
    private final IOException error;

    IndexReadResult(int index, int cellsRead, IOException error) {
      this.index = index;
      this.cellsRead = cellsRead;
      this.error = error;
    }
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

    if (actualRead != expectedRead) {
      throw new IOException(String.format(
          "Inconsistent read for blockID=%s index=%d expectedRead=%d actualRead=%d",
          stream.getBlockID(), currentStreamIndex() + 1, expectedRead, actualRead));
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

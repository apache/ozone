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

import static java.util.Collections.emptySortedSet;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to read EC encoded data from blocks a stripe at a time, when some of
 * the data blocks are not available. The public API for this class is:
 *
 *     readStripe(ByteBuffer[] bufs)
 *     recoverChunks(ByteBuffer[] bufs)
 *
 * The other inherited public APIs will throw a NotImplementedException. This is
 * because this class is intended to only read full stripes into a reusable set
 * of bytebuffers, and the tradition read APIs do not facilitate this.
 *
 * The caller should pass an array of ByteBuffers to readStripe() which:
 *
 * 1. Have EC DataNum buffers in the array.
 * 2. Each buffer should have its position set to zero
 * 3. Each buffer should have ecChunkSize remaining
 *
 * These buffers are either read into directly from the data blocks on the
 * datanodes, or they will be reconstructed from parity data using the EC
 * decoder.
 *
 * The EC Decoder expects to receive an array of elements matching EC Data + EC
 * Parity elements long. Missing or not needed elements should be set to null
 * in the array. The elements should be assigned to the array in EC index order.
 *
 * Assuming we have n missing data locations, where n {@literal <=} parity locations, the
 * ByteBuffers passed in from the client are either assigned to the decoder
 * input array, or they are assigned to the decoder output array, where
 * reconstructed data is written. The required number of parity buffers will be
 * assigned and added to the decoder input so it has sufficient locations to
 * reconstruct the data. After reconstruction the byte buffers received will
 * have the data for a full stripe populated, either by reading directly from
 * the block or by reconstructing the data.
 *
 * The buffers are returned "ready to read" with the position at zero and
 * remaining() indicating how much data was read. If the remaining data is less
 * than a full stripe, the client can simply read upto remaining from each
 * buffer in turn. If there is a full stripe, each buffer should have ecChunk
 * size remaining.
 *
 * recoverChunks() handles the more generic case, where specific buffers, even
 * parity ones, are to be recovered -- these should be passed to the method.
 * The whole stripe is not important for the caller in this case.  The indexes
 * of the buffers that need to be recovered should be set by calling
 * setRecoveryIndexes() before any read operation.
 *
 * Example: with rs-3-2 there are 3 data and 2 parity replicas, indexes are [0,
 * 1, 2, 3, 4].  If replicas 2 and 3 are missing and need to be recovered,
 * caller should {@code setRecoveryIndexes([2, 3])}, and then can recover the
 * part of each stripe for these replicas by calling
 * {@code recoverChunks(bufs)}, passing two buffers.
 */
public class ECBlockReconstructedStripeInputStream extends ECBlockInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECBlockReconstructedStripeInputStream.class);

  // List of buffers, data + parity long, needed by the EC decoder. Missing
  // or not-need locations will be null.
  private ByteBuffer[] decoderInputBuffers;
  // Missing chunks are recovered into these buffers.
  private ByteBuffer[] decoderOutputBuffers;

  // Missing indexes to be recovered into the recovered buffers. Required by the
  // EC decoder
  private final SortedSet<Integer> missingIndexes = new TreeSet<>();

  // indexes for data, padding and parity blocks
  private final SortedSet<Integer> dataIndexes;
  private final SortedSet<Integer> paddingIndexes;
  private final SortedSet<Integer> parityIndexes;
  private final SortedSet<Integer> allIndexes;

  // The blockLocation indexes to use to read data into the output buffers
  private final SortedSet<Integer> selectedIndexes = new TreeSet<>();
  // indexes of internal buffers (ones which are not requested by the caller,
  // but needed for reconstructing missing data)
  private final SortedSet<Integer> internalBuffers = new TreeSet<>();
  // Data Indexes we have tried to read from, and failed for some reason
  private final Set<Integer> failedDataIndexes = new TreeSet<>();
  private final ByteBufferPool byteBufferPool;

  private RawErasureDecoder decoder;

  private boolean initialized = false;

  private final ExecutorService executor;

  // for offline recovery: indexes to be recovered
  private final Set<Integer> recoveryIndexes = new TreeSet<>();

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ECBlockReconstructedStripeInputStream(ECReplicationConfig repConfig,
      BlockLocationInfo blockInfo,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      BlockInputStreamFactory streamFactory,
      ByteBufferPool byteBufferPool,
      ExecutorService ecReconstructExecutor,
      OzoneClientConfig config) {
    super(repConfig, blockInfo, xceiverClientFactory,
        refreshFunction, streamFactory, config);
    this.byteBufferPool = byteBufferPool;
    this.executor = ecReconstructExecutor;

    int expectedDataBlocks = calculateExpectedDataBlocks(repConfig);
    int d = repConfig.getData();
    dataIndexes = setOfRange(0, expectedDataBlocks);
    paddingIndexes = setOfRange(expectedDataBlocks, d);
    parityIndexes = setOfRange(d, repConfig.getRequiredNodes());
    allIndexes = setOfRange(0, repConfig.getRequiredNodes());
  }

  /**
   * Provide a list of datanodes that are known to be bad, and no attempt will
   * be made to read from them. If too many failed nodes are passed, then the
   * reader may not have sufficient locations available to reconstruct the data.
   *
   * Note this call must be made before any attempt it made to read data,
   * as that is when the reader is initialized. Attempting to call this method
   * after a read will result in a runtime exception.
   *
   * @param dns A list of DatanodeDetails that are known to be bad.
   */
  public synchronized void addFailedDatanodes(Collection<DatanodeDetails> dns) {
    if (initialized) {
      throw new IllegalStateException("Cannot add failed datanodes after the " +
          "reader has been initialized");
    }
    DatanodeDetails[] locations = getDataLocations();
    for (DatanodeDetails dn : dns) {
      for (int i = 0; i < locations.length; i++) {
        if (locations[i] != null && locations[i].equals(dn)) {
          failedDataIndexes.add(i);
          break;
        }
      }
    }
    LOG.debug("{}: set failed indexes {}", this, failedDataIndexes);
  }

  /**
   * Returns the set of failed indexes. This will be empty if no errors were
   * encountered reading any of the block indexes, and no failed nodes were
   * added via {@link #addFailedDatanodes(Collection)}.
   * The returned set is a copy of the internal set, so it can be modified.
   */
  public synchronized Set<Integer> getFailedIndexes() {
    return new HashSet<>(failedDataIndexes);
  }

  /**
   * Set the EC indexes that should be recovered by
   * {@link #recoverChunks(ByteBuffer[])}.
   */
  public synchronized void setRecoveryIndexes(Collection<Integer> indexes) {
    if (initialized) {
      throw new IllegalStateException("Cannot set recovery indexes after the " +
          "reader has been initialized");
    }
    Preconditions.assertNotNull(indexes, "recovery indexes");
    recoveryIndexes.clear();
    recoveryIndexes.addAll(indexes);
    LOG.debug("{}: set recovery indexes {}", this, recoveryIndexes);
  }

  private void init() throws InsufficientLocationsException {
    initialized = false;
    if (decoder == null) {
      decoder = CodecUtil.createRawDecoderWithFallback(getRepConfig());
    }
    if (!hasSufficientLocations()) {
      String msg = "There are insufficient datanodes to read the EC block";
      LOG.debug("{}: {}", this, msg);
      throw new InsufficientLocationsException(msg);
    }
    allocateInternalBuffers();
    if (!isOfflineRecovery()) {
      decoderOutputBuffers = new ByteBuffer[missingIndexes.size()];
    }
    initialized = true;
  }

  private void allocateInternalBuffers() {
    // The decoder inputs originally start as all nulls. Then we populate the
    // pieces we have data for. The internal buffers are reused for the block,
    // so we can allocate them now. On re-init, we reuse any internal buffers
    // already allocated.
    final int minIndex = isOfflineRecovery() ? 0 : getRepConfig().getData();
    for (int i = minIndex; i < getRepConfig().getRequiredNodes(); i++) {
      boolean internalInput = selectedIndexes.contains(i)
          || paddingIndexes.contains(i);
      boolean hasBuffer = decoderInputBuffers[i] != null;

      if (internalInput && !hasBuffer) {
        allocateInternalBuffer(i);
      } else if (!internalInput && hasBuffer) {
        releaseInternalBuffer(i);
      }
    }
  }

  private void allocateInternalBuffer(int index) {
    Preconditions.assertTrue(internalBuffers.add(index),
        () -> "Buffer " + index + " already tracked as internal input");
    decoderInputBuffers[index] =
        byteBufferPool.getBuffer(false, getRepConfig().getEcChunkSize());
  }

  private void releaseInternalBuffer(int index) {
    Preconditions.assertTrue(internalBuffers.remove(index),
        () -> "Buffer " + index + " not tracked as internal input");
    byteBufferPool.putBuffer(decoderInputBuffers[index]);
    decoderInputBuffers[index] = null;
  }

  private void markMissingLocationsAsFailed() {
    DatanodeDetails[] locations = getDataLocations();
    for (int i = 0; i < locations.length; i++) {
      if (locations[i] == null && failedDataIndexes.add(i)) {
        LOG.debug("{}: marked [{}] as failed", this, i);
      }
    }
  }

  private boolean isOfflineRecovery() {
    return !recoveryIndexes.isEmpty();
  }

  private void assignBuffers(ByteBuffer[] bufs) {
    Preconditions.assertSame(getExpectedBufferCount(), bufs.length,
        "buffer count");

    if (isOfflineRecovery()) {
      decoderOutputBuffers = bufs;
    } else {
      int recoveryIndex = 0;
      // Here bufs come from the caller and will be filled with data read from
      // the blocks or recovered. Therefore, if the index is missing, we assign
      // the buffer to the decoder outputs, where data is recovered via EC
      // decoding. Otherwise the buffer is set to the input. Note, it may be a
      // buffer which needs padded.
      for (int i = 0; i < bufs.length; i++) {
        if (isMissingIndex(i)) {
          decoderOutputBuffers[recoveryIndex++] = bufs[i];
          if (internalBuffers.contains(i)) {
            releaseInternalBuffer(i);
          } else {
            decoderInputBuffers[i] = null;
          }
        } else {
          decoderInputBuffers[i] = bufs[i];
        }
      }
    }
  }

  private int getExpectedBufferCount() {
    return isOfflineRecovery()
        ? recoveryIndexes.size()
        : getRepConfig().getData();
  }

  private boolean isMissingIndex(int ind) {
    return missingIndexes.contains(ind);
  }

  /**
   * This method should be passed a list of byteBuffers which must contain the
   * same number of entries as previously set recovery indexes. Each Bytebuffer
   * should be at position 0 and have EC ChunkSize bytes remaining.
   * After returning, the buffers will contain the data for the parts to be
   * recovered in the block's next stripe. The buffers will be returned
   * "ready to read" with their position set to zero and the limit set
   * according to how much data they contain.
   *
   * @param bufs A list of byteBuffers into which recovered data is written
   * @return The number of bytes read
   */
  public synchronized int recoverChunks(ByteBuffer[] bufs) throws IOException {
    Preconditions.assertTrue(isOfflineRecovery());
    return read(bufs);
  }

  /**
   * This method should be passed a list of byteBuffers which must contain EC
   * Data Number entries. Each Bytebuffer should be at position 0 and have EC
   * ChunkSize bytes remaining. After returning, the buffers will contain the
   * data for the next stripe in the block. The buffers will be returned
   * "ready to read" with their position set to zero and the limit set
   * according to how much data they contain.
   *
   * @param bufs A list of byteBuffers which must contain EC Data Number
   *             entries. Each Bytebuffer should be at position 0 and have
   *             EC ChunkSize bytes remaining.
   *
   * @return The number of bytes read
   * @throws IOException
   */
  public synchronized int readStripe(ByteBuffer[] bufs) throws IOException {
    return read(bufs);
  }

  @VisibleForTesting
  synchronized int read(ByteBuffer[] bufs) throws IOException {
    int toRead = (int)Math.min(getRemaining(), getStripeSize());
    if (toRead == 0) {
      return EOF;
    }
    if (!initialized) {
      init();
    }
    validateBuffers(bufs);
    while (true) {
      try {
        assignBuffers(bufs);
        clearInternalBuffers();
        // Set the read limits on the buffers so we do not read any garbage data
        // from the end of the block that is unexpected.
        setBufferReadLimits(toRead);
        loadDataBuffersFromStream();
        break;
      } catch (IOException e) {
        // seek to the current position so it rewinds any blocks we read
        // already.
        seek(getPos());
        // Reset the input positions back to zero
        for (ByteBuffer b : bufs) {
          b.position(0);
        }
        // Re-init now the bad block has been excluded. If we have run out of
        // locations, init will throw an InsufficientLocations exception.
        init();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for reads to complete", ie);
      }
    }
    if (!missingIndexes.isEmpty()) {
      padBuffers(toRead);
      flipInputs();
      decodeStripe();
      // Reset the buffer positions and limits to remove any padding added
      // before EC Decode.
      setBufferReadLimits(toRead);
    } else {
      // If we have no missing indexes, then the buffers will be at their
      // limits after reading so we need to flip them to ensure they are ready
      // to read by the caller.
      flipInputs();
    }
    setPos(getPos() + toRead);
    if (remaining() == 0) {
      // If we reach the end of the block (ie remaining is zero) we free
      // the underlying streams and buffers. This is because KeyInputStream,
      // which reads from the EC streams does not close the blocks until it has
      // read all blocks in the key.
      freeAllResourcesWithoutClosing();
    }
    return toRead;
  }

  private void validateBuffers(ByteBuffer[] bufs) {
    Preconditions.assertSame(getExpectedBufferCount(), bufs.length,
        "buffer count");
    int chunkSize = getRepConfig().getEcChunkSize();
    for (ByteBuffer b : bufs) {
      Preconditions.assertSame(chunkSize, b.remaining(), "buf.remaining");
    }
  }

  private void padBuffers(int toRead) {
    int dataNum = getRepConfig().getData();
    int parityNum = getRepConfig().getParity();
    int chunkSize = getRepConfig().getEcChunkSize();
    if (toRead >= getStripeSize()) {
      // There is no padding to do - we are reading a full stripe.
      return;
    }
    int fullChunks = toRead / chunkSize;
    // The size of each chunk is governed by the size of the first chunk.
    // The parity always matches the first chunk size.
    int paritySize = Math.min(toRead, chunkSize);
    // We never need to pad the first chunk - its length dictates the length
    // of all others.
    fullChunks = Math.max(1, fullChunks);
    for (int i = fullChunks; i < dataNum; i++) {
      ByteBuffer buf = decoderInputBuffers[i];
      if (buf != null) {
        buf.limit(paritySize);
        zeroFill(buf);
      }
    }
    // Ensure the available parity buffers are the expected length
    for (int i = dataNum; i < dataNum + parityNum; i++) {
      ByteBuffer b = decoderInputBuffers[i];
      if (b != null) {
        Preconditions.assertSame(paritySize, b.position(), "buf.position");
      }
    }
    // The output buffers need their limit set to the parity size
    for (ByteBuffer b : decoderOutputBuffers) {
      b.limit(paritySize);
    }
  }

  private void setBufferReadLimits(int toRead) {
    int chunkSize = getRepConfig().getEcChunkSize();
    int fullChunks = toRead / chunkSize;
    int data = getRepConfig().getData();
    if (fullChunks == data) {
      // We are reading a full stripe, no concerns over padding.
      return;
    }

    int partialLength = toRead % chunkSize;
    setReadLimits(partialLength, fullChunks, decoderInputBuffers, allIndexes);
    setReadLimits(partialLength, fullChunks, decoderOutputBuffers,
        missingIndexes);
  }

  private void setReadLimits(int partialChunkSize, int fullChunks,
      ByteBuffer[] buffers, Collection<Integer> indexes) {
    int data = getRepConfig().getData();
    Preconditions.assertTrue(buffers.length == indexes.size(),
        "Mismatch: %d buffers but %d indexes", buffers.length, indexes.size());
    Iterator<ByteBuffer> iter = Arrays.asList(buffers).iterator();
    for (int i : indexes) {
      ByteBuffer buf = iter.next();
      if (buf == null) {
        continue;
      }
      if (i == fullChunks) {
        buf.limit(partialChunkSize);
      } else if (fullChunks < i && i < data) {
        buf.position(0);
        buf.limit(0);
      } else if (data <= i && fullChunks == 0) {
        buf.limit(partialChunkSize);
      }
    }
  }

  private void zeroFill(ByteBuffer buf) {
    // fill with zeros from pos to limit.
    if (buf.hasArray()) {
      byte[] a = buf.array();
      Arrays.fill(a, buf.position(), buf.limit(), (byte)0);
      buf.position(buf.limit());
    } else {
      while (buf.hasRemaining()) {
        buf.put((byte)0);
      }
    }
  }

  /**
   * Take the parity indexes which are already used, and the others which are
   * available, and select random indexes to meet numRequired. The resulting
   * list is sorted in ascending order of the indexes.
   */
  @SuppressWarnings("java:S2245") // no need for secure random
  private SortedSet<Integer> selectInternalInputs(
      SortedSet<Integer> available, long count) {

    if (count <= 0) {
      return emptySortedSet();
    }

    if (available.size() == count) {
      return available;
    }

    SortedSet<Integer> selected = new TreeSet<>();
    for (int i : available) {
      if (decoderInputBuffers[i] != null) {
        // If we are re-initializing, we want to make sure we are re-using any
        // previously selected good parity indexes, as the block stream is
        // already opened.
        selected.add(i);
      }
    }
    List<Integer> candidates = new ArrayList<>(available);
    candidates.removeAll(selected);
    Collections.shuffle(candidates);
    selected.addAll(candidates.stream()
        .limit(count - selected.size())
        .collect(toSet()));

    return selected;
  }

  private void flipInputs() {
    for (ByteBuffer b : decoderInputBuffers) {
      if (b != null) {
        b.flip();
      }
    }
  }

  private void clearInternalBuffers() {
    for (int i : internalBuffers) {
      if (decoderInputBuffers[i] != null) {
        decoderInputBuffers[i].clear();
        decoderInputBuffers[i].limit(getRepConfig().getEcChunkSize());
      }
    }
  }

  protected void loadDataBuffersFromStream()
      throws IOException, InterruptedException {
    Queue<ImmutablePair<Integer, Future<Void>>> pendingReads
        = new ArrayDeque<>();
    for (int i : selectedIndexes) {
      ByteBuffer buf = decoderInputBuffers[i];
      pendingReads.add(new ImmutablePair<>(i, executor.submit(() -> {
        readIntoBuffer(i, buf);
        return null;
      })));
    }
    boolean exceptionOccurred = false;
    while (!pendingReads.isEmpty()) {
      int index = -1;
      try {
        ImmutablePair<Integer, Future<Void>> pair = pendingReads.poll();
        index = pair.getKey();
        // Should this future.get() have a timeout? At the end of the call chain
        // we eventually call a grpc or ratis client to read the block data. Its
        // the call to the DNs which could potentially block. There is a timeout
        // on that call controlled by:
        //     OZONE_CLIENT_READ_TIMEOUT = "ozone.client.read.timeout";
        // Which defaults to 30s. So if there is a DN communication problem, it
        // should timeout in the client which should propagate up the stack as
        // an IOException.
        pair.getValue().get();
      } catch (ExecutionException ee) {
        boolean added = failedDataIndexes.add(index);
        Throwable t = ee.getCause() != null ? ee.getCause() : ee;
        String msg = "{}: error reading [{}]";
        if (added) {
          msg += ", marked as failed";
        } else {
          msg += ", already had failed"; // should not really happen
        }
        LOG.info(msg, this, index, t);

        exceptionOccurred = true;
      } catch (InterruptedException ie) {
        // Catch each InterruptedException to ensure all the futures have been
        // handled, and then throw the exception later
        LOG.debug("{}: interrupted while waiting for reads to complete",
            this, ie);
        Thread.currentThread().interrupt();
      }
    }
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException(
          "Interrupted while waiting for reads to complete");
    }
    if (exceptionOccurred) {
      throw new IOException("One or more errors occurred reading block "
          + getBlockID());
    }
  }

  private void readIntoBuffer(int ind, ByteBuffer buf) throws IOException {
    List<DatanodeDetails> failedLocations = new LinkedList<>();
    while (true) {
      int currentBufferPosition = buf.position();
      try {
        readFromCurrentLocation(ind, buf);
        break;
      } catch (IOException e) {
        DatanodeDetails failedLocation = getDataLocations()[ind];
        failedLocations.add(failedLocation);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: read [{}] failed from {} due to {}", this,
              ind, failedLocation, e.getMessage());
        }
        closeStream(ind);
        if (shouldRetryFailedRead(ind)) {
          buf.position(currentBufferPosition);
        } else {
          throw new BadDataLocationException(ind, e, failedLocations);
        }
      }
    }
  }

  private void readFromCurrentLocation(int ind, ByteBuffer buf)
      throws IOException {
    BlockExtendedInputStream stream = getOrOpenStream(ind);
    seekStreamIfNecessary(stream, 0);
    while (buf.hasRemaining()) {
      int read = stream.read(buf);
      if (read == EOF) {
        // We should not reach EOF, as the block should have enough data to
        // fill the buffer. If the block does not, then it indicates the
        // block is not as long as it should be, based on the block length
        // stored in OM. Therefore if there is any remaining space in the
        // buffer, we should throw an exception.
        if (buf.hasRemaining()) {
          LOG.trace("{}: unexpected EOF with {} bytes remaining [{}]",
              this, buf.remaining(), ind);
          throw new IOException("Expected to read " + buf.remaining() +
              " bytes from block " + getBlockID() + " EC index " + (ind + 1) +
              " but reached EOF");
        }
        LOG.debug("{}: EOF for [{}]", this, ind);
        break;
      }
      LOG.trace("{}: read {} bytes for [{}]", this, read, ind);
    }
  }

  /**
   * Take the populated input buffers and missing indexes and create the
   * outputs. Note that the input buffers have to be "ready for read", ie they
   * need to have been flipped after their data was loaded. The created outputs
   * are "ready to read" by the underlying decoder API, so there is no need to
   * flip them after the call. The decoder reads all the inputs leaving the
   * buffer position at the end, so the inputs are flipped after the decode so
   * we have a complete set of "outputs" for the EC Stripe which are ready to
   * read.
   * @throws IOException
   */
  private void decodeStripe() throws IOException {
    int[] erasedIndexes = missingIndexes.stream()
        .mapToInt(Integer::valueOf)
        .toArray();
    decoder.decode(decoderInputBuffers, erasedIndexes, decoderOutputBuffers);
    flipInputs();
  }

  @Override
  public synchronized boolean hasSufficientLocations() {
    if (decoderInputBuffers == null) {
      // The EC decoder needs an array data+parity long, with missing or not
      // needed indexes set to null.
      decoderInputBuffers = new ByteBuffer[getRepConfig().getRequiredNodes()];
    }

    markMissingLocationsAsFailed();
    selectIndexes();

    // The number of locations needed is a function of the EC Chunk size. If the
    // block length is <= the chunk size, we should only have one data location.
    // If it is greater than the chunk size but less than chunk_size * 2, then
    // we must have two locations. If it is greater than chunk_size * data_num,
    // then we must have all data_num locations.
    // The remaining data locations (for small block lengths) can be assumed to
    // be all zeros.
    // Then we need a total of dataNum blocks available across the available
    // data, parity and padding blocks.

    return selectedIndexes.size() >= dataIndexes.size();
  }

  @Override
  protected int readWithStrategy(ByteReaderStrategy strategy) {
    throw new NotImplementedException("readWithStrategy is not implemented. " +
        "Use readStripe() instead");
  }

  @Override
  public synchronized void close() {
    super.close();
    freeBuffers();
  }

  @Override
  public synchronized void unbuffer() {
    super.unbuffer();
    freeBuffers();
  }

  private void freeBuffers() {
    // free any internal buffers
    if (decoderInputBuffers != null) {
      for (int i : new ArrayList<>(internalBuffers)) {
        releaseInternalBuffer(i);
      }
      internalBuffers.clear();
    }
    initialized = false;
  }

  private void freeAllResourcesWithoutClosing() throws IOException {
    LOG.debug("{}: Freeing all resources while leaving the block open", this);
    freeBuffers();
    closeStreams();
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos % getStripeSize() != 0) {
      // As this reader can only return full stripes, we only seek to the start
      // stripe offsets
      throw new IOException("Requested position " + pos
          + " does not align with a stripe offset");
    }
    super.seek(pos);
  }

  /**
   * Determine which indexes are missing, taking into account the length of the
   * block. For a block shorter than a full EC stripe, it is expected that
   * some data locations will not be present.
   * Populates the missingIndexes and selectedIndexes instance variables.
   */
  private void selectIndexes() {
    SortedSet<Integer> candidates;
    int required;

    missingIndexes.clear();
    selectedIndexes.clear();

    if (isOfflineRecovery()) {
      if (!paddingIndexes.isEmpty()) {
        paddingIndexes.forEach(i -> Preconditions.assertTrue(
            !recoveryIndexes.contains(i),
            () -> "Padding index " + i + " should not be selected for recovery")
        );
      }

      missingIndexes.addAll(recoveryIndexes);

      // data locations available for reading
      SortedSet<Integer> availableIndexes = new TreeSet<>();
      availableIndexes.addAll(dataIndexes);
      availableIndexes.addAll(parityIndexes);
      availableIndexes.removeAll(failedDataIndexes);
      availableIndexes.removeAll(recoveryIndexes);

      // choose from all available
      candidates = availableIndexes;
      required = dataIndexes.size();
    } else {
      missingIndexes.addAll(failedDataIndexes);
      missingIndexes.retainAll(dataIndexes);

      SortedSet<Integer> dataAvailable = new TreeSet<>(dataIndexes);
      dataAvailable.removeAll(failedDataIndexes);

      SortedSet<Integer> parityAvailable = new TreeSet<>(parityIndexes);
      parityAvailable.removeAll(failedDataIndexes);

      selectedIndexes.addAll(dataAvailable);

      // choose from parity
      candidates = parityAvailable;
      required = dataIndexes.size() - dataAvailable.size();
    }

    SortedSet<Integer> internal = selectInternalInputs(candidates, required);
    LOG.debug("{}: selected {}, {} as inputs", this, selectedIndexes, internal);
    selectedIndexes.addAll(internal);
  }

  private static SortedSet<Integer> setOfRange(
      int startInclusive, int endExclusive) {

    return range(startInclusive, endExclusive)
        .boxed().collect(toCollection(TreeSet::new));
  }

}

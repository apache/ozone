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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.erasurecode.CodecRegistry;
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/**
 * Class to read EC encoded data from blocks a stripe at a time, when some of
 * the data blocks are not available. The public API for this class is:
 *
 *     readStripe(ByteBuffer[] bufs)
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
 * Assuming we have n missing data locations, where n <= parity locations, the
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
  private int[] missingIndexes;
  // The blockLocation indexes to use to read data into the dataBuffers.
  private List<Integer> dataIndexes = new ArrayList<>();
  // Data Indexes we have tried to read from, and failed for some reason
  private Set<Integer> failedDataIndexes = new HashSet<>();

  private final RawErasureDecoder decoder;

  private boolean initialized = false;

  public ECBlockReconstructedStripeInputStream(ECReplicationConfig repConfig,
      OmKeyLocationInfo blockInfo, boolean verifyChecksum,
       XceiverClientFactory xceiverClientFactory, Function<BlockID,
      Pipeline> refreshFunction, BlockInputStreamFactory streamFactory) {
    super(repConfig, blockInfo, verifyChecksum, xceiverClientFactory,
        refreshFunction, streamFactory);

    decoder = CodecRegistry.getInstance()
        .getCodecFactory(repConfig.getCodec().toString())
        .createDecoder(repConfig);

    // The EC decoder needs an array data+parity long, with missing or not
    // needed indexes set to null.
    decoderInputBuffers = new ByteBuffer[getRepConfig().getRequiredNodes()];
  }

  protected void init() throws InsufficientLocationsException {
    if (!hasSufficientLocations()) {
      throw new InsufficientLocationsException("There are insufficient " +
          "datanodes to read the EC block");
    }
    dataIndexes.clear();
    ECReplicationConfig repConfig = getRepConfig();
    DatanodeDetails[] locations = getDataLocations();
    setMissingIndexesAndDataLocations(locations);
    List<Integer> parityIndexes =
        selectParityIndexes(locations, missingIndexes.length);
    // We read from the selected parity blocks, so add them to the data indexes.
    dataIndexes.addAll(parityIndexes);
    // The decoder inputs originally start as all nulls. Then we populate the
    // pieces we have data for. The parity buffers are reused for the block
    // so we can allocated them now. On re-init, we reuse any parity buffers
    // already allocated.
    for (int i = repConfig.getData(); i < repConfig.getRequiredNodes(); i++) {
      if (parityIndexes.contains(i)) {
        if (decoderInputBuffers[i] == null) {
          decoderInputBuffers[i] = allocateBuffer(repConfig);
        }
      } else {
        decoderInputBuffers[i] = null;
      }
    }
    decoderOutputBuffers = new ByteBuffer[missingIndexes.length];
    initialized = true;
  }

  /**
   * Determine which indexes are missing, taking into account the length of the
   * block. For a block shorter than a full EC stripe, it is expected that
   * some of the data locations will not be present.
   * Populates the missingIndex and dataIndexes instance variables.
   * @param locations Available locations for the block group
   */
  private void setMissingIndexesAndDataLocations(DatanodeDetails[] locations) {
    ECReplicationConfig repConfig = getRepConfig();
    int expectedDataBlocks = calculateExpectedDataBlocks(repConfig);
    List<Integer> missingInd = new ArrayList<>();
    for (int i = 0; i < repConfig.getData(); i++) {
      if ((locations[i] == null || failedDataIndexes.contains(i))
          && i < expectedDataBlocks) {
        missingInd.add(i);
      } else if (locations[i] != null && !failedDataIndexes.contains(i)) {
        dataIndexes.add(i);
      }
    }
    missingIndexes = missingInd.stream().mapToInt(Integer::valueOf).toArray();
  }

  private void assignBuffers(ByteBuffer[] bufs) {
    ECReplicationConfig repConfig = getRepConfig();
    Preconditions.assertTrue(bufs.length == repConfig.getData());
    int recoveryIndex = 0;
    // Here bufs come from the caller and will be filled with data read from
    // the blocks or recovered. Therefore, if the index is missing, we assign
    // the buffer to the decoder outputs, where data is recovered via EC
    // decoding. Otherwise the buffer is set to the input. Note, it may be a
    // buffer which needs padded.
    for (int i = 0; i < repConfig.getData(); i++) {
      if (isMissingIndex(i)) {
        decoderOutputBuffers[recoveryIndex++] = bufs[i];
        decoderInputBuffers[i] = null;
      } else {
        decoderInputBuffers[i] = bufs[i];
      }
    }
  }

  private boolean isMissingIndex(int ind) {
    for (int i : missingIndexes) {
      if (i == ind) {
        return true;
      }
    }
    return false;
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
    if (!initialized) {
      init();
    }
    int toRead = (int)Math.min(getRemaining(), getStripeSize());
    if (toRead == 0) {
      return EOF;
    }
    validateBuffers(bufs);
    while(true) {
      try {
        assignBuffers(bufs);
        clearParityBuffers();
        // Set the read limits on the buffers so we do not read any garbage data
        // from the end of the block that is unexpected.
        setBufferReadLimits(bufs, toRead);
        loadDataBuffersFromStream();
        break;
      } catch (IOException e) {
        // Re-init now the bad block has been excluded. If we have ran out of
        // locations, init will throw an InsufficientLocations exception.
        init();
        // seek to the current position so it rewinds any blocks we read
        // already.
        seek(getPos());
        // Reset the input positions back to zero
        for (ByteBuffer b : bufs) {
          b.position(0);
        }
      }
    }
    if (missingIndexes.length > 0) {
      padBuffers(toRead);
      flipInputs();
      decodeStripe();
      // Reset the buffer positions and limits to remove any padding added
      // before EC Decode.
      setBufferReadLimits(bufs, toRead);
    } else {
      // If we have no missing indexes, then the buffers will be at their
      // limits after reading so we need to flip them to ensure they are ready
      // to read by the caller.
      flipInputs();
    }
    setPos(getPos() + toRead);
    return toRead;
  }

  private void validateBuffers(ByteBuffer[] bufs) {
    Preconditions.assertTrue(bufs.length == getRepConfig().getData());
    int chunkSize = getRepConfig().getEcChunkSize();
    for (ByteBuffer b : bufs) {
      Preconditions.assertTrue(b.remaining() == chunkSize);
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
        Preconditions.assertTrue(b.position() == paritySize);
      }
    }
    // The output buffers need their limit set to the parity size
    for (ByteBuffer b : decoderOutputBuffers) {
      b.limit(paritySize);
    }
  }

  private void setBufferReadLimits(ByteBuffer[] bufs, int toRead) {
    int chunkSize = getRepConfig().getEcChunkSize();
    int fullChunks = toRead / chunkSize;
    if (fullChunks == getRepConfig().getData()) {
      // We are reading a full stripe, no concerns over padding.
      return;
    }

    if (fullChunks == 0) {
      bufs[0].limit(toRead);
      // All buffers except the first contain no data.
      for (int i = 1; i < bufs.length; i++) {
        bufs[i].position(0);
        bufs[i].limit(0);
      }
      // If we have less than 1 chunk, then the parity buffers are the size
      // of the first chunk.
      for (int i = getRepConfig().getData();
           i < getRepConfig().getRequiredNodes(); i++) {
        ByteBuffer b = decoderInputBuffers[i];
        if (b != null) {
          b.limit(toRead);
        }
      }
    } else {
      int remainingLength = toRead % chunkSize;
      // The first partial has the remaining length
      bufs[fullChunks].limit(remainingLength);
      // All others have a zero limit
      for (int i = fullChunks + 1; i < bufs.length; i++) {
        bufs[i].position(0);
        bufs[i].limit(0);
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
   * @param locations The list of locations for all blocks in the block group/
   * @param numRequired The number of parity chunks needed for reconstruction
   * @return A list of indexes indicating which parity locations to read.
   */
  private List<Integer> selectParityIndexes(
      DatanodeDetails[] locations, int numRequired) {
    List<Integer> indexes = new ArrayList<>();
    List<Integer> selected = new ArrayList<>();
    ECReplicationConfig repConfig = getRepConfig();
    for (int i = repConfig.getData(); i < repConfig.getRequiredNodes(); i++) {
      if (locations[i] != null && !failedDataIndexes.contains(i)
          && decoderInputBuffers[i] == null) {
        indexes.add(i);
      }
      // If we are re-initializing, we want to make sure we are re-using any
      // previously selected good parity indexes, as the block stream is already
      // opened.
      if (decoderInputBuffers[i] != null && !failedDataIndexes.contains(i)) {
        selected.add(i);
      }
    }
    Preconditions.assertTrue(indexes.size() + selected.size() >= numRequired);
    Random rand = new Random();
    while (selected.size() < numRequired) {
      selected.add(indexes.remove(rand.nextInt(indexes.size())));
    }
    Collections.sort(selected);
    return selected;
  }

  private ByteBuffer allocateBuffer(ECReplicationConfig repConfig) {
    ByteBuffer buf = ByteBuffer.allocate(repConfig.getEcChunkSize());
    return buf;
  }

  private void flipInputs() {
    for (ByteBuffer b : decoderInputBuffers) {
      if (b != null) {
        b.flip();
      }
    }
  }

  private void clearParityBuffers() {
    for (int i = getRepConfig().getData();
         i < getRepConfig().getRequiredNodes(); i++) {
      if (decoderInputBuffers[i] != null) {
        decoderInputBuffers[i].clear();
      }
    }
  }

  protected void loadDataBuffersFromStream() throws IOException {
    for (int i : dataIndexes) {
      try {
        BlockExtendedInputStream stream = getOrOpenStream(i);
        seekStreamIfNecessary(stream, 0);
        ByteBuffer b = decoderInputBuffers[i];
        while (b.hasRemaining()) {
          int read = stream.read(b);
          if (read == EOF) {
            // We should not reach EOF, as the block should have enough data to
            // fill the buffer. If the block does not, then it indicates the
            // block is not as long as it should be, based on the block length
            // stored in OM. Therefore if there is any remaining space in the
            // buffer, we should throw an exception.
            if (b.hasRemaining()) {
              throw new IOException("Expected to read " + b.remaining() +
                  " bytes from block " + getBlockID() + " EC index " + (i + 1) +
                  " but reached EOF");
            }
            break;
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to read from block {} EC index {}. Excluding the " +
                "block", getBlockID(), i + 1, e);
        failedDataIndexes.add(i);
        throw e;
      }
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
    decoder.decode(decoderInputBuffers, missingIndexes, decoderOutputBuffers);
    flipInputs();
  }

  @Override
  public synchronized boolean hasSufficientLocations() {
    // The number of locations needed is a function of the EC Chunk size. If the
    // block length is <= the chunk size, we should only have one data location.
    // If it is greater than the chunk size but less than chunk_size * 2, then
    // we must have two locations. If it is greater than chunk_size * data_num,
    // then we must have all data_num locations.
    // The remaining data locations (for small block lengths) can be assumed to
    // be all zeros.
    // Then we need a total of dataNum blocks available across the available
    // data, parity and padding blocks.
    ECReplicationConfig repConfig = getRepConfig();
    int expectedDataBlocks = calculateExpectedDataBlocks(repConfig);
    int availableLocations =
        availableDataLocations() + availableParityLocations();
    int paddedLocations = repConfig.getData() - expectedDataBlocks;
    int failedLocations = failedDataIndexes.size();

    if (availableLocations + paddedLocations - failedLocations
        >= repConfig.getData()) {
      return true;
    } else {
      LOG.error("There are insufficient locations. {} available; {} padded;" +
          " {} failed; {} expected;", availableLocations, paddedLocations,
          failedLocations, expectedDataBlocks);
      return false;
    }
  }

  @Override
  protected int readWithStrategy(ByteReaderStrategy strategy) {
    throw new NotImplementedException("readWithStrategy is not implemented. " +
        "Use readStripe() instead");
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

}

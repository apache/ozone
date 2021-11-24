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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Top level class used to read data from EC Encoded blocks. This class decides,
 * based on the block availability, whether to use a reconstruction or non
 * reconstruction read and also handles errors from the non-reconstruction reads
 * failing over to a reconstruction read when they happen.
 */
public class ECBlockInputStreamProxy extends BlockExtendedInputStream {

  private final ECReplicationConfig repConfig;
  private final boolean verifyChecksum;
  private final XceiverClientFactory xceiverClientFactory;
  private final Function<BlockID, Pipeline> refreshFunction;
  private final OmKeyLocationInfo blockInfo;
  private final ECBlockInputStreamFactory ecBlockInputStreamFactory;

  private BlockExtendedInputStream blockReader;
  private boolean reconstructionReader = false;
  private List<DatanodeDetails> failedLocations = new ArrayList<>();

  /**
   * Given the ECReplicationConfig and the block length, calculate how many
   * data locations the block should have.
   * @param repConfig The EC Replication Config
   * @param blockLength The length of the data block in bytes
   * @return The number of expected data locations
   */
  public static int expectedDataLocations(ECReplicationConfig repConfig,
      long blockLength) {
    return (int)Math.min(
        Math.ceil((double)blockLength / repConfig.getEcChunkSize()),
        repConfig.getData());
  }

  /**
   * From ECReplicationConfig and Pipeline with the block locations and location
   * indexes, determine the number of data locations available.
   * @param repConfig The EC Replication Config
   * @param pipeline The pipeline for the data block, givings its locations and
   *                 the index of each location.
   * @return The number of locations available
   */
  public static int availableDataLocations(ECReplicationConfig repConfig,
      Pipeline pipeline) {
    Set<Integer> locations = new HashSet<>();
    for (DatanodeDetails dn : pipeline.getNodes()) {
      int index = pipeline.getReplicaIndex(dn);
      if (index > 0 && index <= repConfig.getData()) {
        locations.add(index);
      }
    }
    return locations.size();
  }

  public ECBlockInputStreamProxy(ECReplicationConfig repConfig,
      OmKeyLocationInfo blockInfo, boolean verifyChecksum,
      XceiverClientFactory xceiverClientFactory, Function<BlockID,
      Pipeline> refreshFunction, ECBlockInputStreamFactory streamFactory) {
    this.repConfig = repConfig;
    this.verifyChecksum = verifyChecksum;
    this.blockInfo = blockInfo;
    this.ecBlockInputStreamFactory = streamFactory;
    this.xceiverClientFactory = xceiverClientFactory;
    this.refreshFunction = refreshFunction;

    setReaderType();
    createBlockReader();
  }

  private void setReaderType() {
    int expected = expectedDataLocations(repConfig, getLength());
    int available = availableDataLocations(repConfig, blockInfo.getPipeline());
    reconstructionReader = available < expected;

  }

  private void createBlockReader() {
    blockReader = ecBlockInputStreamFactory.create(reconstructionReader,
        repConfig, blockInfo, verifyChecksum, xceiverClientFactory,
        refreshFunction);
  }

  @Override
  public BlockID getBlockID() {
    return blockInfo.getBlockID();
  }

  @Override
  public long getRemaining() {
    return blockReader.getRemaining();
  }

  @Override
  public long getLength() {
    return blockInfo.getLength();
  }

  @Override
  public synchronized int read(byte[] b, int off, int len)
      throws IOException {
    return read(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    if (blockReader.getRemaining() == 0) {
      return EOF;
    }
    int totalRead = 0;
    long lastPosition = 0;
    try {
      while (buf.hasRemaining() && getRemaining() > 0) {
        buf.mark();
        lastPosition = blockReader.getPos();
        totalRead += blockReader.read(buf);
      }
    } catch (IOException e) {
      if (reconstructionReader) {
        // If we get an error from the reconstruction reader, there
        // is nothing left to try. It will re-try until it has insufficient
        // locations internally, so if an error comes here, just re-throw it.
        throw e;
      }
      if (e instanceof BadDataLocationException) {
        failoverToReconstructionRead(
            ((BadDataLocationException) e).getFailedLocation(), lastPosition);
        buf.reset();
        totalRead += read(buf);
      } else {
        throw e;
      }
    }
    return totalRead;
  }

  private void failoverToReconstructionRead(DatanodeDetails badLocation,
      long lastPosition) throws IOException {
    failedLocations.add(badLocation);
    blockReader.close();
    reconstructionReader = true;
    createBlockReader();
    // TODO - set the bad location or pass into create block reader
    if (lastPosition != 0) {
      blockReader.seek(lastPosition);
    }
  }

  /**
   * Should never be called in this class.
   */
  @Override
  protected synchronized int readWithStrategy(ByteReaderStrategy strategy)
      throws IOException {
    throw new IOException("Not Implemented");
  }

  private boolean hasRemaining() {
    return blockReader.getRemaining() > 0;
  }

  @Override
  public void unbuffer() {
    blockReader.unbuffer();
  }

  @Override
  public long getPos() throws IOException {
    return blockReader != null ? blockReader.getPos() : 0;
  }

  @Override
  public void seek(long pos) throws IOException {
    // TODO handle seek - does it need to deal with IOExceptions too?
  }
}

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level class used to read data from EC Encoded blocks. This class decides,
 * based on the block availability, whether to use a reconstruction or non
 * reconstruction read and also handles errors from the non-reconstruction reads
 * failing over to a reconstruction read when they happen.
 */
public class ECBlockInputStreamProxy extends BlockExtendedInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECBlockInputStreamProxy.class);

  private final ECReplicationConfig repConfig;
  private final XceiverClientFactory xceiverClientFactory;
  private final Function<BlockID, BlockLocationInfo> refreshFunction;
  private final BlockLocationInfo blockInfo;
  private final ECBlockInputStreamFactory ecBlockInputStreamFactory;

  private BlockExtendedInputStream blockReader;
  private boolean reconstructionReader = false;
  private List<DatanodeDetails> failedLocations = new ArrayList<>();
  private boolean closed = false;
  private OzoneClientConfig config;

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
   * From the Pipeline and expected number of locations, determine the number
   * of data locations available.
   * @param pipeline The pipeline for the data block, givings its locations and
   *                 the index of each location.
   * @param expectedLocs The number of locations we expect for the block to have
   *                     based on its block length and replication config. The
   *                     max value should be the repConfig data number.
   * @return The number of locations available
   */
  public static int availableDataLocations(Pipeline pipeline,
      int expectedLocs) {
    Set<Integer> locations = new HashSet<>();
    for (DatanodeDetails dn : pipeline.getNodes()) {
      int index = pipeline.getReplicaIndex(dn);
      if (index > 0 && index <= expectedLocs) {
        locations.add(index);
      }
    }
    return locations.size();
  }

  public ECBlockInputStreamProxy(ECReplicationConfig repConfig,
      BlockLocationInfo blockInfo,
      XceiverClientFactory xceiverClientFactory, Function<BlockID,
      BlockLocationInfo> refreshFunction,
      ECBlockInputStreamFactory streamFactory,
      OzoneClientConfig config) {
    this.repConfig = repConfig;
    this.blockInfo = blockInfo;
    this.ecBlockInputStreamFactory = streamFactory;
    this.xceiverClientFactory = xceiverClientFactory;
    this.refreshFunction = refreshFunction;
    this.config = config;

    setReaderType();
    createBlockReader();
  }

  private synchronized void setReaderType() {
    int expected = expectedDataLocations(repConfig, getLength());
    int available = availableDataLocations(blockInfo.getPipeline(), expected);
    reconstructionReader = available < expected;
    if (reconstructionReader) {
      LOG.info("Data locations available: {} < expected: {}, using reconstruction read", available, expected);
    }
  }

  private void createBlockReader() {
    if (reconstructionReader) {
      XceiverClientManager.getXceiverClientMetrics()
          .incECReconstructionTotal();
    }
    blockReader = ecBlockInputStreamFactory.create(reconstructionReader,
        failedLocations, repConfig, blockInfo,
        xceiverClientFactory, refreshFunction, config);
  }

  @Override
  public synchronized BlockID getBlockID() {
    return blockInfo.getBlockID();
  }

  @Override
  public synchronized long getRemaining() {
    return blockReader.getRemaining();
  }

  @Override
  public synchronized long getLength() {
    return blockInfo.getLength();
  }

  @Override
  public synchronized int read(byte[] b, int off, int len)
      throws IOException {
    return read(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    ensureNotClosed();
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
        XceiverClientManager.getXceiverClientMetrics()
            .incECReconstructionFailsTotal();
        throw e;
      }
      if (e instanceof BadDataLocationException) {
        String message = "Failing over to reconstruction read due" +
                " to an error in ECBlockReader.";
        if (LOG.isDebugEnabled()) {
          LOG.debug(message, e);
        } else {
          LOG.warn("{} Exception Class: {} , Exception Message: {}",
                  message, e.getClass().getName(), e.getMessage());
        }

        failoverToReconstructionRead(
            ((BadDataLocationException) e).getFailedLocations(), lastPosition);
        buf.reset();
        totalRead += read(buf);
      } else {
        throw e;
      }
    }
    return totalRead;
  }

  private synchronized void failoverToReconstructionRead(
      List<DatanodeDetails> badLocations, long lastPosition)
      throws IOException {
    if (badLocations != null) {
      failedLocations.addAll(badLocations);
    }
    blockReader.close();
    reconstructionReader = true;
    createBlockReader();
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

  @Override
  public synchronized void unbuffer() {
    blockReader.unbuffer();
  }

  @Override
  public synchronized long getPos() {
    return blockReader != null ? blockReader.getPos() : 0;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    ensureNotClosed();
    try {
      blockReader.seek(pos);
    } catch (IOException e) {
      if (reconstructionReader) {
        throw e;
      }
      failoverToReconstructionRead(null, pos);
    }
  }

  @Override
  public void close() throws IOException {
    if (blockReader != null) {
      blockReader.close();
    }
    closed = true;
  }

  private void ensureNotClosed() throws IOException {
    if (closed) {
      throw new IOException("The stream is closed");
    }
  }
}

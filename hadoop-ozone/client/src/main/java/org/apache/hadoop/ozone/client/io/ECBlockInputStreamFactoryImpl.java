/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import java.util.List;
import java.util.function.Function;

/**
 * Factory class to create various BlockStream instances.
 */
public final class ECBlockInputStreamFactoryImpl implements
    ECBlockInputStreamFactory {

  private final BlockInputStreamFactory inputStreamFactory;
  private final ByteBufferPool byteBufferPool;

  public static ECBlockInputStreamFactory getInstance(
      BlockInputStreamFactory streamFactory, ByteBufferPool byteBufferPool) {
    return new ECBlockInputStreamFactoryImpl(streamFactory, byteBufferPool);
  }

  private ECBlockInputStreamFactoryImpl(BlockInputStreamFactory streamFactory,
      ByteBufferPool byteBufferPool) {
    this.byteBufferPool = byteBufferPool;
    this.inputStreamFactory = streamFactory;
  }

  /**
   * Create a new EC InputStream based on the missingLocations boolean. If it is
   * set to false, it indicates all locations are available and an
   * ECBlockInputStream will be created. Otherwise an
   * ECBlockReconstructedInputStream will be created.
   * @param missingLocations Indicates if all the data locations are available
   *                         or not, controlling the type of stream created
   * @param failedLocations List of DatanodeDetails indicating locations we
   *                        know are bad and should not be used.
   * @param repConfig The replication Config
   * @param blockInfo The blockInfo representing the block.
   * @param verifyChecksum Whether to verify checksums or not.
   * @param xceiverFactory Factory to create the xceiver in the client
   * @param refreshFunction Function to refresh the pipeline if needed
   * @return BlockExtendedInputStream of the correct type.
   */
  public BlockExtendedInputStream create(boolean missingLocations,
      List<DatanodeDetails> failedLocations, ReplicationConfig repConfig,
      OmKeyLocationInfo blockInfo, boolean verifyChecksum,
      XceiverClientFactory xceiverFactory,
      Function<BlockID, Pipeline> refreshFunction) {
    if (missingLocations) {
      // We create the reconstruction reader
      ECBlockReconstructedStripeInputStream sis =
          new ECBlockReconstructedStripeInputStream(
              (ECReplicationConfig)repConfig, blockInfo, verifyChecksum,
              xceiverFactory, refreshFunction, inputStreamFactory,
              byteBufferPool);
      if (failedLocations != null) {
        sis.addFailedDatanodes(failedLocations);
      }
      return new ECBlockReconstructedInputStream(
          (ECReplicationConfig) repConfig, byteBufferPool, sis);
    } else {
      // Otherwise create the more efficient non-reconstruction reader
      return new ECBlockInputStream((ECReplicationConfig)repConfig, blockInfo,
          verifyChecksum, xceiverFactory, refreshFunction, inputStreamFactory);
    }
  }

}

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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.io.ByteBufferPool;

/**
 * Factory class to create various BlockStream instances.
 */
public final class ECBlockInputStreamFactoryImpl implements
    ECBlockInputStreamFactory {

  private final BlockInputStreamFactory inputStreamFactory;
  private final ByteBufferPool byteBufferPool;
  private final Supplier<ExecutorService> ecReconstructExecutorSupplier;

  public static ECBlockInputStreamFactory getInstance(
      BlockInputStreamFactory streamFactory, ByteBufferPool byteBufferPool,
      Supplier<ExecutorService> ecReconstructExecutorSupplier) {
    return new ECBlockInputStreamFactoryImpl(streamFactory, byteBufferPool,
        ecReconstructExecutorSupplier);
  }

  private ECBlockInputStreamFactoryImpl(BlockInputStreamFactory streamFactory,
      ByteBufferPool byteBufferPool,
      Supplier<ExecutorService> ecReconstructExecutorSupplier) {
    this.byteBufferPool = byteBufferPool;
    this.inputStreamFactory = streamFactory;
    this.ecReconstructExecutorSupplier = ecReconstructExecutorSupplier;
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
   * @param xceiverFactory Factory to create the xceiver in the client
   * @param refreshFunction Function to refresh the pipeline if needed
   * @return BlockExtendedInputStream of the correct type.
   */
  @Override
  public BlockExtendedInputStream create(boolean missingLocations,
      List<DatanodeDetails> failedLocations, ReplicationConfig repConfig,
      BlockLocationInfo blockInfo,
      XceiverClientFactory xceiverFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) {
    if (missingLocations) {
      // We create the reconstruction reader
      ECBlockReconstructedStripeInputStream sis =
          new ECBlockReconstructedStripeInputStream(
              (ECReplicationConfig)repConfig, blockInfo,
              xceiverFactory, refreshFunction, inputStreamFactory,
              byteBufferPool, ecReconstructExecutorSupplier.get(), config);
      if (failedLocations != null) {
        sis.addFailedDatanodes(failedLocations);
      }
      return new ECBlockReconstructedInputStream(
          (ECReplicationConfig) repConfig, byteBufferPool, sis);
    } else {
      // Otherwise create the more efficient non-reconstruction reader
      return new ECBlockInputStream((ECReplicationConfig)repConfig, blockInfo,
          xceiverFactory, refreshFunction, inputStreamFactory,
          config);
    }
  }

}

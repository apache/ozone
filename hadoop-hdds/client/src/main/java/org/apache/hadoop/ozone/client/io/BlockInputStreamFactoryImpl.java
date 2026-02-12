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

import static org.apache.hadoop.hdds.DatanodeVersion.STREAM_BLOCK_SUPPORT;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.security.token.Token;

/**
 * Factory class to create various BlockStream instances.
 */
public class BlockInputStreamFactoryImpl implements BlockInputStreamFactory {

  private ECBlockInputStreamFactory ecBlockStreamFactory;

  public static BlockInputStreamFactory getInstance(
      ByteBufferPool byteBufferPool,
      Supplier<ExecutorService> ecReconstructExecutorSupplier) {
    return new BlockInputStreamFactoryImpl(byteBufferPool,
        ecReconstructExecutorSupplier);
  }

  public BlockInputStreamFactoryImpl() {
    this(new ElasticByteBufferPool(), Executors::newSingleThreadExecutor);
  }

  public BlockInputStreamFactoryImpl(ByteBufferPool byteBufferPool,
      Supplier<ExecutorService> ecReconstructExecutorSupplier) {
    this.ecBlockStreamFactory =
        ECBlockInputStreamFactoryImpl.getInstance(this, byteBufferPool,
            ecReconstructExecutorSupplier);
  }

  /**
   * Create a new BlockInputStream based on the replication Config. If the
   * replication Config indicates the block is EC, then it will create an
   * ECBlockInputStream, otherwise a BlockInputStream will be returned.
   * @param repConfig The replication Config
   * @param blockInfo The blockInfo representing the block.
   * @param pipeline The pipeline to be used for reading the block
   * @param token The block Access Token
   * @param xceiverFactory Factory to create the xceiver in the client
   * @param refreshFunction Function to refresh the pipeline if needed
   * @return BlockExtendedInputStream of the correct type.
   */
  @Override
  public BlockExtendedInputStream create(ReplicationConfig repConfig,
      BlockLocationInfo blockInfo, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) throws IOException {
    if (repConfig.getReplicationType().equals(HddsProtos.ReplicationType.EC)) {
      return new ECBlockInputStreamProxy((ECReplicationConfig)repConfig,
          blockInfo, xceiverFactory, refreshFunction,
          ecBlockStreamFactory, config);
    } else if (config.isStreamReadBlock() && allDataNodesSupportStreamBlock(pipeline)) {
      return new StreamBlockInputStream(blockInfo.getBlockID(), blockInfo.getLength(), pipeline, token, xceiverFactory,
          refreshFunction, config);
    } else {
      return new BlockInputStream(blockInfo,
          pipeline, token, xceiverFactory, refreshFunction,
          config);
    }
  }

  private boolean allDataNodesSupportStreamBlock(Pipeline pipeline) {
    // return true only if all DataNodes in the pipeline are on a version
    // that supports for reading a block by streaming chunks..
    for (DatanodeDetails dn : pipeline.getNodes()) {
      if (dn.getCurrentVersion() < STREAM_BLOCK_SUPPORT.toProtoValue()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create a new BlockInputStream for RATIS.
   *
   * @param blockInfo The blockInfo representing the block.
   * @param pipeline The pipeline to be used for reading the block
   * @param token The block Access Token
   * @param xceiverFactory Factory to create the xceiver in the client
   * @param refreshFunction Function to refresh the block location if needed
   * @param config The client configuration
   * @return BlockInputStream instance.
   */
  public BlockInputStream createBlockInputStream(BlockLocationInfo blockInfo,
      Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) throws IOException {

    return new BlockInputStream(blockInfo, pipeline, token, xceiverFactory, refreshFunction, config);
  }
}

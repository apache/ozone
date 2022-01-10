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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.Token;

import java.util.function.Function;

/**
 * Factory class to create various BlockStream instances.
 */
public class BlockInputStreamFactoryImpl implements BlockInputStreamFactory {

  private ECBlockInputStreamFactory ecBlockStreamFactory;

  public static BlockInputStreamFactory getInstance(
      ByteBufferPool byteBufferPool) {
    return new BlockInputStreamFactoryImpl(byteBufferPool);
  }

  public BlockInputStreamFactoryImpl() {
    this(new ElasticByteBufferPool());
  }

  public BlockInputStreamFactoryImpl(ByteBufferPool byteBufferPool) {
    this.ecBlockStreamFactory =
        ECBlockInputStreamFactoryImpl.getInstance(this, byteBufferPool);
  }

  /**
   * Create a new BlockInputStream based on the replication Config. If the
   * replication Config indicates the block is EC, then it will create an
   * ECBlockInputStream, otherwise a BlockInputStream will be returned.
   * @param repConfig The replication Config
   * @param blockInfo The blockInfo representing the block.
   * @param pipeline The pipeline to be used for reading the block
   * @param token The block Access Token
   * @param verifyChecksum Whether to verify checksums or not.
   * @param xceiverFactory Factory to create the xceiver in the client
   * @param refreshFunction Function to refresh the pipeline if needed
   * @return BlockExtendedInputStream of the correct type.
   */
  public BlockExtendedInputStream create(ReplicationConfig repConfig,
      OmKeyLocationInfo blockInfo, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token, boolean verifyChecksum,
      XceiverClientFactory xceiverFactory,
      Function<BlockID, Pipeline> refreshFunction) {
    if (repConfig.getReplicationType().equals(HddsProtos.ReplicationType.EC)) {
      return new ECBlockInputStreamProxy((ECReplicationConfig)repConfig,
          blockInfo, verifyChecksum, xceiverFactory, refreshFunction,
          ecBlockStreamFactory);
    } else {
      return new BlockInputStream(blockInfo.getBlockID(), blockInfo.getLength(),
          pipeline, token, verifyChecksum, xceiverFactory, refreshFunction);
    }
  }

}

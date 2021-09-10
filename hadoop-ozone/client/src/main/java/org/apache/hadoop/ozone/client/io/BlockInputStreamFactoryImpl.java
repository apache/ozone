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
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.Token;

import java.util.function.Function;

/**
 * Factory class to create various BlockStream instances.
 */
public class BlockInputStreamFactoryImpl implements BlockInputStreamFactory {

  public static BlockInputStreamFactory getInstance() {
    return new BlockInputStreamFactoryImpl();
  }

  public BlockExtendedInputStream create(BlockID blockId, long blockLen,
      Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
      boolean verifyChecksum, XceiverClientFactory xceiverFactory,
      Function<BlockID, Pipeline> refreshFunction) {
    return new BlockInputStream(blockId, blockLen, pipeline, token,
        verifyChecksum, xceiverFactory, refreshFunction);
  }

  public BlockExtendedInputStream create(ECReplicationConfig repConfig,
      OmKeyLocationInfo blockInfo, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token, boolean verifyChecksum,
      XceiverClientFactory xceiverFactory,
      Function<BlockID, Pipeline> refreshFunction) {
    return null;
  }

//  private BlockExtendedInputStream createECStream() {
//    new ECBlockInputStream()
//  }

}

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
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.util.function.Function;

/**
 * Concrete implementation of a BlockInputStreamProvider to create
 * BlockInputStreams in a real cluster.
 */
public class BlockInputStreamProviderImpl implements BlockInputStreamProvider {

  private final XceiverClientFactory xceiverClientFactory;
  private final Function<BlockID, Pipeline> refreshFunction;

  public BlockInputStreamProviderImpl(XceiverClientFactory xceiverFactory,
      Function<BlockID, Pipeline> refreshFunction) {
    this.xceiverClientFactory = xceiverFactory;
    this.refreshFunction = refreshFunction;
  }

  @Override
  public BlockInputStream provide(BlockID blockId, long blockLen,
      Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
      boolean verifyChecksum) {
    return new BlockInputStream(blockId, blockLen, pipeline, token,
        verifyChecksum, xceiverClientFactory, refreshFunction);
  }
}

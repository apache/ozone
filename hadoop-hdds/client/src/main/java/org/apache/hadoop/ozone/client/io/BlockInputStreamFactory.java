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
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * Interface used by classes which need to obtain BlockStream instances.
 */
public interface BlockInputStreamFactory {

  /**
   * Create a new BlockInputStream based on the replication Config. If the
   * replication Config indicates the block is EC, then it will create an
   * ECBlockInputStream, otherwise a BlockInputStream will be returned.
   * @param repConfig The replication Config
   * @param blockInfo The blockInfo representing the block.
   * @param pipeline The pipeline to be used for reading the block
   * @param token The block Access Token
   * @param xceiverFactory Factory to create the xceiver in the client
   * @param refreshFunction Function to refresh the block location if needed
   * @return BlockExtendedInputStream of the correct type.
   */
  BlockExtendedInputStream create(ReplicationConfig repConfig,
      BlockLocationInfo blockInfo, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
       XceiverClientFactory xceiverFactory,
       Function<BlockID, BlockLocationInfo> refreshFunction,
       OzoneClientConfig config) throws IOException;

}

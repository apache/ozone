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

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * A dummy BlockInputStream to mock read block call to DN.
 */
class DummyBlockInputStream extends BlockInputStream {

  private final List<ChunkInfo> chunks;

  private final Map<String, byte[]> chunkDataMap;

  @SuppressWarnings("parameternumber")
  DummyBlockInputStream(
      BlockID blockId,
      long blockLen,
      Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientManager,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      List<ChunkInfo> chunkList,
      Map<String, byte[]> chunks,
      OzoneClientConfig config) throws IOException {
    super(new BlockLocationInfo(new BlockLocationInfo.Builder().setBlockID(blockId).setLength(blockLen)),
        pipeline, token,
        xceiverClientManager, refreshFunction, config);
    this.chunkDataMap = chunks;
    this.chunks = chunkList;

  }

  @Override
  protected ContainerProtos.BlockData getBlockData() throws IOException {
    BlockID blockID = getBlockID();
    ContainerProtos.DatanodeBlockID datanodeBlockID = blockID.getDatanodeBlockIDProtobuf();
    return ContainerProtos.BlockData.newBuilder().addAllChunks(chunks).setBlockID(datanodeBlockID).build();
  }

  @Override
  protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
    return new DummyChunkInputStream(
        chunkInfo, null, null, false,
        chunkDataMap.get(chunkInfo.getChunkName()).clone(), null);
  }

  @Override
  protected synchronized void checkOpen() throws IOException {
    // No action needed
  }
}

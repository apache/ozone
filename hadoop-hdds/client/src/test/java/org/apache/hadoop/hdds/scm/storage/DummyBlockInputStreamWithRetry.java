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
package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * A dummy BlockInputStream with pipeline refresh function to mock read
 * block call to DN.
 */
final class DummyBlockInputStreamWithRetry
    extends DummyBlockInputStream {

  private int getChunkInfoCount = 0;

  @SuppressWarnings("parameternumber")
  DummyBlockInputStreamWithRetry(
      BlockID blockId,
      long blockLen,
      Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      boolean verifyChecksum,
      XceiverClientFactory xceiverClientManager,
      List<ChunkInfo> chunkList,
      Map<String, byte[]> chunkMap,
      AtomicBoolean isRerfreshed) {
    super(blockId, blockLen, pipeline, token, verifyChecksum,
        xceiverClientManager, blockID -> {
          isRerfreshed.set(true);
          return Pipeline.newBuilder()
              .setState(Pipeline.PipelineState.OPEN)
              .setId(PipelineID.randomId())
              .setType(HddsProtos.ReplicationType.STAND_ALONE)
              .setFactor(HddsProtos.ReplicationFactor.ONE)
              .setNodes(Collections.emptyList())
              .build();
        }, chunkList, chunkMap);
  }

  @Override
  protected List<ChunkInfo> getChunkInfos() throws IOException {
    if (getChunkInfoCount == 0) {
      getChunkInfoCount++;
      throw new ContainerNotFoundException("Exception encountered");
    } else {
      return super.getChunkInfos();
    }
  }
}

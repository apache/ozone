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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * A dummy BlockInputStream with pipeline refresh function to mock read
 * block call to DN.
 */
final class DummyBlockInputStreamWithRetry
    extends DummyBlockInputStream {

  private int getChunkInfoCount = 0;
  private IOException ioException;

  @SuppressWarnings("parameternumber")
  DummyBlockInputStreamWithRetry(
      BlockID blockId,
      long blockLen,
      Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientManager,
      List<ChunkInfo> chunkList,
      Map<String, byte[]> chunkMap,
      AtomicBoolean isRerfreshed, IOException ioException,
      OzoneClientConfig config) throws IOException {
    super(blockId, blockLen, pipeline, token,
        xceiverClientManager, blockID -> {
          isRerfreshed.set(true);
          try {
            BlockLocationInfo blockLocationInfo = mock(BlockLocationInfo.class);
            Pipeline mockPipeline = MockPipeline.createPipeline(1);
            when(blockLocationInfo.getPipeline()).thenReturn(mockPipeline);
            return blockLocationInfo;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

        }, chunkList, chunkMap, config);
    this.ioException  = ioException;
  }

  @Override
  protected ContainerProtos.BlockData getBlockData() throws IOException {
    if (getChunkInfoCount == 0) {
      getChunkInfoCount++;
      if (ioException != null) {
        throw ioException;
      }
      throw new StorageContainerException("Exception encountered",
          CONTAINER_NOT_FOUND);
    } else {
      return super.getBlockData();
    }
  }
}

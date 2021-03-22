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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;

import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * A dummy ChunkInputStream to mock read chunk calls to DN.
 */
public class DummyChunkInputStream extends ChunkInputStream {

  private final byte[] chunkData;

  // Stores the read chunk data in each readChunk call
  private final List<ByteString> readByteBuffers = new ArrayList<>();

  public DummyChunkInputStream(ChunkInfo chunkInfo,
      BlockID blockId,
      XceiverClientFactory xceiverClientFactory,
      boolean verifyChecksum,
      byte[] data, Pipeline pipeline) {
    super(chunkInfo, blockId, xceiverClientFactory, () -> pipeline,
        verifyChecksum, null);
    this.chunkData = data.clone();
  }

  @Override
  protected List<ByteBuffer> readChunk(ChunkInfo readChunkInfo) {
    int offset = (int) readChunkInfo.getOffset();
    int remainingToRead = (int) readChunkInfo.getLen();

    int bufferCapacity = readChunkInfo.getChecksumData().getBytesPerChecksum();
    int bufferLen;
    readByteBuffers.clear();
    while (remainingToRead > 0) {
      if (remainingToRead < bufferCapacity) {
        bufferLen = remainingToRead;
      } else {
        bufferLen = bufferCapacity;
      }
      ByteString byteString = ByteString.copyFrom(chunkData,
          offset, bufferLen);

      readByteBuffers.add(byteString);

      offset += bufferLen;
      remainingToRead -= bufferLen;
    }

    return BufferUtils.getReadOnlyByteBuffers(readByteBuffers);
  }

  @Override
  protected void acquireClient() {
    // No action needed
  }

  @Override
  protected void releaseClient() {
    // no-op
  }

  public List<ByteString> getReadByteBuffers() {
    return readByteBuffers;
  }
}

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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * A dummy ChunkInputStream to mock read chunk calls to DN.
 */
public class DummyChunkInputStream extends ChunkInputStream {

  private byte[] chunkData;

  // Stores the read chunk data in each readChunk call
  private List<ByteString> readByteBuffers = new ArrayList<>();

  public DummyChunkInputStream(TestChunkInputStream testChunkInputStream,
      ChunkInfo chunkInfo,
      BlockID blockId,
      XceiverClientSpi xceiverClient,
      boolean verifyChecksum,
      byte[] data) {
    super(chunkInfo, blockId, xceiverClient, verifyChecksum);
    this.chunkData = data;
  }

  @Override
  protected ByteString readChunk(ChunkInfo readChunkInfo) {
    ByteString byteString = ByteString.copyFrom(chunkData,
        (int) readChunkInfo.getOffset(),
        (int) readChunkInfo.getLen());
    getReadByteBuffers().add(byteString);
    return byteString;
  }

  @Override
  protected void checkOpen() {
    // No action needed
  }

  public List<ByteString> getReadByteBuffers() {
    return readByteBuffers;
  }
}

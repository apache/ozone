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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * A dummy StreamBlockInput to mock read block call to DN.
 */
class DummyStreamBlockInput extends StreamBlockInput {

  private final List<ByteString> readByteBuffers = new ArrayList<>();
  private final List<ChunkInfo> chunks;
  private final long[] chunkOffsets;
  private final Map<String, byte[]> chunkDataMap;

  @SuppressWarnings("parameternumber")
  DummyStreamBlockInput(
      BlockID blockId,
      long blockLen,
      Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientManager,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config,
      List<ChunkInfo> chunks,
      Map<String, byte[]> chunkDataMap) throws IOException {
    super(blockId, blockLen, pipeline, token, xceiverClientManager,
        refreshFunction, config);
    this.chunks = chunks;
    this.chunkDataMap = chunkDataMap;
    chunkOffsets = new long[chunks.size()];
    long temp = 0;
    for (int i = 0; i < chunks.size(); i++) {
      chunkOffsets[i] = temp;
      temp += chunks.get(i).getLen();
    }
  }

  @Override
  protected synchronized void checkOpen() throws IOException {
    // No action needed
  }

  @Override
  protected void acquireClient() {
    // No action needed
  }

  @Override
  protected void releaseClient() {
    // no-op
  }

  @Override
  protected long readData(long offset, long len) {
    int chunkIndex = Arrays.binarySearch(chunkOffsets, offset);
    if (chunkIndex < 0) {
      chunkIndex = -chunkIndex - 2;
    }
    ChunkInfo chunkInfo = chunks.get(chunkIndex);
    readByteBuffers.clear();
    long chunkOffset = offset - chunkInfo.getOffset();
    if (isVerifyChecksum()) {
      ChecksumData checksumData = ChecksumData.getFromProtoBuf(
          chunkInfo.getChecksumData());
      int bytesPerChecksum = checksumData.getBytesPerChecksum();
      chunkOffset = (chunkOffset / bytesPerChecksum) * bytesPerChecksum;
    }
    long bufferOffsetWrtBlockDataData = chunkOffsets[chunkIndex] + chunkOffset;
    while (len > 0) {
      ChunkInfo currentChunk = chunks.get(chunkIndex);
      int bufferCapacity = currentChunk.getChecksumData().getBytesPerChecksum();
      long chunkLen = currentChunk.getLen();
      long remainingToRead = Math.min(chunkLen, len);
      if (isVerifyChecksum()) {
        if (len < chunkLen) {
          ChecksumData checksumData = ChecksumData.getFromProtoBuf(
              chunkInfo.getChecksumData());
          int bytesPerChecksum = checksumData.getBytesPerChecksum();
          remainingToRead = (len / bytesPerChecksum + 1) * bytesPerChecksum;
        } else {
          remainingToRead = chunkLen;
        }
      }

      long bufferLen;
      while (remainingToRead > 0) {
        if (remainingToRead < bufferCapacity) {
          bufferLen = remainingToRead;
        } else {
          bufferLen = bufferCapacity;
        }
        System.out.println(bufferLen);
        ByteString byteString = ByteString.copyFrom(chunkDataMap.get(chunks.get(chunkIndex).getChunkName()),
            (int) chunkOffset, (int) bufferLen);

        readByteBuffers.add(byteString);

        chunkOffset += bufferLen;
        remainingToRead -= bufferLen;
        len -= bufferLen;
      }
      chunkOffset = 0;
      chunkIndex++;
    }
    setBuffers(BufferUtils.getReadOnlyByteBuffers(readByteBuffers));
    return bufferOffsetWrtBlockDataData;
  }

  public List<ByteString> getReadByteBuffers() {
    return readByteBuffers;
  }
}

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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientShortCircuit;
import org.apache.hadoop.hdds.scm.XceiverClientSpi.ShortCircuitValidator;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * An {@link InputStream} called from BlockInputStream to read a chunk from the
 * container. Each chunk may contain multiple underlying {@link ByteBuffer}
 * instances.
 */
public class ShortCircuitChunkInputStream extends ChunkInputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {

  private final ChunkInfo chunkInfo;
  private final FileInputStream blockInputStream;
  private final FileChannel dataIn;
  private final ShortCircuitValidator validator;
  private final XceiverClientShortCircuit xceiverClientShortCircuit;
  private final boolean verifyChecksum;
  public static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitChunkInputStream.class);

  @SuppressWarnings("checkstyle:parameternumber")
  ShortCircuitChunkInputStream(ChunkInfo chunkInfo, BlockID blockId, XceiverClientFactory xceiverClientFactory,
      Supplier<Pipeline> pipelineSupplier, boolean verifyChecksum, Supplier<Token<?>> tokenSupplier,
      XceiverClientShortCircuit xceiverClientShortCircuit, FileInputStream blockInputStream) {
    super(chunkInfo, blockId, xceiverClientFactory, pipelineSupplier, verifyChecksum, tokenSupplier);
    this.chunkInfo = chunkInfo;
    this.blockInputStream = blockInputStream;
    this.dataIn = blockInputStream.getChannel();
    this.xceiverClientShortCircuit = xceiverClientShortCircuit;
    this.validator = this::validateChunk;
    this.verifyChecksum = verifyChecksum;
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} is created for {}", ShortCircuitChunkInputStream.class.getSimpleName(), blockId);
    }
  }

  /**
   * Send RPC call to get the chunk from the container.
   */
  @VisibleForTesting
  @Override
  protected ByteBuffer[] readChunk(ChunkInfo readChunkInfo)
      throws IOException {
    int bytesPerChecksum = chunkInfo.getChecksumData().getBytesPerChecksum();
    final ByteBuffer[] buffers = BufferUtils.assignByteBuffers(readChunkInfo.getLen(),
        bytesPerChecksum);
    dataIn.position(readChunkInfo.getOffset()).read(buffers);
    Arrays.stream(buffers).forEach(ByteBuffer::flip);
    validator.accept(Arrays.asList(buffers), readChunkInfo);
    return buffers;
  }

  private void validateChunk(List<ByteBuffer> bufferList, ChunkInfo readChunkInfo)
      throws OzoneChecksumException {
    if (verifyChecksum) {
      ChecksumData checksumData = ChecksumData.getFromProtoBuf(
          chunkInfo.getChecksumData());

      // ChecksumData stores checksum for each 'numBytesPerChecksum'
      // number of bytes in a list. Compute the index of the first
      // checksum to match with the read data

      long relativeOffset = readChunkInfo.getOffset() -
          chunkInfo.getOffset();
      int bytesPerChecksum = checksumData.getBytesPerChecksum();
      int startIndex = (int) (relativeOffset / bytesPerChecksum);
      Checksum.verifyChecksum(bufferList, checksumData, startIndex);
    }
  }


  /**
   * Acquire short-circuit local read client.
   */
  @Override
  protected synchronized void acquireClient() throws IOException {
   // do nothing, read data doesn't need short-circuit client
  }
}

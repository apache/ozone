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

import com.google.common.annotations.VisibleForTesting;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
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

/**
 * An {@link InputStream} called from BlockInputStream to read a chunk from the local
 * block replica directly. Each chunk may contain multiple underlying {@link ByteBuffer}
 * instances.
 */
public class LocalChunkInputStream extends ChunkInputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {

  private final ChunkInfo chunkInfo;
  private final FileChannel dataIn;
  private final ShortCircuitValidator validator;
  private final boolean verifyChecksum;
  public static final Logger LOG =
      LoggerFactory.getLogger(LocalChunkInputStream.class);

  @SuppressWarnings("checkstyle:parameternumber")
  LocalChunkInputStream(ChunkInfo chunkInfo, BlockID blockId, XceiverClientFactory xceiverClientFactory,
      Supplier<Pipeline> pipelineSupplier, boolean verifyChecksum, Supplier<Token<?>> tokenSupplier,
      XceiverClientShortCircuit xceiverClientShortCircuit, FileInputStream blockInputStream) {
    super(chunkInfo, blockId, xceiverClientFactory, pipelineSupplier, verifyChecksum, tokenSupplier);
    this.chunkInfo = chunkInfo;
    this.dataIn = blockInputStream.getChannel();
    this.validator = this::validateChunk;
    this.verifyChecksum = verifyChecksum;
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} is created for {}", LocalChunkInputStream.class.getSimpleName(), blockId);
    }
  }

  /**
   * Get the chunk from the local block replica.
   */
  @VisibleForTesting
  @Override
  protected ByteBuffer[] readChunk(ChunkInfo readChunkInfo)
      throws IOException {
    int bytesPerChecksum = chunkInfo.getChecksumData().getBytesPerChecksum();
    final ByteBuffer[] buffers = BufferUtils.assignByteBuffers(readChunkInfo.getLen(), bytesPerChecksum);
    readAtOffset(buffers, readChunkInfo.getOffset());
    Arrays.stream(buffers).forEach(ByteBuffer::flip);
    validator.accept(Arrays.asList(buffers), readChunkInfo);
    return buffers;
  }

  /**
   * Read into {@code buffers} starting at {@code fileOffset} using positional
   * {@link FileChannel} reads so concurrent callers on different chunks (which
   * share the same underlying channel) do not stomp each other's cursor.
   */
  private void readAtOffset(ByteBuffer[] buffers, long fileOffset) throws IOException {
    long pos = fileOffset;
    for (ByteBuffer buffer : buffers) {
      final int remaining = buffer.remaining();
      final long read = readAtOffset(buffer, pos);
      if (buffer.hasRemaining()) {
        throw new EOFException("Read only " + read + "bytes but expected to read " + remaining
            + " bytes at offset " + pos + " for chunk " + chunkInfo.getChunkName());
      }
      pos += read;
    }
  }

  private long readAtOffset(ByteBuffer buffer, long fileOffset) throws IOException {
    long pos = fileOffset;
    while (buffer.hasRemaining()) {
      final int n = dataIn.read(buffer, pos);
      if (n < 0) {
        break;
      }
      pos += n;
    }
    return pos - fileOffset;
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
      Checksum.verifyChecksum(bufferList, startIndex, checksumData);
    }
  }
}

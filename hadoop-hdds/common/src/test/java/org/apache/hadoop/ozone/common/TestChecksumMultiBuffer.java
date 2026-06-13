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

package org.apache.hadoop.ozone.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A checksum window that straddles multiple underlying buffers must
 * produce the same checksum bytes as a single contiguous buffer over the
 * same data.
 */
class TestChecksumMultiBuffer {

  private static final int CHUNK_SIZE = 4 * 1024 * 1024;
  private static final int BYTES_PER_CHECKSUM = 1024 * 1024;

  @ParameterizedTest
  @EnumSource(value = ChecksumType.class, names = {"CRC32", "CRC32C",
      "SHA256", "MD5"})
  void splitBufferProducesSameChecksumAsSingleBuffer(ChecksumType type)
      throws Exception {
    byte[] data = new byte[CHUNK_SIZE];
    new Random(0xCAFEBABEL).nextBytes(data);

    Checksum checksum = new Checksum(type, BYTES_PER_CHECKSUM);

    // Single contiguous buffer.
    ChecksumData single = checksum.computeChecksum(
        ChunkBuffer.wrap(ByteBuffer.wrap(data.clone())));

    // Split into 16 pieces of 256KB - each 1MB checksum window straddles
    // exactly 4 underlying buffers.
    int piece = 256 * 1024;
    List<ByteBuffer> pieces = new ArrayList<>();
    for (int off = 0; off < CHUNK_SIZE; off += piece) {
      pieces.add(ByteBuffer.wrap(data, off, piece).slice());
    }
    ChecksumData split = checksum.computeChecksum(ChunkBuffer.wrap(pieces));

    assertEquals(single.getChecksums().size(), split.getChecksums().size(),
        "checksum count must match");
    assertEquals(single.getChecksums(), split.getChecksums(),
        "single-buffer and split-buffer must produce identical checksums "
            + "for " + type);
  }

  @ParameterizedTest
  @EnumSource(value = ChecksumType.class, names = {"CRC32", "CRC32C",
      "SHA256", "MD5"})
  void unalignedTrailingWindowIsHandled(ChecksumType type) throws Exception {
    // Chunk size deliberately not a multiple of bytesPerChecksum, plus
    // unaligned buffer splits, to exercise the trailing-partial-window
    // path in computeChecksumDirect.
    int total = BYTES_PER_CHECKSUM * 3 + 12345;
    byte[] data = new byte[total];
    new Random(0xDEADBEEFL).nextBytes(data);

    Checksum checksum = new Checksum(type, BYTES_PER_CHECKSUM);

    ChecksumData single = checksum.computeChecksum(
        ChunkBuffer.wrap(ByteBuffer.wrap(data.clone())));

    int piece = 333 * 1024; // intentionally awkward
    List<ByteBuffer> pieces = new ArrayList<>();
    for (int off = 0; off < total; off += piece) {
      int len = Math.min(piece, total - off);
      pieces.add(ByteBuffer.wrap(data, off, len).slice());
    }
    ChecksumData split = checksum.computeChecksum(ChunkBuffer.wrap(pieces));

    assertEquals(single.getChecksums(), split.getChecksums(),
        "unaligned split must match single-buffer for " + type);
  }
}

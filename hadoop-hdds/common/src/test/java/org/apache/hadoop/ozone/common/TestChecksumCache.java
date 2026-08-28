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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.ozone.common.Checksum.Algorithm;
import org.apache.hadoop.ozone.common.Checksum.StreamingChecksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class for {@link ChecksumCache}.
 */
class TestChecksumCache {

  @ParameterizedTest
  @EnumSource(value = ChecksumType.class, names = {"CRC32", "CRC32C", "SHA256", "MD5"})
  void testComputeChecksum(ChecksumType checksumType) throws Exception {
    final int bytesPerChecksum = 16;
    ChecksumCache checksumCache = new ChecksumCache(bytesPerChecksum);

    final int size = 66;
    byte[] byteArray = new byte[size];
    // Fill byteArray with bytes from 0 to 127 for deterministic testing
    for (int i = 0; i < size; i++) {
      byteArray[i] = (byte) (i % 128);
    }

    final StreamingChecksum algo = Algorithm.valueOf(checksumType).newStreamingChecksum();

    int iEnd = size / bytesPerChecksum + (size % bytesPerChecksum == 0 ? 0 : 1);
    List<ByteString> lastRes = null;
    for (int i = 0; i < iEnd; i++) {
      int byteBufferLength = Integer.min(byteArray.length, bytesPerChecksum * (i + 1));
      ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray, 0, byteBufferLength);

      try (ChunkBuffer chunkBuffer = ChunkBuffer.wrap(byteBuffer.asReadOnlyBuffer())) {
        List<ByteString> res = checksumCache.computeChecksum(chunkBuffer, algo, bytesPerChecksum);
        // Every entry except the last must be unchanged from the prior iteration
        // (those windows are fully cached and never re-computed).
        if (i > 0) {
          for (int j = 0; j < res.size() - 1; j++) {
            Assertions.assertEquals(lastRes.get(j), res.get(j));
          }
        }
        lastRes = new ArrayList<>(res);
      }
    }

    checksumCache.clear();
  }

  @ParameterizedTest
  @EnumSource(value = ChecksumType.class, names = {"CRC32", "CRC32C", "SHA256", "MD5"})
  void testGrowingMultiBufferMatchesDirectChecksum(ChecksumType checksumType)
      throws Exception {
    final int bytesPerChecksum = 16;
    final byte[] data = new byte[66];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    final Checksum cached = new Checksum(checksumType, bytesPerChecksum, true);
    final Checksum direct = new Checksum(checksumType, bytesPerChecksum);
    final int[] lengths = {1, 7, 15, 16, 17, 22, 31, 32, 33, 47, 48, 49, 65, 66};
    for (int length : lengths) {
      final ChecksumData expected = direct.computeChecksum(
          split(data, length, 7));
      final ChecksumData actual = cached.computeChecksum(
          split(data, length, 7), true);
      Assertions.assertEquals(expected.getChecksums(), actual.getChecksums(),
          "cached checksums must match direct checksums at length " + length);
    }
  }

  @ParameterizedTest
  @EnumSource(value = ChecksumType.class, names = {"CRC32", "CRC32C", "SHA256", "MD5"})
  void testPartiallyPositionedMultiBufferMatchesDirectChecksum(
      ChecksumType checksumType) throws Exception {
    final int bytesPerChecksum = 4;
    final byte[] data = new byte[11];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    final Checksum direct = new Checksum(checksumType, bytesPerChecksum);
    final Checksum cached = new Checksum(checksumType, bytesPerChecksum, true);
    final ChecksumData expected = direct.computeChecksum(partiallyPositioned(data));
    final ChecksumData actual = cached.computeChecksum(partiallyPositioned(data), true);

    Assertions.assertEquals(expected.getChecksums(), actual.getChecksums());
  }

  private static ChunkBuffer split(byte[] data, int length, int bufferSize) {
    final List<ByteBuffer> buffers = new ArrayList<>();
    for (int offset = 0; offset < length; offset += bufferSize) {
      final int size = Math.min(bufferSize, length - offset);
      buffers.add(ByteBuffer.wrap(data, offset, size).slice());
    }
    return ChunkBuffer.wrap(buffers);
  }

  private static ChunkBuffer partiallyPositioned(byte[] data) {
    final List<ByteBuffer> buffers = new ArrayList<>();
    buffers.add(ByteBuffer.wrap(data, 3, 4));
    buffers.add(ByteBuffer.wrap(data, 7, 4).slice());
    return ChunkBuffer.wrap(buffers);
  }
}

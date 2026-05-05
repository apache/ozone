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
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.ozone.common.Checksum.Algorithm;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class for {@link ChecksumCache}.
 */
class TestChecksumCache {

  @ParameterizedTest
  @EnumSource(ChecksumType.class)
  void testComputeChecksum(ChecksumType checksumType) throws Exception {
    final int bytesPerChecksum = 16;
    ChecksumCache checksumCache = new ChecksumCache(bytesPerChecksum);

    final int size = 66;
    byte[] byteArray = new byte[size];
    // Fill byteArray with bytes from 0 to 127 for deterministic testing
    for (int i = 0; i < size; i++) {
      byteArray[i] = (byte) (i % 128);
    }

    final Function<ByteBuffer, ByteString> function = Algorithm.valueOf(checksumType).newChecksumFunction();

    int iEnd = size / bytesPerChecksum + (size % bytesPerChecksum == 0 ? 0 : 1);
    List<ByteString> lastRes = null;
    for (int i = 0; i < iEnd; i++) {
      int byteBufferLength = Integer.min(byteArray.length, bytesPerChecksum * (i + 1));
      ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray, 0, byteBufferLength);

      try (ChunkBuffer chunkBuffer = ChunkBuffer.wrap(byteBuffer.asReadOnlyBuffer())) {
        List<ByteString> res = checksumCache.computeChecksum(chunkBuffer, function);
        System.out.println(res);
        // Verify that every entry in the res list except the last one is the same as the one in lastRes list
        if (i > 0) {
          for (int j = 0; j < res.size() - 1; j++) {
            Assertions.assertEquals(lastRes.get(j), res.get(j));
          }
        }
        lastRes = res;
      }
    }

    // Sanity check
    checksumCache.clear();
  }
}

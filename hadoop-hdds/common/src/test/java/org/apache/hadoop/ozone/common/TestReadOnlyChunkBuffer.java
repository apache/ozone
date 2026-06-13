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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ReadOnlyChunkBuffer}.
 */
class TestReadOnlyChunkBuffer {

  @Test
  void readOnlyBufferMatchesSourceAndRejectsWrites() throws Exception {
    final byte[] source = {1, 2, 3, 4, 5, 6};
    final ChunkBuffer buffer = ChunkBuffer.wrapReadOnly(source, 1, 4);
    assertTrue(buffer.isReadOnly());
    assertEquals(4, buffer.position());
    assertEquals(0, buffer.remaining());

    buffer.rewind();
    final List<byte[]> iterated = new ArrayList<>();
    for (ByteBuffer slice : buffer.iterate(2)) {
      final byte[] copy = new byte[slice.remaining()];
      slice.get(copy);
      iterated.add(copy);
    }
    assertEquals(2, iterated.size());
    assertArrayEquals(new byte[] {2, 3}, iterated.get(0));
    assertArrayEquals(new byte[] {4, 5}, iterated.get(1));

    final Checksum checksum = new Checksum(ChecksumType.CRC32, 2, false);
    final ChecksumData checksumData =
        checksum.computeChecksum(buffer.duplicate(0, buffer.position()), false);
    assertEquals(2, checksumData.getChecksums().size());

    buffer.rewind();
    assertArrayEquals(new byte[] {2, 3, 4, 5},
        buffer.toByteString(
            ByteStringConversion.createByteBufferConversion(true)).toByteArray());

    assertThrows(ReadOnlyBufferException.class, () -> buffer.put((byte) 1));
    assertThrows(ReadOnlyBufferException.class,
        () -> buffer.put(new byte[] {9}, 0, 1));
    assertThrows(ReadOnlyBufferException.class,
        () -> buffer.put(ByteBuffer.wrap(new byte[] {9})));
  }

  @Test
  void duplicateMatchesWriteChunkPathWithSafeWrap() {
    final byte[] data = new byte[262144];
    final ChunkBuffer original = ChunkBuffer.wrapReadOnly(data, 0, data.length);
    final ChunkBuffer chunk = original.duplicate(0, original.position());
    assertEquals(data.length, chunk.remaining());
    assertEquals(data.length,
        chunk.toByteString(
            ByteStringConversion.createByteBufferConversion(false)).size());
  }
}

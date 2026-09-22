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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.junit.jupiter.api.Test;

/** Tests the response ranges selected by {@link BlockReadCursor}. */
class TestBlockReadCursor {
  private static ChunkInfo chunk(long offset, long length, int interval) {
    return ChunkInfo.newBuilder().setChunkName("chunk-" + offset).setOffset(offset).setLen(length)
        .setChecksumData(ContainerProtos.ChecksumData.newBuilder()
            .setType(ContainerProtos.ChecksumType.CRC32).setBytesPerChecksum(interval)).build();
  }

  @Test
  void testShiftedRangesAndLookahead() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4), chunk(12, 7, 3));
    BlockReadCursor cursor = new BlockReadCursor(4, 14, 6, chunks);
    assertEquals(3, cursor.offset());
    assertEquals(4, cursor.nextReadLength());
    assertEquals(4, cursor.nextReadLength());
    assertEquals(chunks.subList(1, 2), cursor.chunksForRead(4));
    cursor.advance(4);
    assertEquals(7, cursor.offset());
    assertEquals(5, cursor.nextReadLength());
    cursor.advance(5);
    assertEquals(12, cursor.offset());
    assertEquals(6, cursor.nextReadLength());
    cursor.advance(6);
    assertFalse(cursor.hasRemaining());
    assertEquals(15, cursor.bytesRead());
  }

  @Test
  void testSpanningChunksAndShortFinalChecksum() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4));
    BlockReadCursor cursor = new BlockReadCursor(0, Long.MAX_VALUE, 8, chunks);
    assertEquals(7, cursor.nextReadLength());
    assertEquals(chunks, cursor.chunksForRead(7));
    cursor.advance(7);
    assertEquals(5, cursor.nextReadLength());
    cursor.advance(5);
    assertFalse(cursor.hasRemaining());
    assertEquals(12, cursor.bytesRead());
  }

  @Test
  void testMinimumBufferAndExactChunkEnd() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4));
    BlockReadCursor cursor = new BlockReadCursor(3, 9, 1, chunks);
    for (int expected : new int[] {4, 4, 1}) {
      assertTrue(cursor.hasRemaining());
      assertEquals(expected, cursor.nextReadLength());
      cursor.advance(expected);
    }
    assertFalse(cursor.hasRemaining());
    assertFalse(new BlockReadCursor(3, 0, 1, chunks).hasRemaining());
  }

  @Test
  void testRangeNearLongLimit() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, Long.MAX_VALUE - 9, 4), chunk(Long.MAX_VALUE - 9, 9, 4));
    BlockReadCursor cursor = new BlockReadCursor(Long.MAX_VALUE - 8, Long.MAX_VALUE, 4, chunks);
    assertEquals(Long.MAX_VALUE - 9, cursor.offset());
    for (int length : new int[] {4, 4, 1}) {
      assertEquals(length, cursor.nextReadLength());
      cursor.advance(length);
    }
    assertEquals(Long.MAX_VALUE, cursor.offset());
    assertFalse(cursor.hasRemaining());
  }

}

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
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

  private static void assertNextRange(BlockReadCursor cursor, long offset, int length, List<ChunkInfo> chunks) {
    assertTrue(cursor.hasNext());
    BlockReadCursor.ReadRange range = cursor.next();
    assertEquals(offset, range.offset());
    assertEquals(length, range.length());
    assertEquals(chunks, range.chunks());
  }

  @Test
  void testShiftedRangesAndLookahead() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4), chunk(12, 7, 3));
    BlockReadCursor cursor = new BlockReadCursor(4, 14, 6, chunks);
    assertTrue(cursor.hasNext());
    assertNextRange(cursor, 3, 4, chunks.subList(1, 2));
    assertNextRange(cursor, 7, 5, chunks.subList(1, 2));
    assertNextRange(cursor, 12, 6, chunks.subList(2, 3));
    assertFalse(cursor.hasNext());
  }

  @Test
  void testSpanningChunksAndShortFinalChecksum() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4));
    BlockReadCursor cursor = new BlockReadCursor(0, Long.MAX_VALUE, 8, chunks);
    assertNextRange(cursor, 0, 7, chunks);
    assertNextRange(cursor, 7, 5, chunks.subList(1, 2));
    assertFalse(cursor.hasNext());
  }

  @Test
  void testIteratesWholeRangeOnce() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4), chunk(12, 7, 3));
    BlockReadCursor cursor = new BlockReadCursor(0, Long.MAX_VALUE, 5, chunks);
    List<Integer> lengths = new ArrayList<>();
    long offset = 0;
    while (cursor.hasNext()) {
      BlockReadCursor.ReadRange range = cursor.next();
      assertEquals(offset, range.offset());
      lengths.add(range.length());
      offset += range.length();
    }
    assertEquals(Arrays.asList(3, 4, 5, 3, 4), lengths);
    assertFalse(cursor.hasNext());
    assertThrows(NoSuchElementException.class, cursor::next);
    assertFalse(cursor.hasNext());
    assertThrows(NoSuchElementException.class, new BlockReadCursor(3, 0, 1, chunks)::next);
  }

  @Test
  void testReadsAtChunkAndChecksumBoundaries() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4), chunk(12, 7, 3));
    for (int[] range : new int[][] {{0, 3, 0}, {3, 7, 1}, {7, 11, 1}, {11, 12, 1},
        {12, 15, 2}, {15, 18, 2}, {18, 19, 2}}) {
      for (int requestedOffset : new int[] {range[0], range[1] - 1}) {
        BlockReadCursor cursor = new BlockReadCursor(requestedOffset, 1, 8, chunks);
        assertNextRange(cursor, range[0], range[1] - range[0], Collections.singletonList(chunks.get(range[2])));
        assertFalse(cursor.hasNext());
      }
    }
  }

  @Test
  void testMinimumBufferAndExactChunkEnd() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, 3, 4), chunk(3, 9, 4));
    BlockReadCursor cursor = new BlockReadCursor(3, 9, 1, chunks);
    for (int expected : new int[] {4, 4, 1}) {
      assertTrue(cursor.hasNext());
      assertEquals(expected, cursor.next().length());
    }
    assertFalse(cursor.hasNext());
    assertFalse(new BlockReadCursor(3, 0, 1, chunks).hasNext());
    BlockReadCursor shortChunk = new BlockReadCursor(0, 3, 1,
        Collections.singletonList(chunk(0, 3, Integer.MAX_VALUE)));
    assertEquals(3, shortChunk.responseDataSize());
    assertEquals(3, shortChunk.next().length());
  }

  @Test
  void testRangeNearLongLimit() throws Exception {
    List<ChunkInfo> chunks = Arrays.asList(chunk(0, Long.MAX_VALUE - 9, 4), chunk(Long.MAX_VALUE - 9, 9, 4));
    BlockReadCursor cursor = new BlockReadCursor(Long.MAX_VALUE - 8, Long.MAX_VALUE, 4, chunks);
    long offset = Long.MAX_VALUE - 9;
    for (int length : new int[] {4, 4, 1}) {
      assertNextRange(cursor, offset, length, chunks.subList(1, 2));
      offset += length;
    }
    assertFalse(cursor.hasNext());
  }

}

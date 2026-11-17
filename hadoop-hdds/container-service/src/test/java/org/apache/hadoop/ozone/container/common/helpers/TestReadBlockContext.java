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

package org.apache.hadoop.ozone.container.common.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.client.BlockID;
import org.junit.jupiter.api.Test;

/** Tests for {@link ReadBlockContext}. */
class TestReadBlockContext {
  @Test
  void blockDataIsCachedPerBlockAndClearedOnClose() {
    final ReadBlockContext context = new ReadBlockContext();

    // the stored BlockData has the block's committed BCSID, which may be higher than the client's
    final BlockID blockID = new BlockID(1, 2);
    final BlockData blockData = new BlockData(new BlockID(1, 2));
    blockData.setBlockCommitSequenceId(7);
    assertNull(context.getCachedBlockData(blockID));

    context.cacheBlockData(blockData);
    assertSame(blockData, context.getCachedBlockData(blockID));
    assertNull(context.getCachedBlockData(new BlockID(1, 3)));
    final BlockID newer = new BlockID(1, 2);
    newer.setBlockCommitSequenceId(8);
    assertNull(context.getCachedBlockData(newer));

    context.close();
    assertNull(context.getCachedBlockData(blockID));
  }

  @Test
  void readBufferIsReusedGrownAndDroppedOnClose() {
    final ReadBlockContext context = new ReadBlockContext();

    final ByteBuffer first = context.getReadBuffer(64);
    assertEquals(64, first.capacity());
    first.position(10).limit(20);

    assertSame(first, context.getReadBuffer(64), "same capacity reuses the buffer");
    assertEquals(0, first.position());
    assertEquals(64, first.limit());
    assertSame(first, context.getReadBuffer(16), "smaller capacity reuses the buffer");

    final ByteBuffer grown = context.getReadBuffer(128);
    assertNotSame(first, grown, "larger capacity reallocates");
    assertEquals(128, grown.capacity());
    assertSame(grown, context.getReadBuffer(64));

    context.close();
    assertNotSame(grown, context.getReadBuffer(64), "buffer dropped on close");
  }
}

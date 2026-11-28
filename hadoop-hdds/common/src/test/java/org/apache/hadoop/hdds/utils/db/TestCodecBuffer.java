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

package org.apache.hadoop.hdds.utils.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ratis.thirdparty.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.UnpooledHeapByteBuf;
import org.junit.jupiter.api.Test;

/**
 * Test {@link CodecBuffer#release()}.
 */
final class TestCodecBuffer {

  /**
   * After its last release, a pooled ByteBuf object can be handed to another thread right away, which sets its
   * refCnt back to 1 (HDDS-17212). This buffer reports exactly that.
   */
  private static final class ReusedAfterReleaseByteBuf extends UnpooledHeapByteBuf {
    private volatile boolean reused;

    ReusedAfterReleaseByteBuf(int capacity) {
      super(UnpooledByteBufAllocator.DEFAULT, capacity, capacity);
    }

    @Override
    public boolean release() {
      final boolean deallocated = super.release();
      reused = deallocated;
      return deallocated;
    }

    @Override
    public int refCnt() {
      return reused ? 1 : super.refCnt();
    }
  }

  @Test
  void testReleaseWhenThePoolReusesTheBuffer() {
    final ReusedAfterReleaseByteBuf buf = new ReusedAfterReleaseByteBuf(16);
    final CodecBuffer buffer = CodecBuffer.allocate(16, capacity -> buf);

    buffer.release();
    assertTrue(buf.reused);
  }

  @Test
  void testDoubleReleaseFails() {
    final CodecBuffer buffer = CodecBuffer.allocateDirect(16);
    buffer.release();
    assertThrows(IllegalStateException.class, buffer::release);
  }

  @Test
  void testReleaseWhileAnotherOwnerHoldsTheBufferFails() {
    final UnpooledHeapByteBuf buf = new UnpooledHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, 16, 16);
    final CodecBuffer buffer = CodecBuffer.allocate(16, capacity -> buf);
    buf.retain();

    assertThrows(IllegalStateException.class, buffer::release);
    assertTrue(buf.release());
  }

  /** The ByteBuf of a released buffer still reports refCnt 1 once the pool hands it to another owner. */
  @Test
  void testAccessAfterReleaseFails() {
    final ReusedAfterReleaseByteBuf buf = new ReusedAfterReleaseByteBuf(16);
    final CodecBuffer buffer = CodecBuffer.allocate(16, capacity -> buf);
    buffer.release();

    assertThrows(IllegalStateException.class, () -> buffer.putInt(1));
    assertThrows(IllegalStateException.class, buffer::asReadOnlyByteBuffer);
    assertThrows(IllegalStateException.class, buffer::asWritableByteBuffer);
    assertThat(buffer.toString()).contains("released");
  }

  @Test
  void testZeroCapacityBufferIsAccessibleAfterRelease() {
    final CodecBuffer buffer = CodecBuffer.allocateDirect(0);
    buffer.release();
    buffer.release();
    assertEquals(0, buffer.asReadOnlyByteBuffer().remaining());
  }
}

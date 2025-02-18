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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.ozone.container.keyvalue.impl.KeyValueStreamDataChannel.WriteMethod;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.ReferenceCountedObject;

/**
 * Keep the last {@link org.apache.hadoop.ozone.container.keyvalue.impl.Buffers#max} bytes in the buffer
 * in order to create putBlockRequest
 * at {@link #closeBuffers(org.apache.hadoop.ozone.container.keyvalue.impl.Buffers, WriteMethod)}}.
 */
class Buffers {
  private final Deque<ReferenceCountedObject<ByteBuffer>> deque = new LinkedList<>();
  private final int max;
  private int length;

  Buffers(int max) {
    this.max = max;
  }

  private boolean isExtra(int n) {
    return length - n >= max;
  }

  private boolean hasExtraBuffer() {
    return Optional
            .ofNullable(deque.peek())
            .map(ReferenceCountedObject::get)
            .filter(b -> isExtra(b.remaining())).isPresent();
  }

  /**
   * @return extra buffers which are safe to be written.
   */
  Iterable<ReferenceCountedObject<ByteBuffer>> offer(ReferenceCountedObject<ByteBuffer> ref) {
    final ByteBuffer buffer = ref.retain();
    KeyValueStreamDataChannel.LOG.debug("offer {}", buffer);
    final boolean offered = deque.offer(ref);
    Preconditions.checkState(offered, "Failed to offer");
    length += buffer.remaining();

    return () -> new Iterator<ReferenceCountedObject<ByteBuffer>>() {
      @Override
      public boolean hasNext() {
        return hasExtraBuffer();
      }

      @Override
      public ReferenceCountedObject<ByteBuffer> next() {
        final ReferenceCountedObject<ByteBuffer> polled = poll();
        length -= polled.get().remaining();
        Preconditions.checkState(length >= max);
        return polled;
      }
    };
  }

  ReferenceCountedObject<ByteBuffer> poll() {
    final ReferenceCountedObject<ByteBuffer> polled = Objects.requireNonNull(deque.poll());
    RatisHelper.debug(polled.get(), "polled", KeyValueStreamDataChannel.LOG);
    return polled;
  }

  ReferenceCountedObject<ByteBuf> pollAll() {
    Preconditions.checkState(!deque.isEmpty(), "The deque is empty");
    final ByteBuffer[] array = new ByteBuffer[deque.size()];
    final List<ReferenceCountedObject<ByteBuffer>> refs = new ArrayList<>(deque.size());
    for (int i = 0; i < array.length; i++) {
      final ReferenceCountedObject<ByteBuffer> ref = poll();
      refs.add(ref);
      array[i] = ref.get();
    }
    final ByteBuf buf = Unpooled.wrappedBuffer(array).asReadOnly();
    return ReferenceCountedObject.wrap(buf, () -> { }, () -> {
      buf.release();
      refs.forEach(ReferenceCountedObject::release);
    });
  }

  void cleanUpAll() {
    while (!deque.isEmpty()) {
      poll().release();
    }
  }
}

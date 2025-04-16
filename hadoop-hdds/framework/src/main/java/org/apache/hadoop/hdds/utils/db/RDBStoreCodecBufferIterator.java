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

import java.io.IOException;
import java.util.Deque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.commons.lang3.exception.UncheckedInterruptedException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;

/**
 * Implement {@link RDBStoreAbstractIterator} using {@link CodecBuffer}.
 * Any Key or Value returned will be only valid within the lifecycle of this iterator.
 */
class RDBStoreCodecBufferIterator
    extends RDBStoreAbstractIterator<CodecBuffer> {
  static class Buffer {
    private final CodecBuffer.Capacity initialCapacity;
    private final PutToByteBuffer<RuntimeException> source;
    private CodecBuffer buffer;

    Buffer(CodecBuffer.Capacity initialCapacity,
        PutToByteBuffer<RuntimeException> source) {
      this.initialCapacity = initialCapacity;
      this.source = source;
    }

    void release() {
      if (buffer != null) {
        buffer.release();
      }
    }

    private void prepare() {
      if (buffer == null) {
        allocate();
      } else {
        buffer.clear();
      }
    }

    private void allocate() {
      if (buffer != null) {
        buffer.release();
      }
      buffer = CodecBuffer.allocateDirect(-initialCapacity.get());
    }

    CodecBuffer getFromDb() {
      for (prepare(); ; allocate()) {
        final Integer required = buffer.putFromSource(source);
        if (required == null) {
          return null; // the source is unavailable
        } else if (required == buffer.readableBytes()) {
          return buffer; // buffer size is big enough
        }
        // buffer size too small, try increasing the capacity.
        if (buffer.setCapacity(required)) {
          buffer.clear();
          // retry with the new capacity
          final int retried = buffer.putFromSource(source);
          Preconditions.assertSame(required.intValue(), retried, "required");
          return buffer;
        }

        // failed to increase the capacity
        // increase initial capacity and reallocate it
        initialCapacity.increase(required);
      }
    }
  }

  private final BlockingDeque<RawKeyValue<Buffer>> availableBufferStack;
  private final Set<RawKeyValue<Buffer>> inUseBuffers;
  private final AtomicReference<Boolean> closed = new AtomicReference<>(false);

  RDBStoreCodecBufferIterator(ManagedRocksIterator iterator, RDBTable table,
      CodecBuffer prefix, int maxNumberOfBuffersInMemory) {
    super(iterator, table, prefix);
    // We need atleast 2 buffers one for setting next value and one for sending the current value.
    maxNumberOfBuffersInMemory = Math.max(2, maxNumberOfBuffersInMemory);
    final String name = table != null ? table.getName() : null;
    this.availableBufferStack = new LinkedBlockingDeque<>(maxNumberOfBuffersInMemory);
    this.inUseBuffers = new HashSet<>();
    for (int i = 0; i < maxNumberOfBuffersInMemory; i++) {
      Buffer keyBuffer = new Buffer(
          new CodecBuffer.Capacity(name + "-iterator-key-" + i, 1 << 10),
          buffer -> getRocksDBIterator().get().key(buffer));
      Buffer valueBuffer = new Buffer(
          new CodecBuffer.Capacity(name + "-iterator-value-" + i, 4 << 10),
          buffer -> getRocksDBIterator().get().value(buffer));
      availableBufferStack.add(new RawKeyValue<>(keyBuffer, valueBuffer));
    }

    seekToFirst();
  }

  void assertOpen() {
    Preconditions.assertTrue(!closed.get(), "Already closed");
  }

  private <V> V getFromDeque(BlockingDeque<V> deque, Set<V> inUseSet) {
    V popped;
    do {
      try {
        popped = deque.poll(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new UncheckedInterruptedException(e);
      }
    } while(popped == null);
    assertOpen();
    inUseSet.add(popped);
    return popped;
  }

  private ReferenceCountedObject<RawKeyValue<CodecBuffer>> getReferenceCountedBuffer(
      RawKeyValue<Buffer> key, Deque<RawKeyValue<Buffer>> stack, Set<RawKeyValue<Buffer>> inUseSet,
      Function<RawKeyValue<Buffer>, RawKeyValue<CodecBuffer>> transformer) {
    RawKeyValue<CodecBuffer> value = transformer.apply(key);
    return ReferenceCountedObject.wrap(value, () -> {
    }, completelyReleased -> {
      if (!completelyReleased) {
        return;
      }
      closed.updateAndGet((prev) -> {
        // If already closed the data structure should not be manipulated since the buffer would have already been
        // closed.
        if (!prev) {
          //Entire block done inside this code block to avoid race condition with close() method.
          //Remove from the set before adding it back to the stack. Otherwise there could be a race condition with
          // #getFromDeque function.
          inUseSet.remove(key);
          stack.push(key);
        }
        return prev;
      });
    });
  }

  @Override
  ReferenceCountedObject<RawKeyValue<CodecBuffer>> getKeyValue() {
    assertOpen();
    RawKeyValue<Buffer> kvBuffer = getFromDeque(availableBufferStack, inUseBuffers);
    Function<RawKeyValue<Buffer>, RawKeyValue<CodecBuffer>> transformer =
        kv -> new RawKeyValue<>(kv.getKey().getFromDb(), kv.getValue().getFromDb());
    return getReferenceCountedBuffer(kvBuffer, availableBufferStack, inUseBuffers, transformer);
  }

  @Override
  void seek0(CodecBuffer key) {
    assertOpen();
    getRocksDBIterator().get().seek(key.asReadOnlyByteBuffer());
  }

  @Override
  void delete(CodecBuffer key) throws IOException {
    assertOpen();
    getRocksDBTable().delete(key.asReadOnlyByteBuffer());
  }

  @Override
  boolean startsWithPrefix(CodecBuffer key) {
    assertOpen();
    final CodecBuffer prefix = getPrefix();
    if (prefix == null) {
      return true;
    }
    if (key == null) {
      return false;
    }
    return key.startsWith(prefix);
  }

  private <V> void release(Deque<V> valueStack, Set<V> inUseSet, Function<V, Void> releaser) {
    while (!valueStack.isEmpty()) {
      V popped = valueStack.pop();
      releaser.apply(popped);
    }

    for (V inUseValue : inUseSet) {
      releaser.apply(inUseValue);
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      super.close();
      Optional.ofNullable(getPrefix()).ifPresent(CodecBuffer::release);
      release(availableBufferStack, inUseBuffers, kv -> {
        kv.getKey().release();
        kv.getValue().release();
        return null;
      });
    }
  }
}

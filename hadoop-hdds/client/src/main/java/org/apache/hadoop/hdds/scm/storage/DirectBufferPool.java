/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.classification.VisibleForTesting;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple class for pooling direct ByteBuffers. This is necessary
 * because Direct Byte Buffers do not take up much space on the heap,
 * and hence will not trigger GCs on their own. However, they do take
 * native memory, and thus can cause high memory usage if not pooled.
 * The pooled instances are referred to only via weak references, allowing
 * them to be collected when a GC does run.
 *
 * This class only does effective pooling when many buffers will be
 * allocated at the same size. There is no attempt to reuse larger
 * buffers to satisfy smaller allocations.
 */
public class DirectBufferPool {

  private final ConcurrentMap<Integer, Queue<WeakReference<ByteBuffer>>> buffersBySize =
      new ConcurrentHashMap<Integer, Queue<WeakReference<ByteBuffer>>>();
 
  /**
   * Check if in pool buffer is available for the specified size,
   * If it is available return that else allocate a new buffer.
   *
   * @param size size.
   * @return ByteBuffer.
   */
  public ByteBuffer allocateBuffer(int size) {
    Queue<WeakReference<ByteBuffer>> list = buffersBySize.get(size);
    if (list == null) {
      // Buffer is not available for the specified size
      // allocate new buffer in this case
      return ByteBuffer.allocateDirect(size);
    }
    
    WeakReference<ByteBuffer> ref;
    while ((ref = list.poll()) != null) {
      ByteBuffer b = ref.get();
      if (b != null) {
        return b;
      }
    }

    return ByteBuffer.allocateDirect(size);
  }
  
  /**
   * Return a buffer into the pool. After being returned,
   * the buffer may be recycled, so the user must not
   * continue to use it in any way.
   * @param buf the buffer to return
   */
  public void releaseBuffer(ByteBuffer buf) {
    buf.clear();
    int size = buf.capacity();
    Queue<WeakReference<ByteBuffer>> list = buffersBySize.get(size);
    if (list == null) {
      list = new ConcurrentLinkedQueue<WeakReference<ByteBuffer>>();
      Queue<WeakReference<ByteBuffer>> prev = buffersBySize.putIfAbsent(size, list);
      if (prev != null) {
        list = prev;
      }
    }
    list.add(new WeakReference<ByteBuffer>(buf));
  }

  /**
   * Return the number of available buffers of a given size.
   */
  @VisibleForTesting
  int countBuffersOfSize(int size) {
    Queue<WeakReference<ByteBuffer>> list = buffersBySize.get(size);
    if (list == null) {
      return 0;
    }
    return list.size();
  }
}

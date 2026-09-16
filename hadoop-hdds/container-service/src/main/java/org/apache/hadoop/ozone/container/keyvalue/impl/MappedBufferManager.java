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

import com.google.common.util.concurrent.Striped;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Manager who manages the mapped buffers to under a predefined total count, also support reuse mapped buffers.
 */
public class MappedBufferManager {

  private static ConcurrentHashMap<String, WeakReference<ByteBuffer>> mappedBuffers =
      new ConcurrentHashMap<String, WeakReference<ByteBuffer>>();
  private static final Logger LOG = LoggerFactory.getLogger(MappedBufferManager.class);
  private final Semaphore semaphore;
  private final AtomicBoolean cleanupInProgress = new AtomicBoolean(false);
  private final Striped<Lock> lock;

  public MappedBufferManager(int capacity) {
    this.semaphore = new Semaphore(capacity);
    this.lock = Striped.lazyWeakLock(1024);
  }

  public boolean getQuota(int permits) {
    boolean ret = semaphore.tryAcquire(permits);
    if (ret) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("quota is decreased by {} to total {}", permits, semaphore.availablePermits());
      }
    } else {
      if (cleanupInProgress.compareAndSet(false, true)) {
        CompletableFuture.runAsync(() -> {
          int p = 0;
          try {
            // remove(key, value) only counts entries we observed cleared,
            // so a concurrent put() that replaced the WeakReference is not
            // miscounted as freed.
            for (Map.Entry<String, WeakReference<ByteBuffer>> entry
                : mappedBuffers.entrySet()) {
              final WeakReference<ByteBuffer> ref = entry.getValue();
              if (ref.get() == null
                  && mappedBuffers.remove(entry.getKey(), ref)) {
                p++;
              }
            }
            if (p > 0) {
              releaseQuota(p);
            }
          } finally {
            cleanupInProgress.set(false);
          }
        });
      }
    }
    return ret;
  }

  public void releaseQuota(int permits) {
    semaphore.release(permits);
    if (LOG.isDebugEnabled()) {
      LOG.debug("quota is increased by {} to total {}", permits, semaphore.availablePermits());
    }
  }

  public int availableQuota() {
    return semaphore.availablePermits();
  }

  public ByteBuffer computeIfAbsent(String file, long position, long size,
      Supplier<ByteBuffer> supplier) {
    String key = file + "-" + position + "-" + size;
    Lock fileLock = lock.get(key);
    fileLock.lock();
    try {
      // Hold a strong reference for the rest of this method so GC cannot
      // clear the WeakReference between the null check and the return.
      final WeakReference<ByteBuffer> refer = mappedBuffers.get(key);
      final ByteBuffer cached = refer != null ? refer.get() : null;
      if (cached != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("find buffer for key {}", key);
        }
        releaseQuota(1);
        return cached;
      }

      ByteBuffer buffer = supplier.get();
      if (buffer != null) {
        mappedBuffers.put(key, new WeakReference<>(buffer));
        if (LOG.isDebugEnabled()) {
          LOG.debug("add buffer for key {}", key);
        }
      }
      return buffer;
    } finally {
      fileLock.unlock();
    }
  }
}

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
            for (String key : mappedBuffers.keySet()) {
              ByteBuffer buf = mappedBuffers.get(key).get();
              if (buf == null) {
                mappedBuffers.remove(key);
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
      WeakReference<ByteBuffer> refer = mappedBuffers.get(key);
      if (refer != null && refer.get() != null) {
        // reuse the mapped buffer
        if (LOG.isDebugEnabled()) {
          LOG.debug("find buffer for key {}", key);
        }
        releaseQuota(1);
        return refer.get();
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

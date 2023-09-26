/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import org.xerial.snappy.pool.BufferPool;
import org.xerial.snappy.pool.CachingBufferPool;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * {@link ChecksumByteBuffer} implementation
 * that delegates checksum calculation to {@link CRC32}.
 *
 */
public class CRC32ByteBufferImpl implements ChecksumByteBuffer {

  public static final BufferPool POOL = CachingBufferPool.getInstance();
  private final CRC32 checksum;

  public CRC32ByteBufferImpl(CRC32 checksum) {
    this.checksum = checksum;
  }

  @Override
  public void update(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset(),
          buffer.remaining());
    } else {
      // The buffer that is passed into this method is readOnly,
      // thus buffer.hasArray() (see hasArray implementation.
      // So we either need to get data byte-by-byte, which is slow,
      // or copy it to an intermediate byte array or buffer.
      // Here we use cached direct byte buffers that we take from the cache
      // or allocate a new one (if no enough instances in the cache) and
      // return back to the cache after the checksum is calculated.
      withCachedBuffer(buffer.capacity(), cachedBuffer -> {
        cachedBuffer.put(buffer);
        cachedBuffer.flip();
        checksum.update(cachedBuffer);
      });
    }
  }

  @Override
  public void update(byte[] b, int off, int len) {
    checksum.update(b, off, len);
  }

  @Override
  public void update(int i) {
    checksum.update(i);
  }

  @Override
  public long getValue() {
    return checksum.getValue();
  }

  @Override
  public void reset() {
    checksum.reset();
  }

  private void withCachedBuffer(int size, Consumer<ByteBuffer> action) {
    ByteBuffer cachedBuffer = POOL.allocateDirect(size);
    action.accept(cachedBuffer);
    POOL.releaseDirect(cachedBuffer);
  }

}

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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.utils.io.RandomAccessFileChannel;

/**
 * State kept across the ReadBlock requests of one gRPC stream, which serves one block: the block file, the block's
 * {@link BlockData} and the read buffer. Unlike a per-request DispatcherContext, it lives until {@link #close()}.
 */
public class ReadBlockContext implements Closeable {
  private final RandomAccessFileChannel blockFile = new RandomAccessFileChannel();
  private final AtomicReference<BlockData> cachedBlockData = new AtomicReference<>();
  private ByteBuffer readBuffer;

  public RandomAccessFileChannel getBlockFile() {
    return blockFile;
  }

  /**
   * @return the {@link BlockData} cached for the given block, or null if none is cached for it or the cached one has
   * a lower BCSID than requested.
   */
  public BlockData getCachedBlockData(BlockID blockID) {
    final BlockData cached = cachedBlockData.get();
    return cached != null && cached.getBlockID().getContainerBlockID().equals(blockID.getContainerBlockID())
        && cached.getBlockCommitSequenceId() >= blockID.getBlockCommitSequenceId() ? cached : null;
  }

  /** Cache the {@link BlockData} of the block this context serves until {@link #close()}. */
  public void cacheBlockData(BlockData blockData) {
    cachedBlockData.set(blockData);
  }

  /**
   * @return a cleared read buffer of at least the given capacity, reallocated only when it has to grow.
   * Not thread-safe: the buffer is shared across requests, so the caller must serialize them (gRPC serializes a
   * stream's requests).
   */
  public ByteBuffer getReadBuffer(int capacity) {
    if (readBuffer == null || readBuffer.capacity() < capacity) {
      readBuffer = ByteBuffer.allocate(capacity);
    } else {
      readBuffer.clear();
    }
    return readBuffer;
  }

  /** Close the block file and drop the cached {@link BlockData} and the read buffer. */
  @Override
  public void close() {
    cachedBlockData.set(null);
    readBuffer = null;
    blockFile.close();
  }
}

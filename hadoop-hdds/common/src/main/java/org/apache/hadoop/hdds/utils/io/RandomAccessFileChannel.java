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

package org.apache.hadoop.hdds.utils.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RandomAccessFile} and its {@link FileChannel}. A ReadBlock stream serves one block through one instance,
 * which therefore also caches the block's {@link BlockData} and the read buffer across the stream's requests.
 */
public class RandomAccessFileChannel implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RandomAccessFileChannel.class);

  private File blockFile;
  private RandomAccessFile raf;
  private FileChannel channel;
  private BlockID cachedBlockID;
  private BlockData cachedBlockData;
  private ByteBuffer readBuffer;

  public RandomAccessFileChannel() {
  }

  /** Is this file open? */
  public synchronized boolean isOpen() {
    return blockFile != null;
  }

  /** Open the given file in read-only mode. */
  public synchronized void open(File file) throws FileNotFoundException {
    Preconditions.assertNull(blockFile, "blockFile");
    final File f = Objects.requireNonNull(file, "blockFile == null");
    final RandomAccessFile newRaf = new RandomAccessFile(f, "r");
    final FileChannel newChannel = newRaf.getChannel();
    blockFile = f;
    raf = newRaf;
    channel = newChannel;
  }

  /** @return the {@link BlockData} cached for the given block, or null. */
  public synchronized BlockData getCachedBlockData(BlockID blockID) {
    return blockID.equals(cachedBlockID) ? cachedBlockData : null;
  }

  /** Cache the {@link BlockData} of the block this channel serves until {@link #close()}. */
  public synchronized void cacheBlockData(BlockID blockID, BlockData blockData) {
    cachedBlockID = blockID;
    cachedBlockData = blockData;
  }

  /** @return a cleared read buffer of at least the given capacity, reallocated only when it has to grow. */
  public synchronized ByteBuffer getReadBuffer(int capacity) {
    if (readBuffer == null || readBuffer.capacity() < capacity) {
      readBuffer = ByteBuffer.allocate(capacity);
    } else {
      readBuffer.clear();
    }
    return readBuffer;
  }

  /** Similar to {@link FileChannel#position(long)}. */
  public synchronized void position(long newPosition) throws IOException {
    Preconditions.assertTrue(isOpen(), "Not opened");
    final long oldPosition = channel.position();
    if (newPosition != oldPosition) {
      LOG.debug("seek {} -> {} for file {}", oldPosition, newPosition, blockFile);
      channel.position(newPosition);
    }
  }

  /**
   * Similar to {@link FileChannel#read(ByteBuffer)} except that
   * this method tries to fill up the buffer until either
   * (1) the buffer is full, or (2) it has reached end-of-stream.
   *
   * @return true if the caller should continue to read;
   *         otherwise, it has reached end-of-stream, return false;
   */
  public synchronized boolean read(ByteBuffer buffer) throws IOException {
    Preconditions.assertTrue(isOpen(), "Not opened");
    while (buffer.hasRemaining()) {
      final int r = channel.read(buffer);
      if (r == -1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Close the underlying channel and file.
   * In case of exception, this method catches the exception, logs a warning message,
   * and then continue closing the remaining resources.
   */
  @Override
  public synchronized void close() {
    final File fileToClose = blockFile;
    if (fileToClose == null) {
      return;
    }
    blockFile = null;
    cachedBlockID = null;
    cachedBlockData = null;
    readBuffer = null;

    try {
      if (channel != null) {
        channel.close();
      }
    } catch (IOException e) {
      LOG.warn("Failed to close channel for {}", fileToClose, e);
    } finally {
      channel = null;
    }
    try {
      if (raf != null) {
        raf.close();
      }
    } catch (IOException e) {
      LOG.warn("Failed to close RandomAccessFile for {}", fileToClose, e);
    } finally {
      raf = null;
    }
  }
}

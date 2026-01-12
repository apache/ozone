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
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link RandomAccessFile} and its {@link FileChannel}. */
public class RandomAccessFileChannel implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RandomAccessFileChannel.class);

  private File blockFile;
  private RandomAccessFile raf;
  private FileChannel channel;

  public RandomAccessFileChannel() {
  }

  /** Is this file open? */
  public synchronized boolean isOpen() {
    return blockFile != null;
  }

  /** Open the given file in read-only mode. */
  public synchronized void open(File file) throws FileNotFoundException {
    Preconditions.assertNull(blockFile, "blockFile");
    blockFile = Objects.requireNonNull(file, "blockFile == null");
    raf = new RandomAccessFile(blockFile, "r");
    channel = raf.getChannel();
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
   * @return ture if the caller should continue to read;
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
  public synchronized void close() {
    if (blockFile == null) {
      return;
    }
    blockFile = null;
    try {
      channel.close();
      channel = null;
    } catch (IOException e) {
      LOG.warn("Failed to close channel for {}", blockFile, e);
    }
    try {
      raf.close();
      raf = null;
    } catch (IOException e) {
      LOG.warn("Failed to close RandomAccessFile for {}", blockFile, e);
    }
  }
}

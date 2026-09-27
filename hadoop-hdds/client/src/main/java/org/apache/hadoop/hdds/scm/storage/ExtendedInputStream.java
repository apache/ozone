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

package org.apache.hadoop.hdds.scm.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.StringUtils;

/**
 * Abstact class which extends InputStream and some common interfaces used by
 * various Ozone InputStream classes.
 */
public abstract class ExtendedInputStream extends FSInputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable, ByteBufferPositionedReadable, StreamCapabilities {

  protected static final int EOF = -1;

  @Override
  public int read(long position, ByteBuffer buffer) throws IOException {
    if (position < 0) {
      throw new EOFException("position is negative: " + position);
    }
    if (buffer.isReadOnly()) {
      throw new ReadOnlyBufferException();
    }
    return buffer.hasRemaining() ? readPositioned(position, buffer) : 0;
  }

  /**
   * Fallback for streams without independent range reads. Uses the same monitor as sequential operations.
   */
  protected synchronized int readPositioned(long position, ByteBuffer buffer) throws IOException {
    final long oldPosition = getPos();
    try {
      seek(position);
      return read(buffer);
    } catch (EOFException e) {
      return EOF;
    } finally {
      seek(oldPosition);
    }
  }

  @Override
  public void readFully(long position, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      int n = read(position, buffer);
      if (n < 0) {
        throw new EOFException("End of stream at position " + position);
      }
      if (n == 0) {
        throw new IOException("No progress reading at position " + position);
      }
      position += n;
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    validatePositionedReadArgs(position, buffer, offset, length);
    return read(position, ByteBuffer.wrap(buffer, offset, length));
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] buf = new byte[1];
    if (read(buf, 0, 1) == EOF) {
      return EOF;
    }
    return Byte.toUnsignedInt(buf[0]);
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    return read(new ByteArrayReader(b, off, len));
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    return read(new ByteBufferReader(byteBuffer));
  }

  public synchronized int read(ByteReaderStrategy strategy) throws IOException {
    if (strategy.getTargetLength() == 0) {
      return 0;
    }
    return readWithStrategy(strategy);
  }

  /**
   * This must be overridden by the extending classes to call read on the
   * underlying stream they are reading from. The last stream in the chain (the
   * one which provides the actual data) needs to provide a real read via the
   * read methods. For example if a test is extending this class, then it will
   * need to override both read methods above and provide a dummy
   * readWithStrategy implementation, as it will never be called by the tests.
   *
   * @param strategy
   * @throws IOException
   */
  protected abstract int readWithStrategy(ByteReaderStrategy strategy) throws
      IOException;

  @Override
  public synchronized void seek(long l) throws IOException {
    throw new NotImplementedException("Seek is not implemented");
  }

  @Override
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (StringUtils.toLowerCase(capability)) {
    case StreamCapabilities.PREADBYTEBUFFER:
    case StreamCapabilities.READBYTEBUFFER:
    case StreamCapabilities.UNBUFFER:
      return true;
    default:
      return false;
    }
  }
}

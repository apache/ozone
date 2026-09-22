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

package org.apache.hadoop.fs.ozone;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.scm.storage.ExtendedInputStream;
import org.apache.hadoop.hdds.tracing.TracingUtil;

/**
 * The input stream for Ozone file system.
 * <p>
 * Sequential reads are NOT thread safe.
 * <p>
 * Positioned reads are thread safe.
 * When the underlying stream is an {@link ExtendedInputStream} and
 * when it supports {@link ExtendedInputStream#readFully(long, ByteBuffer)},
 * they delegate to it.
 * Otherwise, they fall back to the default synchronized seek-read-restore implementation.
 * <p>
 * Applications must use either sequential reads or position reads at any given time,
 * but not concurrent sequential/position reads
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneFSInputStream extends FSInputStream
    implements ByteBufferReadable, CanUnbuffer, ByteBufferPositionedReadable {

  private final InputStream inputStream;
  private final Statistics statistics;
  private final Object positionedReadLock = new Object();

  public OzoneFSInputStream(InputStream inputStream, Statistics statistics) {
    this.inputStream = inputStream;
    this.statistics = statistics;
  }

  @Override
  public int read() throws IOException {
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("OzoneFSInputStream.read")) {
      int byteRead = inputStream.read();
      if (statistics != null && byteRead >= 0) {
        statistics.incrementBytesRead(1);
      }
      return byteRead;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("OzoneFSInputStream.read")) {
      TracingUtil.getActiveSpan().setAttribute("offset", off)
          .setAttribute("length", len);
      int bytesRead = inputStream.read(b, off, len);
      if (statistics != null && bytesRead >= 0) {
        statistics.incrementBytesRead(bytesRead);
      }
      return bytesRead;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    TracingUtil.executeInNewSpan("OzoneFSInputStream.close",
        inputStream::close);
  }

  @Override
  public void seek(long pos) throws IOException {
    ((Seekable) inputStream).seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return ((Seekable) inputStream).getPos();
  }

  @Override
  public long skip(long n) throws IOException {
    return inputStream.skip(n);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  /**
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @return the number of bytes read, possibly zero, or -1 if
   *         reach end-of-stream
   * @throws IOException if there is some error performing the read
   */
  @Override
  public int read(ByteBuffer buf) throws IOException {
    return TracingUtil.executeInNewSpan("OzoneFSInputStream.read(ByteBuffer)",
        () -> readInTrace(buf));
  }

  private int readInTrace(ByteBuffer buf) throws IOException {
    if (buf.isReadOnly()) {
      throw new ReadOnlyBufferException();
    }

    int bytesRead;
    if (inputStream instanceof ByteBufferReadable) {
      bytesRead = ((ByteBufferReadable)inputStream).read(buf);
    } else {
      int readLen = Math.min(buf.remaining(), available());
      if (buf.hasArray()) {
        int pos = buf.position();
        bytesRead = read(buf.array(), pos, readLen);
        if (bytesRead > 0) {
          buf.position(pos + bytesRead);
        }
      } else {
        byte[] readData = new byte[readLen];
        bytesRead = read(readData, 0, readLen);
        if (bytesRead > 0) {
          buf.put(readData);
        }
      }
    }

    if (statistics != null && bytesRead >= 0) {
      statistics.incrementBytesRead(bytesRead);
    }
    return bytesRead;
  }

  @Override
  public void unbuffer() {
    if (inputStream instanceof CanUnbuffer) {
      ((CanUnbuffer) inputStream).unbuffer();
    }
  }

  /**
   * @param buffer to receive the results of the read operation.
   * @param position offset
   * @return the number of bytes read, possibly zero, or -1 if
   *         reach end-of-stream
   * @throws IOException if there is some error performing the read
   */
  @Override
  public int read(long position, ByteBuffer buffer) throws IOException {
    if (!buffer.hasRemaining()) {
      return 0;
    }
    if (position < 0) {
      throw new EOFException("position is negative: " + position);
    }
    return readImpl(position, buffer);
  }

  @Override
  public int read(long position, byte[] array, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    validatePositionedReadArgs(position, array, offset, length);
    return readImpl(position, ByteBuffer.wrap(array, offset, length));
  }

  private int readImpl(long position, ByteBuffer buffer) throws IOException {
    final Integer n = bestEffortExtendedInputStreamRead(position, buffer, false);
    if (n != null) {
      return n;
    }
    // Fallback: stateful seek-read-restore on the shared cursor.
    return bestEffortReadFullySynchronized(position, buffer);
  }

  /**
   * @param buffer to receive the results of the read operation.
   * @param position offset
   * @throws IOException if there is some error performing the read
   * @throws EOFException if end of file reached before reading fully
   */
  @Override
  public void readFully(long position, ByteBuffer buffer) throws IOException {
    if (!buffer.hasRemaining()) {
      return;
    }
    if (position < 0) {
      throw new EOFException("position is negative: " + position);
    }
    readFullyImpl(position, buffer);
  }

  @Override
  public void readFully(long position, byte[] array, int offset, int length) throws IOException {
    if (length == 0) {
      return;
    }
    validatePositionedReadArgs(position, array, offset, length);
    readFullyImpl(position, ByteBuffer.wrap(array, offset, length));
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  private void readFullyImpl(long position, ByteBuffer buffer) throws IOException {
    final int length = buffer.remaining();
    final Integer n = bestEffortExtendedInputStreamRead(position, buffer, true);
    if (n != null) {
      if (n < length) {
        throw new EOFException("Failed to read fully length " + length + " at position " + position);
      }
      return;
    }
    final int m = bestEffortReadFullySynchronized(position, buffer);
    if (m < length) {
      throw new EOFException("Failed to read fully length " + length + " at position " + position);
    }
  }

  /**
   * Best effort read via {@link ExtendedInputStream#readFully(long, ByteBuffer)}.
   *
   * @return the number of bytes read; or null when the native stateless path is unsupported.
   */
  private Integer bestEffortExtendedInputStreamRead(long position, ByteBuffer buffer,
      boolean isReadFully) throws IOException {
    if (!(inputStream instanceof ExtendedInputStream)) {
      return null; // not an ExtendedInputStream
    }
    final int remainingBeforeRead = buffer.remaining();
    try {
      if (!((ExtendedInputStream) inputStream).readFully(position, buffer)) {
        return null; // ExtendedInputStream does not support readFully
      }
    } catch (EOFException e) {
      if (isReadFully) {
        throw e;
      }
      // read() semantics: EOF -> -1 (fall through to bytesRead == 0 check below)
    }
    final int bytesRead = remainingBeforeRead - buffer.remaining();
    if (bytesRead == 0) {
      return -1;
    }
    if (statistics != null) {
      statistics.incrementBytesRead(bytesRead);
    }
    return bytesRead;
  }

  /**
   * Fallback positioned read via synchronized seek-read-restore.
   * Uses {@link #read(ByteBuffer)} -- which handles streams that do not implement
   * {@link org.apache.hadoop.fs.ByteBufferReadable} (e.g. GDPR
   * {@code javax.crypto.CipherInputStream}) -- so this works for any {@link Seekable}
   * inner stream.
   *
   * @return the number of bytes read
   */
  private int bestEffortReadFullySynchronized(long position, ByteBuffer buffer) throws IOException {
    synchronized (positionedReadLock) {
      final long oldPos = getPos();
      try {
        ((Seekable) inputStream).seek(position);
        final int n = bestEffortRead(buffer);
        if (statistics != null) {
          statistics.incrementBytesRead(n);
        }
        return n;
      } finally {
        ((Seekable) inputStream).seek(oldPos);
      }
    }
  }

  /**
   * Best effort read to fill up the buffer using {@link #read(ByteBuffer)}.
   *
   * @return the number of bytes read
   */
  private int bestEffortRead(ByteBuffer buffer) throws IOException {
    int readLength = 0;
    try {
      while (buffer.hasRemaining()) {
        final int n = read(buffer);
        if (n < 0) {
          return readLength;
        }
        readLength += n;
      }
      return readLength;
    } catch (EOFException e) {
      return readLength;
    }
  }
}

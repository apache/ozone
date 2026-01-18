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
 *
 * TODO: Make inputStream generic for both rest and rpc clients
 * This class is not thread safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneFSInputStream extends FSInputStream
    implements ByteBufferReadable, CanUnbuffer, ByteBufferPositionedReadable {

  private final InputStream inputStream;
  private final Statistics statistics;

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
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @param position offset
   * @return the number of bytes read, possibly zero, or -1 if
   *         reach end-of-stream
   * @throws IOException if there is some error performing the read
   */
  @Override
  public int read(long position, ByteBuffer buf) throws IOException {
    if (!buf.hasRemaining()) {
      return 0;
    }
    if (inputStream instanceof ExtendedInputStream) {
      final int remainingBeforeRead = buf.remaining();
      if (((ExtendedInputStream) inputStream).readFully(position, buf)) {
        return remainingBeforeRead - buf.remaining();
      }
    }

    long oldPos = this.getPos();
    int bytesRead;
    try {
      ((Seekable) inputStream).seek(position);
      bytesRead = ((ByteBufferReadable) inputStream).read(buf);
    } catch (EOFException e) {
      // Either position is negative or it has reached EOF
      return -1;
    } finally {
      ((Seekable) inputStream).seek(oldPos);
    }
    return bytesRead;
  }

  /**
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @param position offset
   * @throws IOException if there is some error performing the read
   * @throws EOFException if end of file reached before reading fully
   */
  @Override
  public void readFully(long position, ByteBuffer buf) throws IOException {
    int bytesRead;
    for (int readCount = 0; buf.hasRemaining(); readCount += bytesRead) {
      bytesRead = this.read(position + (long)readCount, buf);
      if (bytesRead < 0) {
        // Still buffer has space to read but stream has already reached EOF
        throw new EOFException("End of file reached before reading fully.");
      }
    }
  }
}

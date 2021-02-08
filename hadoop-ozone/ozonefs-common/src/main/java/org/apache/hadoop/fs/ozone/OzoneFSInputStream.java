/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Seekable;

/**
 * The input stream for Ozone file system.
 *
 * TODO: Make inputStream generic for both rest and rpc clients
 * This class is not thread safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneFSInputStream extends FSInputStream
    implements ByteBufferReadable, CanUnbuffer {

  private final InputStream inputStream;
  private final Statistics statistics;

  public OzoneFSInputStream(InputStream inputStream, Statistics statistics) {
    this.inputStream = inputStream;
    this.statistics = statistics;
  }

  @Override
  public int read() throws IOException {
    int byteRead = inputStream.read();
    if (statistics != null && byteRead >= 0) {
      statistics.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = inputStream.read(b, off, len);
    if (statistics != null && bytesRead >= 0) {
      statistics.incrementBytesRead(bytesRead);
    }
    return bytesRead;
  }

  @Override
  public synchronized void close() throws IOException {
    inputStream.close();
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
    if (buf.isReadOnly()){
      throw new ReadOnlyBufferException();
    }

    int readLen = Math.min(buf.remaining(), available());

    int bytesRead;
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

    return bytesRead;
  }

  @Override
  public void unbuffer() {
    if (inputStream instanceof CanUnbuffer) {
      ((CanUnbuffer) inputStream).unbuffer();
    }
  }
}

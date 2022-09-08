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
package org.apache.hadoop.hdds.scm.storage;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Abstact class which extends InputStream and some common interfaces used by
 * various Ozone InputStream classes.
 */
public abstract class ExtendedInputStream extends InputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {

  protected static final int EOF = -1;

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
    ByteReaderStrategy strategy = new ByteArrayReader(b, off, len);
    int bufferLen = strategy.getTargetLength();
    if (bufferLen == 0) {
      return 0;
    }
    return readWithStrategy(strategy);
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    ByteReaderStrategy strategy = new ByteBufferReader(byteBuffer);
    int bufferLen = strategy.getTargetLength();
    if (bufferLen == 0) {
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
   * @return
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
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * OzoneInputStream is used to read data from Ozone.
 * It uses {@link KeyInputStream} for reading the data.
 */
public class OzoneInputStream extends InputStream implements CanUnbuffer,
    ByteBufferReadable {

  private final InputStream inputStream;

  public OzoneInputStream() {
    inputStream = null;
  }
  /**
   * Constructs OzoneInputStream with KeyInputStream.
   *
   * @param inputStream
   */
  public OzoneInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputStream.read(b, off, len);
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    if (inputStream instanceof ByteBufferReadable) {
      return ((ByteBufferReadable)inputStream).read(byteBuffer);
    } else {
      throw new UnsupportedOperationException("Read with ByteBuffer is not " +
          " supported by " + inputStream.getClass().getName());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    inputStream.close();
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  @Override
  public long skip(long n) throws IOException {
    return inputStream.skip(n);
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public void unbuffer() {
    if (inputStream instanceof CanUnbuffer) {
      ((CanUnbuffer) inputStream).unbuffer();
    }
  }
}

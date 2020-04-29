/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.io;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.ozone.client.io.KeyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * S3Wrapper Input Stream which encapsulates KeyInputStream from ozone.
 */
public class S3WrapperInputStream extends FSInputStream {
  private final KeyInputStream inputStream;
  private static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

  /**
   * Constructs S3WrapperInputStream with KeyInputStream.
   *
   * @param inputStream
   */
  public S3WrapperInputStream(InputStream inputStream) {
    this.inputStream = (KeyInputStream) inputStream;
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
  public synchronized void close() throws IOException {
    inputStream.close();
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public void seek(long pos) throws IOException {
    inputStream.seek(pos);
  }
  @Override
  public long getPos() throws IOException {
    return inputStream.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * Copies some or all bytes from a large (over 2GB) <code>InputStream</code>
   * to an <code>OutputStream</code>, optionally skipping input bytes.
   * <p>
   * Copy the method from IOUtils of commons-io to reimplement skip by seek
   * rather than read. The reason why IOUtils of commons-io implement skip
   * by read can be found at
   * <a href="https://issues.apache.org/jira/browse/IO-203">IO-203</a>.
   * </p>
   * <p>
   * This method buffers the input internally, so there is no need to use a
   * <code>BufferedInputStream</code>.
   * </p>
   * The buffer size is given by {@link #DEFAULT_BUFFER_SIZE}.
   *
   * @param output the <code>OutputStream</code> to write to
   * @param inputOffset : number of bytes to skip from input before copying
   * -ve values are ignored
   * @param length : number of bytes to copy. -ve means all
   * @return the number of bytes copied
   * @throws NullPointerException if the input or output is null
   * @throws IOException          if an I/O error occurs
   */
  public long copyLarge(final OutputStream output, final long inputOffset,
      final long length) throws IOException {
    return inputStream.copyLarge(output, inputOffset, length,
        new byte[DEFAULT_BUFFER_SIZE]);
  }
}

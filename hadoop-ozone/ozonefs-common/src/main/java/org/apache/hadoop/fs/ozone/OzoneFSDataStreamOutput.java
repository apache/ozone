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
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * The ByteBuffer output stream for Ozone file system.
 */
public class OzoneFSDataStreamOutput extends OutputStream
    implements ByteBufferStreamOutput {

  private final ByteBufferStreamOutput byteBufferStreamOutput;

  public OzoneFSDataStreamOutput(
      ByteBufferStreamOutput byteBufferStreamOutput) {
    this.byteBufferStreamOutput = byteBufferStreamOutput;
  }

  /**
   * Try to write the [off:off + len) slice in ByteBuf b to DataStream.
   *
   * @param b   the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public void write(ByteBuffer b, int off, int len)
      throws IOException {
    byteBufferStreamOutput.write(b, off, len);
  }

  /**
   * Writes the specified byte to this output stream. The general
   * contract for <code>write</code> is that one byte is written
   * to the output stream. The byte to be written is the eight
   * low-order bits of the argument <code>b</code>. The 24
   * high-order bits of <code>b</code> are ignored.
   * <p>
   * Subclasses of <code>OutputStream</code> must provide an
   * implementation for this method.
   *
   * @param b the <code>byte</code>.
   * @throws IOException if an I/O error occurs. In particular,
   *                     an <code>IOException</code> may be thrown if the
   *                     output stream has been closed.
   */
  @Override
  public void write(int b) throws IOException {
    byte[] singleBytes = new byte[1];
    singleBytes[0] = (byte) b;
    byteBufferStreamOutput.write(ByteBuffer.wrap(singleBytes));
  }

  /**
   * Flushes this DataStream output and forces any buffered output bytes
   * to be written out.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public void flush() throws IOException {
    byteBufferStreamOutput.flush();
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    byteBufferStreamOutput.close();
  }
}

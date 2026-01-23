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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.client.io.ByteBufferOutputStream;

/**
 * The ByteBuffer output stream for Ozone file system.
 */
public class OzoneFSDataStreamOutput extends ByteBufferOutputStream {

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

  @Override
  public void hflush() throws IOException {
    hsync();
  }

  @Override
  public void hsync() throws IOException {
    TracingUtil.executeInNewSpan("OzoneFSDataStreamOutput.hsync",
        byteBufferStreamOutput::hsync);
  }

  protected ByteBufferStreamOutput getByteBufferStreamOutput() {
    return byteBufferStreamOutput;
  }
}

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.hdds.scm.storage.DataStreamOutput;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OzoneOutputStream is used to write data into Ozone.
 * It uses SCM's {@link KeyOutputStream} for writing the data.
 */
public class OzoneDataStreamOutput extends OutputStream implements DataStreamOutput {

  private final OutputStream outputStream;

  /**
   * Constructs OzoneOutputStream with KeyOutputStream.
   *
   * @param outputStream
   */
  public OzoneDataStreamOutput(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public void write(ByteBuf b) throws IOException {
    byte[] data = new byte[b.readableBytes()];
    b.readBytes(data);
    outputStream.write(data);
  }

  @Override
  public void write(ByteBuf b, int off, int len) throws IOException {
    write(b.slice(off, len));
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    write(Unpooled.wrappedBuffer(b, off, len));
  }

  @Override
  public synchronized void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    outputStream.close();
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    if (outputStream instanceof KeyOutputStream) {
      return ((KeyOutputStream) outputStream).getCommitUploadPartInfo();
    } else  if (outputStream instanceof CryptoOutputStream) {
      OutputStream wrappedStream =
          ((CryptoOutputStream) outputStream).getWrappedStream();
      if (wrappedStream instanceof KeyDataStreamOutput) {
        return ((KeyOutputStream) wrappedStream).getCommitUploadPartInfo();
      }
    }
    // Otherwise return null.
    return null;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }
}

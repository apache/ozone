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

import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Optional;

/**
 * OzoneOutputStream is used to write data into Ozone.
 */
public class OzoneOutputStream extends ByteArrayStreamOutput {

  private final OutputStream outputStream;
  private final Syncable syncable;

  /**
   * Constructs an instance with a {@link Syncable} {@link OutputStream}.
   *
   * @param outputStream an {@link OutputStream} which is {@link Syncable}.
   */
  public OzoneOutputStream(Syncable outputStream) {
    this(Optional.of(Objects.requireNonNull(outputStream,
                "outputStream == null"))
        .filter(s -> s instanceof OutputStream)
        .map(s -> (OutputStream)s)
        .orElseThrow(() -> new IllegalArgumentException(
            "The parameter syncable is not an OutputStream")),
        outputStream);
  }

  /**
   * Constructs an instance with a (non-{@link Syncable}) {@link OutputStream}
   * with an optional {@link Syncable} object.
   *
   * @param outputStream for writing data.
   * @param syncable an optional parameter
   *                 for accessing the {@link Syncable} feature.
   */
  public OzoneOutputStream(OutputStream outputStream, Syncable syncable) {
    this.outputStream = Objects.requireNonNull(outputStream,
        "outputStream == null");
    this.syncable = syncable != null ? syncable
        : outputStream instanceof Syncable ? (Syncable) outputStream
        : null;
  }

  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    outputStream.write(b, off, len);
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

  public void hsync() throws IOException {
    if (syncable != null) {
      if (outputStream != syncable) {
        outputStream.flush();
      }
      syncable.hsync();
    } else {
      throw new UnsupportedOperationException(outputStream.getClass()
          + " is not " + Syncable.class.getSimpleName());
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    if (outputStream instanceof KeyOutputStream) {
      return ((KeyOutputStream) outputStream).getCommitUploadPartInfo();
    } else  if (outputStream instanceof CryptoOutputStream) {
      OutputStream wrappedStream =
          ((CryptoOutputStream) outputStream).getWrappedStream();
      if (wrappedStream instanceof KeyOutputStream) {
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

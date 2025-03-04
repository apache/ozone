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

package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

/**
 * OzoneDataStreamOutput is used to write data into Ozone.
 */
public class OzoneDataStreamOutput extends ByteBufferOutputStream
    implements  KeyMetadataAware {

  private final ByteBufferStreamOutput byteBufferStreamOutput;
  private boolean enableHsync;
  private final Syncable syncable;

  /**
   * Constructs an instance with a {@link Syncable} {@link OutputStream}.
   *
   * @param outputStream an {@link OutputStream} which is {@link Syncable}.
   * @param enableHsync if false, hsync() executes flush() instead.
   */
  public OzoneDataStreamOutput(Syncable outputStream, boolean enableHsync) {
    this(Optional.of(Objects.requireNonNull(outputStream,
                "outputStream == null"))
            .filter(s -> s instanceof OzoneDataStreamOutput)
            .map(s -> (OzoneDataStreamOutput)s)
            .orElseThrow(() -> new IllegalArgumentException(
                "The parameter syncable is not an OutputStream")),
        outputStream, enableHsync);
  }

  /**
   * Constructs an instance with a (non-{@link Syncable}) {@link ByteBufferStreamOutput}
   * with an optional {@link Syncable} object.
   *
   * @param byteBufferStreamOutput for writing data.
   * @param syncable an optional parameter
   *                 for accessing the {@link Syncable} feature.
   */
  public OzoneDataStreamOutput(ByteBufferStreamOutput byteBufferStreamOutput, Syncable syncable) {
    this(byteBufferStreamOutput, syncable, false);
  }

  /**
   * Constructs an instance with a (non-{@link Syncable}) {@link ByteBufferStreamOutput}
   * with an optional {@link Syncable} object.
   *
   * @param byteBufferStreamOutput for writing data.
   * @param syncable an optional parameter
   *                 for accessing the {@link Syncable} feature.
   * @param enableHsync if false, hsync() executes flush() instead.
   */
  public OzoneDataStreamOutput(ByteBufferStreamOutput byteBufferStreamOutput, Syncable syncable,
                               boolean enableHsync) {
    this.byteBufferStreamOutput = Objects.requireNonNull(byteBufferStreamOutput,
        "byteBufferStreamOutput == null");
    this.syncable = syncable != null ? syncable : byteBufferStreamOutput;
    this.enableHsync = enableHsync;
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    byteBufferStreamOutput.write(b, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    byteBufferStreamOutput.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    byteBufferStreamOutput.close();
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    KeyDataStreamOutput keyDataStreamOutput = getKeyDataStreamOutput();
    if (keyDataStreamOutput != null) {
      return keyDataStreamOutput.getCommitUploadPartInfo();
    }
    // Otherwise return null.
    return null;
  }

  public KeyDataStreamOutput getKeyDataStreamOutput() {
    if (byteBufferStreamOutput instanceof OzoneOutputStream) {
      OutputStream outputStream =
          ((OzoneOutputStream) byteBufferStreamOutput).getOutputStream();
      if (outputStream instanceof KeyDataStreamOutput) {
        return ((KeyDataStreamOutput) outputStream);
      } else if (outputStream instanceof CryptoOutputStream) {
        OutputStream wrappedStream =
            ((CryptoOutputStream) outputStream).getWrappedStream();
        if (wrappedStream instanceof KeyDataStreamOutput) {
          return ((KeyDataStreamOutput) wrappedStream);
        }
      } else if (outputStream instanceof CipherOutputStreamOzone) {
        OutputStream wrappedStream =
            ((CipherOutputStreamOzone) outputStream).getWrappedStream();
        if (wrappedStream instanceof KeyDataStreamOutput) {
          return ((KeyDataStreamOutput) wrappedStream);
        }
      }
    } else if (byteBufferStreamOutput instanceof KeyDataStreamOutput) {
      return ((KeyDataStreamOutput) byteBufferStreamOutput);
    }
    // Otherwise return null.
    return null;
  }

  @Override
  public void hflush() throws IOException {
    hsync();
  }

  @Override
  public void hsync() throws IOException {
    // Disable the feature flag restores the prior behavior.
    if (!enableHsync) {
      byteBufferStreamOutput.flush();
      return;
    }
    if (syncable != null) {
      if (byteBufferStreamOutput != syncable) {
        byteBufferStreamOutput.flush();
      }
      syncable.hsync();
    } else {
      throw new UnsupportedOperationException(byteBufferStreamOutput.getClass()
          + " is not " + Syncable.class.getSimpleName());
    }
  }

  public ByteBufferStreamOutput getByteBufStreamOutput() {
    return byteBufferStreamOutput;
  }

  @Override
  public Map<String, String> getMetadata() {
    return ((KeyMetadataAware)this.byteBufferStreamOutput).getMetadata();
  }

}

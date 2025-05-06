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

package org.apache.hadoop.ozone.freon;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;

/**
 * Utility class to write random keys from a limited buffer.
 */
@SuppressWarnings("java:S2245") // no need for secure random
public class ContentGenerator {

  /**
   * Size of the destination object (key or file).
   */
  private long keySize;

  /**
   * Buffer for the pre-allocated content (will be reused if less than the
   * keySize).
   */
  private int bufferSize;

  /**
   * Number of bytes to write in one call.
   * <p>
   * Should be no larger than the bufferSize.
   */
  private final int copyBufferSize;

  private final byte[] buffer;

  private SyncOptions flushOrSync;

  enum SyncOptions {
    NONE,
    HFLUSH,
    HSYNC
  }

  ContentGenerator(long keySize, int bufferSize) {
    this(keySize, bufferSize, bufferSize);
  }

  ContentGenerator(long keySize, int bufferSize, int copyBufferSize) {
    this.keySize = keySize;
    this.bufferSize = bufferSize;
    this.copyBufferSize = copyBufferSize;
    buffer = RandomStringUtils.secure().nextAscii(bufferSize)
        .getBytes(StandardCharsets.UTF_8);
    this.flushOrSync = SyncOptions.NONE;
  }

  ContentGenerator(long keySize, int bufferSize, int copyBufferSize,
      SyncOptions flushOrSync) {
    this(keySize, bufferSize, copyBufferSize);
    this.flushOrSync = flushOrSync;
  }

  /**
   * Write the required bytes to the output stream.
   */
  public void write(OutputStream outputStream) throws IOException {
    for (long nrRemaining = keySize;
         nrRemaining > 0; nrRemaining -= bufferSize) {
      int curSize = (int) Math.min(bufferSize, nrRemaining);
      if (copyBufferSize == 1) {
        for (int i = 0; i < curSize; i++) {
          outputStream.write(buffer[i]);
          doFlushOrSync(outputStream);
        }
      } else {
        for (int i = 0; i < curSize; i += copyBufferSize) {
          outputStream.write(buffer, i,
              Math.min(copyBufferSize, curSize - i));
          doFlushOrSync(outputStream);
        }
      }
    }
  }

  private void doFlushOrSync(OutputStream outputStream) throws IOException {
    switch (flushOrSync) {
    case NONE:
      // noop
      break;
    case HFLUSH:
      if (outputStream instanceof Syncable) {
        ((Syncable) outputStream).hflush();
      }
      break;
    case HSYNC:
      if (outputStream instanceof Syncable) {
        ((Syncable) outputStream).hsync();
      }
      break;
    default:
      throw new IllegalArgumentException("Unsupported sync option" + flushOrSync);
    }
  }

  /**
   * Write the required bytes to the streaming output stream.
   */
  public void write(OzoneDataStreamOutput out) throws IOException {
    for (long nrRemaining = keySize;
         nrRemaining > 0; nrRemaining -= bufferSize) {
      int curSize = (int) Math.min(bufferSize, nrRemaining);
      for (int i = 0; i < curSize; i += copyBufferSize) {
        ByteBuffer bb =
            ByteBuffer.wrap(buffer, i, Math.min(copyBufferSize, curSize - i));
        out.write(bb);
      }
    }
    out.close();
  }

  @VisibleForTesting
  byte[] getBuffer() {
    return buffer;
  }
}

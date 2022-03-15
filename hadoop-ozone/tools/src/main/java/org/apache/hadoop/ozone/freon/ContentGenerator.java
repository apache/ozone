/*
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
package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * Utility class to write random keys from a limited buffer.
 */
public class ContentGenerator {

  /**
   * Size of the destination object (key or file).
   */
  private final long keySize;

  /**
   * Buffer for the pre-allocated content (will be reused if less than the
   * keySize).
   */
  private final int bufferSize;

  /**
   * Number of bytes to write in one call.
   * <p>
   * Should be no larger than the bufferSize.
   */
  private final int copyBufferSize;

  private final byte[] buffer;

  /**
   * Issue Hsync after every write ( Cannot be used with Hflush ).
   */
  private final boolean hSync;

  /**
   * Issue Hflush after every write ( Cannot be used with Hsync ).
   */
  private final boolean hFlush;

  ContentGenerator(long keySize, int bufferSize) {
    this(keySize, bufferSize, bufferSize, false, false);
  }

  ContentGenerator(long keySize, int bufferSize, int copyBufferSize) {
    this(keySize, bufferSize, copyBufferSize, false, false);
  }

  ContentGenerator(long keySize, int bufferSize, boolean hsync,
                   boolean hflush) {
    this(keySize, bufferSize, bufferSize, hsync, hflush);
  }

  ContentGenerator(long keySize, int bufferSize, int copyBufferSize,
                   boolean hSync, boolean hFlush) {
    this.keySize = keySize;
    this.bufferSize = bufferSize;
    this.copyBufferSize = copyBufferSize;
    this.hSync = hSync;
    this.hFlush = hFlush;

    buffer = RandomStringUtils.randomAscii(bufferSize)
        .getBytes(StandardCharsets.UTF_8);
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
          flushOrSync(outputStream);
        }
      } else {
        for (int i = 0; i < curSize; i += copyBufferSize) {
          outputStream.write(buffer, i,
              Math.min(copyBufferSize, curSize - i));
          flushOrSync(outputStream);
        }
      }
    }
  }

  private void flushOrSync(OutputStream outputStream) throws IOException {
    if (outputStream instanceof FSDataOutputStream) {
      if (hSync) {
        ((FSDataOutputStream) outputStream).hsync();
      } else if (hFlush) {
        ((FSDataOutputStream) outputStream).hflush();
      }
    }
  }

  @VisibleForTesting
  byte[] getBuffer() {
    return buffer;
  }
}

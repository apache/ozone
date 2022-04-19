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

  /**
   * Type of flags.
   */
  public enum Flags {
    hSync,
    hFlush,
    None,
  }

  ContentGenerator(Builder objectBuild) {
    this.keySize = objectBuild.keySize;
    this.bufferSize = objectBuild.bufferSize;
    this.copyBufferSize = objectBuild.copyBufferSize;
    this.hSync = objectBuild.hSync;
    this.hFlush = objectBuild.hFlush;
    buffer = RandomStringUtils.randomAscii(bufferSize)
        .getBytes(StandardCharsets.UTF_8);
  }


  public void write(OutputStream outputStream) throws IOException {
    for (long nrRemaining = keySize; nrRemaining > 0;
         nrRemaining -= bufferSize) {
      int curSize = (int) Math.min(bufferSize, nrRemaining);
      if (copyBufferSize == 1) {
        for (int i = 0; i < curSize; i++) {
          outputStream.write(buffer[i]);
          flushOrSync(outputStream);
        }
      } else {
        /**
         * Write the required bytes to the output stream.
         */
        for (int i = 0; i < curSize; i += copyBufferSize) {
          outputStream.write(buffer, i, Math.min(copyBufferSize, curSize - i));
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

  /**
   * Builder Class for Content generator.
   */
  public static class Builder {

    private long keySize = 1024;
    private int bufferSize = 1024;
    private int copyBufferSize = 1024;
    private boolean hSync = false;
    private boolean hFlush = false;
    private String flushMode;

    public Builder seyKeySize(long keysize) {
      this.keySize = keysize;
      return this;
    }

    public Builder setBufferSize(int buffersize) {
      this.bufferSize = buffersize;
      return this;
    }

    public Builder setCopyBufferSize(int copybuffersize) {
      this.copyBufferSize = copybuffersize;
      return this;
    }

    /**
     * Type of flags permitted
     * 1. hSync
     * 2. hFlush
     * 3. None.
     */
    public Builder setFlushMode(String flushmode) {
      this.flushMode = flushmode;
      Flags type = Flags.valueOf(flushmode);
      switch (type) {
      case hSync:
        hSync = true;
        break;
      case hFlush:
        hFlush = true;
        break;
      case None:
        break;
      default:
        throw new IllegalArgumentException(
            flushmode + " is not a valid benchmarkType.");
      }
      return this;
    }

    //Return the final constructed builder object
    public ContentGenerator build() {
      return new ContentGenerator(this);
    }
  }

  @VisibleForTesting
  byte[] getBuffer() {
    return buffer;
  }
}

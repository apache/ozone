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

package org.apache.hadoop.hdds.utils.io;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Warp a {@link ByteBuffer} as an {@link InputStream}. */
public class ByteBufferInputStream extends InputStream {
  private final ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer.asReadOnlyBuffer();
  }

  @Override
  public int read() throws IOException {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    return buffer.get() & 0xFF;
  }

  @Override
  public int read(@Nonnull byte[] array, int offset, int length) throws IOException {
    assertArrayIndex(array, offset, length);

    if (length == 0) {
      return 0;
    }
    final int remaining = buffer.remaining();
    if (remaining <= 0) {
      return -1;
    }
    final int min = Math.min(remaining, length);
    buffer.get(array, offset, min);
    return min;
  }

  static void assertArrayIndex(@Nonnull byte[] array, int offset, int length) {
    Objects.requireNonNull(array, "array == null");
    if (offset < 0) {
      throw new ArrayIndexOutOfBoundsException("offset = " + offset + " < 0");
    } else if (length < 0) {
      throw new ArrayIndexOutOfBoundsException("length = " + length + " < 0");
    }
    final int end = offset + length;
    if (end < 0) {
      throw new ArrayIndexOutOfBoundsException(
          "Overflow: offset+length > Integer.MAX_VALUE, offset=" + offset + ", length=" + length);
    } else if (end > array.length) {
      throw new ArrayIndexOutOfBoundsException(
          "offset+length > array.length = " + array.length + ", offset=" + offset + ", length=" + length);
    }
  }
}

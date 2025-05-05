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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * An {@link OutputStream} first write data to a buffer up to the capacity.
 * Then, select {@code Underlying} by the number of bytes written.
 * When {@link #flush()}, {@link #hflush()}, {@link #hsync()}
 * or {@link #close()} is invoked,
 * it will force flushing the buffer and {@link OutputStream} selection.
 * <p>
 * This class, like many {@link OutputStream} subclasses, is NOT threadsafe.
 *
 * @param <OUT> The underlying {@link OutputStream} type.
 */
public class SelectorOutputStream<OUT extends OutputStream>
    extends OutputStream implements Syncable, StreamCapabilities {

  private final ByteArrayBuffer buffer;
  private final Underlying underlying;

  /** A buffer backed by a byte[]. */
  static final class ByteArrayBuffer {
    private byte[] array;
    /** Write offset of {@link #array}. */
    private int offset = 0;

    private ByteArrayBuffer(int capacity) {
      this.array = new byte[capacity];
    }

    private void assertRemaining(int outstandingBytes) {
      Objects.requireNonNull(array, "array == null");

      final int remaining = array.length - offset;
      if (remaining < 0) {
        throw new IllegalStateException("remaining = " + remaining + " <= 0");
      }
      if (remaining < outstandingBytes) {
        throw new IllegalArgumentException("Buffer overflow: remaining = "
            + remaining + " < outstandingBytes = " + outstandingBytes);
      }
    }

    void write(byte b) {
      assertRemaining(1);
      array[offset] = b;
      offset++;
    }

    void write(byte[] src, int srcOffset, int length) {
      Objects.requireNonNull(src, "src == null");
      assertRemaining(length);
      System.arraycopy(src, srcOffset, array, offset, length);
      offset += length;
    }

    <OUT extends OutputStream> OUT selectAndClose(
        int outstandingBytes, boolean force,
        CheckedFunction<Integer, OUT, IOException> selector)
        throws IOException {
      assertRemaining(0);
      final int required = offset + outstandingBytes;
      if (force || required > array.length) {
        final OUT out = selector.apply(required);
        out.write(array, 0, offset);
        array = null;
        return out;
      }
      return null;
    }
  }

  /** To select the underlying {@link OutputStream}. */
  final class Underlying {
    /** Select an {@link OutputStream} by the number of bytes. */
    private final CheckedFunction<Integer, OUT, IOException> selector;
    private OUT out;

    private Underlying(CheckedFunction<Integer, OUT, IOException> selector) {
      this.selector = selector;
    }

    private OUT select(int outstandingBytes, boolean force) throws IOException {
      if (out == null) {
        out = buffer.selectAndClose(outstandingBytes, force, selector);
      }
      return out;
    }
  }

  /**
   * Construct a {@link SelectorOutputStream} which first writes to a buffer.
   * Once the buffer has become full, select an {@link OutputStream}.
   *
   * @param selectionThreshold The buffer capacity.
   * @param selector Use bytes-written to select an {@link OutputStream}.
   */
  public SelectorOutputStream(int selectionThreshold,
      CheckedFunction<Integer, OUT, IOException> selector) {
    this.buffer = new ByteArrayBuffer(selectionThreshold);
    this.underlying = new Underlying(selector);
  }

  public OUT getUnderlying() {
    return underlying.out;
  }

  @Override
  public void write(int b) throws IOException {
    final OUT out = underlying.select(1, false);
    if (out != null) {
      out.write(b);
    } else {
      buffer.write((byte) b);
    }
  }

  @Override
  public void write(@Nonnull byte[] array, int off, int len)
      throws IOException {
    final OUT selected = underlying.select(len, false);
    if (selected != null) {
      selected.write(array, off, len);
    } else {
      buffer.write(array, off, len);
    }
  }

  private OUT select() throws IOException {
    return underlying.select(0, true);
  }

  @Override
  public void flush() throws IOException {
    select().flush();
  }

  @Override
  public void hflush() throws IOException {
    final OUT out = select();
    if (out instanceof Syncable) {
      ((Syncable)out).hflush();
    } else {
      throw new IllegalStateException(
          "Failed to hflush: The underlying OutputStream ("
              + out.getClass() + ") is not Syncable.");
    }
  }

  @Override
  public void hsync() throws IOException {
    final OUT out = select();
    if (out instanceof Syncable) {
      ((Syncable)out).hsync();
    } else {
      throw new IllegalStateException(
          "Failed to hsync: The underlying OutputStream ("
              + out.getClass() + ") is not Syncable.");
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    try {
      final OUT out = select();
      if (out instanceof StreamCapabilities) {
        return ((StreamCapabilities) out).hasCapability(capability);
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    select().close();
  }
}

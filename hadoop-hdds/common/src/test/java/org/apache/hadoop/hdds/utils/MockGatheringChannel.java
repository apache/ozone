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

package org.apache.hadoop.hdds.utils;

import static com.google.common.base.Preconditions.checkElementIndex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ThreadLocalRandom;

/**
 * {@link GatheringByteChannel} implementation for testing.  Delegates
 * to a {@link WritableByteChannel}.
 *
 * @see java.nio.channels.Channels#newChannel(java.io.OutputStream)
 */
public class MockGatheringChannel implements GatheringByteChannel {

  private final WritableByteChannel delegate;

  public MockGatheringChannel(WritableByteChannel delegate) {
    this.delegate = delegate;
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length)
      throws IOException {

    checkElementIndex(offset, srcs.length, "offset");
    checkElementIndex(offset + length - 1, srcs.length, "offset+length");

    long fullLength = 0;
    for (int i = offset; i < srcs.length; i++) {
      fullLength += srcs[i].remaining();
    }
    if (fullLength <= 0) {
      return 0;
    }

    // simulate partial write by setting a random partial length
    final long partialLength = ThreadLocalRandom.current().nextLong(fullLength + 1);

    long written = 0;
    for (int i = offset; i < srcs.length; i++) {
      for (final ByteBuffer src = srcs[i]; src.hasRemaining();) {
        final long n = partialLength - written;  // write at most n bytes
        assertThat(n).isGreaterThanOrEqualTo(0);
        if (n == 0) {
          return written;
        }

        final int remaining = src.remaining();
        final int adjustment = remaining <= n ? 0 : Math.toIntExact(remaining - n);
        written += adjustedWrite(src, adjustment);
      }
    }
    return written;
  }

  @Override
  public long write(ByteBuffer[] srcs) throws IOException {
    return write(srcs, 0, srcs.length);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    final int remaining = src.remaining();
    if (remaining <= 0) {
      return 0;
    }
    // Simulate partial write by a random adjustment.
    final int adjustment = ThreadLocalRandom.current().nextInt(remaining + 1);
    return adjustedWrite(src, adjustment);
  }

  /** Simulate partial write by the given adjustment. */
  private int adjustedWrite(ByteBuffer src, int adjustment) throws IOException {
    assertThat(adjustment).isGreaterThanOrEqualTo(0);
    final int remaining = src.remaining();
    if (remaining <= 0) {
      return 0;
    }
    assertThat(adjustment).isLessThanOrEqualTo(remaining);

    final int oldLimit = src.limit();
    final int newLimit = oldLimit - adjustment;
    src.limit(newLimit);
    assertEquals(newLimit, src.limit());
    final int toWrite = remaining - adjustment;
    assertEquals(toWrite, src.remaining());

    final int written = delegate.write(src);
    assertEquals(newLimit, src.limit());
    assertEquals(toWrite - written, src.remaining());

    src.limit(oldLimit);
    assertEquals(oldLimit, src.limit());
    assertEquals(remaining - written, src.remaining());

    return written;
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}

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

package org.apache.hadoop.ozone.common;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

/** For converting to {@link ByteString}s. */
public interface ChunkBufferToByteString {
  /**
   * Wrap the given list of {@link ByteBuf}s
   * as a {@link ChunkBufferToByteString}.
   */
  static ChunkBufferToByteString wrap(List<ByteBuf> buffers) {
    return new ChunkBufferToByteStringByByteBufs(buffers);
  }

  /** Release the underlying resource. */
  default void release() {
  }

  /**
   * Convert this buffer to a {@link ByteString}.
   * The position and limit of this {@link ChunkBufferToByteString}
   * remains unchanged.
   * The given function must preserve the position and limit
   * of the input {@link ByteBuffer}.
   */
  default ByteString toByteString(Function<ByteBuffer, ByteString> function) {
    return toByteStringImpl(b -> applyAndAssertFunction(b, function, this));
  }

  /**
   * Convert this buffer(s) to a list of {@link ByteString}.
   * The position and limit of this {@link ChunkBufferToByteString}
   * remains unchanged.
   * The given function must preserve the position and limit
   * of the input {@link ByteBuffer}.
   */
  default List<ByteString> toByteStringList(
      Function<ByteBuffer, ByteString> function) {
    return toByteStringListImpl(b -> applyAndAssertFunction(b, function, this));
  }

  // for testing
  default ByteString toByteString() {
    return toByteString(ByteStringConversion::safeWrap);
  }

  ByteString toByteStringImpl(Function<ByteBuffer, ByteString> function);

  List<ByteString> toByteStringListImpl(
      Function<ByteBuffer, ByteString> function);

  static void assertInt(int expected, int computed, Supplier<String> prefix) {
    if (expected != computed) {
      throw new IllegalStateException(prefix.get()
          + ": expected = " + expected + " but computed = " + computed);
    }
  }

  /** Apply the function and assert if it preserves position and limit. */
  static ByteString applyAndAssertFunction(ByteBuffer buffer,
      Function<ByteBuffer, ByteString> function, Object name) {
    final int pos = buffer.position();
    final int lim = buffer.limit();
    final ByteString bytes = function.apply(buffer);
    assertInt(pos, buffer.position(), () -> name + ": Unexpected position");
    assertInt(lim, buffer.limit(), () -> name + ": Unexpected limit");
    return bytes;
  }
}

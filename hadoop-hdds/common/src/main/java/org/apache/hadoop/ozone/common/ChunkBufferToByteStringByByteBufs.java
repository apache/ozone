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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

/**
 * A {@link ChunkBufferToByteString} implementation
 * using a list of {@link ByteBuf}s.
 */
class ChunkBufferToByteStringByByteBufs implements ChunkBufferToByteString {
  private final List<ByteBuf> buffers;

  private volatile List<ByteString> byteStrings;
  private volatile ByteString concatenated;

  ChunkBufferToByteStringByByteBufs(List<ByteBuf> buffers) {
    this.buffers = buffers == null || buffers.isEmpty() ?
        Collections.emptyList() : Collections.unmodifiableList(buffers);
  }

  @Override
  public void release() {
    for (ByteBuf buf : buffers) {
      buf.release();
    }
  }

  @Override
  public ByteString toByteStringImpl(Function<ByteBuffer, ByteString> f) {
    if (concatenated != null) {
      return concatenated;
    }
    initByteStrings(f);
    return Objects.requireNonNull(concatenated, "concatenated == null");
  }

  @Override
  public List<ByteString> toByteStringListImpl(Function<ByteBuffer, ByteString> f) {
    if (byteStrings != null) {
      return byteStrings;
    }
    return initByteStrings(f);
  }

  private synchronized List<ByteString> initByteStrings(Function<ByteBuffer, ByteString> f) {
    if (byteStrings != null) {
      return byteStrings;
    }
    if (buffers.isEmpty()) {
      byteStrings = Collections.emptyList();
      concatenated = ByteString.EMPTY;
      return byteStrings;
    }

    final List<ByteString> array = new ArrayList<>();
    concatenated = convert(buffers, array, f);
    byteStrings = Collections.unmodifiableList(array);
    return byteStrings;
  }

  static ByteString convert(List<ByteBuf> bufs, List<ByteString> byteStrings, Function<ByteBuffer, ByteString> f) {
    ByteString concatenated = ByteString.EMPTY;
    for (ByteBuf buf : bufs) {
      for (ByteBuffer b : buf.nioBuffers()) {
        final ByteString s = f.apply(b);
        byteStrings.add(s);
        concatenated = concatenated.concat(s);
      }
    }
    return concatenated;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":n=" + buffers.size();
  }
}

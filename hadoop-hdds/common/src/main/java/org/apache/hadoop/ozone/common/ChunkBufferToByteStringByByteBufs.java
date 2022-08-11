/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@link ChunkBufferToByteString} implementation
 * using a list of {@link ByteBuf}s.
 */
class ChunkBufferToByteStringByByteBufs implements ChunkBufferToByteString {
  private final List<ByteBuf> buffers;

  private volatile List<ByteString> byteStrings;
  private volatile ByteString byteString;

  ChunkBufferToByteStringByByteBufs(List<ByteBuf> buffers) {
    this.buffers = buffers == null || buffers.isEmpty() ?
        Collections.emptyList() : Collections.unmodifiableList(buffers);
  }

  @Override
  public ByteString toByteStringImpl(Function<ByteBuffer, ByteString> f) {
    if (byteString != null) {
      return byteString;
    }
    initByteStrings(f);
    return Objects.requireNonNull(byteString, "byteString == null");
  }

  @Override
  public List<ByteString> toByteStringListImpl(
      Function<ByteBuffer, ByteString> f) {
    if (byteStrings != null) {
      return byteStrings;
    }
    return initByteStrings(f);
  }

  private synchronized List<ByteString> initByteStrings(
      Function<ByteBuffer, ByteString> f) {
    if (byteStrings != null) {
      return byteStrings;
    }

    byteStrings = new ArrayList<>();
    byteString = ByteString.EMPTY;
    for (ByteBuf buf : buffers) {
      final ByteString s = f.apply(buf.nioBuffer());
      buf.release();
      byteStrings.add(s);
      byteString = byteString.concat(s);
    }
    return byteStrings;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":n=" + buffers.size();
  }
}

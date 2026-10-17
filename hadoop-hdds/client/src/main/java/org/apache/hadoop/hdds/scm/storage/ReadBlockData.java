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

package org.apache.hadoop.hdds.scm.storage;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

final class ReadBlockData {
  private static final int RATIS_READ_BLOCK_STREAM_HEADER_BYTES =
      Integer.BYTES;

  private final ContainerCommandResponseProto response;
  private final List<ByteBuffer> data;

  private ReadBlockData(ContainerCommandResponseProto response,
      List<ByteBuffer> data) {
    this.response = response;
    this.data = data;
  }

  /**
   * Parses a reply in the buffers it was received in. A reply may span many of them, so the data stays in them
   * instead of being copied into one buffer.
   */
  static ReadBlockData parse(DataStreamReply reply)
      throws InvalidProtocolBufferException {
    final ByteBuffer[] buffers = reply.nioBuffers();
    final Deque<ByteBuffer> remaining = new ArrayDeque<>(buffers.length);
    for (ByteBuffer buffer : buffers) {
      if (buffer.hasRemaining()) {
        remaining.add(buffer.slice());
      }
    }
    final ByteBuffer header = take(remaining, RATIS_READ_BLOCK_STREAM_HEADER_BYTES);
    if (header == null) {
      throw new InvalidProtocolBufferException(
          "Missing Ratis ReadBlock metadata length");
    }
    final int metadataLength = header.getInt(header.position());
    final ByteBuffer metadata = metadataLength < 0 ? null : take(remaining, metadataLength);
    if (metadata == null) {
      throw new InvalidProtocolBufferException(
          "Invalid Ratis ReadBlock metadata length " + metadataLength);
    }
    final List<ByteBuffer> data = new ArrayList<>(remaining.size());
    for (ByteBuffer buffer : remaining) {
      data.add(buffer.slice());
    }
    return new ReadBlockData(
        ContainerCommandResponseProto.parseFrom(metadata), data);
  }

  /**
   * Removes the next {@code length} bytes from {@code buffers}. The header and metadata of a reply are small, so they
   * are copied when they span two buffers.
   *
   * @return the bytes, or null when {@code buffers} have fewer
   */
  private static ByteBuffer take(Deque<ByteBuffer> buffers, int length) {
    final ByteBuffer first = buffers.peekFirst();
    if (first != null && first.remaining() >= length) {
      final ByteBuffer taken = first.slice();
      taken.limit(length);
      first.position(first.position() + length);
      if (!first.hasRemaining()) {
        buffers.removeFirst();
      }
      return taken;
    }
    final ByteBuffer copy = ByteBuffer.allocate(length);
    while (copy.hasRemaining()) {
      final ByteBuffer buffer = buffers.pollFirst();
      if (buffer == null) {
        return null;
      }
      final int n = Math.min(buffer.remaining(), copy.remaining());
      final ByteBuffer part = buffer.duplicate();
      part.limit(part.position() + n);
      copy.put(part);
      buffer.position(buffer.position() + n);
      if (buffer.hasRemaining()) {
        buffers.addFirst(buffer);
      }
    }
    copy.flip();
    return copy;
  }

  ContainerCommandResponseProto getResponse() {
    return response;
  }

  /** @return the data in the buffers the reply was received in, each from position 0 */
  List<ByteBuffer> getData() {
    return data;
  }
}

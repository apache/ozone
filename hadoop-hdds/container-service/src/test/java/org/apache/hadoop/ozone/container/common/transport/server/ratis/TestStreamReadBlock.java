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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher.ReadBlockObserver;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher.ReadBlockResponse;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ContainerStateMachine#streamReadBlock}.
 */
class TestStreamReadBlock {
  private static final ContainerCommandRequestProto REQUEST = ContainerCommandRequestProto.newBuilder()
      .setCmdType(Type.ReadBlock)
      .setContainerID(1)
      .setDatanodeUuid("datanode")
      .build();

  /**
   * The data of each response is read into a direct buffer right after the header and metadata of its reply, and the
   * stream sends that buffer: the data is not copied.
   */
  @Test
  void sendsTheBufferTheDataIsReadInto() throws Exception {
    final byte[][] data = {bytes(100, 0), bytes(40, 100)};
    final ContainerDispatcher dispatcher = dispatcher(observer -> {
      long offset = 0;
      for (byte[] d : data) {
        final ContainerCommandResponseProto response = response(offset);
        final ByteBuffer buffer = observer.allocate(response, d.length);
        buffer.put(d);
        buffer.flip();
        observer.onNext(new ReadBlockResponse(response, buffer.asReadOnlyBuffer()));
        offset += d.length;
      }
    });
    final Stream stream = new Stream();

    final long sent = ContainerStateMachine.streamReadBlock(dispatcher, REQUEST, stream);

    assertEquals(data.length, stream.replies.size());
    long offset = 0;
    long length = 0;
    for (int i = 0; i < data.length; i++) {
      assertTrue(stream.direct.get(i));
      final ByteBuffer reply = ByteBuffer.wrap(stream.replies.get(i));
      assertEquals(response(offset), metadata(reply));
      final byte[] replyData = new byte[reply.remaining()];
      reply.get(replyData);
      assertArrayEquals(data[i], replyData);
      offset += data[i].length;
      length += stream.replies.get(i).length;
    }
    assertEquals(length, sent);
    assertFalse(stream.isOpen());
  }

  /** When reading the data fails after its buffer was allocated, the stream sends the error response. */
  @Test
  void sendsTheErrorWhenTheReadFails() throws Exception {
    final ContainerCommandResponseProto error = ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setResult(Result.IO_EXCEPTION)
        .setMessage("Failed to read")
        .build();
    final ContainerDispatcher dispatcher = dispatcher(observer -> {
      observer.allocate(response(0), 100);
      observer.onNext(new ReadBlockResponse(error, null));
    });
    final Stream stream = new Stream();

    ContainerStateMachine.streamReadBlock(dispatcher, REQUEST, stream);

    assertEquals(1, stream.replies.size());
    final ByteBuffer reply = ByteBuffer.wrap(stream.replies.get(0));
    assertEquals(error, metadata(reply));
    assertFalse(reply.hasRemaining());
  }

  private static ContainerDispatcher dispatcher(Consumer<ReadBlockObserver> readBlock) {
    final ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    doAnswer(invocation -> {
      readBlock.accept(invocation.getArgument(1));
      return null;
    }).when(dispatcher).streamDataReadOnly(any(), any(), any(), any());
    return dispatcher;
  }

  private static ContainerCommandResponseProto response(long offset) {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setResult(Result.SUCCESS)
        .setReadBlock(ReadBlockResponseProto.newBuilder()
            .setOffset(offset)
            .setData(ByteString.EMPTY))
        .build();
  }

  /** Reads the header and metadata of a reply, leaving {@code reply} at its data. */
  private static ContainerCommandResponseProto metadata(ByteBuffer reply) throws Exception {
    final int length = reply.getInt();
    final ByteBuffer metadata = reply.slice();
    metadata.limit(length);
    reply.position(reply.position() + length);
    return ContainerCommandResponseProto.parseFrom(metadata);
  }

  private static byte[] bytes(int length, int first) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (first + i);
    }
    return bytes;
  }

  /** Records each reply written, as the Ratis read-only stream sends it. */
  private static final class Stream implements WritableByteChannel {
    private final List<byte[]> replies = new ArrayList<>();
    private final List<Boolean> direct = new ArrayList<>();
    private boolean open = true;

    @Override
    public int write(ByteBuffer src) {
      direct.add(src.isDirect());
      final byte[] reply = new byte[src.remaining()];
      src.get(reply);
      replies.add(reply);
      return reply.length;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }
  }
}

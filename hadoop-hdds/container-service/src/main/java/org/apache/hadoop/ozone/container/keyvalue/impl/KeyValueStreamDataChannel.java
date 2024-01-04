/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.util.Time;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is used to get the DataChannel for streaming.
 */
public class KeyValueStreamDataChannel extends StreamDataChannelBase {
  public static final Logger LOG =
      LoggerFactory.getLogger(KeyValueStreamDataChannel.class);

  /**
   * Keep the last {@link Buffers#max} bytes in the buffer
   * in order to create putBlockRequest
   * at {@link #closeBuffers(Buffers, WriteMethod)}}.
   */
  static class Buffers {
    private final Deque<ReferenceCountedObject<ByteBuffer>> deque
        = new LinkedList<>();
    private final int max;
    private int length;

    Buffers(int max) {
      this.max = max;
    }

    private boolean isExtra(int n) {
      return length - n >= max;
    }

    private boolean hasExtraBuffer() {
      return Optional.ofNullable(deque.peek())
          .map(ReferenceCountedObject::get)
          .filter(b -> isExtra(b.remaining()))
          .isPresent();
    }

    /**
     * @return extra buffers which are safe to be written.
     */
    Iterable<ReferenceCountedObject<ByteBuffer>> offer(
        ReferenceCountedObject<ByteBuffer> ref) {
      final ByteBuffer buffer = ref.retain();
      LOG.debug("offer {}", buffer);
      final boolean offered = deque.offer(ref);
      Preconditions.checkState(offered, "Failed to offer");
      length += buffer.remaining();

      return () -> new Iterator<ReferenceCountedObject<ByteBuffer>>() {
        @Override
        public boolean hasNext() {
          return hasExtraBuffer();
        }

        @Override
        public ReferenceCountedObject<ByteBuffer> next() {
          final ReferenceCountedObject<ByteBuffer> polled = poll();
          length -= polled.get().remaining();
          Preconditions.checkState(length >= max);
          return polled;
        }
      };
    }

    ReferenceCountedObject<ByteBuffer> poll() {
      final ReferenceCountedObject<ByteBuffer> polled
          = Objects.requireNonNull(deque.poll());
      RatisHelper.debug(polled.get(), "polled", LOG);
      return polled;
    }

    ReferenceCountedObject<ByteBuf> pollAll() {
      Preconditions.checkState(!deque.isEmpty(), "The deque is empty");
      final ByteBuffer[] array = new ByteBuffer[deque.size()];
      final List<ReferenceCountedObject<ByteBuffer>> refs
          = new ArrayList<>(deque.size());
      for (int i = 0; i < array.length; i++) {
        final ReferenceCountedObject<ByteBuffer> ref = poll();
        refs.add(ref);
        array[i] = ref.get();
      }
      final ByteBuf buf = Unpooled.wrappedBuffer(array).asReadOnly();
      return ReferenceCountedObject.wrap(buf, () -> {
      }, () -> {
        buf.release();
        refs.forEach(ReferenceCountedObject::release);
      });
    }

    void cleanUpAll() {
      while (!deque.isEmpty()) {
        poll().release();
      }
    }
  }

  interface WriteMethod {
    int applyAsInt(ByteBuffer src) throws IOException;
  }

  private final Buffers buffers = new Buffers(
      BlockDataStreamOutput.PUT_BLOCK_REQUEST_LENGTH_MAX);
  private final AtomicReference<ContainerCommandRequestProto> putBlockRequest
      = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  KeyValueStreamDataChannel(File file, ContainerData containerData,
                            ContainerMetrics metrics)
      throws StorageContainerException {
    super(file, containerData, metrics);
  }

  @Override
  ContainerProtos.Type getType() {
    return ContainerProtos.Type.StreamWrite;
  }

  @Override
  public int write(ReferenceCountedObject<ByteBuffer> referenceCounted)
      throws IOException {
    getMetrics().incContainerOpsMetrics(getType());
    assertOpen();

    final long l = Time.monotonicNow();
    int len = writeBuffers(referenceCounted, buffers, super::writeFileChannel);
    getMetrics()
        .incContainerOpsLatencies(getType(), Time.monotonicNow() - l);
    return len;
  }

  static int writeBuffers(ReferenceCountedObject<ByteBuffer> src,
      Buffers buffers, WriteMethod writeMethod)
      throws IOException {
    for (ReferenceCountedObject<ByteBuffer> b : buffers.offer(src)) {
      try {
        writeFully(b.get(), writeMethod);
      } finally {
        b.release();
      }
    }
    return src.get().remaining();
  }

  private static void writeFully(ByteBuffer b, WriteMethod writeMethod)
      throws IOException {
    for (; b.remaining() > 0;) {
      final int written = writeMethod.applyAsInt(b);
      if (written <= 0) {
        throw new IOException("Unable to write");
      }
    }
  }

  public ContainerCommandRequestProto getPutBlockRequest() {
    return Objects.requireNonNull(putBlockRequest.get(),
        () -> "putBlockRequest == null, " + this);
  }

  void assertOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Already closed: " + this);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      try {
        putBlockRequest.set(closeBuffers(buffers, super::writeFileChannel));
      } finally {
        super.close();
      }
    }
  }

  @Override
  protected void cleanupInternal() throws IOException {
    buffers.cleanUpAll();
    if (closed.compareAndSet(false, true)) {
      super.close();
    }
  }

  static ContainerCommandRequestProto closeBuffers(
      Buffers buffers, WriteMethod writeMethod) throws IOException {
    final ReferenceCountedObject<ByteBuf> ref = buffers.pollAll();
    final ByteBuf buf = ref.retain();
    final ContainerCommandRequestProto putBlockRequest;
    try {
      putBlockRequest = readPutBlockRequest(buf);
      // write the remaining data
      writeFully(buf.nioBuffer(), writeMethod);
    } finally {
      ref.release();
    }
    return putBlockRequest;
  }

  private static int readProtoLength(ByteBuf b, int lengthIndex) {
    final int readerIndex = b.readerIndex();
    LOG.debug("{}, lengthIndex = {}, readerIndex = {}",
        b, lengthIndex, readerIndex);
    if (lengthIndex > readerIndex) {
      b.readerIndex(lengthIndex);
    } else {
      Preconditions.checkState(lengthIndex == readerIndex);
    }
    RatisHelper.debug(b, "readProtoLength", LOG);
    return b.nioBuffer().getInt();
  }

  static ContainerCommandRequestProto readPutBlockRequest(ByteBuf b)
      throws IOException {
    //   readerIndex   protoIndex   lengthIndex    readerIndex+readableBytes
    //         V            V             V                              V
    // format: |--- data ---|--- proto ---|--- proto length (4 bytes) ---|
    final int readerIndex = b.readerIndex();
    final int lengthIndex = readerIndex + b.readableBytes() - 4;
    final int protoLength = readProtoLength(b.duplicate(), lengthIndex);
    final int protoIndex = lengthIndex - protoLength;

    final ContainerCommandRequestProto proto;
    try {
      proto = readPutBlockRequest(b.slice(protoIndex, protoLength).nioBuffer());
    } catch (Throwable t) {
      RatisHelper.debug(b, "catch", LOG);
      throw new IOException("Failed to readPutBlockRequest from " + b
          + ": readerIndex=" + readerIndex
          + ", protoIndex=" + protoIndex
          + ", protoLength=" + protoLength
          + ", lengthIndex=" + lengthIndex, t);
    }

    // set index for reading data
    b.writerIndex(protoIndex);

    return proto;
  }

  private static ContainerCommandRequestProto readPutBlockRequest(ByteBuffer b)
      throws IOException {
    RatisHelper.debug(b, "readPutBlockRequest", LOG);
    final ByteString byteString = ByteString.copyFrom(b);

    final ContainerCommandRequestProto request =
        ContainerCommandRequestMessage.toProto(byteString, null);

    if (!request.hasPutBlock()) {
      throw new StorageContainerException(
          "Malformed PutBlock request. trace ID: " + request.getTraceID(),
          ContainerProtos.Result.MALFORMED_REQUEST);
    }
    return request;
  }
}

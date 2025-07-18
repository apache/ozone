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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to get the DataChannel for streaming.
 */
public class KeyValueStreamDataChannel extends StreamDataChannelBase {
  static final Logger LOG = LoggerFactory.getLogger(KeyValueStreamDataChannel.class);

  private final Buffers buffers = new Buffers(BlockDataStreamOutput.PUT_BLOCK_REQUEST_LENGTH_MAX);

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
    assertSpaceAvailability(referenceCounted.get().remaining());

    return writeBuffers(referenceCounted, buffers, this::writeFileChannel);
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

  static void writeFully(ByteBuffer b, WriteMethod writeMethod)
      throws IOException {
    while (b.remaining() > 0) {
      final int written = writeMethod.applyAsInt(b);
      if (written <= 0) {
        throw new IOException("Unable to write");
      }
    }
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
        writeBuffers();
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

  /**
   * Write the data in {@link #buffers} to the channel.
   * Note that the PutBlock proto at the end is ignored; see HDDS-12007.
   */
  private void writeBuffers() throws IOException {
    final ReferenceCountedObject<ByteBuf> ref = buffers.pollAll();
    final ByteBuf buf = ref.retain();
    try {
      setEndIndex(buf);
      // write the remaining data
      writeFully(buf.nioBuffer(), super::writeFileChannel);
    } finally {
      ref.release();
    }
  }

  static int readProtoLength(ByteBuf b, int lengthIndex) {
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

  /** Set end index to the proto index in order to ignore the proto. */
  static void setEndIndex(ByteBuf b) {
    //   readerIndex   protoIndex   lengthIndex    readerIndex+readableBytes
    //         V            V             V                              V
    // format: |--- data ---|--- proto ---|--- proto length (4 bytes) ---|
    final int readerIndex = b.readerIndex();
    final int lengthIndex = readerIndex + b.readableBytes() - 4;
    final int protoLength = readProtoLength(b.duplicate(), lengthIndex);
    final int protoIndex = lengthIndex - protoLength;

    // set index for reading data
    b.writerIndex(protoIndex);
  }

  interface WriteMethod {
    int applyAsInt(ByteBuffer src) throws IOException;
  }
}

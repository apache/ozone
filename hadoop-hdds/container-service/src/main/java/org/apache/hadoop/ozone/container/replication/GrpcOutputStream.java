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
package org.apache.hadoop.ozone.container.replication;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Adapter from {@code OutputStream} to gRPC {@code StreamObserver}.
 * Data is buffered in a limited buffer of the specified size.
 */
abstract class GrpcOutputStream<T> extends OutputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOutputStream.class);

  private final StreamObserver<T> streamObserver;

  private final ByteString.Output buffer;

  private final long containerId;

  private final int bufferSize;

  private long writtenBytes;

  GrpcOutputStream(StreamObserver<T> streamObserver,
      long containerId, int bufferSize) {
    this.streamObserver = streamObserver;
    this.containerId = containerId;
    this.bufferSize = bufferSize;
    buffer = ByteString.newOutput(bufferSize);
  }

  @Override
  public void write(int b) {
    try {
      buffer.write(b);
      if (buffer.size() >= bufferSize) {
        flushBuffer(false);
      }
    } catch (Exception ex) {
      streamObserver.onError(ex);
    }
  }

  @Override
  public void write(@Nonnull byte[] data, int offset, int length) {
    if ((offset < 0) || (offset > data.length) || (length < 0) ||
        ((offset + length) > data.length) || ((offset + length) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return;
    }

    try {
      if (buffer.size() >= bufferSize) {
        flushBuffer(false);
      }

      int remaining = length;
      int off = offset;
      int len = Math.min(remaining, bufferSize - buffer.size());
      while (remaining > 0) {
        buffer.write(data, off, len);
        if (buffer.size() >= bufferSize) {
          flushBuffer(false);
        }
        off += len;
        remaining -= len;
        len = Math.min(bufferSize, remaining);
      }
    } catch (Exception ex) {
      streamObserver.onError(ex);
    }
  }

  @Override
  public void close() throws IOException {
    flushBuffer(true);
    LOG.info("Sent {} bytes for container {}",
        writtenBytes, containerId);
    streamObserver.onCompleted();
    buffer.close();
  }

  protected long getContainerId() {
    return containerId;
  }

  protected long getWrittenBytes() {
    return writtenBytes;
  }

  protected StreamObserver<T> getStreamObserver() {
    return streamObserver;
  }

  private void flushBuffer(boolean eof) {
    int length = buffer.size();
    if (length > 0) {
      ByteString data = buffer.toByteString();
      LOG.debug("Sending {} bytes (of type {}) for container {}",
          length, data.getClass().getSimpleName(), containerId);
      sendPart(eof, length, data);
      writtenBytes += length;
      buffer.reset();
    }
  }

  protected abstract void sendPart(boolean eof, int length, ByteString data);

}

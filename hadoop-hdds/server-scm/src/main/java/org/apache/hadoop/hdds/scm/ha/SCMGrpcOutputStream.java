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

package org.apache.hadoop.hdds.scm.ha;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream to which the tar db checkpoint will be transferred over to the
 * destination over grpc.
 * TODO: Make it a generic utility to be used both during container replication
 * as well as SCM checkpoint transfer
 * Adapter from {@code OutputStream} to gRPC {@code StreamObserver}.
 * Data is buffered in a limited buffer of the specified size.
 */
class SCMGrpcOutputStream extends OutputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMGrpcOutputStream.class);

  private final StreamObserver<InterSCMProtocolProtos.
      CopyDBCheckpointResponseProto> responseObserver;

  private final ByteString.Output buffer;

  private final String clusterId;

  private final int bufferSize;

  private long writtenBytes;

  SCMGrpcOutputStream(
      StreamObserver<InterSCMProtocolProtos.
          CopyDBCheckpointResponseProto> responseObserver,
      String clusterId, int bufferSize) {
    this.responseObserver = responseObserver;
    this.clusterId = clusterId;
    this.bufferSize = bufferSize;
    buffer = ByteString.newOutput(bufferSize);
  }

  @Override public void write(int b) {
    try {
      buffer.write(b);
      if (buffer.size() >= bufferSize) {
        flushBuffer(false);
      }
    } catch (Exception ex) {
      responseObserver.onError(ex);
    }
  }

  @Override public void write(@Nonnull byte[] data, int offset, int length) {
    if ((offset < 0) || (offset > data.length) || (length < 0) || (
        (offset + length) > data.length) || ((offset + length) < 0)) {
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
      responseObserver.onError(ex);
    }
  }

  @Override
  public void close() throws IOException {
    flushBuffer(true);
    LOG.info("Sent {} bytes for cluster {}", writtenBytes, clusterId);
    responseObserver.onCompleted();
    buffer.close();
  }

  private void flushBuffer(boolean eof) {
    int length = buffer.size();
    if (length > 0) {
      ByteString data = buffer.toByteString();
      LOG.debug("Sending {} bytes (of type {})", length,
          data.getClass().getSimpleName());
      InterSCMProtocolProtos.CopyDBCheckpointResponseProto response =
          InterSCMProtocolProtos.CopyDBCheckpointResponseProto.newBuilder()
              .setClusterId(clusterId).setData(data).setEof(eof)
              .setReadOffset(writtenBytes).setLen(length).build();
      responseObserver.onNext(response);
      writtenBytes += length;
      buffer.reset();
    }
  }
}

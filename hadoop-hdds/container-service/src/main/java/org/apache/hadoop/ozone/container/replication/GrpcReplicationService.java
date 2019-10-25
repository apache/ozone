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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to make containers available for replication.
 */
public class GrpcReplicationService extends
    IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceImplBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcReplicationService.class);

  private final ContainerReplicationSource source;

  public GrpcReplicationService(ContainerReplicationSource source) {
    this.source = source;
  }

  @Override
  public void download(CopyContainerRequestProto request,
      StreamObserver<CopyContainerResponseProto> responseObserver) {
    long containerID = request.getContainerID();
    LOG.info("Streaming container data ({}) to other datanode", containerID);
    try {
      GrpcOutputStream outputStream =
          new GrpcOutputStream(responseObserver, containerID);
      source.copyData(containerID, outputStream);
    } catch (IOException e) {
      LOG.error("Error streaming container {}", containerID, e);
      responseObserver.onError(e);
    }
  }

  private static class GrpcOutputStream extends OutputStream {

    private static final int BUFFER_SIZE_IN_BYTES = 1024 * 1024;

    private final StreamObserver<CopyContainerResponseProto> responseObserver;

    private final ByteString.Output buffer =
        ByteString.newOutput(BUFFER_SIZE_IN_BYTES);

    private final long containerId;

    private int writtenBytes;

    GrpcOutputStream(
        StreamObserver<CopyContainerResponseProto> responseObserver,
        long containerId) {
      this.responseObserver = responseObserver;
      this.containerId = containerId;
    }

    @Override
    public void write(int b) {
      try {
        buffer.write(b);
        if (buffer.size() >= BUFFER_SIZE_IN_BYTES) {
          flushBuffer(false);
        }
      } catch (Exception ex) {
        responseObserver.onError(ex);
      }
    }

    private void flushBuffer(boolean eof) {
      int length = buffer.size();
      if (length > 0) {
        ByteString data = buffer.toByteString();
        LOG.debug("Sending {} bytes (of type {}) for container {}",
            length, data.getClass().getSimpleName(), containerId);
        CopyContainerResponseProto response =
            CopyContainerResponseProto.newBuilder()
                .setContainerID(containerId)
                .setData(data)
                .setEof(eof)
                .setReadOffset(writtenBytes)
                .setLen(length)
                .build();
        responseObserver.onNext(response);
        writtenBytes += length;
        buffer.reset();
      }
    }

    @Override
    public void close() throws IOException {
      flushBuffer(true);
      LOG.info("{} bytes written to the rpc stream from container {}",
          writtenBytes, containerId);
      responseObserver.onCompleted();
    }
  }
}

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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.fromProto;

/**
 * Service to make containers available for replication.
 */
public class GrpcReplicationService extends
    IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceImplBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcReplicationService.class);

  static final int BUFFER_SIZE = 1024 * 1024;

  private final ContainerReplicationSource source;
  private final ContainerImporter importer;

  public GrpcReplicationService(ContainerReplicationSource source,
      ContainerImporter importer) {
    this.source = source;
    this.importer = importer;
  }

  @Override
  public void download(CopyContainerRequestProto request,
      StreamObserver<CopyContainerResponseProto> responseObserver) {
    long containerID = request.getContainerID();
    CopyContainerCompression compression = fromProto(request.getCompression());
    LOG.info("Streaming container data ({}) to other datanode " +
        "with compression {}", containerID, compression);
    OutputStream outputStream = null;
    try {
      outputStream = new CopyContainerResponseStream(
          // gRPC runtime always provides implementation of CallStreamObserver
          // that allows flow control.
          (CallStreamObserver<CopyContainerResponseProto>) responseObserver,
          containerID, BUFFER_SIZE);
      source.copyData(containerID, outputStream, compression);
    } catch (IOException e) {
      LOG.warn("Error streaming container {}", containerID, e);
      responseObserver.onError(e);
    } finally {
      // output may have already been closed, ignore such errors
      IOUtils.cleanupWithLogger(LOG, outputStream);
    }
  }

  @Override
  public StreamObserver<SendContainerRequest> upload(
      StreamObserver<SendContainerResponse> responseObserver) {

    return new SendContainerRequestHandler(importer, responseObserver);
  }
}

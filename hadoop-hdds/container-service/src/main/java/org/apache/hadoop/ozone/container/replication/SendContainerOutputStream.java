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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Output stream adapter for SendContainerResponse.
 */
class SendContainerOutputStream extends GrpcOutputStream<SendContainerRequest> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SendContainerOutputStream.class);

  private final CopyContainerCompression compression;
  private final AutoCloseable client;

  SendContainerOutputStream(
      AutoCloseable client, StreamObserver<SendContainerRequest> streamObserver,
      long containerId, int bufferSize, CopyContainerCompression compression) {
    super(streamObserver, containerId, bufferSize);
    this.compression = compression;
    this.client = client;
  }

  @Override
  protected void sendPart(boolean eof, int length, ByteString data) {
    SendContainerRequest request = SendContainerRequest.newBuilder()
        .setContainerID(getContainerId())
        .setData(data)
        .setOffset(getWrittenBytes())
        .setCompression(compression.toProto())
        .build();
    getStreamObserver().onNext(request);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      IOUtils.close(LOG, client);
    }
  }
}

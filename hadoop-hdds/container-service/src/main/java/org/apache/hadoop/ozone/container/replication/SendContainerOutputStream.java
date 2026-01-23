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

package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;

/**
 * Output stream adapter for SendContainerResponse.
 */
class SendContainerOutputStream extends GrpcOutputStream<SendContainerRequest> {

  private final CopyContainerCompression compression;
  private final Long size;

  SendContainerOutputStream(
      CallStreamObserver<SendContainerRequest> streamObserver,
      long containerId, int bufferSize, CopyContainerCompression compression,
      Long size) {
    super(streamObserver, containerId, bufferSize);
    this.compression = compression;
    this.size = size;
  }

  @Override
  protected void sendPart(boolean eof, int length, ByteString data) {
    SendContainerRequest.Builder requestBuilder = SendContainerRequest.newBuilder()
        .setContainerID(getContainerId())
        .setData(data)
        .setOffset(getWrittenBytes())
        .setCompression(compression.toProto());
    
    // Include container size in the first request
    if (getWrittenBytes() == 0 && size != null) {
      requestBuilder.setSize(size);
    }
    getStreamObserver().onNext(requestBuilder.build());
  }
}

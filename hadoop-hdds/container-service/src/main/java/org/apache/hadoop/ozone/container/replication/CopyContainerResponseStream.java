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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerResponseProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;

/**
 * Output stream adapter for CopyContainerResponse.
 */
class CopyContainerResponseStream
    extends GrpcOutputStream<CopyContainerResponseProto> {

  CopyContainerResponseStream(
      CallStreamObserver<CopyContainerResponseProto> streamObserver,
      long containerId, int bufferSize) {
    super(streamObserver, containerId, bufferSize);
  }

  protected void sendPart(boolean eof, int length, ByteString data) {
    CopyContainerResponseProto response =
        CopyContainerResponseProto.newBuilder()
            .setContainerID(getContainerId())
            .setData(data)
            .setEof(eof)
            .setReadOffset(getWrittenBytes())
            .setLen(length)
            .build();
    getStreamObserver().onNext(response);
  }
}

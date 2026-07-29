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

package org.apache.hadoop.hdds.scm.storage;

import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

final class ReadBlockData {
  private static final int RATIS_READ_BLOCK_STREAM_HEADER_BYTES =
      Integer.BYTES;

  private final ContainerCommandResponseProto response;
  private final ByteBuffer data;

  private ReadBlockData(ContainerCommandResponseProto response,
      ByteBuffer data) {
    this.response = response;
    this.data = data;
  }

  static ReadBlockData parse(DataStreamReply reply)
      throws InvalidProtocolBufferException {
    return parse(reply.nioBuffer());
  }

  private static ReadBlockData parse(ByteBuffer replyBuffer)
      throws InvalidProtocolBufferException {
    final ByteBuffer duplicate = replyBuffer.duplicate();
    if (duplicate.remaining() < RATIS_READ_BLOCK_STREAM_HEADER_BYTES) {
      throw new InvalidProtocolBufferException(
          "Missing Ratis ReadBlock metadata length");
    }
    final int metadataLength = duplicate.getInt(duplicate.position());
    if (metadataLength < 0
        || metadataLength > duplicate.remaining()
            - RATIS_READ_BLOCK_STREAM_HEADER_BYTES) {
      throw new InvalidProtocolBufferException(
          "Invalid Ratis ReadBlock metadata length " + metadataLength);
    }
    duplicate.position(
        duplicate.position() + RATIS_READ_BLOCK_STREAM_HEADER_BYTES);
    final ByteBuffer metadata = duplicate.slice();
    metadata.limit(metadataLength);
    duplicate.position(duplicate.position() + metadataLength);
    final ByteBuffer data = duplicate.slice();
    return new ReadBlockData(
        ContainerCommandResponseProto.parseFrom(metadata), data);
  }

  ContainerCommandResponseProto getResponse() {
    return response;
  }

  ByteBuffer getData() {
    return data;
  }
}

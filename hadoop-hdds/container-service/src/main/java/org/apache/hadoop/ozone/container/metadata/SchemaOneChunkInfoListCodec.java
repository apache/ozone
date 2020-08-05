/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;

public class SchemaOneChunkInfoListCodec implements Codec<ChunkInfoList> {
  @Override
  public byte[] toPersistedFormat(ChunkInfoList chunkList) {
    return chunkList.getProtoBufMessage().toByteArray();
  }

  @Override
  public ChunkInfoList fromPersistedFormat(byte[] rawData) throws IOException {
    try {
      return ChunkInfoList.getFromProtoBuf(
              ContainerProtos.ChunkInfoList.parseFrom(rawData));
    } catch (InvalidProtocolBufferException ex) {
      throw new IllegalArgumentException("Invalid chunk information. " +
              "This data may have been written using datanode " +
              "schema version one, which did not save chunk information.", ex);
    }
  }

  @Override
  public ChunkInfoList copyObject(ChunkInfoList object) {
    throw new UnsupportedOperationException();
  }
}

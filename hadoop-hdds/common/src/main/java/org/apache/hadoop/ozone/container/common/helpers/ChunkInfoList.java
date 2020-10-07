/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import java.util.Collections;
import java.util.List;

/**
 * Helper class to convert between protobuf lists and Java lists of
 * {@link ContainerProtos.ChunkInfo} objects.
 */
public class ChunkInfoList {
  private List<ContainerProtos.ChunkInfo> chunks;

  public ChunkInfoList(List<ContainerProtos.ChunkInfo> chunks) {
    this.chunks = chunks;
  }

  public List<ContainerProtos.ChunkInfo> asList() {
    return Collections.unmodifiableList(chunks);
  }

  /**
   * @return A new {@link ChunkInfoList} created from protobuf data.
   */
  public static ChunkInfoList getFromProtoBuf(
          ContainerProtos.ChunkInfoList chunksProto) {
    return new ChunkInfoList(chunksProto.getChunksList());
  }

  /**
   * @return A protobuf message of this object.
   */
  public ContainerProtos.ChunkInfoList getProtoBufMessage() {
    return ContainerProtos.ChunkInfoList.newBuilder()
            .addAllChunks(this.chunks)
            .build();
  }
}

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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;

import java.io.IOException;

/**
 * Supports encoding and decoding {@link BlockData} objects.
 */
public class BlockDataCodec implements Codec<BlockData> {

  @Override
  public byte[] toPersistedFormat(BlockData blockData) {
    return blockData.getProtoBufMessage().toByteArray();
  }

  @Override
  public BlockData fromPersistedFormat(byte[] rawData) throws IOException {
    // Convert raw bytes -> protobuf version of BlockData -> BlockData object.
    return BlockData.getFromProtoBuf(
            ContainerProtos.BlockData.parseFrom(rawData));
  }

  @Override
  public BlockData copyObject(BlockData object) {
    throw new UnsupportedOperationException();
  }
}

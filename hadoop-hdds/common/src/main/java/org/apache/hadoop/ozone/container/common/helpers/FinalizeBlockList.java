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
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto3Codec;

import java.util.Collections;
import java.util.List;

/**
 * Helper class to convert between protobuf lists and Java lists of
 * {@link ContainerProtos.FinalizeBlockList} objects.
 * <p>
 * This class is immutable.
 */
public class FinalizeBlockList {
  private static final Codec<FinalizeBlockList> CODEC = new DelegatedCodec<>(
      Proto3Codec.get(ContainerProtos.FinalizeBlockList.class),
      FinalizeBlockList::getFromProtoBuf,
      FinalizeBlockList::getProtoBufMessage,
      DelegatedCodec.CopyType.SHALLOW);

  public static Codec<FinalizeBlockList> getCodec() {
    return CODEC;
  }

  private final List<Long> blockList;

  public FinalizeBlockList(List<Long> blockList) {
    this.blockList = Collections.unmodifiableList(blockList);
  }

  public List<Long> asList() {
    return blockList;
  }

  /**
   * @return A new {@link FinalizeBlockList} created from protobuf data.
   */
  public static FinalizeBlockList getFromProtoBuf(
          ContainerProtos.FinalizeBlockList finalizeBlockProto) {
    return new FinalizeBlockList(finalizeBlockProto.getLocalIDList());
  }

  /**
   * @return A protobuf message of this object.
   */
  public ContainerProtos.FinalizeBlockList getProtoBufMessage() {
    return ContainerProtos.FinalizeBlockList.newBuilder()
        .addAllLocalID(blockList)
            .build();
  }
}

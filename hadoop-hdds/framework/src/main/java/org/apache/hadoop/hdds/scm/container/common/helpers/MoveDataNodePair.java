/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package org.apache.hadoop.hdds.scm.container.common.helpers;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;

/**
 * MoveDataNodePair encapsulates the source and target
 * datanodes of a move option.
 */
public class MoveDataNodePair {
  /**
   * source datanode of current move option.
   */
  private final DatanodeDetails src;

  /**
   * target datanode of current move option.
   */
  private final DatanodeDetails tgt;

  public MoveDataNodePair(DatanodeDetails src, DatanodeDetails tgt) {
    this.src = src;
    this.tgt = tgt;
  }

  public DatanodeDetails getTgt() {
    return tgt;
  }

  public DatanodeDetails getSrc() {
    return src;
  }

  public HddsProtos.MoveDataNodePairProto getProtobufMessage(int clientVersion)
      throws IOException {
    HddsProtos.MoveDataNodePairProto.Builder builder =
        HddsProtos.MoveDataNodePairProto.newBuilder()
            .setSrc(src.toProto(clientVersion))
            .setTgt(tgt.toProto(clientVersion));
    return builder.build();
  }

  public static MoveDataNodePair getFromProtobuf(
      HddsProtos.MoveDataNodePairProto mdnpp) {
    Preconditions.assertNotNull(mdnpp, "MoveDataNodePair is null");
    DatanodeDetails src = DatanodeDetails.getFromProtoBuf(mdnpp.getSrc());
    DatanodeDetails tgt = DatanodeDetails.getFromProtoBuf(mdnpp.getTgt());
    return new MoveDataNodePair(src, tgt);
  }
}

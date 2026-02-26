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

package org.apache.hadoop.hdds.scm.container.common.helpers;

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MoveDataNodePairProto;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;

/**
 * MoveDataNodePair encapsulates the source and target
 * datanodes of a move option.
 * <p>
 * This class is immutable.
 */
public class MoveDataNodePair {
  private static final Codec<MoveDataNodePair> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(MoveDataNodePairProto.getDefaultInstance()),
      MoveDataNodePair::getFromProtobuf,
      pair -> pair.getProtobufMessage(ClientVersion.CURRENT.serialize()),
      MoveDataNodePair.class,
      DelegatedCodec.CopyType.SHALLOW);

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

  public static Codec<MoveDataNodePair> getCodec() {
    return CODEC;
  }

  public DatanodeDetails getTgt() {
    return tgt;
  }

  public DatanodeDetails getSrc() {
    return src;
  }

  public MoveDataNodePairProto getProtobufMessage(int clientVersion) {
    return MoveDataNodePairProto.newBuilder()
        .setSrc(src.toProto(clientVersion))
        .setTgt(tgt.toProto(clientVersion))
        .build();
  }

  public static MoveDataNodePair getFromProtobuf(
      MoveDataNodePairProto mdnpp) {
    Objects.requireNonNull(mdnpp, "mdnpp == null");
    DatanodeDetails src = DatanodeDetails.getFromProtoBuf(mdnpp.getSrc());
    DatanodeDetails tgt = DatanodeDetails.getFromProtoBuf(mdnpp.getTgt());
    return new MoveDataNodePair(src, tgt);
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.scm.metadata;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MoveDataNodePairProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.ClientVersion;

import java.io.IOException;

/**
 * Codec to serialize / deserialize MoveDataNodePair.
 */

public class MoveDataNodePairCodec implements Codec<MoveDataNodePair> {
  @Override
  public byte[] toPersistedFormat(MoveDataNodePair mdnp)
      throws IOException {
    return mdnp
        .getProtobufMessage(ClientVersion.latest().version()).toByteArray();
  }

  @Override
  public MoveDataNodePair fromPersistedFormat(byte[] rawData)
      throws IOException {
    MoveDataNodePairProto.Builder builder =
        MoveDataNodePairProto.newBuilder(
            MoveDataNodePairProto.PARSER.parseFrom(rawData));
    return MoveDataNodePair.getFromProtobuf(builder.build());
  }

  @Override
  public MoveDataNodePair copyObject(MoveDataNodePair object) {
    throw new UnsupportedOperationException();
  }
}

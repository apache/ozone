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

package org.apache.hadoop.ozone.container.metadata;

import java.util.function.Supplier;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto3Codec;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * ContainerCreateInfo is a class that holds information about the state and other information on creation
 * This class is immutable.
 */
@Immutable
public final class ContainerCreateInfo {
  public static final int INVALID_REPLICA_INDEX = -1;
  private static final Codec<ContainerCreateInfo> CODEC = new DelegatedCodec<>(
      Proto3Codec.get(ContainerProtos.ContainerCreateInfo.getDefaultInstance()),
      ContainerCreateInfo::getFromProtobuf, ContainerCreateInfo::getProtobuf,
      ContainerCreateInfo.class);

  private final ContainerProtos.ContainerDataProto.State state;
  private final int replicaIndex;
  private final Supplier<ContainerProtos.ContainerCreateInfo> proto;

  public static Codec<ContainerCreateInfo> getCodec() {
    return CODEC;
  }

  public static Codec<ContainerCreateInfo> getNewCodec() {
    return CODEC;
  }

  private ContainerCreateInfo(ContainerProtos.ContainerDataProto.State state, int replicaIndex) {
    this.state = state;
    this.replicaIndex = replicaIndex;
    this.proto = MemoizedSupplier.valueOf(
        () -> ContainerProtos.ContainerCreateInfo.newBuilder().setState(state).setReplicaIndex(replicaIndex).build());
  }

  /**
   * Factory method for creation of ContainerCreateInfo.
   *
   * @param state        State
   * @param replicaIndex replica index
   * @return ContainerCreateInfo.
   */
  public static ContainerCreateInfo valueOf(final ContainerProtos.ContainerDataProto.State state, int replicaIndex) {
    return new ContainerCreateInfo(state, replicaIndex);
  }

  public ContainerProtos.ContainerCreateInfo getProtobuf() {
    return proto.get();
  }

  public static ContainerCreateInfo getFromProtobuf(ContainerProtos.ContainerCreateInfo proto) {
    return ContainerCreateInfo.valueOf(proto.getState(), proto.getReplicaIndex());
  }

  public ContainerProtos.ContainerDataProto.State getState() {
    return state;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }
}

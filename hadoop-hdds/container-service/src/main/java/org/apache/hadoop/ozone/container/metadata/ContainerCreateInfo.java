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
  private static final Codec<ContainerCreateInfo> CODEC = new DelegatedCodec<>(
      Proto3Codec.get(ContainerProtos.ContainerCreateInfo.getDefaultInstance()),
      ContainerCreateInfo::getFromProtobuf, ContainerCreateInfo::getProtobuf,
      ContainerCreateInfo.class);

  private final ContainerProtos.ContainerDataProto.State state;
  private final Supplier<ContainerProtos.ContainerCreateInfo> proto;

  public static Codec<ContainerCreateInfo> getCodec() {
    return CODEC;
  }

  public static Codec<ContainerCreateInfo> getNewCodec() {
    return CODEC;
  }

  private ContainerCreateInfo(ContainerProtos.ContainerDataProto.State state) {
    this.state = state;
    this.proto = MemoizedSupplier.valueOf(
        () -> ContainerProtos.ContainerCreateInfo.newBuilder().setState(state).build());
  }

  /**
   * Factory method for creation of ContainerCreateInfo.
   * @param state  State
   * @return ContainerCreateInfo.
   */
  public static ContainerCreateInfo valueOf(final ContainerProtos.ContainerDataProto.State state) {
    return new ContainerCreateInfo(state);
  }

  public ContainerProtos.ContainerCreateInfo getProtobuf() {
    return proto.get();
  }

  public static ContainerCreateInfo getFromProtobuf(ContainerProtos.ContainerCreateInfo proto) {
    return ContainerCreateInfo.valueOf(proto.getState());
  }

  public ContainerProtos.ContainerDataProto.State getState() {
    return state;
  }
}

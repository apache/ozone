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
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto3Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
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
  private static final Codec<ContainerCreateInfo> CODEC_OLD_VERSION = new ContainerCreateInfoCodec();

  private final ContainerProtos.ContainerDataProto.State state;
  private final Supplier<ContainerProtos.ContainerCreateInfo> proto;

  public static Codec<ContainerCreateInfo> getCodec() {
    if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.CONTAINERID_TABLE_SCHEMA_CHANGE)) {
      // If the container ID table schema is finalized, we can use the proto3 codec directly.
      return CODEC;
    }
    return CODEC_OLD_VERSION;
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

  /**
   * ContainerCreateInfoCodec handles compatibility for containerIds Table, where old format from String is changed
   * to proto3 format, ContainerCreateInfo. So this codec can read both formats based on the HDDSLayoutFeature.
   * For write case, it will create ContainerCreateInfo in proto3 format, but write is allowed only after the
   * finalization  of feature.
   */
  public static class ContainerCreateInfoCodec implements Codec<ContainerCreateInfo> {
    @Override
    public Class<ContainerCreateInfo> getTypeClass() {
      return ContainerCreateInfo.class;
    }

    @Override
    public boolean supportCodecBuffer() {
      return CODEC.supportCodecBuffer();
    }

    @Override
    public ContainerCreateInfo fromPersistedFormat(byte[] rawData) throws CodecException {
      if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.CONTAINERID_TABLE_SCHEMA_CHANGE)) {
        // If the container ID table schema is finalized, we can use the proto3 codec directly.
        return CODEC.fromPersistedFormat(rawData);
      }
      String val = StringCodec.get().fromPersistedFormat(rawData);
      return valueOf(ContainerProtos.ContainerDataProto.State.valueOf(val));
    }

    @Override
    public byte[] toPersistedFormat(ContainerCreateInfo object) throws CodecException {
      return CODEC.toPersistedFormat(object);
    }

    @Override
    public CodecBuffer toCodecBuffer(ContainerCreateInfo object, CodecBuffer.Allocator allocator)
            throws CodecException {
      return CODEC.toCodecBuffer(object, allocator);
    }

    @Override
    public CodecBuffer toDirectCodecBuffer(ContainerCreateInfo object) throws CodecException {
      return CODEC.toDirectCodecBuffer(object);
    }

    @Override
    public CodecBuffer toHeapCodecBuffer(ContainerCreateInfo object) throws CodecException {
      return CODEC.toHeapCodecBuffer(object);
    }

    @Override
    public ContainerCreateInfo fromCodecBuffer(CodecBuffer buffer) throws CodecException {
      return CODEC.fromCodecBuffer(buffer);
    }

    @Override
    public ContainerCreateInfo copyObject(ContainerCreateInfo object) {
      return CODEC.copyObject(object);
    }
  }
}

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

package org.apache.hadoop.hdds.scm.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.UuidCodec;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * ID for the pipeline, the ID is based on UUID.
 * <p>
 * This class is immutable.
 */
public final class PipelineID {
  private static final Codec<PipelineID> CODEC = new DelegatedCodec<>(
      UuidCodec.get(), PipelineID::valueOf, c -> c.id,
      PipelineID.class, DelegatedCodec.CopyType.SHALLOW);

  private final UUID id;
  private final Supplier<HddsProtos.PipelineID> protoSupplier;

  public static Codec<PipelineID> getCodec() {
    return CODEC;
  }

  private PipelineID(UUID id) {
    this.id = id;
    this.protoSupplier = MemoizedSupplier.valueOf(() -> buildProtobuf(id));
  }

  public static PipelineID randomId() {
    return new PipelineID(UUID.randomUUID());
  }

  public static PipelineID valueOf(UUID id) {
    return new PipelineID(id);
  }

  public static PipelineID valueOf(String id) {
    return valueOf(UUID.fromString(id));
  }

  public UUID getId() {
    return id;
  }

  @JsonIgnore
  public HddsProtos.PipelineID getProtobuf() {
    return protoSupplier.get();
  }

  static HddsProtos.PipelineID buildProtobuf(UUID id) {
    HddsProtos.UUID uuid128 = HddsProtos.UUID.newBuilder()
        .setMostSigBits(id.getMostSignificantBits())
        .setLeastSigBits(id.getLeastSignificantBits())
        .build();

    return HddsProtos.PipelineID.newBuilder().setId(id.toString())
        .setUuid128(uuid128).build();
  }

  public static PipelineID getFromProtobuf(HddsProtos.PipelineID protos) {
    if (protos.hasUuid128()) {
      HddsProtos.UUID uuid = protos.getUuid128();
      return new PipelineID(
          new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
    } else if (protos.hasId()) {
      return new PipelineID(UUID.fromString(protos.getId()));
    } else {
      throw new IllegalArgumentException(
          "Pipeline does not has uuid128 in proto");
    }
  }

  @Override
  public String toString() {
    return "Pipeline-" + id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PipelineID that = (PipelineID) o;

    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

}

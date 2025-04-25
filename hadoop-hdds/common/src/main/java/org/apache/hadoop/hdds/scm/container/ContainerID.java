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

package org.apache.hadoop.hdds.scm.container;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * Container ID is an integer that is a value between 1..MAX_CONTAINER ID.
 * <p>
 * We are creating a specific type for this to avoid mixing this with
 * normal integers in code.
 * <p>
 * This class is immutable.
 */
@Immutable
public final class ContainerID implements Comparable<ContainerID> {
  private static final Codec<ContainerID> CODEC = new DelegatedCodec<>(
      LongCodec.get(), ContainerID::valueOf, c -> c.id,
      ContainerID.class, DelegatedCodec.CopyType.SHALLOW);

  public static final ContainerID MIN = ContainerID.valueOf(0);

  private final long id;
  private final Supplier<HddsProtos.ContainerID> proto;
  private final Supplier<Integer> hash;

  public static Codec<ContainerID> getCodec() {
    return CODEC;
  }

  /**
   * Constructs ContainerID.
   *
   * @param id int
   */
  private ContainerID(long id) {
    Preconditions.checkState(id >= 0,
        "Container ID should be positive. %s.", id);
    this.id = id;
    this.proto = MemoizedSupplier.valueOf(() -> HddsProtos.ContainerID.newBuilder().setId(id).build());
    this.hash = MemoizedSupplier.valueOf(() -> 61 * 71 + Long.hashCode(id));
  }

  /**
   * Factory method for creation of ContainerID.
   * @param containerID  long
   * @return ContainerID.
   */
  public static ContainerID valueOf(final long containerID) {
    return new ContainerID(containerID);
  }

  /**
   * Returns int representation of ID.
   *
   * @return int
   */
  @Deprecated
  /*
   * Don't expose the int value.
   */
  public long getId() {
    return id;
  }

  public static byte[] getBytes(long id) {
    return LongCodec.get().toPersistedFormat(id);
  }

  public HddsProtos.ContainerID getProtobuf() {
    return proto.get();
  }

  public static ContainerID getFromProtobuf(HddsProtos.ContainerID proto) {
    return ContainerID.valueOf(proto.getId());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerID that = (ContainerID) o;
    return this.id == that.id;
  }

  @Override
  public int hashCode() {
    return hash.get();
  }

  @Override
  public int compareTo(@Nonnull final ContainerID that) {
    Objects.requireNonNull(that, "that == null");
    return Long.compare(this.id, that.id);
  }

  @Override
  public String toString() {
    return "#" + id;
  }
}

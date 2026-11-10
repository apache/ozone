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

package org.apache.hadoop.hdds.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

/**
 * {@link ReplicationConfig} for a {@link ReplicationType}.
 */
@Immutable
abstract class NCopyReplicationConfig implements ReplicatedReplicationConfig {
  private final ReplicationType type;
  /** The number of copies. */
  private final ReplicationFactor factor;
  private final String name;

  NCopyReplicationConfig(ReplicationType type, ReplicationFactor factor) {
    this.type = Objects.requireNonNull(type, "type == null");
    this.factor = Objects.requireNonNull(factor, "factor == null");
    this.name = type + "/" + factor;
  }

  @Override
  @JsonProperty("replicationType")
  public final ReplicationType getReplicationType() {
    return type;
  }

  @Override
  public ReplicationFactor getReplicationFactor() {
    return factor;
  }

  @Override
  public int getRequiredNodes() {
    return factor.getNumber();
  }

  @Override
  @JsonIgnore
  public String getReplication() {
    return factor.name();
  }

  @Override
  public final boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final ReplicatedReplicationConfig that = (ReplicatedReplicationConfig) obj;
    return this.getReplicationFactor() == that.getReplicationFactor();
  }

  @Override
  public final int hashCode() {
    return Objects.hash(factor);
  }

  @Override
  public final String toString() {
    return name;
  }

  @Override
  public final String configFormat() {
    return name;
  }

  @Override
  public int getMinimumNodes() {
    return 1;
  }

  static <C extends NCopyReplicationConfig> Map<ReplicationFactor, C> newMap(
      Function<ReplicationFactor, C> constructor) {
    final EnumMap<ReplicationFactor, C> map = new EnumMap<>(ReplicationFactor.class);
    for (ReplicationFactor f : ReplicationFactor.values()) {
      map.put(f, constructor.apply(f));
    }
    return Collections.unmodifiableMap(map);
  }
}

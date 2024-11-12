/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

import java.util.Objects;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;

/**
 * Replication configuration for SHORT_CIRCUIT replication.
 */
@Immutable
public final class ShortCircuitReplicationConfig implements
    ReplicatedReplicationConfig {

  private final ReplicationFactor replicationFactor;
  private static final String REPLICATION_TYPE = "SHORT_CIRCUIT";

  private static final ShortCircuitReplicationConfig SHORT_CIRCUIT_ONE_CONFIG =
      new ShortCircuitReplicationConfig(ONE);

  private static final ShortCircuitReplicationConfig SHORT_CIRCUIT_THREE_CONFIG =
      new ShortCircuitReplicationConfig(THREE);

  /**
   * Get an instance of Short-circuit Replication Config with the requested factor.
   * The same static instance will be returned for all requests for the same
   * factor.
   * @param factor Replication Factor requested
   * @return ShortCircuitReplicationConfig object of the requested factor
   */
  public static ShortCircuitReplicationConfig getInstance(
      ReplicationFactor factor) {
    if (factor == ONE) {
      return SHORT_CIRCUIT_ONE_CONFIG;
    } else if (factor == THREE) {
      return SHORT_CIRCUIT_THREE_CONFIG;
    }
    return new ShortCircuitReplicationConfig(factor);
  }

  /**
   * Use the static getInstance method instead of the private constructor.
   * @param replicationFactor
   */
  private ShortCircuitReplicationConfig(ReplicationFactor replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  @Override
  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  @Override
  public int getRequiredNodes() {
    return replicationFactor.getNumber();
  }

  @Override
  @JsonIgnore
  public String getReplication() {
    return String.valueOf(this.replicationFactor);
  }

  @Override
  public ReplicationType getReplicationType() {
    return ReplicationType.SHORT_CIRCUIT;
  }

  /**
   * This method is here only to allow the string value for replicationType to
   * be output in JSON.
   */
  @JsonProperty("replicationType")
  public String replicationType() {
    return REPLICATION_TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShortCircuitReplicationConfig that = (ShortCircuitReplicationConfig) o;
    return replicationFactor == that.replicationFactor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(replicationFactor);
  }

  @Override
  public String toString() {
    return REPLICATION_TYPE + "/" + replicationFactor;
  }

  @Override
  public String configFormat() {
    return toString();
  }
}

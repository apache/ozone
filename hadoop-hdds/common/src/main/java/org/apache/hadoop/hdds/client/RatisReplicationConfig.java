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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

/**
 * Replication configuration for Ratis replication.
 */
@Immutable
public final class RatisReplicationConfig
    implements ReplicatedReplicationConfig {

  private final ReplicationFactor replicationFactor;
  private static final ReplicationType REPLICATION_TYPE = ReplicationType.RATIS;

  private static final RatisReplicationConfig RATIS_ONE_CONFIG =
      new RatisReplicationConfig(ONE);

  private static final RatisReplicationConfig RATIS_THREE_CONFIG =
      new RatisReplicationConfig(THREE);

  /**
   * Get an instance of Ratis Replication Config with the requested factor.
   * The same static instance will be returned for all requests for the same
   * factor.
   * @param factor Replication Factor requested
   * @return RatisReplicationConfig object of the requested factor
   */
  public static RatisReplicationConfig getInstance(ReplicationFactor factor) {
    if (factor == ONE) {
      return RATIS_ONE_CONFIG;
    } else if (factor == THREE) {
      return RATIS_THREE_CONFIG;
    }
    return new RatisReplicationConfig(factor);
  }

  /**
   * Use the static getInstance method rather than the private constructor.
   * @param replicationFactor
   */
  private RatisReplicationConfig(ReplicationFactor replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public static boolean hasFactor(ReplicationConfig replicationConfig,
      ReplicationFactor factor) {
    if (replicationConfig instanceof RatisReplicationConfig) {
      return ((RatisReplicationConfig) replicationConfig).getReplicationFactor()
          .equals(factor);
    }
    return false;
  }

  @Override
  @JsonProperty("replicationType")
  public ReplicationType getReplicationType() {
    return REPLICATION_TYPE;
  }

  @Override
  public int getRequiredNodes() {
    return replicationFactor.getNumber();
  }

  @Override
  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  @Override
  @JsonIgnore
  public String getReplication() {
    return String.valueOf(replicationFactor);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RatisReplicationConfig that = (RatisReplicationConfig) o;
    return replicationFactor == that.replicationFactor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(replicationFactor);
  }

  @Override
  public String toString() {
    return REPLICATION_TYPE.name() + "/" + replicationFactor;
  }

  @Override
  public String configFormat() {
    return toString();
  }

  @Override
  public int getMinimumNodes() {
    return 1;
  }
}

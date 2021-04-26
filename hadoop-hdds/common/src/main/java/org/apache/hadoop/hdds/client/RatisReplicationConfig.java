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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

import java.util.Objects;

/**
 * Replication configuration for EC replication.
 */
public class RatisReplicationConfig
    implements ReplicationConfig {

  private final ReplicationFactor replicationFactor;

  public RatisReplicationConfig(ReplicationFactor replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public RatisReplicationConfig(String factorString) {
    ReplicationFactor factor = null;
    try {
      factor = ReplicationFactor.valueOf(Integer.parseInt(factorString));
    } catch (NumberFormatException ex) {
      try {
        factor = ReplicationFactor.valueOf(factorString);
      } catch (IllegalArgumentException x) {
        throw new IllegalArgumentException("Invalid RatisReplicationFactor '" +
                factorString + "'. Please use ONE or THREE!");
      }
    }
    this.replicationFactor = factor;
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
  public ReplicationType getReplicationType() {
    return ReplicationType.RATIS;
  }

  @Override
  public int getRequiredNodes() {
    return replicationFactor.getNumber();
  }

  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
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
  public String toString() {
    return "RATIS/" + replicationFactor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(replicationFactor);
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.Objects;

/**
 * Replication configuration for EC replication.
 */
public class DefaultReplicationConfig {

  private final ReplicationType type;
  private final ReplicationFactor factor;
  private final ECReplicationConfig ecReplicationConfig;
  private final ReplicationConfig replicationConfig;

  public DefaultReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
    type = ReplicationType.fromProto(replicationConfig.getReplicationType());
    if (replicationConfig instanceof ECReplicationConfig) {
      ecReplicationConfig = (ECReplicationConfig) replicationConfig;
      factor = null;
    } else {
      factor = ReplicationFactor.valueOf(replicationConfig.getRequiredNodes());
      ecReplicationConfig = null;
    }
  }

  public static DefaultReplicationConfig fromProto(
      HddsProtos.DefaultReplicationConfig proto) {
    if (proto == null) {
      throw new IllegalArgumentException(
          "Invalid argument: default replication config is null");
    }
    ReplicationConfig config = proto.hasEcReplicationConfig()
        ? new ECReplicationConfig(proto.getEcReplicationConfig())
        : ReplicationConfig.fromProtoTypeAndFactor(
            proto.getType(), proto.getFactor());
    return new DefaultReplicationConfig(config);
  }

  public ReplicationType getType() {
    return this.type;
  }

  public DefaultReplicationConfig copy() {
    return new DefaultReplicationConfig(replicationConfig);
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public HddsProtos.DefaultReplicationConfig toProto() {
    final HddsProtos.DefaultReplicationConfig.Builder builder =
        HddsProtos.DefaultReplicationConfig.newBuilder()
            .setType(ReplicationType.toProto(this.type));
    if (this.factor != null) {
      builder.setFactor(ReplicationFactor.toProto(this.factor));
    }
    if (this.ecReplicationConfig != null) {
      builder.setEcReplicationConfig(this.ecReplicationConfig.toProto());
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DefaultReplicationConfig that = (DefaultReplicationConfig) o;
    return Objects.equals(replicationConfig, that.replicationConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(replicationConfig);
  }

  @Override
  public String toString() {
    return replicationConfig.toString();
  }
}


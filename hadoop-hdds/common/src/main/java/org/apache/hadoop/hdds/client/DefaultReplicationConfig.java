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

  private ReplicationType type;
  private ReplicationFactor factor;
  private ECReplicationConfig ecReplicationConfig;

  public DefaultReplicationConfig(ReplicationType type,
      ReplicationFactor factor) {
    this.type = type;
    this.factor = factor;
    this.ecReplicationConfig = null;
  }

  public DefaultReplicationConfig(ReplicationConfig replicationConfig) {
    this.type =
        ReplicationType.fromProto(replicationConfig.getReplicationType());
    if (replicationConfig instanceof ECReplicationConfig) {
      this.ecReplicationConfig = (ECReplicationConfig) replicationConfig;
    } else {
      this.factor =
          ReplicationFactor.valueOf(replicationConfig.getRequiredNodes());
    }
  }

  public DefaultReplicationConfig(ReplicationType type,
      ECReplicationConfig ecReplicationConfig) {
    this.type = type;
    this.factor = null;
    this.ecReplicationConfig = ecReplicationConfig;
  }

  public DefaultReplicationConfig(ReplicationType type,
      ReplicationFactor factor, ECReplicationConfig ecReplicationConfig) {
    this.type = type;
    this.factor = factor;
    this.ecReplicationConfig = ecReplicationConfig;
  }

  public DefaultReplicationConfig(
      org.apache.hadoop.hdds.protocol.proto.HddsProtos.DefaultReplicationConfig
          defaultReplicationConfig) {
    this.type = ReplicationType.fromProto(defaultReplicationConfig.getType());
    if (defaultReplicationConfig.hasEcReplicationConfig()) {
      this.ecReplicationConfig = new ECReplicationConfig(
          defaultReplicationConfig.getEcReplicationConfig());
    } else {
      this.factor =
          ReplicationFactor.fromProto(defaultReplicationConfig.getFactor());
    }
  }

  public ReplicationType getType() {
    return this.type;
  }

  public ReplicationFactor getFactor() {
    return this.factor;
  }

  public DefaultReplicationConfig copy() {
    return new DefaultReplicationConfig(this.type, this.factor,
        this.ecReplicationConfig);
  }

  public ECReplicationConfig getEcReplicationConfig() {
    return this.ecReplicationConfig;
  }

  public int getRequiredNodes() {
    if (this.type == ReplicationType.EC) {
      return ecReplicationConfig.getRequiredNodes();
    }
    return this.factor.getValue();
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
    return Objects.equals(type, that.type) && Objects
        .equals(factor, that.factor) && Objects
        .equals(ecReplicationConfig, that.ecReplicationConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, factor, ecReplicationConfig);
  }
}


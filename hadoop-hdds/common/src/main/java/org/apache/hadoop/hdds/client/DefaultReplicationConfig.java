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

import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Replication configuration for EC replication.
 */
@Immutable
public class DefaultReplicationConfig {

  private final ECReplicationConfig ecReplicationConfig;
  private final ReplicationConfig replicationConfig;

  public DefaultReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
    if (replicationConfig instanceof ECReplicationConfig) {
      ecReplicationConfig = (ECReplicationConfig) replicationConfig;
    } else {
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
    return ReplicationType.fromProto(replicationConfig.getReplicationType());
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public HddsProtos.DefaultReplicationConfig toProto() {
    final HddsProtos.DefaultReplicationConfig.Builder builder =
        HddsProtos.DefaultReplicationConfig.newBuilder()
            .setType(replicationConfig.getReplicationType());
    if (this.ecReplicationConfig != null) {
      builder.setEcReplicationConfig(this.ecReplicationConfig.toProto());
    } else {
      ReplicationFactor factor =
          ReplicationFactor.valueOf(replicationConfig.getRequiredNodes());
      builder.setFactor(factor.toProto());
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


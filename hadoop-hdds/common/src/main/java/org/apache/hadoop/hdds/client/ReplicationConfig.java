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

/**
 * Replication configuration for any ReplicationType with all the required
 * parameters..
 */
public interface ReplicationConfig {

  /**
   * Helper method to create proper replication method from old-style
   * factor+type definition.
   * <p>
   * Note: it's never used for EC replication where config is created.
   */
  static ReplicationConfig fromTypeAndFactor(
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor
  ) {
    switch (type) {
    case RATIS:
      return new RatisReplicationConfig(factor);
    case STAND_ALONE:
      return new StandaloneReplicationConfig(factor);
    default:
      throw new UnsupportedOperationException(
          "Not supported replication: " + type);
    }
  }

  /**
   * Helper method to serialize from proto.
   * <p>
   * This uses either the old type/factor or the new ecConfig depends on the
   * type.
   * <p>
   * Note: It will support all the available replication types (including EC).
   * <p>
   * Separated to remain be synced with the EC feature branch, as later it
   * will have different signature.
   */
  static ReplicationConfig fromProto(
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor) {
    switch (type) {
    case RATIS:
    case STAND_ALONE:
      return fromTypeAndFactor(type, factor);
    default:
      throw new UnsupportedOperationException(
          "Not supported replication: " + type);
    }
  }

  static HddsProtos.ReplicationFactor getLegacyFactor(
      ReplicationConfig replicationConfig) {
    if (replicationConfig instanceof RatisReplicationConfig) {
      return ((RatisReplicationConfig) replicationConfig)
          .getReplicationFactor();
    } else if (replicationConfig instanceof StandaloneReplicationConfig) {
      return ((StandaloneReplicationConfig) replicationConfig)
          .getReplicationFactor();
    }
    throw new UnsupportedOperationException(
        "factor is not valid property of replication " + replicationConfig
            .getReplicationType());
  }

  /**
   * Replication type supported by the replication config.
   */
  HddsProtos.ReplicationType getReplicationType();

  /**
   * Number of required nodes for this replication.
   */
  int getRequiredNodes();

}

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
 * Replication configuration for STANDALONE replication.
 */
public class StandaloneReplicationConfig implements ReplicationConfig {

  private final ReplicationFactor replicationFactor;

  public StandaloneReplicationConfig(ReplicationFactor replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public StandaloneReplicationConfig(String factorString) {
    ReplicationFactor factor = null;
    try {
      factor = ReplicationFactor.valueOf(Integer.parseInt(factorString));
    } catch (NumberFormatException ex) {
      factor = ReplicationFactor.valueOf(factorString);
    }
    this.replicationFactor = factor;
  }
  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  @Override
  public int getRequiredNodes() {
    return replicationFactor.getNumber();
  }

  @Override
  public ReplicationType getReplicationType() {
    return ReplicationType.STAND_ALONE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StandaloneReplicationConfig that = (StandaloneReplicationConfig) o;
    return replicationFactor == that.replicationFactor;
  }

  @Override
  public String toString() {
    return "STANDALONE/" + replicationFactor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(replicationFactor);
  }
}

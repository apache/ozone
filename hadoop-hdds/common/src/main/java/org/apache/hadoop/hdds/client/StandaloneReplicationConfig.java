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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

import java.util.Objects;

/**
 * Replication configuration for STANDALONE replication.
 */
public class StandaloneReplicationConfig implements
    ReplicatedReplicationConfig {

  private final ReplicationFactor replicationFactor;
  private static final String REPLICATION_TYPE = "STANDALONE";

  public StandaloneReplicationConfig(ReplicationFactor replicationFactor) {
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
  public ReplicationType getReplicationType() {
    return ReplicationType.STAND_ALONE;
  }

  /**
   * This method is here only to allow the string value for replicationType to
   * be output in JSON. The enum defining the replication type STAND_ALONE has a
   * string value of "STAND_ALONE", however various tests expect to see
   * "STANDALONE" as the string.
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
    StandaloneReplicationConfig that = (StandaloneReplicationConfig) o;
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
}

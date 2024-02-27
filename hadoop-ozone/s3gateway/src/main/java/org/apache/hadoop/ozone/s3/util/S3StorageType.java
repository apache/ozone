/*
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

package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Maps S3 storage class values to Ozone replication values.
 */

public enum S3StorageType {

  REDUCED_REDUNDANCY(ReplicationType.RATIS, ReplicationFactor.ONE),
  STANDARD(ReplicationType.RATIS, ReplicationFactor.THREE);

  private final ReplicationType type;
  private final ReplicationFactor factor;

  S3StorageType(
      ReplicationType type,
      ReplicationFactor factor) {
    this.type = type;
    this.factor = factor;
  }

  public ReplicationFactor getFactor() {
    return factor;
  }

  public ReplicationType getType() {
    return type;
  }

  /**
   * Get default S3StorageType for a new key to be uploaded.
   * This should align to the ozone cluster configuration.
   * @param config OzoneConfiguration
   * @return S3StorageType which wraps ozone replication type and factor
   */
  public static S3StorageType getDefault(ConfigurationSource config) {
    String replicationString = config.get(OzoneConfigKeys.OZONE_REPLICATION);
    ReplicationFactor configFactor;
    if (replicationString == null) {
      // if no config is set then let server take decision
      return null;
    }
    try {
      configFactor = ReplicationFactor.valueOf(
          Integer.parseInt(replicationString));
    } catch (NumberFormatException ex) {
      // conservatively defaults to STANDARD on wrong config value
      return STANDARD;
    }
    return configFactor == ReplicationFactor.ONE
        ? REDUCED_REDUNDANCY : STANDARD;
  }

  public static S3StorageType fromReplicationConfig(ReplicationConfig config) {
    if (config instanceof ECReplicationConfig) {
      return S3StorageType.STANDARD;
    }
    if (config.getReplicationType() == HddsProtos.ReplicationType.STAND_ALONE ||
        config.getRequiredNodes() == 1) {
      return S3StorageType.REDUCED_REDUNDANCY;
    } else {
      return S3StorageType.STANDARD;
    }
  }
}

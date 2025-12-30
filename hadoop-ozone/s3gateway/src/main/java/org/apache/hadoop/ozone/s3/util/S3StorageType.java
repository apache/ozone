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

package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Maps S3 storage class values to Ozone replication values.
 */

public enum S3StorageType {

  REDUCED_REDUNDANCY(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE)),
  STANDARD(
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE)),
  STANDARD_IA(new ECReplicationConfig(3, 2));

  private final ReplicationConfig replicationConfig;

  S3StorageType(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public static S3StorageType fromReplicationConfig(ReplicationConfig config) {
    if (config.getReplicationType() == HddsProtos.ReplicationType.EC) {
      return STANDARD_IA;
    }
    if (config.getReplicationType() == HddsProtos.ReplicationType.STAND_ALONE ||
        config.getRequiredNodes() == 1) {
      return S3StorageType.REDUCED_REDUNDANCY;
    } else {
      return S3StorageType.STANDARD;
    }
  }
}

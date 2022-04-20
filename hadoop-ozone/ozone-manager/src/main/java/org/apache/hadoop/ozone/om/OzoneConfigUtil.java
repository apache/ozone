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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Utility class for ozone configurations.
 */
public final class OzoneConfigUtil {
  private OzoneConfigUtil() {
  }

  public static ReplicationConfig resolveReplicationConfigPreference(
      HddsProtos.ReplicationType clientType,
      HddsProtos.ReplicationFactor clientFactor,
      HddsProtos.ECReplicationConfig clientECReplicationConfig,
      DefaultReplicationConfig bucketDefaultReplicationConfig,
      ReplicationConfig omDefaultReplicationConfig) {
    ReplicationConfig replicationConfig = null;
    if (clientType != HddsProtos.ReplicationType.NONE) {
      // Client passed the replication config, so let's use it.
      replicationConfig = ReplicationConfig
          .fromProto(clientType, clientFactor, clientECReplicationConfig);
    } else {
      // type is NONE, so, let's look for the bucket defaults.
      if (bucketDefaultReplicationConfig != null) {
        boolean hasECReplicationConfig = bucketDefaultReplicationConfig
            .getType() == ReplicationType.EC && bucketDefaultReplicationConfig
            .getEcReplicationConfig() != null;
        // Since Bucket defaults are available, let's inherit
        replicationConfig = ReplicationConfig.fromProto(
            ReplicationType.toProto(bucketDefaultReplicationConfig.getType()),
            ReplicationFactor
                .toProto(bucketDefaultReplicationConfig.getFactor()),
            hasECReplicationConfig ?
                bucketDefaultReplicationConfig.getEcReplicationConfig()
                    .toProto() :
                null);
      } else {
        // if bucket defaults also not available, then use server defaults.
        replicationConfig = omDefaultReplicationConfig;
      }
    }
    return replicationConfig;
  }
}

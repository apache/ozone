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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to calculate quota related usage.
 */
public final class QuotaUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(QuotaUtil.class);

  private QuotaUtil() {
  }

  /**
   * From the used space and replicationConfig, calculate the expected
   * replicated size of the data.
   * @param dataSize The number of bytes of data stored
   * @param repConfig The replicationConfig used to store the data
   * @return Number of bytes required to store the dataSize with replication
   */
  public static long getReplicatedSize(
      long dataSize, ReplicationConfig repConfig) {
    if (repConfig.getReplicationType() == RATIS) {
      return dataSize * ((RatisReplicationConfig) repConfig)
          .getReplicationFactor().getNumber();
    } else if (repConfig.getReplicationType() == EC) {
      ECReplicationConfig rc = (ECReplicationConfig) repConfig;
      int dataStripeSize = rc.getData() * rc.getEcChunkSize();
      long fullStripes = dataSize / dataStripeSize;
      long partialFirstChunk =
          Math.min(rc.getEcChunkSize(), dataSize % dataStripeSize);
      long replicationOverhead =
          fullStripes * rc.getParity() * rc.getEcChunkSize()
              + partialFirstChunk * rc.getParity();
      return dataSize + replicationOverhead;
    } else {
      LOG.warn("Unknown replication type '{}'. Returning original data size.",
          repConfig.getReplicationType());
      return dataSize;
    }
  }

  /**
   * Get an estimated data size (before replication) from the replicated size.
   * An (inaccurate) reverse of getReplicatedSize().
   * @param replicatedSize size after replication.
   * @param repConfig The replicationConfig used to store the data.
   * @return Data size before replication.
   */
  public static long getDataSize(long replicatedSize,
                                 ReplicationConfig repConfig) {
    if (repConfig.getReplicationType() == RATIS) {
      final int ratisReplicationFactor = ((RatisReplicationConfig) repConfig)
          .getReplicationFactor().getNumber();
      // May not be divisible. But it's fine to ignore remainder in our use case
      return replicatedSize / ratisReplicationFactor;
    } else if (repConfig.getReplicationType() == EC) {
      ECReplicationConfig rc = (ECReplicationConfig) repConfig;
      // In the case of EC, we won't know if keys have partial chunks or not,
      // so we assume no partial chunks as an estimate.
      return replicatedSize * rc.getData() / rc.getRequiredNodes();
    } else {
      LOG.warn("Unknown replication type '{}'. Returning replicatedSize.",
          repConfig.getReplicationType());
      return replicatedSize;
    }
  }

}

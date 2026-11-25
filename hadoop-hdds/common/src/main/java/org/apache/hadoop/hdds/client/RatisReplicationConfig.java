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

import java.util.Map;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

/**
 * {@link ReplicationConfig} for {@link ReplicationType#RATIS}.
 */
@Immutable
public final class RatisReplicationConfig extends NCopyReplicationConfig {
  private static final Map<ReplicationFactor, RatisReplicationConfig> CONFIGS = newMap(RatisReplicationConfig::new);

  /** @return the conf with the given factor. */
  public static RatisReplicationConfig getInstance(ReplicationFactor factor) {
    return CONFIGS.get(factor);
  }

  private RatisReplicationConfig(ReplicationFactor replicationFactor) {
    super(ReplicationType.RATIS, replicationFactor);
  }

  public static boolean hasFactor(ReplicationConfig replicationConfig,
      ReplicationFactor factor) {
    if (replicationConfig instanceof RatisReplicationConfig) {
      return ((RatisReplicationConfig) replicationConfig).getReplicationFactor()
          .equals(factor);
    }
    return false;
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;

/**
 * StorageClassConverter utility class.
 */
public final class StorageClassConverter {
  private StorageClassConverter() {
  }

  public static StorageClass convert(
      ConfigurationSource conf,
      ReplicationFactor factor,
      ReplicationType type
  ) {
    if (conf != null) {
      boolean useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
          DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
      if (factor == null) {
        factor = useRatis ? ReplicationFactor.THREE : ReplicationFactor.ONE;
      }
      if (type == null) {
        type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
      }
    }

    if (factor == ReplicationFactor.THREE && type == ReplicationType.RATIS) {
      return StaticStorageClassRegistry.STANDARD;
    }

    if (factor == ReplicationFactor.ONE && type == ReplicationType.RATIS) {
      return StaticStorageClassRegistry.REDUCED_REDUNDANCY;
    }

    if (factor == ReplicationFactor.ONE
        && type == ReplicationType.STAND_ALONE) {
      return StaticStorageClassRegistry.LEGACY;
    }

    return StaticStorageClassRegistry.STANDARD;
  }

  public static StorageClass convert(OzoneConfiguration conf,
      org.apache.hadoop.hdds.client.ReplicationFactor factor,
      org.apache.hadoop.hdds.client.ReplicationType type) {
    ReplicationFactor replicationFactor =
        ReplicationFactor.valueOf(factor.name());
    ReplicationType replicationType = ReplicationType.valueOf(type.name());
    return convert(conf, replicationFactor, replicationType);
  }

  public static StorageClass convert(OzoneConfiguration conf,
      org.apache.hadoop.hdds.client.ReplicationFactor factor,
      ReplicationType type) {
    ReplicationFactor replicationFactor =
        ReplicationFactor.valueOf(factor.name());
    return convert(conf, replicationFactor, type);
  }

  public static StorageClass convert(Pipeline pipeline) {
    return convert(null, pipeline.getFactor(), pipeline.getType());
  }
}
